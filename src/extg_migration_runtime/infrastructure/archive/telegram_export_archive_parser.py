from __future__ import annotations

import asyncio
import hashlib
import io
import json
import mimetypes
import subprocess
import tempfile
import zipfile
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from urllib.parse import urlencode

from extg_migration_runtime.application.telegram_export_archive import (
    ParsedTelegramExportSnapshot,
)
from extg_shared.contracts.errors import ConfigurationError, FatalItemError
from extg_shared.contracts.models import (
    CanonicalAttachment,
    ContentType,
    DownloadedAttachment,
    TelegramAuthor,
    TelegramSourceMessage,
)
from extg_shared.contracts.ports import TelegramExportArchiveStageStore, TelegramExportSnapshotRepository


class TelegramExportArchiveParser:
    def __init__(self, *, unar_binary: str = "unar") -> None:
        self._unar_binary = unar_binary

    def supports_embedded_attachments(self, filename: str | None) -> bool:
        return self._archive_supports_attachment_extraction(filename)

    async def parse_upload(
        self,
        *,
        filename: str | None,
        content: bytes,
    ) -> ParsedTelegramExportSnapshot:
        result_json = await asyncio.to_thread(
            self._extract_result_json_bytes,
            filename,
            content,
        )
        payload = self._load_payload(result_json)
        payload_sha256 = hashlib.sha256(result_json).hexdigest()
        export_chat_id = self._string_or_none(payload.get("id"))
        source_chat_title = self._normalize_title(payload.get("name"))
        source_chat_type = self._map_chat_type(payload.get("type"))
        messages = payload.get("messages")
        if not isinstance(messages, list):
            raise ConfigurationError(
                "Telegram export archive does not contain a valid messages list in result.json",
            )
        return ParsedTelegramExportSnapshot(
            source_chat_id=self._build_source_chat_id(
                export_chat_id=export_chat_id,
                payload_sha256=payload_sha256,
            ),
            source_chat_title=source_chat_title,
            source_chat_type=source_chat_type,
            original_filename=filename,
            export_chat_id=export_chat_id,
            payload_sha256=payload_sha256,
            message_count=len(messages),
            media_count=sum(1 for item in messages if self._message_has_media(item)),
            approximate_bytes=len(result_json),
        )

    async def load_messages(
        self,
        source_chat_id: str,
        snapshot_repository: TelegramExportSnapshotRepository,
        stage_store: TelegramExportArchiveStageStore,
    ) -> tuple[TelegramSourceMessage, ...]:
        snapshot = await snapshot_repository.get(source_chat_id)
        if snapshot is None:
            raise ConfigurationError(
                f"staged Telegram export archive was not found for source_chat_id={source_chat_id}",
            )
        archive_content = await stage_store.read(snapshot.archive_locator)
        return await asyncio.to_thread(self._load_messages_sync, snapshot, archive_content)

    async def download_attachment(
        self,
        *,
        original_filename: str | None,
        archive_content: bytes,
        attachment: CanonicalAttachment,
        member_path: str,
    ) -> DownloadedAttachment:
        content = await asyncio.to_thread(
            self._extract_attachment_bytes,
            original_filename,
            archive_content,
            member_path,
        )
        return DownloadedAttachment(
            attachment=CanonicalAttachment(
                source_file_id=attachment.source_file_id,
                filename=attachment.filename,
                mime_type=attachment.mime_type,
                size_bytes=len(content),
                media_kind=attachment.media_kind,
                duration_seconds=attachment.duration_seconds,
                download_url=attachment.download_url,
                temp_local_path=attachment.temp_local_path,
                sha256=attachment.sha256,
            ),
            content=content,
            size_bytes=len(content),
        )

    def _load_messages_sync(
        self,
        snapshot,
        archive_content: bytes,
    ) -> tuple[TelegramSourceMessage, ...]:
        payload = self._load_payload(
            self._extract_result_json_bytes(snapshot.original_filename, archive_content),
        )
        raw_messages = payload.get("messages")
        if not isinstance(raw_messages, list):
            raise ConfigurationError(
                f"staged Telegram export snapshot is invalid for source_chat_id={snapshot.source_chat_id}",
            )
        messages: list[TelegramSourceMessage] = []
        for item in raw_messages:
            if not isinstance(item, dict):
                continue
            source_message = self._message_from_export_item(
                source_chat_id=snapshot.source_chat_id,
                original_filename=snapshot.original_filename,
                item=item,
            )
            if source_message is None:
                continue
            messages.append(source_message)
        messages.sort(
            key=lambda item: (
                item.sent_at_utc,
                self._message_sort_key(item.message_id),
            ),
        )
        return tuple(messages)

    def _extract_result_json_bytes(
        self,
        filename: str | None,
        content: bytes,
    ) -> bytes:
        suffix = Path(filename or "").suffix.lower()
        if suffix == ".json" or self._looks_like_json(content):
            return content
        if suffix == ".zip":
            return self._extract_result_json_from_zip(content)
        if suffix == ".rar":
            return self._extract_result_json_from_rar(content)
        raise ConfigurationError(
            "unsupported Telegram export format; upload .json, .zip or .rar with result.json inside",
        )

    def _extract_result_json_from_zip(self, content: bytes) -> bytes:
        try:
            with zipfile.ZipFile(io.BytesIO(content)) as archive:
                result_name = self._find_result_json_member(archive.namelist())
                if result_name is None:
                    raise ConfigurationError(
                        "archive does not contain Telegram result.json",
                    )
                return archive.read(result_name)
        except zipfile.BadZipFile as error:
            raise ConfigurationError("uploaded .zip archive is corrupted") from error

    def _extract_result_json_from_rar(self, content: bytes) -> bytes:
        if not self._unar_binary.strip():
            raise ConfigurationError("RAR support is not configured on this runtime")
        with tempfile.TemporaryDirectory(prefix="extg-archive-") as workdir:
            workdir_path = Path(workdir)
            archive_path = workdir_path / "upload.rar"
            archive_path.write_bytes(content)
            try:
                completed = subprocess.run(
                    [
                        self._unar_binary,
                        "-quiet",
                        "-output-directory",
                        str(workdir_path / "out"),
                        str(archive_path),
                    ],
                    capture_output=True,
                    text=True,
                    check=False,
                )
            except FileNotFoundError as error:
                raise ConfigurationError(
                    "RAR import requires `unar` to be installed in the runtime image",
                ) from error
            if completed.returncode != 0:
                stderr = (completed.stderr or completed.stdout or "").strip()
                raise ConfigurationError(
                    f"failed to unpack .rar Telegram export: {stderr or 'unsupported archive'}",
                )
            result_candidates = sorted((workdir_path / "out").rglob("result.json"))
            if not result_candidates:
                raise ConfigurationError("archive does not contain Telegram result.json")
            return result_candidates[0].read_bytes()

    def _load_payload(self, payload: bytes) -> dict[str, Any]:
        try:
            loaded = json.loads(payload.decode("utf-8-sig"))
        except (UnicodeDecodeError, json.JSONDecodeError) as error:
            raise ConfigurationError(
                "Telegram export result.json is not valid UTF-8 JSON",
            ) from error
        if not isinstance(loaded, dict):
            raise ConfigurationError("Telegram export result.json has unsupported structure")
        return loaded

    def _message_from_export_item(
        self,
        *,
        source_chat_id: str,
        original_filename: str | None,
        item: dict[str, Any],
    ) -> TelegramSourceMessage | None:
        message_id = self._string_or_none(item.get("id"))
        sent_at = self._parse_datetime(item)
        if not message_id or sent_at is None:
            return None
        message_type = self._string_or_none(item.get("type")) or "message"
        is_service = message_type == "service"
        content_type = self._content_type_from_export_item(
            item,
            is_service=is_service,
            original_filename=original_filename,
        )
        attachments = self._attachments_from_export_item(
            source_chat_id=source_chat_id,
            original_filename=original_filename,
            message_id=message_id,
            item=item,
            content_type=content_type,
        )
        body = self._coerce_text(item.get("text"))
        has_media = self._message_has_media(item)
        media_type = self._string_or_none(item.get("media_type")) or "attachment"
        if not body and has_media and not is_service and not attachments:
            media_label = (
                content_type.value
                if content_type is ContentType.VIDEO_NOTE
                else media_type
            )
            body = f"[{media_label} omitted from Telegram export]"
        author_display_name = (
            self._string_or_none(item.get("from"))
            or self._string_or_none(item.get("actor"))
            or "Telegram Export"
        )
        return TelegramSourceMessage(
            chat_id=source_chat_id,
            message_id=message_id,
            sent_at_utc=sent_at,
            author=TelegramAuthor(
                external_id=self._string_or_none(item.get("from_id")),
                display_name=author_display_name,
                username=None,
            ),
            body=body,
            reply_to_message_id=self._string_or_none(item.get("reply_to_message_id")),
            edited_at_utc=self._parse_edited_datetime(item),
            content_type=content_type,
            attachments=attachments,
            raw_payload={
                "id": message_id,
                "type": message_type,
                "date": item.get("date"),
                "from": item.get("from"),
                "from_id": item.get("from_id"),
                "media_type": item.get("media_type"),
                "file": item.get("file"),
            },
        )

    def _content_type_from_export_item(
        self,
        item: dict[str, Any],
        *,
        is_service: bool,
        original_filename: str | None,
    ) -> ContentType:
        if is_service:
            return ContentType.SERVICE
        if not self._archive_supports_attachment_extraction(original_filename):
            return ContentType.TEXT
        member_path = self._archive_attachment_member_path(item)
        if member_path is None:
            return ContentType.TEXT
        media_type = (self._string_or_none(item.get("media_type")) or "").strip().lower()
        mime_type = (self._string_or_none(item.get("mime_type")) or "").strip().lower()
        if media_type in {"video message", "video_note", "video note", "round video"}:
            return ContentType.VIDEO_NOTE
        if media_type == "photo" or self._string_or_none(item.get("photo")) is not None:
            return ContentType.PHOTO
        if media_type in {"video file", "video", "animation", "gif"} or mime_type.startswith("video/"):
            return ContentType.VIDEO
        if media_type in {"voice message", "voice"}:
            return ContentType.VOICE
        if media_type in {"audio file", "audio", "music file"}:
            return ContentType.AUDIO
        if media_type == "sticker" or mime_type in {"image/webp", "application/x-tgsticker"}:
            return ContentType.STICKER
        if self._string_or_none(item.get("file")) is not None:
            return ContentType.DOCUMENT
        return ContentType.TEXT

    def _attachments_from_export_item(
        self,
        *,
        source_chat_id: str,
        original_filename: str | None,
        message_id: str,
        item: dict[str, Any],
        content_type: ContentType,
    ) -> list[CanonicalAttachment]:
        if content_type in {ContentType.TEXT, ContentType.SERVICE}:
            return []
        if not self._archive_supports_attachment_extraction(original_filename):
            return []
        member_path = self._archive_attachment_member_path(item)
        if member_path is None:
            return []
        filename = Path(member_path).name or None
        mime_type = self._string_or_none(item.get("mime_type")) or mimetypes.guess_type(
            filename or "",
        )[0]
        return [
            CanonicalAttachment(
                source_file_id=f"{source_chat_id}:{message_id}:{member_path}",
                filename=filename,
                mime_type=mime_type,
                size_bytes=self._optional_int(item.get("file_size")),
                media_kind=content_type.value,
                duration_seconds=self._optional_int(
                    item.get("duration_seconds") or item.get("duration"),
                ),
                download_url=self._archive_download_url(
                    source_chat_id=source_chat_id,
                    member_path=member_path,
                ),
            ),
        ]

    def _parse_datetime(self, item: dict[str, Any]) -> datetime | None:
        raw_unixtime = self._string_or_none(item.get("date_unixtime"))
        if raw_unixtime:
            try:
                return datetime.fromtimestamp(int(raw_unixtime), tz=UTC)
            except ValueError:
                return None
        raw_iso = self._string_or_none(item.get("date"))
        if not raw_iso:
            return None
        try:
            parsed = datetime.fromisoformat(raw_iso.replace("Z", "+00:00"))
        except ValueError:
            return None
        if parsed.tzinfo is None:
            return parsed.replace(tzinfo=UTC)
        return parsed.astimezone(UTC)

    def _parse_edited_datetime(self, item: dict[str, Any]) -> datetime | None:
        raw_unixtime = self._string_or_none(item.get("edited_unixtime"))
        if raw_unixtime:
            try:
                return datetime.fromtimestamp(int(raw_unixtime), tz=UTC)
            except ValueError:
                return None
        raw_iso = self._string_or_none(item.get("edited"))
        if not raw_iso:
            return None
        try:
            parsed = datetime.fromisoformat(raw_iso.replace("Z", "+00:00"))
        except ValueError:
            return None
        if parsed.tzinfo is None:
            return parsed.replace(tzinfo=UTC)
        return parsed.astimezone(UTC)

    def _find_result_json_member(self, names: list[str]) -> str | None:
        for name in names:
            normalized = name.strip("/").replace("\\", "/")
            if normalized.endswith("/result.json") or normalized == "result.json":
                return name
        return None

    def _find_archive_member(
        self,
        names: list[str],
        *,
        member_path: str,
    ) -> str | None:
        normalized_target = self._normalize_archive_member_path(member_path)
        for name in names:
            normalized = name.strip("/").replace("\\", "/")
            if normalized == normalized_target or normalized.endswith(f"/{normalized_target}"):
                return name
        return None

    def _normalize_title(self, raw_title: Any) -> str:
        normalized = self._string_or_none(raw_title)
        return normalized or "Telegram Export"

    def _map_chat_type(self, raw_type: Any) -> str:
        normalized = (self._string_or_none(raw_type) or "").strip().lower()
        if normalized in {"personal_chat", "saved_messages"}:
            return "private"
        if normalized in {"private_group", "group"}:
            return "group"
        if normalized in {"private_supergroup", "public_supergroup", "supergroup"}:
            return "supergroup"
        if normalized in {"public_channel", "private_channel", "channel"}:
            return "channel"
        return "group"

    def _build_source_chat_id(
        self,
        *,
        export_chat_id: str | None,
        payload_sha256: str,
    ) -> str:
        if export_chat_id:
            return f"archive:{export_chat_id}"
        return f"archive:sha256:{payload_sha256[:16]}"

    def _looks_like_json(self, content: bytes) -> bool:
        stripped = content.lstrip()
        return stripped.startswith(b"{")

    def _extract_attachment_bytes(
        self,
        filename: str | None,
        content: bytes,
        member_path: str,
    ) -> bytes:
        suffix = Path(filename or "").suffix.lower()
        if suffix == ".zip":
            return self._extract_archive_member_from_zip(
                content,
                member_path=member_path,
            )
        if suffix == ".rar":
            return self._extract_archive_member_from_rar(
                content,
                member_path=member_path,
            )
        if suffix == ".json" or self._looks_like_json(content):
            raise FatalItemError(
                "plain Telegram export .json upload does not contain bundled attachment files",
            )
        raise FatalItemError(
            "unsupported Telegram export archive source for attachment download",
        )

    def _extract_archive_member_from_zip(
        self,
        content: bytes,
        *,
        member_path: str,
    ) -> bytes:
        try:
            with zipfile.ZipFile(io.BytesIO(content)) as archive:
                member_name = self._find_archive_member(
                    archive.namelist(),
                    member_path=member_path,
                )
                if member_name is None:
                    raise FatalItemError(
                        f"Telegram export archive does not contain attachment {member_path!r}",
                    )
                return archive.read(member_name)
        except zipfile.BadZipFile as error:
            raise FatalItemError("uploaded .zip archive is corrupted") from error

    def _extract_archive_member_from_rar(
        self,
        content: bytes,
        *,
        member_path: str,
    ) -> bytes:
        if not self._unar_binary.strip():
            raise FatalItemError("RAR support is not configured on this runtime")
        normalized_member_path = self._normalize_archive_member_path(member_path)
        with tempfile.TemporaryDirectory(prefix="extg-archive-") as workdir:
            workdir_path = Path(workdir)
            archive_path = workdir_path / "upload.rar"
            archive_path.write_bytes(content)
            try:
                completed = subprocess.run(
                    [
                        self._unar_binary,
                        "-quiet",
                        "-output-directory",
                        str(workdir_path / "out"),
                        str(archive_path),
                    ],
                    capture_output=True,
                    text=True,
                    check=False,
                )
            except FileNotFoundError as error:
                raise FatalItemError(
                    "RAR import requires `unar` to be installed in the runtime image",
                ) from error
            if completed.returncode != 0:
                stderr = (completed.stderr or completed.stdout or "").strip()
                raise FatalItemError(
                    f"failed to unpack .rar Telegram export: {stderr or 'unsupported archive'}",
                )
            extracted_root = workdir_path / "out"
            extracted_members = [
                path
                for path in extracted_root.rglob("*")
                if path.is_file()
            ]
            relative_names = [
                str(path.relative_to(extracted_root)).replace("\\", "/")
                for path in extracted_members
            ]
            matched_name = self._find_archive_member(
                relative_names,
                member_path=normalized_member_path,
            )
            if matched_name is None:
                raise FatalItemError(
                    f"Telegram export archive does not contain attachment {member_path!r}",
                )
            for path in extracted_members:
                relative_name = str(path.relative_to(extracted_root)).replace("\\", "/")
                if relative_name == matched_name:
                    return path.read_bytes()
        raise FatalItemError(
            f"Telegram export archive does not contain attachment {member_path!r}",
        )

    def _archive_attachment_member_path(self, item: dict[str, Any]) -> str | None:
        candidates = (
            item.get("file"),
            item.get("photo"),
        )
        for candidate in candidates:
            normalized = self._normalize_archive_member_path(candidate)
            if normalized is not None:
                return normalized
        return None

    def _normalize_archive_member_path(self, value: Any) -> str | None:
        raw_value = self._string_or_none(value)
        if raw_value is None:
            return None
        normalized = raw_value.replace("\\", "/").strip().lstrip("/")
        if not normalized or normalized.startswith("../") or "/../" in normalized:
            return None
        return normalized

    def _archive_supports_attachment_extraction(self, original_filename: str | None) -> bool:
        suffix = Path(original_filename or "").suffix.lower()
        return suffix in {".zip", ".rar"}

    def _archive_download_url(
        self,
        *,
        source_chat_id: str,
        member_path: str,
    ) -> str:
        return "tg-export://attachment?" + urlencode(
            {
                "chat_id": source_chat_id,
                "path": member_path,
            },
        )

    def _message_has_media(self, item: Any) -> bool:
        if not isinstance(item, dict):
            return False
        return any(
            item.get(key) not in (None, "", [], {})
            for key in ("photo", "file", "thumbnail", "media_type", "mime_type")
        )

    def _optional_int(self, value: Any) -> int | None:
        if isinstance(value, int):
            return value
        if isinstance(value, float):
            return int(value)
        if isinstance(value, str):
            normalized = value.strip()
            if not normalized:
                return None
            try:
                return int(normalized)
            except ValueError:
                return None
        return None

    def _coerce_text(self, value: Any) -> str | None:
        if isinstance(value, str):
            return value or None
        if not isinstance(value, list):
            return None
        parts: list[str] = []
        for item in value:
            if isinstance(item, str):
                parts.append(item)
                continue
            if isinstance(item, dict):
                text = item.get("text")
                if isinstance(text, str):
                    parts.append(text)
        body = "".join(parts)
        return body or None

    def _string_or_none(self, value: Any) -> str | None:
        if value is None:
            return None
        normalized = str(value).strip()
        return normalized or None

    def _message_sort_key(self, message_id: str) -> tuple[int, str]:
        try:
            return (0, f"{int(message_id):020d}")
        except ValueError:
            return (1, message_id)
