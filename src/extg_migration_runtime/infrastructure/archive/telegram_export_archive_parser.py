from __future__ import annotations

import asyncio
import hashlib
import io
import json
import subprocess
import tempfile
import zipfile
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from extg_migration_runtime.application.telegram_export_archive import (
    ParsedTelegramExportSnapshot,
)
from extg_shared.contracts.errors import ConfigurationError
from extg_shared.contracts.models import (
    ContentType,
    TelegramAuthor,
    TelegramSourceMessage,
)
from extg_shared.contracts.ports import TelegramExportArchiveStageStore, TelegramExportSnapshotRepository


class TelegramExportArchiveParser:
    def __init__(self, *, unar_binary: str = "unar") -> None:
        self._unar_binary = unar_binary

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
        item: dict[str, Any],
    ) -> TelegramSourceMessage | None:
        message_id = self._string_or_none(item.get("id"))
        sent_at = self._parse_datetime(item)
        if not message_id or sent_at is None:
            return None
        message_type = self._string_or_none(item.get("type")) or "message"
        is_service = message_type == "service"
        body = self._coerce_text(item.get("text"))
        has_media = self._message_has_media(item)
        media_type = self._string_or_none(item.get("media_type")) or "attachment"
        if not body and has_media and not is_service:
            body = f"[{media_type} omitted from Telegram export]"
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
            content_type=ContentType.SERVICE if is_service else ContentType.TEXT,
            raw_payload={
                "id": message_id,
                "type": message_type,
                "date": item.get("date"),
                "from": item.get("from"),
                "from_id": item.get("from_id"),
                "media_type": item.get("media_type"),
            },
        )

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

    def _message_has_media(self, item: Any) -> bool:
        if not isinstance(item, dict):
            return False
        return any(
            item.get(key) not in (None, "", [], {})
            for key in ("photo", "file", "thumbnail", "media_type", "mime_type")
        )

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
