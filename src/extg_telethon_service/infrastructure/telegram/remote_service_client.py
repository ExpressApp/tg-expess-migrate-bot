from __future__ import annotations

import os
from pathlib import Path
import tempfile
from typing import Any
from urllib.parse import unquote

from aiofiles import open as aiofiles_open
import httpx
from extg_shared.contracts.api.telethon_service import (
    CANONICAL_ATTACHMENT_ADAPTER,
    ChannelAccessProfileRequest,
    DownloadAttachmentRequest,
    FetchHistoryRequest,
    GetSourceDialogRequest,
    HISTORY_BATCH_ADAPTER,
    HISTORY_CURSOR_ADAPTER,
    ListAvailableDialogsRequest,
    ListDialogsRequest,
    ListParticipantsRequest,
    ListTopicsRequest,
    SOURCE_DIALOG_ADAPTER,
    SOURCE_CHANNEL_ACCESS_PROFILE_ADAPTER,
    SOURCE_DIALOG_LIST_ADAPTER,
    SOURCE_PARTICIPANT_LIST_ADAPTER,
    SOURCE_TOPIC_LIST_ADAPTER,
    SessionCompleteCodeRequest,
    SessionCompletePasswordRequest,
    SessionResolveBindingRequest,
    SessionResultEnvelope,
    SessionStartRequest,
    TELEGRAM_CONNECTION_CHALLENGE_ADAPTER,
    TELEGRAM_DISCONNECT_ADAPTER,
    TELEGRAM_PASSWORD_CHALLENGE_ADAPTER,
    TELEGRAM_SESSION_STATUS_ADAPTER,
)
from extg_shared.contracts.errors import parse_service_error_envelope

from extg_telethon_service.application.telegram_session_context import TelegramSessionContext
from extg_telethon_service.application.telegram_session_service import (
    TelegramConnectionChallengeResult,
    TelegramDisconnectResult,
    TelegramPasswordChallengeResult,
    TelegramSessionStatusResult,
)
from extg_shared.contracts.errors import (
    ConfigurationError,
    FatalItemError,
    RecoverableItemError,
)
from extg_shared.contracts.manifest import MigrationManifest
from extg_shared.contracts.models import (
    CanonicalAttachment,
    DownloadedAttachment,
    HistoryBatch,
    HistoryCursor,
    SourceChannelAccessProfile,
    SourceDialog,
    SourceParticipant,
    SourceTopic,
)


class RemoteTelethonServiceClient:
    def __init__(
        self,
        *,
        base_url: str,
        request_timeout_seconds: float = 60.0,
        attachment_request_timeout_seconds: float | None = 300.0,
        internal_token: str | None = None,
        default_attachment_dir: str | None = None,
        http_client: httpx.AsyncClient | None = None,
    ) -> None:
        if not base_url:
            raise ConfigurationError("telethon service base_url is required for remote client mode")
        self._base_url = base_url.rstrip("/")
        self._internal_token = internal_token.strip() if internal_token else None
        self._default_attachment_dir = Path(default_attachment_dir or tempfile.gettempdir())
        self._request_timeout_seconds = request_timeout_seconds
        self._attachment_request_timeout_seconds = (
            attachment_request_timeout_seconds
            if attachment_request_timeout_seconds is not None
            else request_timeout_seconds
        )
        self._owns_client = http_client is None
        self._client = http_client or httpx.AsyncClient(
            base_url=self._base_url,
            timeout=httpx.Timeout(request_timeout_seconds),
        )

    async def close(self) -> None:
        if self._owns_client:
            await self._client.aclose()

    async def start_connection(
        self,
        *,
        operator_huid: str,
        phone_number: str,
        force_sms: bool = False,
    ) -> TelegramConnectionChallengeResult | TelegramSessionStatusResult:
        envelope = await self._post_json(
            "/internal/telegram/session/start",
            SessionStartRequest(
                operator_huid=operator_huid,
                phone_number=phone_number,
                force_sms=force_sms,
            ).model_dump(mode="json"),
        )
        return _parse_session_result_envelope(envelope)

    async def complete_code(
        self,
        *,
        operator_huid: str,
        challenge_id: str,
        code: str,
    ) -> TelegramPasswordChallengeResult | TelegramSessionStatusResult:
        envelope = await self._post_json(
            "/internal/telegram/session/complete-code",
            SessionCompleteCodeRequest(
                operator_huid=operator_huid,
                challenge_id=challenge_id,
                code=code,
            ).model_dump(mode="json"),
        )
        return _parse_session_result_envelope(envelope)

    async def complete_password(
        self,
        *,
        operator_huid: str,
        challenge_id: str,
        password: str,
    ) -> TelegramSessionStatusResult:
        payload = await self._post_json(
            "/internal/telegram/session/complete-password",
            SessionCompletePasswordRequest(
                operator_huid=operator_huid,
                challenge_id=challenge_id,
                password=password,
            ).model_dump(mode="json"),
        )
        return TELEGRAM_SESSION_STATUS_ADAPTER.validate_python(payload)

    async def status(
        self,
        *,
        operator_huid: str,
    ) -> TelegramSessionStatusResult:
        payload = await self._get_json(
            "/internal/telegram/session/status",
            params={"operator_huid": operator_huid},
        )
        return TELEGRAM_SESSION_STATUS_ADAPTER.validate_python(payload)

    async def disconnect(
        self,
        *,
        operator_huid: str,
    ) -> TelegramDisconnectResult:
        payload = await self._delete_json(
            f"/internal/telegram/session/{operator_huid}",
        )
        return TELEGRAM_DISCONNECT_ADAPTER.validate_python(payload)

    async def ensure_session_binding(
        self,
        *,
        operator_huid: str,
        mark_used: bool = True,
    ) -> None:
        await self._post_json(
            "/internal/telegram/session/resolve-binding",
            SessionResolveBindingRequest(
                operator_huid=operator_huid,
                mark_used=mark_used,
            ).model_dump(mode="json"),
        )

    async def list_available_dialogs(
        self,
        *,
        operator_huid: str,
        limit: int = 100,
        query: str | None = None,
        source_backend: str = "telethon_user_session",
    ) -> list[SourceDialog]:
        payload = await self._post_json(
            "/internal/telegram/dialogs/list-available",
            ListAvailableDialogsRequest(
                operator_huid=operator_huid,
                limit=limit,
                query=query,
                source_backend=source_backend,
            ).model_dump(mode="json"),
        )
        return SOURCE_DIALOG_LIST_ADAPTER.validate_python(payload)

    async def get_source_dialog(
        self,
        *,
        operator_huid: str,
        dialog_id: str,
        source_backend: str = "telethon_user_session",
    ) -> SourceDialog | None:
        payload = await self._post_json(
            "/internal/telegram/dialogs/get",
            GetSourceDialogRequest(
                operator_huid=operator_huid,
                dialog_id=dialog_id,
                source_backend=source_backend,
            ).model_dump(mode="json"),
        )
        if payload is None:
            return None
        return SOURCE_DIALOG_ADAPTER.validate_python(payload)

    async def list_dialogs(
        self,
        *,
        operator_huid: str,
        manifest: MigrationManifest,
    ) -> list[SourceDialog]:
        payload = await self._post_json(
            "/internal/telegram/dialogs/list-manifest",
            ListDialogsRequest(
                operator_huid=operator_huid,
                manifest=manifest.model_dump(mode="json"),
            ).model_dump(mode="json"),
        )
        return SOURCE_DIALOG_LIST_ADAPTER.validate_python(payload)

    async def fetch_history(
        self,
        *,
        operator_huid: str,
        dialog_id: str,
        cursor: HistoryCursor | None,
        limit: int,
        source_backend: str = "telethon_user_session",
        thread_id: str | None = None,
    ) -> HistoryBatch:
        payload = await self._post_json(
            "/internal/telegram/history/fetch",
            FetchHistoryRequest(
                operator_huid=operator_huid,
                dialog_id=dialog_id,
                cursor=(
                    HISTORY_CURSOR_ADAPTER.dump_python(cursor, mode="json")
                    if cursor is not None
                    else None
                ),
                limit=limit,
                source_backend=source_backend,
                thread_id=thread_id,
            ).model_dump(mode="json"),
        )
        return HISTORY_BATCH_ADAPTER.validate_python(payload)

    async def download_attachment(
        self,
        *,
        operator_huid: str,
        attachment: CanonicalAttachment,
        source_backend: str = "telethon_user_session",
    ) -> DownloadedAttachment:
        request_payload = DownloadAttachmentRequest(
            operator_huid=operator_huid,
            attachment=CANONICAL_ATTACHMENT_ADAPTER.dump_python(attachment, mode="json"),
            source_backend=source_backend,
        ).model_dump(mode="json")
        try:
            async with self._client.stream(
                "POST",
                "/internal/telegram/attachments/download",
                headers=self._headers(),
                json=request_payload,
                timeout=httpx.Timeout(
                    self._request_timeout_seconds,
                    read=self._attachment_request_timeout_seconds,
                ),
            ) as response:
                if response.status_code >= 400:
                    await response.aread()
                    _raise_telethon_service_error(response)

                raw_filename = response.headers.get("x-extg-filename")
                if raw_filename is None:
                    encoded_filename = response.headers.get("x-extg-filename-urlencoded")
                    raw_filename = unquote(encoded_filename) if encoded_filename else None
                filename = self._sanitize_attachment_filename(raw_filename)
                if filename is None:
                    filename = self._sanitize_attachment_filename(attachment.filename)
                media_kind = response.headers.get("x-extg-media-kind") or attachment.media_kind
                mime_type = response.headers.get("x-extg-mime-type") or attachment.mime_type
                sha256 = response.headers.get("x-extg-sha256")
                size_header = response.headers.get("x-extg-size-bytes")
                declared_size_bytes = (
                    int(size_header) if size_header and size_header.isdigit() else None
                )
                target_dir = self._ensure_private_attachment_dir(self._default_attachment_dir)
                local_path = self._temporary_storage_path(
                    target_dir=target_dir,
                    attachment=attachment,
                    media_kind=media_kind,
                    filename=filename,
                )
                streamed_size_bytes = 0
                try:
                    async with aiofiles_open(local_path, "wb") as file_obj:
                        async for chunk in response.aiter_bytes():
                            if not chunk:
                                continue
                            streamed_size_bytes += len(chunk)
                            await file_obj.write(chunk)
                except Exception:
                    local_path.unlink(missing_ok=True)
                    raise
        except httpx.TimeoutException as error:
            raise RecoverableItemError(
                self._transport_error_message("telethon service timeout", error),
            ) from error
        except httpx.NetworkError as error:
            raise RecoverableItemError(
                self._transport_error_message("telethon service network failure", error),
            ) from error

        size_bytes = declared_size_bytes or streamed_size_bytes
        updated_attachment = CanonicalAttachment(
            source_file_id=attachment.source_file_id,
            filename=filename,
            mime_type=mime_type,
            size_bytes=size_bytes,
            media_kind=media_kind,
            duration_seconds=attachment.duration_seconds,
            download_url=attachment.download_url,
            temp_local_path=str(local_path),
            sha256=sha256,
        )
        return DownloadedAttachment(
            attachment=updated_attachment,
            local_path=str(local_path),
            size_bytes=size_bytes,
        )

    async def list_participants(
        self,
        *,
        operator_huid: str,
        dialog_id: str,
        source_backend: str = "telethon_user_session",
    ) -> list[SourceParticipant]:
        payload = await self._post_json(
            "/internal/telegram/participants/list",
            ListParticipantsRequest(
                operator_huid=operator_huid,
                dialog_id=dialog_id,
                source_backend=source_backend,
            ).model_dump(mode="json"),
        )
        return SOURCE_PARTICIPANT_LIST_ADAPTER.validate_python(payload)

    async def list_topics(
        self,
        *,
        operator_huid: str,
        dialog_id: str,
        source_backend: str = "telethon_user_session",
    ) -> list[SourceTopic]:
        payload = await self._post_json(
            "/internal/telegram/topics/list",
            ListTopicsRequest(
                operator_huid=operator_huid,
                dialog_id=dialog_id,
                source_backend=source_backend,
            ).model_dump(mode="json"),
        )
        return SOURCE_TOPIC_LIST_ADAPTER.validate_python(payload)

    async def get_channel_access_profile(
        self,
        *,
        operator_huid: str,
        dialog_id: str,
        source_backend: str = "telethon_user_session",
    ) -> SourceChannelAccessProfile:
        payload = await self._post_json(
            "/internal/telegram/channel-access-profile",
            ChannelAccessProfileRequest(
                operator_huid=operator_huid,
                dialog_id=dialog_id,
                source_backend=source_backend,
            ).model_dump(mode="json"),
        )
        return SOURCE_CHANNEL_ACCESS_PROFILE_ADAPTER.validate_python(payload)

    async def _post_json(self, path: str, payload: dict[str, Any]) -> dict[str, Any]:
        response = await self._request("POST", path, json=payload)
        response.raise_for_status()
        return response.json()

    async def _get_json(self, path: str, *, params: dict[str, Any]) -> dict[str, Any]:
        response = await self._request("GET", path, params=params)
        response.raise_for_status()
        return response.json()

    async def _delete_json(self, path: str) -> dict[str, Any]:
        response = await self._request("DELETE", path)
        response.raise_for_status()
        return response.json()

    async def _request(self, method: str, path: str, **kwargs) -> httpx.Response:
        try:
            response = await self._client.request(
                method,
                path,
                headers=self._headers(),
                **kwargs,
            )
        except httpx.TimeoutException as error:
            raise RecoverableItemError(
                self._transport_error_message("telethon service timeout", error),
            ) from error
        except httpx.NetworkError as error:
            raise RecoverableItemError(
                self._transport_error_message("telethon service network failure", error),
            ) from error
        if response.status_code >= 400:
            _raise_telethon_service_error(response)
        return response

    def _headers(self) -> dict[str, str]:
        headers: dict[str, str] = {}
        if self._internal_token:
            headers["X-Internal-Token"] = self._internal_token
        return headers

    def _sanitize_attachment_filename(self, value: str | None) -> str | None:
        if not value:
            return None
        sanitized = Path(value).name
        return sanitized or None

    def _ensure_private_attachment_dir(self, target_dir: Path) -> Path:
        target_dir.mkdir(parents=True, exist_ok=True, mode=0o700)
        try:
            target_dir.chmod(0o700)
        except OSError:
            pass
        return target_dir

    def _temporary_storage_path(
        self,
        *,
        target_dir: Path,
        attachment: CanonicalAttachment,
        media_kind: str | None,
        filename: str | None,
    ) -> Path:
        suffix = ""
        if filename:
            suffix = Path(filename).suffix
        elif attachment.filename:
            suffix = Path(attachment.filename).suffix
        file_descriptor, raw_path = tempfile.mkstemp(
            prefix="extg-remote-",
            suffix=suffix,
            dir=str(target_dir),
        )
        os.close(file_descriptor)
        return Path(raw_path)

    def _transport_error_message(self, prefix: str, error: Exception) -> str:
        details = str(error).strip()
        if details:
            return f"{prefix}: {details}"
        return f"{prefix}: {error.__class__.__name__}"


class RemoteTelegramSessionService:
    def __init__(self, *, client: RemoteTelethonServiceClient) -> None:
        self._client = client

    async def start_connection(self, *, operator_huid: str, phone_number: str, force_sms: bool = False):
        return await self._client.start_connection(
            operator_huid=operator_huid,
            phone_number=phone_number,
            force_sms=force_sms,
        )

    async def complete_code(self, *, operator_huid: str, challenge_id: str, code: str):
        return await self._client.complete_code(
            operator_huid=operator_huid,
            challenge_id=challenge_id,
            code=code,
        )

    async def complete_password(self, *, operator_huid: str, challenge_id: str, password: str):
        return await self._client.complete_password(
            operator_huid=operator_huid,
            challenge_id=challenge_id,
            password=password,
        )

    async def status(self, *, operator_huid: str) -> TelegramSessionStatusResult:
        return await self._client.status(operator_huid=operator_huid)

    async def disconnect(self, *, operator_huid: str) -> TelegramDisconnectResult:
        return await self._client.disconnect(operator_huid=operator_huid)

    async def resolve_session_string(
        self,
        *,
        operator_huid: str,
        mark_used: bool = True,
    ) -> str | None:
        await self._client.ensure_session_binding(
            operator_huid=operator_huid,
            mark_used=mark_used,
        )
        return None

    async def close(self) -> None:
        await self._client.close()


class RemoteTelegramGateway:
    def __init__(
        self,
        *,
        session_context: TelegramSessionContext,
        client: RemoteTelethonServiceClient,
    ) -> None:
        self._session_context = session_context
        self._client = client

    async def list_available_dialogs(
        self,
        *,
        limit: int = 100,
        query: str | None = None,
        source_backend: str = "telethon_user_session",
    ) -> list[SourceDialog]:
        return await self._client.list_available_dialogs(
            operator_huid=self._require_operator_huid(),
            limit=limit,
            query=query,
            source_backend=source_backend,
        )

    async def get_source_dialog(
        self,
        dialog_id: str,
        *,
        source_backend: str = "telethon_user_session",
    ) -> SourceDialog | None:
        return await self._client.get_source_dialog(
            operator_huid=self._require_operator_huid(),
            dialog_id=dialog_id,
            source_backend=source_backend,
        )

    async def list_dialogs(self, manifest: MigrationManifest) -> list[SourceDialog]:
        return await self._client.list_dialogs(
            operator_huid=self._require_operator_huid(),
            manifest=manifest,
        )

    async def fetch_history(
        self,
        dialog_id: str,
        cursor: HistoryCursor | None,
        limit: int,
        *,
        source_backend: str = "telethon_user_session",
        thread_id: str | None = None,
    ) -> HistoryBatch:
        return await self._client.fetch_history(
            operator_huid=self._require_operator_huid(),
            dialog_id=dialog_id,
            cursor=cursor,
            limit=limit,
            source_backend=source_backend,
            thread_id=thread_id,
        )

    async def download_attachment(
        self,
        attachment: CanonicalAttachment,
        *,
        source_backend: str = "telethon_user_session",
    ) -> DownloadedAttachment:
        return await self._client.download_attachment(
            operator_huid=self._require_operator_huid(),
            attachment=attachment,
            source_backend=source_backend,
        )

    async def list_participants(
        self,
        dialog_id: str,
        *,
        source_backend: str = "telethon_user_session",
    ) -> list[SourceParticipant]:
        return await self._client.list_participants(
            operator_huid=self._require_operator_huid(),
            dialog_id=dialog_id,
            source_backend=source_backend,
        )

    async def list_topics(
        self,
        dialog_id: str,
        *,
        source_backend: str = "telethon_user_session",
    ) -> list[SourceTopic]:
        return await self._client.list_topics(
            operator_huid=self._require_operator_huid(),
            dialog_id=dialog_id,
            source_backend=source_backend,
        )

    async def get_channel_access_profile(
        self,
        dialog_id: str,
        *,
        source_backend: str = "telethon_user_session",
    ) -> SourceChannelAccessProfile:
        return await self._client.get_channel_access_profile(
            operator_huid=self._require_operator_huid(),
            dialog_id=dialog_id,
            source_backend=source_backend,
        )

    async def subscribe_delta(self, *_args, **_kwargs):
        raise ConfigurationError(
            "remote telethon service does not support delta subscription yet; keep delta-sync on local Telegram runtime for now",
        )
        yield None

    async def close(self) -> None:
        await self._client.close()

    def _require_operator_huid(self) -> str:
        operator_huid = self._session_context.current_operator_huid()
        if not operator_huid:
            raise ConfigurationError(
                "telegram operator context is missing for remote telethon service request",
            )
        return operator_huid


def _parse_session_result_envelope(
    payload: dict[str, Any],
) -> TelegramConnectionChallengeResult | TelegramPasswordChallengeResult | TelegramSessionStatusResult:
    envelope = SessionResultEnvelope.model_validate(payload)
    if envelope.kind == "challenge":
        return TELEGRAM_CONNECTION_CHALLENGE_ADAPTER.validate_python(envelope.payload)
    if envelope.kind == "password_challenge":
        return TELEGRAM_PASSWORD_CHALLENGE_ADAPTER.validate_python(envelope.payload)
    return TELEGRAM_SESSION_STATUS_ADAPTER.validate_python(envelope.payload)


def _raise_telethon_service_error(response: httpx.Response) -> None:
    try:
        payload = response.json()
    except ValueError:
        payload = {"detail": response.text}
    detail = payload.get("detail") if isinstance(payload, dict) else response.text
    envelope = parse_service_error_envelope(detail)
    if envelope is not None:
        message = envelope.message
    else:
        message = str(detail or f"telethon service returned HTTP {response.status_code}")
    if response.status_code in {408, 429} or response.status_code >= 500:
        raise RecoverableItemError(message)
    if response.status_code == 400:
        raise ConfigurationError(message)
    raise FatalItemError(message)
