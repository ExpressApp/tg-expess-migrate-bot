from __future__ import annotations

from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any
from urllib.parse import quote

from starlette.background import BackgroundTask
from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse, Response
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
    MIGRATION_MANIFEST_ADAPTER,
    SOURCE_CHANNEL_ACCESS_PROFILE_ADAPTER,
    SOURCE_DIALOG_ADAPTER,
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
from extg_shared.contracts.errors import build_service_error_envelope

from extg_telethon_service.bootstrap.container import (
    TelethonServiceContainer,
    create_container as create_telethon_service_container,
)
from extg_telethon_service.application.telegram_session_service import (
    TelegramConnectionChallengeResult,
    TelegramPasswordChallengeResult,
    TelegramSessionStatusResult,
)
from extg_shared.contracts.errors import (
    ConfigurationError,
    FatalItemError,
    RecoverableItemError,
)
from extg_shared.utils.runtime import close_runtime_resources


def create_telethon_service_app(
    container: TelethonServiceContainer | None = None,
) -> FastAPI:
    container = container or create_telethon_service_container()
    settings = container.settings()
    if settings.telegram.api_id is None:
        raise ConfigurationError("EXTG_TELEGRAM__API_ID is required for extg-telethon-service")
    if not (settings.telegram.api_hash or "").strip():
        raise ConfigurationError("EXTG_TELEGRAM__API_HASH is required for extg-telethon-service")
    facade = container.telethon_integration_service()
    session_service = container.local_telegram_session_service()
    attachment_temp_file_janitor = container.attachment_temp_file_janitor()

    @asynccontextmanager
    async def lifespan(_: FastAPI):
        await attachment_temp_file_janitor.start()
        try:
            yield
        finally:
            await attachment_temp_file_janitor.stop()
            await close_runtime_resources(
                container,
                provider_names=("local_telegram_gateway",),
            )

    app = FastAPI(title="extg-telethon-service", lifespan=lifespan)

    @app.middleware("http")
    async def verify_internal_token(request, call_next):
        if request.url.path == "/health":
            return await call_next(request)
        expected = (settings.telethon_service.internal_token or "").strip()
        if expected:
            actual = request.headers.get("X-Internal-Token", "")
            if actual != expected:
                return Response(status_code=401, content="unauthorized")
        return await call_next(request)

    @app.post("/internal/telegram/session/start")
    async def start_session(request: SessionStartRequest) -> dict[str, Any]:
        try:
            result = await session_service.start_connection(
                operator_huid=request.operator_huid,
                phone_number=request.phone_number,
                force_sms=request.force_sms,
            )
            return _serialize_session_result(result)
        except Exception as error:
            _raise_http_error(error)

    @app.post("/internal/telegram/session/complete-code")
    async def complete_session_code(request: SessionCompleteCodeRequest) -> dict[str, Any]:
        try:
            result = await session_service.complete_code(
                operator_huid=request.operator_huid,
                challenge_id=request.challenge_id,
                code=request.code,
            )
            return _serialize_session_result(result)
        except Exception as error:
            _raise_http_error(error)

    @app.post("/internal/telegram/session/complete-password")
    async def complete_session_password(request: SessionCompletePasswordRequest) -> dict[str, Any]:
        try:
            result = await session_service.complete_password(
                operator_huid=request.operator_huid,
                challenge_id=request.challenge_id,
                password=request.password,
            )
            return TELEGRAM_SESSION_STATUS_ADAPTER.dump_python(result, mode="json")
        except Exception as error:
            _raise_http_error(error)

    @app.get("/internal/telegram/session/status")
    async def session_status(operator_huid: str) -> dict[str, Any]:
        try:
            result = await session_service.status(operator_huid=operator_huid)
            return TELEGRAM_SESSION_STATUS_ADAPTER.dump_python(result, mode="json")
        except Exception as error:
            _raise_http_error(error)

    @app.delete("/internal/telegram/session/{operator_huid}")
    async def disconnect_session(operator_huid: str) -> dict[str, Any]:
        try:
            result = await session_service.disconnect(operator_huid=operator_huid)
            return TELEGRAM_DISCONNECT_ADAPTER.dump_python(result, mode="json")
        except Exception as error:
            _raise_http_error(error)

    @app.post("/internal/telegram/session/resolve-binding")
    async def resolve_session_binding(request: SessionResolveBindingRequest) -> dict[str, Any]:
        try:
            await facade.ensure_session_binding(
                operator_huid=request.operator_huid,
                mark_used=request.mark_used,
            )
            return {"bound": True}
        except Exception as error:
            _raise_http_error(error)

    @app.post("/internal/telegram/dialogs/list-available")
    async def list_available_dialogs(request: ListAvailableDialogsRequest) -> list[dict[str, Any]]:
        try:
            result = await facade.list_available_dialogs(
                operator_huid=request.operator_huid,
                limit=request.limit,
                query=request.query,
                source_backend=request.source_backend,
            )
            return SOURCE_DIALOG_LIST_ADAPTER.dump_python(result, mode="json")
        except Exception as error:
            _raise_http_error(error)

    @app.post("/internal/telegram/dialogs/get")
    async def get_source_dialog(request: GetSourceDialogRequest) -> dict[str, Any] | None:
        try:
            result = await facade.get_source_dialog(
                operator_huid=request.operator_huid,
                dialog_id=request.dialog_id,
                source_backend=request.source_backend,
            )
            if result is None:
                return None
            return SOURCE_DIALOG_ADAPTER.dump_python(result, mode="json")
        except Exception as error:
            _raise_http_error(error)

    @app.post("/internal/telegram/dialogs/list-manifest")
    async def list_manifest_dialogs(request: ListDialogsRequest) -> list[dict[str, Any]]:
        try:
            manifest = MIGRATION_MANIFEST_ADAPTER.validate_python(request.manifest)
            result = await facade.list_dialogs(
                operator_huid=request.operator_huid,
                manifest=manifest,
            )
            return SOURCE_DIALOG_LIST_ADAPTER.dump_python(result, mode="json")
        except Exception as error:
            _raise_http_error(error)

    @app.post("/internal/telegram/history/fetch")
    async def fetch_history(request: FetchHistoryRequest) -> dict[str, Any]:
        try:
            parsed_cursor = (
                None
                if request.cursor is None
                else HISTORY_CURSOR_ADAPTER.validate_python(request.cursor)
            )
            result = await facade.fetch_history(
                operator_huid=request.operator_huid,
                dialog_id=request.dialog_id,
                cursor=parsed_cursor,
                limit=request.limit,
                source_backend=request.source_backend,
                thread_id=request.thread_id,
            )
            return HISTORY_BATCH_ADAPTER.dump_python(result, mode="json")
        except Exception as error:
            _raise_http_error(error)

    @app.post("/internal/telegram/participants/list")
    async def list_participants(request: ListParticipantsRequest) -> list[dict[str, Any]]:
        try:
            result = await facade.list_participants(
                operator_huid=request.operator_huid,
                dialog_id=request.dialog_id,
                source_backend=request.source_backend,
            )
            return SOURCE_PARTICIPANT_LIST_ADAPTER.dump_python(result, mode="json")
        except Exception as error:
            _raise_http_error(error)

    @app.post("/internal/telegram/topics/list")
    async def list_topics(request: ListTopicsRequest) -> list[dict[str, Any]]:
        try:
            result = await facade.list_topics(
                operator_huid=request.operator_huid,
                dialog_id=request.dialog_id,
                source_backend=request.source_backend,
            )
            return SOURCE_TOPIC_LIST_ADAPTER.dump_python(result, mode="json")
        except Exception as error:
            _raise_http_error(error)

    @app.post("/internal/telegram/channel-access-profile")
    async def get_channel_access_profile(request: ChannelAccessProfileRequest) -> dict[str, Any]:
        try:
            result = await facade.get_channel_access_profile(
                operator_huid=request.operator_huid,
                dialog_id=request.dialog_id,
                source_backend=request.source_backend,
            )
            return SOURCE_CHANNEL_ACCESS_PROFILE_ADAPTER.dump_python(result, mode="json")
        except Exception as error:
            _raise_http_error(error)

    @app.post("/internal/telegram/attachments/download")
    async def download_attachment(request: DownloadAttachmentRequest) -> Response:
        try:
            attachment = CANONICAL_ATTACHMENT_ADAPTER.validate_python(request.attachment)
            downloaded = await facade.download_attachment(
                operator_huid=request.operator_huid,
                attachment=attachment,
                source_backend=request.source_backend,
            )
            if downloaded.content is not None:
                content = downloaded.content
            elif downloaded.local_path is None:
                raise FatalItemError("downloaded attachment has no content")
            headers = {
                "X-Extg-Media-Kind": downloaded.attachment.media_kind,
            }
            if downloaded.attachment.filename:
                if _is_latin1_header_value(downloaded.attachment.filename):
                    headers["X-Extg-Filename"] = downloaded.attachment.filename
                else:
                    headers["X-Extg-Filename-Urlencoded"] = quote(
                        downloaded.attachment.filename,
                        safe="",
                    )
            if downloaded.attachment.mime_type:
                headers["X-Extg-Mime-Type"] = downloaded.attachment.mime_type
            if downloaded.attachment.sha256:
                headers["X-Extg-Sha256"] = downloaded.attachment.sha256
            if downloaded.size_bytes is not None:
                headers["X-Extg-Size-Bytes"] = str(downloaded.size_bytes)
            if downloaded.local_path is not None:
                local_path = Path(downloaded.local_path)
                if not local_path.exists():
                    raise FatalItemError("attachment temp file is missing")
                return FileResponse(
                    path=str(local_path),
                    media_type="application/octet-stream",
                    headers=headers,
                    background=BackgroundTask(_safe_unlink, str(local_path)),
                )
            return Response(content=content, media_type="application/octet-stream", headers=headers)
        except Exception as error:
            _raise_http_error(error)

    @app.get("/health")
    async def health() -> dict[str, str]:
        return {"status": "ok"}

    return app


def main() -> None:
    import uvicorn

    container = create_telethon_service_container()
    app = create_telethon_service_app(container)
    settings = container.settings()
    uvicorn.run(
        app,
        host=settings.telethon_service.host,
        port=settings.telethon_service.port,
    )


def _serialize_session_result(
    result: TelegramConnectionChallengeResult | TelegramPasswordChallengeResult | TelegramSessionStatusResult,
) -> dict[str, Any]:
    if isinstance(result, TelegramConnectionChallengeResult):
        return SessionResultEnvelope(
            kind="challenge",
            payload=TELEGRAM_CONNECTION_CHALLENGE_ADAPTER.dump_python(result, mode="json"),
        ).model_dump(mode="json")
    if isinstance(result, TelegramPasswordChallengeResult):
        return SessionResultEnvelope(
            kind="password_challenge",
            payload=TELEGRAM_PASSWORD_CHALLENGE_ADAPTER.dump_python(result, mode="json"),
        ).model_dump(mode="json")
    return SessionResultEnvelope(
        kind="status",
        payload=TELEGRAM_SESSION_STATUS_ADAPTER.dump_python(result, mode="json"),
    ).model_dump(mode="json")


def _safe_unlink(path: str) -> None:
    try:
        Path(path).unlink(missing_ok=True)
    except OSError:
        return


def _raise_http_error(error: Exception) -> None:
    if isinstance(error, ConfigurationError):
        raise HTTPException(
            status_code=400,
            detail=build_service_error_envelope(
                code="configuration_error",
                message=str(error),
                retryable=False,
            ).model_dump(mode="json"),
        ) from error
    if isinstance(error, RecoverableItemError):
        raise HTTPException(
            status_code=503,
            detail=build_service_error_envelope(
                code="recoverable_error",
                message=str(error),
                retryable=True,
            ).model_dump(mode="json"),
        ) from error
    if isinstance(error, FatalItemError):
        raise HTTPException(
            status_code=409,
            detail=build_service_error_envelope(
                code="fatal_error",
                message=str(error),
                retryable=False,
            ).model_dump(mode="json"),
        ) from error
    raise HTTPException(
        status_code=500,
        detail=build_service_error_envelope(
            code="internal_error",
            message=str(error),
            retryable=False,
        ).model_dump(mode="json"),
    ) from error


def _is_latin1_header_value(value: str) -> bool:
    try:
        value.encode("latin-1")
    except UnicodeEncodeError:
        return False
    return True
