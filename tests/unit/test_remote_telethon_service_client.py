from __future__ import annotations

from contextlib import asynccontextmanager
from pathlib import Path
import tempfile

from dependency_injector import providers
import httpx
import pytest

from extg_shared.config.common import (
    PostgresSettings,
    SecuritySettings,
    TelegramSettings,
    TelethonServiceSettings,
)
from extg_shared.contracts.errors import RecoverableItemError
from extg_shared.contracts.models import (
    CanonicalAttachment,
    DownloadedAttachment,
    SourceDialog,
)
from extg_telethon_service.application.telegram_session_service import TelegramConnectionChallengeResult
from extg_telethon_service.bootstrap.config import TelethonServiceAppSettings
from extg_telethon_service.bootstrap.container import TelethonServiceContainer
from extg_telethon_service.infrastructure.telegram.remote_service_client import RemoteTelethonServiceClient
from extg_telethon_service.presentation.telethon_service.app import create_telethon_service_app


class StubTelethonIntegrationService:
    async def list_available_dialogs(
        self,
        *,
        operator_huid: str,
        limit: int = 100,
        query: str | None = None,
        source_backend: str = "telethon_user_session",
    ) -> list[SourceDialog]:
        if query == "fail":
            raise RecoverableItemError("temporary telegram outage")
        return [
            SourceDialog(
                dialog_id="-100500",
                chat_type="supergroup",
                title="Remote Dialog",
            ),
        ]

    async def download_attachment(
        self,
        *,
        operator_huid: str,
        attachment: CanonicalAttachment,
        source_backend: str = "telethon_user_session",
    ) -> DownloadedAttachment:
        if attachment.download_url == "fake://stream-local":
            with tempfile.NamedTemporaryFile(delete=False) as handle:
                handle.write(b"voice")
                local_path = handle.name
            return DownloadedAttachment(
                attachment=CanonicalAttachment(
                    source_file_id=attachment.source_file_id,
                    filename=(f"../{attachment.filename}" if attachment.filename else None),
                    mime_type=attachment.mime_type or "audio/ogg",
                    size_bytes=5,
                    media_kind=attachment.media_kind,
                    sha256="sha-1",
                ),
                local_path=local_path,
                size_bytes=5,
            )
        return DownloadedAttachment(
            attachment=CanonicalAttachment(
                source_file_id=attachment.source_file_id,
                filename=(f"../{attachment.filename}" if attachment.filename else None),
                mime_type=attachment.mime_type or "audio/ogg",
                size_bytes=5,
                media_kind=attachment.media_kind,
                sha256="sha-1",
            ),
            content=b"voice",
            size_bytes=5,
        )


class StubTelegramSessionService:
    async def start_connection(
        self,
        *,
        operator_huid: str,
        phone_number: str,
        force_sms: bool = False,
    ) -> TelegramConnectionChallengeResult:
        return TelegramConnectionChallengeResult(
            operator_huid=operator_huid,
            phone_number="+7***22",
            challenge_id="challenge-remote",
        )


class _StreamResponse:
    status_code = 200
    headers = {
        "x-extg-filename": "voice.ogg",
        "x-extg-media-kind": "voice",
        "x-extg-mime-type": "audio/ogg",
        "x-extg-size-bytes": "5",
    }

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False

    async def aiter_bytes(self):
        yield b"voice"

    async def aread(self):
        return b""


class _RecordingStreamClient:
    def __init__(self) -> None:
        self.stream_calls: list[dict[str, object]] = []

    @asynccontextmanager
    async def stream(self, method: str, path: str, **kwargs):
        self.stream_calls.append(
            {
                "method": method,
                "path": path,
                **kwargs,
            },
        )
        yield _StreamResponse()


def _build_settings() -> TelethonServiceAppSettings:
    return TelethonServiceAppSettings.model_construct(
        runtime_role="telethon_service",
        environment="test",
        timezone_name="Europe/Moscow",
        telegram=TelegramSettings(api_id=20225351, api_hash="api-hash"),
        telethon_service=TelethonServiceSettings(internal_token="internal-secret"),
        postgres=PostgresSettings(),
        security=SecuritySettings(),
    )


def _build_app() -> httpx.ASGITransport:
    container = TelethonServiceContainer()
    container.settings.override(providers.Object(_build_settings()))
    container.telethon_integration_service.override(
        providers.Object(StubTelethonIntegrationService()),
    )
    container.local_telegram_session_service.override(
        providers.Object(StubTelegramSessionService()),
    )
    app = create_telethon_service_app(container)
    return httpx.ASGITransport(app=app)


@pytest.mark.asyncio
async def test_remote_telethon_service_client_round_trips_session_and_attachment(tmp_path: Path) -> None:
    http_client = httpx.AsyncClient(
        transport=_build_app(),
        base_url="http://telethon-service.test",
    )
    client = RemoteTelethonServiceClient(
        base_url="http://telethon-service.test",
        internal_token="internal-secret",
        default_attachment_dir=str(tmp_path),
        http_client=http_client,
    )

    try:
        session_result = await client.start_connection(
            operator_huid="operator-1",
            phone_number="+79990001122",
        )
        dialogs = await client.list_available_dialogs(operator_huid="operator-1")
        downloaded = await client.download_attachment(
            operator_huid="operator-1",
            attachment=CanonicalAttachment(
                source_file_id="file-1",
                filename="voice.ogg",
                mime_type="audio/ogg",
                size_bytes=5,
                media_kind="voice",
            ),
        )
    finally:
        await http_client.aclose()

    assert isinstance(session_result, TelegramConnectionChallengeResult)
    assert session_result.challenge_id == "challenge-remote"
    assert dialogs[0].dialog_id == "-100500"
    assert downloaded.attachment.filename == "voice.ogg"
    assert downloaded.attachment.media_kind == "voice"
    assert downloaded.attachment.sha256 == "sha-1"
    assert downloaded.local_path is not None
    assert Path(downloaded.local_path).exists()
    assert Path(downloaded.local_path).parent == tmp_path
    assert Path(downloaded.local_path).name != "voice.ogg"
    assert Path(downloaded.local_path).suffix == ".ogg"
    assert Path(downloaded.local_path).read_bytes() == b"voice"


@pytest.mark.asyncio
async def test_remote_telethon_service_client_preserves_missing_filename_for_downstream_fallbacks(
    tmp_path: Path,
) -> None:
    http_client = httpx.AsyncClient(
        transport=_build_app(),
        base_url="http://telethon-service.test",
    )
    client = RemoteTelethonServiceClient(
        base_url="http://telethon-service.test",
        internal_token="internal-secret",
        default_attachment_dir=str(tmp_path),
        http_client=http_client,
    )

    try:
        downloaded = await client.download_attachment(
            operator_huid="operator-1",
            attachment=CanonicalAttachment(
                source_file_id="file-photo-1",
                filename=None,
                mime_type="image/jpeg",
                size_bytes=5,
                media_kind="photo",
            ),
        )
    finally:
        await http_client.aclose()

    assert downloaded.attachment.filename is None
    assert downloaded.local_path is not None
    assert Path(downloaded.local_path).exists()
    assert Path(downloaded.local_path).name != "attachment.bin"
    assert Path(downloaded.local_path).read_bytes() == b"voice"


@pytest.mark.asyncio
async def test_remote_telethon_service_client_decodes_urlencoded_filename_header(
    tmp_path: Path,
) -> None:
    http_client = httpx.AsyncClient(
        transport=_build_app(),
        base_url="http://telethon-service.test",
    )
    client = RemoteTelethonServiceClient(
        base_url="http://telethon-service.test",
        internal_token="internal-secret",
        default_attachment_dir=str(tmp_path),
        http_client=http_client,
    )

    try:
        downloaded = await client.download_attachment(
            operator_huid="operator-1",
            attachment=CanonicalAttachment(
                source_file_id="file-photo-ru",
                filename="фото.jpg",
                mime_type="image/jpeg",
                size_bytes=5,
                media_kind="photo",
            ),
        )
    finally:
        await http_client.aclose()

    assert downloaded.attachment.filename == "фото.jpg"
    assert downloaded.local_path is not None
    assert Path(downloaded.local_path).exists()
    assert Path(downloaded.local_path).name != "фото.jpg"
    assert Path(downloaded.local_path).suffix == ".jpg"


@pytest.mark.asyncio
async def test_remote_telethon_service_client_streams_attachment_response_to_local_file(
    tmp_path: Path,
) -> None:
    http_client = httpx.AsyncClient(
        transport=_build_app(),
        base_url="http://telethon-service.test",
    )
    client = RemoteTelethonServiceClient(
        base_url="http://telethon-service.test",
        internal_token="internal-secret",
        default_attachment_dir=str(tmp_path),
        http_client=http_client,
    )

    try:
        downloaded = await client.download_attachment(
            operator_huid="operator-1",
            attachment=CanonicalAttachment(
                source_file_id="file-stream",
                filename="voice.ogg",
                mime_type="audio/ogg",
                size_bytes=5,
                media_kind="voice",
                download_url="fake://stream-local",
            ),
        )
    finally:
        await http_client.aclose()

    assert downloaded.local_path is not None
    assert Path(downloaded.local_path).exists()
    assert Path(downloaded.local_path).read_bytes() == b"voice"


@pytest.mark.asyncio
async def test_remote_telethon_service_client_maps_service_http_errors() -> None:
    http_client = httpx.AsyncClient(
        transport=_build_app(),
        base_url="http://telethon-service.test",
    )
    client = RemoteTelethonServiceClient(
        base_url="http://telethon-service.test",
        internal_token="internal-secret",
        http_client=http_client,
    )

    try:
        with pytest.raises(RecoverableItemError, match="temporary telegram outage"):
            await client.list_available_dialogs(
                operator_huid="operator-1",
                query="fail",
            )
    finally:
        await http_client.aclose()


@pytest.mark.asyncio
async def test_remote_telethon_service_client_uses_attachment_specific_timeout(
    tmp_path: Path,
) -> None:
    stream_client = _RecordingStreamClient()
    client = RemoteTelethonServiceClient(
        base_url="http://telethon-service.test",
        request_timeout_seconds=15.0,
        attachment_request_timeout_seconds=180.0,
        internal_token="internal-secret",
        default_attachment_dir=str(tmp_path),
        http_client=stream_client,
    )

    downloaded = await client.download_attachment(
        operator_huid="operator-1",
        attachment=CanonicalAttachment(
            source_file_id="file-1",
            filename="voice.ogg",
            mime_type="audio/ogg",
            size_bytes=5,
            media_kind="voice",
        ),
    )

    stream_call = stream_client.stream_calls[0]
    timeout = stream_call["timeout"]

    assert downloaded.local_path is not None
    assert isinstance(timeout, httpx.Timeout)
    assert timeout.connect == 15.0
    assert timeout.read == 180.0
