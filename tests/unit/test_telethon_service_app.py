from __future__ import annotations

import tempfile
from dependency_injector import providers
from fastapi.testclient import TestClient
import pytest

from extg_telethon_service.application.telegram_session_service import (
    TelegramConnectionChallengeResult,
)
from extg_shared.config.common import (
    PostgresSettings,
    SecuritySettings,
    TelegramSettings,
    TelethonServiceSettings,
)
from extg_shared.contracts.errors import ConfigurationError, RecoverableItemError
from extg_shared.contracts.models import (
    CanonicalAttachment,
    DownloadedAttachment,
    SourceDialog,
)
from extg_telethon_service.bootstrap.config import TelethonServiceAppSettings
from extg_telethon_service.bootstrap.container import TelethonServiceContainer
from extg_telethon_service.presentation.telethon_service.app import create_telethon_service_app


class StubTelethonIntegrationService:
    def __init__(self) -> None:
        self.download_requests: list[str] = []

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
                dialog_id="-100123",
                chat_type="supergroup",
                title="Engineering",
                message_count=10,
                media_count=2,
                approximate_bytes=1024,
            ),
        ]

    async def download_attachment(
        self,
        *,
        operator_huid: str,
        attachment: CanonicalAttachment,
        source_backend: str = "telethon_user_session",
    ) -> DownloadedAttachment:
        self.download_requests.append(operator_huid)
        if attachment.download_url == "fake://stream-local":
            with tempfile.NamedTemporaryFile(delete=False) as handle:
                handle.write(b"jpg")
                local_path = handle.name
            return DownloadedAttachment(
                attachment=CanonicalAttachment(
                    source_file_id=attachment.source_file_id,
                    filename=attachment.filename,
                    mime_type="image/jpeg",
                    size_bytes=3,
                    media_kind="photo",
                    sha256="abc123",
                ),
                local_path=local_path,
                size_bytes=3,
            )
        return DownloadedAttachment(
            attachment=CanonicalAttachment(
                source_file_id=attachment.source_file_id,
                filename=attachment.filename,
                mime_type="image/jpeg",
                size_bytes=3,
                media_kind="photo",
                sha256="abc123",
            ),
            content=b"jpg",
            size_bytes=3,
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
            challenge_id="challenge-1",
        )


def _build_settings(
    *,
    internal_token: str | None = None,
    api_id: int | None = 20225351,
) -> TelethonServiceAppSettings:
    return TelethonServiceAppSettings.model_construct(
        runtime_role="telethon_service",
        environment="test",
        timezone_name="Europe/Moscow",
        telegram=TelegramSettings(api_id=api_id, api_hash="api-hash"),
        telethon_service=TelethonServiceSettings(internal_token=internal_token),
        postgres=PostgresSettings(),
        security=SecuritySettings(),
    )


def _build_container(*, internal_token: str | None = None) -> TelethonServiceContainer:
    container = TelethonServiceContainer()
    container.settings.override(providers.Object(_build_settings(internal_token=internal_token)))
    container.telethon_integration_service.override(
        providers.Object(StubTelethonIntegrationService()),
    )
    container.local_telegram_session_service.override(
        providers.Object(StubTelegramSessionService()),
    )
    return container


def test_create_telethon_service_app_requires_telegram_credentials() -> None:
    container = TelethonServiceContainer()
    container.settings.override(providers.Object(_build_settings(api_id=None)))

    with pytest.raises(ConfigurationError, match="EXTG_TELEGRAM__API_ID"):
        create_telethon_service_app(container)


def test_create_telethon_service_app_streams_attachment_from_local_file() -> None:
    app = create_telethon_service_app(_build_container(internal_token="internal-secret"))

    with TestClient(app) as client:
        response = client.post(
            "/internal/telegram/attachments/download",
            headers={"X-Internal-Token": "internal-secret"},
            json={
                "operator_huid": "operator-1",
                "attachment": {
                    "source_file_id": "file-1",
                    "filename": "photo.jpg",
                    "mime_type": "image/jpeg",
                    "size_bytes": 3,
                    "media_kind": "photo",
                    "download_url": "fake://stream-local",
                },
            },
        )

    assert response.status_code == 200
    assert response.content == b"jpg"
    assert response.headers["x-extg-size-bytes"] == "3"


def test_create_telethon_service_app_enforces_internal_token() -> None:
    app = create_telethon_service_app(_build_container(internal_token="internal-secret"))

    with TestClient(app) as client:
        health = client.get("/health")
        unauthorized = client.post(
            "/internal/telegram/dialogs/list-available",
            json={"operator_huid": "operator-1", "limit": 5},
        )
        authorized = client.post(
            "/internal/telegram/dialogs/list-available",
            headers={"X-Internal-Token": "internal-secret"},
            json={"operator_huid": "operator-1", "limit": 5},
        )

    assert health.status_code == 200
    assert health.json() == {"status": "ok"}
    assert unauthorized.status_code == 401
    assert authorized.status_code == 200
    assert authorized.json()[0]["dialog_id"] == "-100123"


def test_create_telethon_service_app_serializes_session_and_attachment_routes() -> None:
    app = create_telethon_service_app(_build_container())

    with TestClient(app) as client:
        health_response = client.get("/health")
        session_response = client.post(
            "/internal/telegram/session/start",
            json={
                "operator_huid": "operator-1",
                "phone_number": "+79990001122",
            },
        )
        attachment_response = client.post(
            "/internal/telegram/attachments/download",
            json={
                "operator_huid": "operator-1",
                "attachment": {
                    "source_file_id": "file-1",
                    "filename": "photo.jpg",
                    "mime_type": "image/jpeg",
                    "size_bytes": 3,
                    "media_kind": "photo",
                    "duration_seconds": None,
                    "download_url": "telethon://chat/1",
                    "temp_local_path": None,
                    "sha256": None,
                },
            },
        )

    assert health_response.status_code == 200
    assert health_response.json() == {"status": "ok"}
    assert session_response.status_code == 200
    assert session_response.json()["kind"] == "challenge"
    assert attachment_response.status_code == 200
    assert attachment_response.content == b"jpg"
    assert attachment_response.headers["x-extg-filename"] == "photo.jpg"
    assert attachment_response.headers["x-extg-media-kind"] == "photo"
    assert attachment_response.headers["x-extg-mime-type"] == "image/jpeg"
    assert attachment_response.headers["x-extg-sha256"] == "abc123"
    assert attachment_response.headers["x-extg-size-bytes"] == "3"


def test_create_telethon_service_app_omits_filename_header_when_attachment_has_no_name() -> None:
    app = create_telethon_service_app(_build_container())

    with TestClient(app) as client:
        attachment_response = client.post(
            "/internal/telegram/attachments/download",
            json={
                "operator_huid": "operator-1",
                "attachment": {
                    "source_file_id": "file-2",
                    "filename": None,
                    "mime_type": "image/jpeg",
                    "size_bytes": 3,
                    "media_kind": "photo",
                    "duration_seconds": None,
                    "download_url": "telethon://chat/2",
                    "temp_local_path": None,
                    "sha256": None,
                },
            },
        )

    assert attachment_response.status_code == 200
    assert "x-extg-filename" not in attachment_response.headers


def test_create_telethon_service_app_urlencodes_non_latin_attachment_filename_header() -> None:
    app = create_telethon_service_app(_build_container())

    with TestClient(app) as client:
        attachment_response = client.post(
            "/internal/telegram/attachments/download",
            json={
                "operator_huid": "operator-1",
                "attachment": {
                    "source_file_id": "file-3",
                    "filename": "фото.jpg",
                    "mime_type": "image/jpeg",
                    "size_bytes": 3,
                    "media_kind": "photo",
                    "duration_seconds": None,
                    "download_url": "telethon://chat/3",
                    "temp_local_path": None,
                    "sha256": None,
                },
            },
        )

    assert attachment_response.status_code == 200
    assert "x-extg-filename" not in attachment_response.headers
    assert attachment_response.headers["x-extg-filename-urlencoded"] == "%D1%84%D0%BE%D1%82%D0%BE.jpg"


def test_create_telethon_service_app_maps_recoverable_errors_to_http_503() -> None:
    app = create_telethon_service_app(_build_container())

    with TestClient(app) as client:
        response = client.post(
            "/internal/telegram/dialogs/list-available",
            json={"operator_huid": "operator-1", "query": "fail"},
        )

    assert response.status_code == 503
    assert response.json()["detail"]["code"] == "recoverable_error"
    assert response.json()["detail"]["message"] == "temporary telegram outage"
    assert response.json()["detail"]["retryable"] is True
