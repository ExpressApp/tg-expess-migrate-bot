from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import pytest

from extg_bot_ui.bootstrap.config import BotUiSettings
from extg_telethon_service.bootstrap.config import TelethonServiceAppSettings
from extg_shared.config.common import (
    BackfillSettings,
    BotSettings,
    ExpressSettings,
    PostgresSettings,
    RetrySettings,
    SecuritySettings,
    TelegramSettings,
)
from extg_shared.contracts.errors import (
    ConfigurationError,
    TwoFactorPasswordRequiredError,
)
from extg_shared.contracts.models import AuditEvent, OperatorTelegramSessionRecord
from extg_telethon_service.application.telegram_session_cipher import TelegramSessionCipher
from extg_telethon_service.application.telegram_session_service import (
    TelegramConnectionChallengeResult,
    TelegramPasswordChallengeResult,
    TelegramSessionService,
)
from extg_telethon_service.infrastructure.persistence.in_memory import (
    InMemoryOperatorTelegramSessionRepository,
)
from extg_telethon_service.infrastructure.telegram.ops import (
    TelegramLoginCodeRequestResult,
    TelegramSessionBootstrapResult,
)


class StubAuditRepository:
    def __init__(self) -> None:
        self.events: list[AuditEvent] = []

    async def add(self, event: AuditEvent) -> None:
        self.events.append(event)


class StubTelegramGateway:
    def __init__(self) -> None:
        self.closed_sessions: list[str] = []

    async def close_session(self, session_string: str) -> None:
        self.closed_sessions.append(session_string)


def build_settings(_tmp_path: Path, *, operator_huids: list[str] | None = None) -> BotUiSettings:
    return BotUiSettings.model_construct(
        environment="test",
        timezone_name="Europe/Moscow",
        retry=RetrySettings(),
        backfill=BackfillSettings(batch_size=20),
        telegram=TelegramSettings(
            api_id=20225351,
            api_hash="api-hash",
        ),
        express=ExpressSettings(),
        postgres=PostgresSettings(),
        security=SecuritySettings(
            telegram_session_master_key="telegram-master-key",
        ),
        bot=BotSettings(
            migration_id="migration-bot",
            operator_huids=operator_huids or ["operator-1"],
        ),
    )


def build_settings_without_allowlist(_tmp_path: Path) -> BotUiSettings:
    return BotUiSettings.model_construct(
        environment="test",
        timezone_name="Europe/Moscow",
        retry=RetrySettings(),
        backfill=BackfillSettings(batch_size=20),
        telegram=TelegramSettings(
            api_id=20225351,
            api_hash="api-hash",
        ),
        express=ExpressSettings(),
        postgres=PostgresSettings(),
        security=SecuritySettings(
            telegram_session_master_key="telegram-master-key",
        ),
        bot=BotSettings(
            migration_id="migration-bot",
            operator_huids=[],
        ),
    )


@pytest.mark.asyncio
async def test_start_connection_requests_code_and_returns_challenge(tmp_path: Path):
    received = {}
    gateway = StubTelegramGateway()

    async def fake_request_login_code(**kwargs):
        received.update(kwargs)
        return TelegramLoginCodeRequestResult(
            phone_number=kwargs["phone_number"],
            session_string="pending-session",
            code_sent=True,
            phone_code_hash="hash-123",
        )

    service = TelegramSessionService(
        settings=build_settings(tmp_path),
        operator_session_repository=InMemoryOperatorTelegramSessionRepository(),
        audit_repository=StubAuditRepository(),
        session_cipher=TelegramSessionCipher(master_key="telegram-master-key"),
        telegram_gateway=gateway,
        request_login_code=fake_request_login_code,
    )

    result = await service.start_connection(
        operator_huid="operator-1",
        phone_number="+79990001122",
    )

    assert isinstance(result, TelegramConnectionChallengeResult)
    assert result.challenge_id
    assert result.phone_number == "+7***22"
    assert received["session_string"] is None
    assert gateway.closed_sessions == []


@pytest.mark.asyncio
async def test_start_connection_converts_low_level_type_error_to_configuration_error(tmp_path: Path):
    async def broken_request_login_code(**_kwargs):
        raise TypeError("bytes or str expected, not <class 'NoneType'>")

    service = TelegramSessionService(
        settings=build_settings(tmp_path),
        operator_session_repository=InMemoryOperatorTelegramSessionRepository(),
        audit_repository=StubAuditRepository(),
        session_cipher=TelegramSessionCipher(master_key="telegram-master-key"),
        telegram_gateway=StubTelegramGateway(),
        request_login_code=broken_request_login_code,
    )

    with pytest.raises(ConfigurationError, match="format \\+79990001122"):
        await service.start_connection(
            operator_huid="operator-1",
            phone_number="+79990001122",
        )


@pytest.mark.asyncio
async def test_complete_code_returns_password_challenge_when_2fa_is_required(tmp_path: Path):
    async def fake_request_login_code(**kwargs):
        return TelegramLoginCodeRequestResult(
            phone_number=kwargs["phone_number"],
            session_string="pending-session",
            code_sent=True,
            phone_code_hash="hash-123",
        )

    async def fake_complete_login_code(**_kwargs):
        raise TwoFactorPasswordRequiredError("2fa required")

    service = TelegramSessionService(
        settings=build_settings(tmp_path),
        operator_session_repository=InMemoryOperatorTelegramSessionRepository(),
        audit_repository=StubAuditRepository(),
        session_cipher=TelegramSessionCipher(master_key="telegram-master-key"),
        telegram_gateway=StubTelegramGateway(),
        request_login_code=fake_request_login_code,
        complete_login_code=fake_complete_login_code,
    )

    challenge = await service.start_connection(
        operator_huid="operator-1",
        phone_number="+79990001122",
    )
    assert isinstance(challenge, TelegramConnectionChallengeResult)

    result = await service.complete_code(
        operator_huid="operator-1",
        challenge_id=challenge.challenge_id,
        code="12345",
    )

    assert isinstance(result, TelegramPasswordChallengeResult)
    assert result.phone_number == "+7***22"
    assert result.challenge_id == challenge.challenge_id


@pytest.mark.asyncio
async def test_complete_password_persists_encrypted_session_and_disconnects_cleanly(tmp_path: Path):
    audit_repository = StubAuditRepository()
    gateway = StubTelegramGateway()

    async def fake_request_login_code(**kwargs):
        return TelegramLoginCodeRequestResult(
            phone_number=kwargs["phone_number"],
            session_string="pending-session",
            code_sent=True,
            phone_code_hash="hash-123",
        )

    async def fake_complete_login_code(**_kwargs):
        raise TwoFactorPasswordRequiredError("2fa required")

    async def fake_complete_password(**kwargs):
        return TelegramSessionBootstrapResult(
            phone_number=kwargs["phone_number"],
            session_string="final-session",
            already_authorized=False,
            user_id="42",
            username="telegram-user",
            display_name="Telegram User",
        )

    repository = InMemoryOperatorTelegramSessionRepository()
    service = TelegramSessionService(
        settings=build_settings(tmp_path),
        operator_session_repository=repository,
        audit_repository=audit_repository,
        session_cipher=TelegramSessionCipher(master_key="telegram-master-key"),
        telegram_gateway=gateway,
        request_login_code=fake_request_login_code,
        complete_login_code=fake_complete_login_code,
        complete_login_password=fake_complete_password,
    )

    challenge = await service.start_connection(
        operator_huid="operator-1",
        phone_number="+79990001122",
    )
    assert isinstance(challenge, TelegramConnectionChallengeResult)

    password_challenge = await service.complete_code(
        operator_huid="operator-1",
        challenge_id=challenge.challenge_id,
        code="12345",
    )
    assert isinstance(password_challenge, TelegramPasswordChallengeResult)

    status = await service.complete_password(
        operator_huid="operator-1",
        challenge_id=password_challenge.challenge_id,
        password="very-secret",
    )

    assert status.connected is True
    assert status.telegram_user_id == "42"
    assert status.session_bound is True
    assert status.phone_number == "+7***22"
    persisted = await repository.get_by_operator("operator-1")
    assert persisted is not None
    assert persisted.session_path != "final-session"

    resolved = await service.resolve_session_string(operator_huid="operator-1")
    assert resolved == "final-session"

    disconnect_result = await service.disconnect(operator_huid="operator-1")

    assert disconnect_result.disconnected is True
    assert gateway.closed_sessions == ["final-session"]
    assert await repository.get_by_operator("operator-1") is None
    assert [event.event_type for event in audit_repository.events] == [
        "telegram_session_code_requested",
        "telegram_session_password_required",
        "telegram_session_connected",
        "telegram_session_disconnected",
    ]


@pytest.mark.asyncio
async def test_resolve_session_string_requires_bound_operator_session(tmp_path: Path):
    service = TelegramSessionService(
        settings=build_settings(tmp_path),
        operator_session_repository=InMemoryOperatorTelegramSessionRepository(),
        audit_repository=StubAuditRepository(),
        session_cipher=TelegramSessionCipher(master_key="telegram-master-key"),
        telegram_gateway=StubTelegramGateway(),
    )

    with pytest.raises(ConfigurationError, match="/connect"):
        await service.resolve_session_string(operator_huid="operator-1")


@pytest.mark.asyncio
async def test_start_connection_allows_any_user_when_allowlist_is_empty(tmp_path: Path):
    async def fake_request_login_code(**kwargs):
        return TelegramLoginCodeRequestResult(
            phone_number=kwargs["phone_number"],
            session_string="pending-session",
            code_sent=True,
            phone_code_hash="hash-123",
        )

    service = TelegramSessionService(
        settings=build_settings_without_allowlist(tmp_path),
        operator_session_repository=InMemoryOperatorTelegramSessionRepository(),
        audit_repository=StubAuditRepository(),
        session_cipher=TelegramSessionCipher(master_key="telegram-master-key"),
        telegram_gateway=StubTelegramGateway(),
        request_login_code=fake_request_login_code,
    )

    result = await service.start_connection(
        operator_huid="operator-2",
        phone_number="+79990001122",
    )

    assert isinstance(result, TelegramConnectionChallengeResult)


@pytest.mark.asyncio
async def test_status_works_without_bot_settings_on_telethon_service_runtime() -> None:
    service = TelegramSessionService(
        settings=TelethonServiceAppSettings.model_construct(
            runtime_role="telethon_service",
            environment="test",
            timezone_name="Europe/Moscow",
            telegram=TelegramSettings(
                api_id=20225351,
                api_hash="api-hash",
            ),
            postgres=PostgresSettings(),
            security=SecuritySettings(
                telegram_session_master_key="telegram-master-key",
            ),
        ),
        operator_session_repository=InMemoryOperatorTelegramSessionRepository(),
        audit_repository=StubAuditRepository(),
        session_cipher=TelegramSessionCipher(master_key="telegram-master-key"),
        telegram_gateway=StubTelegramGateway(),
    )

    result = await service.status(operator_huid="operator-1")

    assert result.connected is False
    assert result.source == "not_connected"


@pytest.mark.asyncio
async def test_status_only_exposes_operator_own_session(tmp_path: Path) -> None:
    repository = InMemoryOperatorTelegramSessionRepository()
    service = TelegramSessionService(
        settings=build_settings(tmp_path, operator_huids=["operator-1", "operator-2"]),
        operator_session_repository=repository,
        audit_repository=StubAuditRepository(),
        session_cipher=TelegramSessionCipher(master_key="telegram-master-key"),
        telegram_gateway=StubTelegramGateway(),
    )
    await repository.save(
        OperatorTelegramSessionRecord(
            operator_huid="operator-1",
            phone_number="+7***22",
            session_path=TelegramSessionCipher(master_key="telegram-master-key").encrypt(
                "session-operator-1",
            ),
            telegram_user_id="42",
            telegram_username="operator-one",
            telegram_display_name="Operator One",
            created_at=datetime(2026, 3, 27, 12, 0, tzinfo=UTC),
            updated_at=datetime(2026, 3, 27, 12, 0, tzinfo=UTC),
            last_used_at=datetime(2026, 3, 27, 12, 0, tzinfo=UTC),
        ),
    )

    result = await service.status(operator_huid="operator-2")

    assert result.connected is False
    assert result.source == "not_connected"
