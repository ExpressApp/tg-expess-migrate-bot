import pytest
from dependency_injector import providers

from extg_bot_ui.bootstrap.config import BotUiSettings
from extg_bot_ui.bootstrap.container import (
    BotUiContainer,
    create_container as create_bot_container,
)
from extg_migration_runtime.bootstrap.config import MigrationRuntimeSettings
from extg_migration_runtime.bootstrap.container import (
    MigrationRuntimeContainer,
    create_container as create_worker_container,
)
from extg_migration_runtime.infrastructure.events.confluent_kafka_event_publisher import (
    ConfluentKafkaIntegrationEventPublisher,
)
from extg_migration_runtime.infrastructure.express.router import ExpressGatewayRouter
from extg_migration_runtime.infrastructure.telegram.multi_source_gateway import (
    MultiSourceTelegramGateway,
)
from extg_shared.config.common import (
    BotSettings,
    ExpressBotAccountSettings,
    ExpressSettings,
    KafkaSettings,
    PostgresSettings,
    SecuritySettings,
    TelethonServiceSettings,
)
from extg_telethon_service.bootstrap.config import TelethonServiceAppSettings
from extg_telethon_service.bootstrap.container import (
    create_container as create_telethon_service_container,
)
from extg_telethon_service.infrastructure.telegram.remote_service_client import (
    RemoteTelegramGateway,
    RemoteTelegramSessionService,
)
from extg_shared.contracts.errors import ConfigurationError
from extg_bot_ui.infrastructure.persistence.fsm_state_repository import (
    PostgresFSMStateRepository,
)
from extg_migration_runtime.infrastructure.persistence.repositories import (
    PostgresAttachmentMappingRepository,
)
from extg_migration_runtime.infrastructure.persistence.in_memory import (
    InMemoryAttachmentMappingRepository,
)
from extg_bot_ui.infrastructure.persistence.in_memory_fsm_state import (
    InMemoryFSMStateRepository,
)


def test_bot_ui_settings_allow_in_memory_backend_in_local_environment():
    settings = BotUiSettings(
        environment="local",
        postgres=PostgresSettings(),
        bot=BotSettings(migration_id="migration-bot"),
    )

    assert settings.resolve_persistence_backend() == "in_memory"


def test_bot_ui_settings_allow_in_memory_backend_in_test_environment():
    settings = BotUiSettings(
        environment="test",
        postgres=PostgresSettings(),
        bot=BotSettings(migration_id="migration-bot"),
    )

    assert settings.resolve_persistence_backend() == "in_memory"


def test_bot_ui_settings_require_postgres_dsn_outside_local_and_test():
    with pytest.raises(
        ConfigurationError,
        match="EXTG_POSTGRES__DSN is required when EXTG_ENVIRONMENT is not local/test",
    ):
        BotUiSettings(
            environment="production",
            postgres=PostgresSettings(),
            security=SecuritySettings(
                message_hmac_secret="secret",
                telegram_session_master_key="telegram-secret",
            ),
            bot=BotSettings(migration_id="migration-bot"),
        )


def test_bot_ui_settings_require_message_hmac_secret_outside_local_and_test(
    monkeypatch: pytest.MonkeyPatch,
):
    monkeypatch.delenv("EXTG_SECURITY__MESSAGE_HMAC_SECRET", raising=False)
    with pytest.raises(
        ConfigurationError,
        match="EXTG_SECURITY__MESSAGE_HMAC_SECRET is required when EXTG_ENVIRONMENT is not local/test",
    ):
        BotUiSettings(
            environment="production",
            postgres=PostgresSettings(dsn="postgresql+asyncpg://user:pass@localhost:5432/extg"),
            bot=BotSettings(migration_id="migration-bot"),
        )


def test_bot_ui_settings_require_telegram_session_master_key_outside_local_and_test():
    with pytest.raises(
        ConfigurationError,
        match="EXTG_SECURITY__TELEGRAM_SESSION_MASTER_KEY is required when EXTG_ENVIRONMENT is not local/test",
    ):
        BotUiSettings(
            environment="production",
            postgres=PostgresSettings(dsn="postgresql+asyncpg://user:pass@localhost:5432/extg"),
            security=SecuritySettings(message_hmac_secret="secret"),
            bot=BotSettings(migration_id="migration-bot"),
        )


def test_bot_ui_settings_use_postgres_backend_when_dsn_is_configured():
    settings = BotUiSettings(
        environment="production",
        postgres=PostgresSettings(dsn="postgresql+asyncpg://user:pass@localhost:5432/extg"),
        security=SecuritySettings(
            message_hmac_secret="secret",
            telegram_session_master_key="telegram-secret",
        ),
        bot=BotSettings(migration_id="migration-bot"),
    )

    assert settings.resolve_persistence_backend() == "postgres"


def test_telethon_service_settings_allow_missing_migration_id() -> None:
    settings = TelethonServiceAppSettings(
        environment="test",
        postgres=PostgresSettings(),
    )

    assert settings.normalized_runtime_role == "telethon_service"


def test_migration_runtime_settings_require_bootstrap_servers_when_kafka_enabled():
    with pytest.raises(
        ConfigurationError,
        match="EXTG_KAFKA__BOOTSTRAP_SERVERS is required when EXTG_KAFKA__ENABLED=true",
    ):
        MigrationRuntimeSettings(
            environment="test",
            postgres=PostgresSettings(),
            kafka=KafkaSettings(enabled=True, bootstrap_servers=[]),
            bot=BotSettings(migration_id="migration-bot"),
        )


def test_bot_ui_settings_require_migration_id(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("EXTG_BOT__MIGRATION_ID", raising=False)
    with pytest.raises(
        ConfigurationError,
        match="EXTG_BOT__MIGRATION_ID is required for bot-ui runtime",
    ):
        BotUiSettings(
            environment="test",
            postgres=PostgresSettings(),
        )


def test_migration_runtime_settings_require_migration_id(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("EXTG_BOT__MIGRATION_ID", raising=False)
    with pytest.raises(
        ConfigurationError,
        match="EXTG_BOT__MIGRATION_ID is required for migration-runtime",
    ):
        MigrationRuntimeSettings(
            environment="test",
            postgres=PostgresSettings(),
        )


def test_bot_ui_container_uses_validated_in_memory_backend_from_settings():
    container = BotUiContainer()
    container.settings.override(
        providers.Object(
            BotUiSettings(
                environment="test",
                postgres=PostgresSettings(),
                bot=BotSettings(migration_id="migration-bot"),
            ),
        ),
    )

    assert container.persistence_backend() == "in_memory"


def test_bot_ui_container_uses_in_memory_attachment_repository_for_local_backend():
    container = BotUiContainer()
    container.settings.override(
        providers.Object(
            BotUiSettings(
                environment="test",
                postgres=PostgresSettings(),
                bot=BotSettings(migration_id="migration-bot"),
            ),
        ),
    )

    repository = container.attachment_mapping_repository()

    assert isinstance(repository, InMemoryAttachmentMappingRepository)


def test_bot_ui_container_builds_postgres_attachment_repository_when_backend_is_postgres():
    container = BotUiContainer()
    session_factory = object()
    container.settings.override(
        providers.Object(
            BotUiSettings(
                environment="production",
                postgres=PostgresSettings(
                    dsn="postgresql+asyncpg://user:pass@localhost:5432/extg",
                ),
                security=SecuritySettings(
                    message_hmac_secret="secret",
                    telegram_session_master_key="telegram-secret",
                ),
                bot=BotSettings(migration_id="migration-bot"),
            ),
        ),
    )
    container.persistence_backend.override(providers.Object("postgres"))
    container.session_factory.override(providers.Object(session_factory))

    repository = container.attachment_mapping_repository()

    assert isinstance(repository, PostgresAttachmentMappingRepository)
    assert repository._session_factory is session_factory


def test_bot_ui_container_raises_configuration_error_without_postgres_dsn_in_production(
    monkeypatch: pytest.MonkeyPatch,
):
    monkeypatch.setenv("EXTG_ENVIRONMENT", "production")
    monkeypatch.delenv("EXTG_POSTGRES__DSN", raising=False)
    monkeypatch.setenv("EXTG_SECURITY__MESSAGE_HMAC_SECRET", "secret")
    monkeypatch.setenv("EXTG_SECURITY__TELEGRAM_SESSION_MASTER_KEY", "telegram-secret")
    monkeypatch.setenv("EXTG_BOT__MIGRATION_ID", "migration-bot")

    container = BotUiContainer()

    with pytest.raises(
        ConfigurationError,
        match="EXTG_POSTGRES__DSN is required when EXTG_ENVIRONMENT is not local/test",
    ):
        container.persistence_backend()


def test_bot_ui_container_uses_operator_huids_as_default_participants_when_not_configured():
    operator_huid_1 = "00000000-0000-0000-0000-000000000101"
    operator_huid_2 = "00000000-0000-0000-0000-000000000102"
    container = BotUiContainer()
    container.settings.override(
        providers.Object(
            BotUiSettings(
                environment="test",
                postgres=PostgresSettings(),
                express=ExpressSettings(
                    bot_id="00000000-0000-0000-0000-000000000001",
                    cts_url="https://cts-main.example.test",
                    secret_key="primary-secret",
                    default_participant_huids=[],
                ),
                bot=BotSettings(
                    migration_id="migration-bot",
                    operator_huids=[operator_huid_1, operator_huid_2],
                ),
            ),
        ),
    )

    gateway = container.express_gateway()

    assert isinstance(gateway, ExpressGatewayRouter)
    primary_gateway = gateway._gateways[gateway.primary_cts_host]
    assert [str(value) for value in primary_gateway._default_participants] == [
        operator_huid_1,
        operator_huid_2,
    ]


def test_express_settings_build_account_registry_from_legacy_settings():
    settings = ExpressSettings(
        bot_id="00000000-0000-0000-0000-000000000001",
        cts_url="https://cts-main.example.test/",
        secret_key="secret",
    )

    account = settings.require_primary_account()

    assert account.bot_id == "00000000-0000-0000-0000-000000000001"
    assert account.cts_url == "https://cts-main.example.test"
    assert account.cts_host == "cts-main.example.test"
    assert account.role == "primary"
    assert account.effective_visible is True


def test_express_settings_support_primary_and_technical_accounts():
    settings = ExpressSettings(
        accounts=[
            ExpressBotAccountSettings(
                role="primary",
                bot_id="00000000-0000-0000-0000-000000000001",
                bot_huid="main-bot-huid",
                cts_url="https://cts-main.example.test",
                secret_key="primary-secret",
            ),
            ExpressBotAccountSettings(
                role="technical",
                bot_id="00000000-0000-0000-0000-000000000002",
                bot_huid="helper-bot-huid",
                cts_url="https://cts-helper.example.test/",
                secret_key="helper-secret",
                visible=False,
            ),
        ],
    )

    registry = settings.build_account_registry()

    assert registry.primary_account.bot_id == "00000000-0000-0000-0000-000000000001"
    assert registry.primary_account.configured_bot_huid == "main-bot-huid"
    assert registry.get_by_cts_host("cts-helper.example.test") is not None
    assert registry.get_by_cts_host("cts-helper.example.test").effective_visible is False
    assert registry.get_by_cts_host("cts-helper.example.test").configured_bot_huid == "helper-bot-huid"


def test_bot_ui_container_uses_primary_account_from_multi_account_settings():
    container = BotUiContainer()
    container.settings.override(
        providers.Object(
            BotUiSettings(
                environment="test",
                postgres=PostgresSettings(),
                express=ExpressSettings(
                    accounts=[
                        ExpressBotAccountSettings(
                            role="primary",
                            bot_id="00000000-0000-0000-0000-000000000011",
                            cts_url="https://cts-main.example.test",
                            secret_key="primary-secret",
                        ),
                        ExpressBotAccountSettings(
                            role="technical",
                            bot_id="00000000-0000-0000-0000-000000000012",
                            cts_url="https://cts-helper.example.test",
                            secret_key="helper-secret",
                            visible=False,
                        ),
                    ],
                ),
                bot=BotSettings(migration_id="migration-bot"),
            ),
        ),
    )

    gateway = container.express_gateway()

    assert isinstance(gateway, ExpressGatewayRouter)
    assert gateway.primary_cts_host == "cts-main.example.test"
    assert set(gateway.registered_cts_hosts) == {
        "cts-main.example.test",
        "cts-helper.example.test",
    }


def test_migration_runtime_container_builds_kafka_event_publisher_when_enabled():
    container = MigrationRuntimeContainer()
    container.settings.override(
        providers.Object(
            MigrationRuntimeSettings(
                environment="test",
                postgres=PostgresSettings(),
                kafka=KafkaSettings(
                    enabled=True,
                    bootstrap_servers=["kafka:9092"],
                ),
                bot=BotSettings(migration_id="migration-bot"),
            ),
        ),
    )

    publisher = container.integration_event_publisher()

    assert isinstance(publisher, ConfluentKafkaIntegrationEventPublisher)


@pytest.mark.asyncio
async def test_bot_ui_container_switches_to_remote_telegram_runtime_when_base_url_is_configured():
    container = BotUiContainer()
    container.settings.override(
        providers.Object(
            BotUiSettings(
                environment="test",
                postgres=PostgresSettings(),
                telethon_service=TelethonServiceSettings(
                    base_url="http://telethon-service.internal:8092",
                ),
                bot=BotSettings(migration_id="migration-bot"),
            ),
        ),
    )

    gateway = container.telegram_gateway()
    session_service = container.telegram_session_service()

    try:
        assert container.telegram_runtime_mode() == "remote"
        assert isinstance(gateway, MultiSourceTelegramGateway)
        assert isinstance(gateway._primary_gateway, RemoteTelegramGateway)
        assert isinstance(session_service, RemoteTelegramSessionService)
    finally:
        await gateway.close()


def test_create_telethon_service_container_defaults_runtime_role(
    monkeypatch: pytest.MonkeyPatch,
):
    monkeypatch.delenv("EXTG_RUNTIME_ROLE", raising=False)
    monkeypatch.setenv("EXTG_ENVIRONMENT", "test")
    monkeypatch.setenv("EXTG_TELEGRAM__API_ID", "20225351")
    monkeypatch.setenv("EXTG_TELEGRAM__API_HASH", "api-hash")

    container = create_telethon_service_container()

    assert hasattr(container, "telethon_integration_service")
    assert not hasattr(container, "express_gateway")
    assert container.settings().normalized_runtime_role == "telethon_service"


def test_create_bot_container_returns_service_specific_surface(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("EXTG_RUNTIME_ROLE", raising=False)
    monkeypatch.setenv("EXTG_ENVIRONMENT", "test")
    monkeypatch.setenv("EXTG_BOT__MIGRATION_ID", "migration-bot")

    container = create_bot_container()

    assert hasattr(container, "bot_control_service")
    assert hasattr(container, "fsm_state_repository")
    assert not hasattr(container, "migration_command_consumer")
    assert container.settings().normalized_runtime_role == "bot_ui"


def test_create_worker_container_returns_service_specific_surface(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("EXTG_RUNTIME_ROLE", raising=False)
    monkeypatch.setenv("EXTG_ENVIRONMENT", "test")
    monkeypatch.setenv("EXTG_BOT__MIGRATION_ID", "migration-bot")

    container = create_worker_container()

    assert hasattr(container, "migration_job_worker")
    assert hasattr(container, "migration_command_consumer")
    assert not hasattr(container, "fsm_state_repository")
    assert container.settings().normalized_runtime_role == "migration_runtime"


def test_bot_ui_container_uses_in_memory_fsm_repository_by_default():
    container = BotUiContainer()
    container.settings.override(
        providers.Object(
            BotUiSettings(
                environment="production",
                postgres=PostgresSettings(dsn="postgresql+asyncpg://user:pass@localhost:5432/extg"),
                security=SecuritySettings(
                    message_hmac_secret="secret",
                    telegram_session_master_key="telegram-secret",
                ),
                bot=BotSettings(migration_id="migration-bot"),
            ),
        ),
    )

    repository = container.fsm_state_repository()

    assert isinstance(repository, InMemoryFSMStateRepository)


def test_bot_ui_container_builds_postgres_fsm_repository_when_explicitly_enabled():
    container = BotUiContainer()
    session_factory = object()
    container.fsm_storage_backend.override(providers.Object("postgres"))
    container.session_factory.override(providers.Object(session_factory))

    repository = container.fsm_state_repository()

    assert isinstance(repository, PostgresFSMStateRepository)
    assert repository._session_factory is session_factory
