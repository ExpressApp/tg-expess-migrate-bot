from __future__ import annotations

from dependency_injector import containers, providers
import structlog

from extg_telethon_service.application.telethon_integration_service import (
    TelethonIntegrationService,
)
from extg_telethon_service.application.telegram_login_challenge_store import (
    InMemoryTelegramLoginChallengeStore,
)
from extg_telethon_service.application.telegram_session_context import (
    TelegramSessionContext,
)
from extg_telethon_service.application.telegram_session_cipher import (
    TelegramSessionCipher,
)
from extg_telethon_service.application.telegram_session_service import TelegramSessionService
from extg_telethon_service.bootstrap.config import load_settings
from extg_telethon_service.infrastructure.persistence.in_memory import (
    InMemoryAuditRepository,
    InMemoryOperatorTelegramSessionRepository,
)
from extg_telethon_service.infrastructure.persistence.repositories import (
    PostgresAuditRepository,
    PostgresOperatorTelegramSessionRepository,
)
from extg_telethon_service.infrastructure.telegram.session_aware_gateway import (
    SessionAwareTelegramGateway,
)
from extg_shared.persistence.db import Database
from extg_shared.utils.temp_files import AttachmentTempFileJanitor


class TelethonServiceContainer(containers.DeclarativeContainer):
    settings = providers.Singleton(load_settings)
    logger = providers.Singleton(structlog.get_logger, "extg_telethon_service")
    telegram_session_context = providers.Singleton(TelegramSessionContext)
    persistence_backend = providers.Callable(
        lambda app_settings: app_settings.resolve_persistence_backend(),
        settings,
    )
    database = providers.Singleton(
        Database,
        dsn=providers.Callable(
            lambda app_settings: app_settings.postgres.dsn or "",
            settings,
        ),
        echo=providers.Callable(
            lambda app_settings: app_settings.postgres.echo,
            settings,
        ),
        pool_size=providers.Callable(
            lambda app_settings: app_settings.postgres.pool_size,
            settings,
        ),
        max_overflow=providers.Callable(
            lambda app_settings: app_settings.postgres.max_overflow,
            settings,
        ),
    )
    session_factory = providers.Callable(lambda database: database.session_factory, database)
    audit_repository = providers.Selector(
        persistence_backend,
        postgres=providers.Singleton(
            PostgresAuditRepository,
            session_factory=session_factory,
            payload_storage_mode=providers.Callable(
                lambda app_settings: app_settings.postgres.payload_storage_mode,
                settings,
            ),
        ),
        in_memory=providers.Singleton(InMemoryAuditRepository),
    )
    operator_telegram_session_repository = providers.Selector(
        persistence_backend,
        postgres=providers.Singleton(
            PostgresOperatorTelegramSessionRepository,
            session_factory=session_factory,
        ),
        in_memory=providers.Singleton(InMemoryOperatorTelegramSessionRepository),
    )
    local_telegram_gateway = providers.Singleton(
        SessionAwareTelegramGateway,
        session_context=telegram_session_context,
        api_id=providers.Callable(lambda app_settings: app_settings.telegram.api_id, settings),
        api_hash=providers.Callable(
            lambda app_settings: app_settings.telegram.api_hash,
            settings,
        ),
        request_timeout_seconds=providers.Callable(
            lambda app_settings: app_settings.telegram.request_timeout_seconds,
            settings,
        ),
        connection_retries=providers.Callable(
            lambda app_settings: app_settings.telegram.connection_retries,
            settings,
        ),
        default_attachment_dir=providers.Callable(
            lambda app_settings: app_settings.telegram.default_attachment_dir,
            settings,
        ),
    )
    attachment_temp_file_janitor = providers.Singleton(
        AttachmentTempFileJanitor,
        directory=providers.Callable(
            lambda app_settings: app_settings.telegram.default_attachment_dir,
            settings,
        ),
        enabled=providers.Callable(
            lambda app_settings: app_settings.telegram.attachment_temp_file_cleanup_enabled,
            settings,
        ),
        ttl_seconds=providers.Callable(
            lambda app_settings: app_settings.telegram.attachment_temp_file_ttl_seconds,
            settings,
        ),
        cleanup_interval_seconds=providers.Callable(
            lambda app_settings: app_settings.telegram.attachment_temp_file_cleanup_interval_seconds,
            settings,
        ),
        logger=logger,
    )
    telegram_session_cipher = providers.Singleton(
        TelegramSessionCipher,
        master_key=providers.Callable(
            lambda app_settings: (
                app_settings.security.telegram_session_master_key
                or "extg-local-dev-telegram-session-master-key"
            ),
            settings,
        ),
        key_version=providers.Callable(
            lambda app_settings: app_settings.security.telegram_session_key_version,
            settings,
        ),
    )
    telegram_login_challenge_store = providers.Singleton(
        InMemoryTelegramLoginChallengeStore,
    )
    local_telegram_session_service = providers.Singleton(
        TelegramSessionService,
        settings=settings,
        operator_session_repository=operator_telegram_session_repository,
        audit_repository=audit_repository,
        session_cipher=telegram_session_cipher,
        telegram_gateway=local_telegram_gateway,
        challenge_store=telegram_login_challenge_store,
    )
    telethon_integration_service = providers.Factory(
        TelethonIntegrationService,
        telegram_gateway=local_telegram_gateway,
        telegram_session_service=local_telegram_session_service,
        telegram_session_context=telegram_session_context,
    )


def create_container() -> TelethonServiceContainer:
    return TelethonServiceContainer()


__all__ = ["TelethonServiceContainer", "create_container"]
