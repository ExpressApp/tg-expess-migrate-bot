from __future__ import annotations

import asyncio
from dependency_injector import containers, providers
import structlog

from extg_bot_ui.application.bot_control import MigrationBotControlService
from extg_migration_runtime.application.integration_outbox_publisher import (
    IntegrationOutboxPublisher,
)
from extg_migration_runtime.application.message_delivery import MessageDeliveryService
from extg_migration_runtime.application.migration_command_handler import (
    MigrationCommandHandler,
)
from extg_migration_runtime.application.migration_job_worker import (
    MigrationJobWorker,
)
from extg_migration_runtime.application.use_cases.backfill_chat import BackfillChatUseCase
from extg_migration_runtime.application.use_cases.delta_sync import DeltaSyncUseCase
from extg_migration_runtime.application.use_cases.finalize_cutover import (
    FinalizeCutoverUseCase,
)
from extg_migration_runtime.application.use_cases.inventory import InventoryUseCase
from extg_migration_runtime.application.use_cases.manage_migration_state import (
    ManageMigrationStateUseCase,
)
from extg_migration_runtime.application.use_cases.migration_state_control import (
    FreezeCutoverUseCase,
    PauseDeltaUseCase,
    ResumeDeltaUseCase,
)
from extg_migration_runtime.application.use_cases.reconcile_migration import (
    ReconcileMigrationUseCase,
)
from extg_migration_runtime.application.use_cases.replay_failed import ReplayFailedUseCase
from extg_migration_runtime.application.worker_runtime_metrics import (
    WorkerRuntimeMetricsReporter,
)
from extg_migration_runtime.bootstrap.config import load_settings
from extg_migration_runtime.infrastructure.events.confluent_kafka_command_consumer import (
    ConfluentKafkaMigrationCommandConsumer,
)
from extg_migration_runtime.infrastructure.events.confluent_kafka_event_publisher import (
    ConfluentKafkaIntegrationEventPublisher,
)
from extg_migration_runtime.infrastructure.events.logging_integration_event_publisher import (
    LoggingIntegrationEventPublisher,
)
from extg_migration_runtime.infrastructure.archive.telegram_export_archive_parser import (
    TelegramExportArchiveParser,
)
from extg_migration_runtime.infrastructure.archive.stage_store import (
    InMemoryTelegramExportArchiveStageStore,
    LocalTelegramExportArchiveStageStore,
)
from extg_migration_runtime.infrastructure.express.router import (
    ExpressFileStoreRouter,
    ExpressGatewayRouter,
)
from extg_migration_runtime.infrastructure.telegram.multi_source_gateway import (
    MultiSourceTelegramGateway,
    TelegramExportSnapshotGateway,
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
from extg_telethon_service.application.telegram_session_service import (
    TelegramSessionService,
)
from extg_telethon_service.infrastructure.persistence.in_memory import (
    InMemoryAuditRepository,
    InMemoryOperatorTelegramSessionRepository,
)
from extg_telethon_service.infrastructure.persistence.repositories import (
    PostgresAuditRepository,
    PostgresOperatorTelegramSessionRepository,
)
from extg_telethon_service.infrastructure.telegram.remote_service_client import (
    RemoteTelethonServiceClient,
    RemoteTelegramGateway,
    RemoteTelegramSessionService,
)
from extg_telethon_service.infrastructure.telegram.session_aware_gateway import (
    SessionAwareTelegramGateway,
)
from extg_migration_runtime.application.cts_resolution import CtsResolutionService
from extg_migration_runtime.application.identity import UsernameEmailIdentityDirectory
from extg_migration_runtime.application.message_checksum import MessageChecksumService
from extg_migration_runtime.application.normalizer import (
    DirectoryBackedIdentityResolver,
    TelegramMessageNormalizer,
)
from extg_migration_runtime.application.renderer import MessageRenderer
from extg_migration_runtime.application.target_chat_provisioning import (
    TargetChatProvisioningService,
)
from extg_migration_runtime.infrastructure.persistence.in_memory import (
    InMemoryAttachmentMappingRepository,
    InMemoryAttachmentStageRepository,
    InMemoryExpressBotHuidBindingRepository,
    InMemoryChatMigrationConfigRepository,
    InMemoryChatMappingRepository,
    InMemoryCheckpointRepository,
    InMemoryExpressUserCtsBindingRepository,
    InMemoryIdentityMappingRepository,
    InMemoryInboxRepository,
    InMemoryInventorySnapshotRepository,
    InMemoryMessageMappingRepository,
    InMemoryMigrationJobRepository,
    InMemoryOperatorMigrationDefaultsRepository,
    InMemoryMigrationStateRepository,
    InMemoryOutboxRepository,
    InMemoryServiceWatermarkRepository,
    InMemoryTelegramExportSnapshotRepository,
)
from extg_migration_runtime.infrastructure.persistence.repositories import (
    PostgresAttachmentMappingRepository,
    PostgresAttachmentStageRepository,
    PostgresExpressBotHuidBindingRepository,
    PostgresChatMigrationConfigRepository,
    PostgresChatMappingRepository,
    PostgresCheckpointRepository,
    PostgresExpressUserCtsBindingRepository,
    PostgresIdentityMappingRepository,
    PostgresInboxRepository,
    PostgresInventorySnapshotRepository,
    PostgresMessageMappingRepository,
    PostgresMigrationJobRepository,
    PostgresOperatorMigrationDefaultsRepository,
    PostgresMigrationStateRepository,
    PostgresOutboxRepository,
    PostgresServiceWatermarkRepository,
    PostgresTelegramExportSnapshotRepository,
)
from extg_shared.persistence.db import Database
from extg_shared.utils.retry import AsyncRetryPolicy
from extg_shared.utils.temp_files import AttachmentTempFileJanitor


class MigrationRuntimeContainer(containers.DeclarativeContainer):
    settings = providers.Singleton(load_settings)
    logger = providers.Singleton(structlog.get_logger, "extg_migration_runtime")
    telegram_session_context = providers.Singleton(TelegramSessionContext)
    telegram_runtime_mode = providers.Callable(
        lambda app_settings: (
            "remote"
            if (app_settings.telethon_service.base_url or "").strip()
            else "local"
        ),
        settings,
    )
    persistence_backend = providers.Callable(
        lambda app_settings: app_settings.resolve_persistence_backend(),
        settings,
    )
    command_transport_mode = providers.Callable(
        lambda app_settings: "kafka" if app_settings.kafka.enabled else "shadow",
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
    remote_telethon_service_client = providers.Singleton(
        RemoteTelethonServiceClient,
        base_url=providers.Callable(
            lambda app_settings: app_settings.telethon_service.base_url or "",
            settings,
        ),
        request_timeout_seconds=providers.Callable(
            lambda app_settings: app_settings.telethon_service.request_timeout_seconds,
            settings,
        ),
        attachment_request_timeout_seconds=providers.Callable(
            lambda app_settings: app_settings.telethon_service.attachment_request_timeout_seconds,
            settings,
        ),
        internal_token=providers.Callable(
            lambda app_settings: app_settings.telethon_service.internal_token,
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
    remote_telegram_gateway = providers.Singleton(
        RemoteTelegramGateway,
        session_context=telegram_session_context,
        client=remote_telethon_service_client,
    )
    primary_telegram_gateway = providers.Selector(
        telegram_runtime_mode,
        local=local_telegram_gateway,
        remote=remote_telegram_gateway,
    )
    telegram_export_archive_parser = providers.Singleton(TelegramExportArchiveParser)
    telegram_export_snapshot_repository = providers.Selector(
        persistence_backend,
        postgres=providers.Singleton(
            PostgresTelegramExportSnapshotRepository,
            session_factory=session_factory,
        ),
        in_memory=providers.Singleton(InMemoryTelegramExportSnapshotRepository),
    )
    telegram_export_archive_stage_store = providers.Selector(
        persistence_backend,
        postgres=providers.Singleton(
            LocalTelegramExportArchiveStageStore,
            directory=providers.Callable(
                lambda app_settings: app_settings.telegram.archive_import_dir,
                settings,
            ),
        ),
        in_memory=providers.Singleton(InMemoryTelegramExportArchiveStageStore),
    )
    archive_import_temp_file_janitor = providers.Singleton(
        AttachmentTempFileJanitor,
        directory=providers.Callable(
            lambda app_settings: app_settings.telegram.archive_import_dir,
            settings,
        ),
        enabled=providers.Callable(
            lambda app_settings: app_settings.telegram.archive_import_cleanup_enabled,
            settings,
        ),
        ttl_seconds=providers.Callable(
            lambda app_settings: app_settings.telegram.archive_import_ttl_seconds,
            settings,
        ),
        cleanup_interval_seconds=providers.Callable(
            lambda app_settings: app_settings.telegram.archive_import_cleanup_interval_seconds,
            settings,
        ),
        filename_prefix="extg-archive-",
        logger=logger,
    )
    archive_telegram_gateway = providers.Singleton(
        TelegramExportSnapshotGateway,
        snapshot_repository=telegram_export_snapshot_repository,
        parser=telegram_export_archive_parser,
        stage_store=telegram_export_archive_stage_store,
    )
    telegram_gateway = providers.Singleton(
        MultiSourceTelegramGateway,
        primary_gateway=primary_telegram_gateway,
        archive_gateway=archive_telegram_gateway,
    )
    express_account_registry = providers.Singleton(
        lambda app_settings: app_settings.express.build_account_registry(),
        settings,
    )
    express_gateway = providers.Singleton(
        ExpressGatewayRouter,
        account_registry=express_account_registry,
        chat_type=providers.Callable(
            lambda app_settings: app_settings.express.chat_type,
            settings,
        ),
        default_participant_huids=providers.Callable(
            lambda app_settings: (
                app_settings.express.default_participant_huids
                or app_settings.bot.operator_huids
            ),
            settings,
        ),
        request_timeout_seconds=providers.Callable(
            lambda app_settings: app_settings.express.request_timeout_seconds,
            settings,
        ),
        attachment_request_timeout_seconds=providers.Callable(
            lambda app_settings: app_settings.express.attachment_request_timeout_seconds,
            settings,
        ),
        local_idempotency_cache_enabled=providers.Callable(
            lambda app_settings: app_settings.express.local_idempotency_cache_enabled,
            settings,
        ),
    )
    express_file_store = providers.Singleton(
        ExpressFileStoreRouter,
        account_registry=express_account_registry,
        request_timeout_seconds=providers.Callable(
            lambda app_settings: app_settings.express.request_timeout_seconds,
            settings,
        ),
        attachment_request_timeout_seconds=providers.Callable(
            lambda app_settings: app_settings.express.attachment_request_timeout_seconds,
            settings,
        ),
        max_upload_size_bytes=providers.Callable(
            lambda app_settings: app_settings.worker.attachment_max_upload_size_bytes,
            settings,
        ),
        spool_max_memory_bytes=providers.Callable(
            lambda app_settings: app_settings.worker.attachment_spool_max_memory_bytes,
            settings,
        ),
    )
    chat_mapping_repository = providers.Selector(
        persistence_backend,
        postgres=providers.Singleton(
            PostgresChatMappingRepository,
            session_factory=session_factory,
        ),
        in_memory=providers.Singleton(InMemoryChatMappingRepository),
    )
    chat_migration_config_repository = providers.Selector(
        persistence_backend,
        postgres=providers.Singleton(
            PostgresChatMigrationConfigRepository,
            session_factory=session_factory,
        ),
        in_memory=providers.Singleton(InMemoryChatMigrationConfigRepository),
    )
    operator_migration_defaults_repository = providers.Selector(
        persistence_backend,
        postgres=providers.Singleton(
            PostgresOperatorMigrationDefaultsRepository,
            session_factory=session_factory,
        ),
        in_memory=providers.Singleton(InMemoryOperatorMigrationDefaultsRepository),
    )
    message_mapping_repository = providers.Selector(
        persistence_backend,
        postgres=providers.Singleton(
            PostgresMessageMappingRepository,
            session_factory=session_factory,
            persist_message_bodies=providers.Callable(
                lambda app_settings: app_settings.postgres.persist_message_bodies,
                settings,
            ),
            payload_storage_mode=providers.Callable(
                lambda app_settings: app_settings.postgres.payload_storage_mode,
                settings,
            ),
        ),
        in_memory=providers.Singleton(InMemoryMessageMappingRepository),
    )
    checkpoint_repository = providers.Selector(
        persistence_backend,
        postgres=providers.Singleton(
            PostgresCheckpointRepository,
            session_factory=session_factory,
        ),
        in_memory=providers.Singleton(InMemoryCheckpointRepository),
    )
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
    outbox_repository = providers.Selector(
        persistence_backend,
        postgres=providers.Singleton(
            PostgresOutboxRepository,
            session_factory=session_factory,
            payload_storage_mode=providers.Callable(
                lambda app_settings: app_settings.postgres.payload_storage_mode,
                settings,
            ),
        ),
        in_memory=providers.Singleton(InMemoryOutboxRepository),
    )
    service_watermark_repository = providers.Selector(
        persistence_backend,
        postgres=providers.Singleton(
            PostgresServiceWatermarkRepository,
            session_factory=session_factory,
        ),
        in_memory=providers.Singleton(InMemoryServiceWatermarkRepository),
    )
    inbox_repository = providers.Selector(
        persistence_backend,
        postgres=providers.Singleton(
            PostgresInboxRepository,
            session_factory=session_factory,
            payload_storage_mode=providers.Callable(
                lambda app_settings: app_settings.postgres.payload_storage_mode,
                settings,
            ),
        ),
        in_memory=providers.Singleton(InMemoryInboxRepository),
    )
    attachment_mapping_repository = providers.Selector(
        persistence_backend,
        postgres=providers.Singleton(
            PostgresAttachmentMappingRepository,
            session_factory=session_factory,
            payload_storage_mode=providers.Callable(
                lambda app_settings: app_settings.postgres.payload_storage_mode,
                settings,
            ),
        ),
        in_memory=providers.Singleton(InMemoryAttachmentMappingRepository),
    )
    attachment_stage_repository = providers.Selector(
        persistence_backend,
        postgres=providers.Singleton(
            PostgresAttachmentStageRepository,
            session_factory=session_factory,
            payload_storage_mode=providers.Callable(
                lambda app_settings: app_settings.postgres.payload_storage_mode,
                settings,
            ),
        ),
        in_memory=providers.Singleton(InMemoryAttachmentStageRepository),
    )
    inventory_snapshot_repository = providers.Selector(
        persistence_backend,
        postgres=providers.Singleton(
            PostgresInventorySnapshotRepository,
            session_factory=session_factory,
        ),
        in_memory=providers.Singleton(InMemoryInventorySnapshotRepository),
    )
    migration_state_repository = providers.Selector(
        persistence_backend,
        postgres=providers.Singleton(
            PostgresMigrationStateRepository,
            session_factory=session_factory,
        ),
        in_memory=providers.Singleton(InMemoryMigrationStateRepository),
    )
    migration_job_repository = providers.Selector(
        persistence_backend,
        postgres=providers.Singleton(
            PostgresMigrationJobRepository,
            session_factory=session_factory,
            payload_storage_mode=providers.Callable(
                lambda app_settings: app_settings.postgres.payload_storage_mode,
                settings,
            ),
        ),
        in_memory=providers.Singleton(
            InMemoryMigrationJobRepository,
            outbox_repository=outbox_repository,
        ),
    )
    identity_mapping_repository = providers.Selector(
        persistence_backend,
        postgres=providers.Singleton(
            PostgresIdentityMappingRepository,
            session_factory=session_factory,
        ),
        in_memory=providers.Singleton(InMemoryIdentityMappingRepository),
    )
    express_user_cts_binding_repository = providers.Selector(
        persistence_backend,
        postgres=providers.Singleton(
            PostgresExpressUserCtsBindingRepository,
            session_factory=session_factory,
        ),
        in_memory=providers.Singleton(InMemoryExpressUserCtsBindingRepository),
    )
    express_bot_huid_binding_repository = providers.Selector(
        persistence_backend,
        postgres=providers.Singleton(
            PostgresExpressBotHuidBindingRepository,
            session_factory=session_factory,
        ),
        in_memory=providers.Singleton(InMemoryExpressBotHuidBindingRepository),
    )
    operator_telegram_session_repository = providers.Selector(
        persistence_backend,
        postgres=providers.Singleton(
            PostgresOperatorTelegramSessionRepository,
            session_factory=session_factory,
        ),
        in_memory=providers.Singleton(InMemoryOperatorTelegramSessionRepository),
    )
    cts_resolution_service = providers.Singleton(
        CtsResolutionService,
        express_gateway=express_gateway,
        binding_repository=express_user_cts_binding_repository,
    )
    identity_directory = providers.Singleton(
        UsernameEmailIdentityDirectory,
        identity_mapping_repository=identity_mapping_repository,
        express_gateway=express_gateway,
        cts_resolution_service=cts_resolution_service,
        express_host=providers.Callable(
            lambda app_settings: app_settings.express.require_primary_account().cts_url,
            settings,
        ),
    )
    identity_resolver = providers.Singleton(
        DirectoryBackedIdentityResolver,
        identity_directory=identity_directory,
    )
    normalizer = providers.Singleton(
        TelegramMessageNormalizer,
        identity_resolver=identity_resolver,
        identity_directory=identity_directory,
    )
    renderer = providers.Singleton(
        MessageRenderer,
        timezone_name=providers.Callable(
            lambda app_settings: app_settings.timezone_name,
            settings,
        ),
    )
    retry_policy = providers.Factory(
        AsyncRetryPolicy,
        max_attempts=providers.Callable(
            lambda app_settings: app_settings.retry.max_attempts,
            settings,
        ),
        base_delay_seconds=providers.Callable(
            lambda app_settings: app_settings.retry.base_delay_seconds,
            settings,
        ),
        max_delay_seconds=providers.Callable(
            lambda app_settings: app_settings.retry.max_delay_seconds,
            settings,
        ),
        jitter_seconds=providers.Callable(
            lambda app_settings: app_settings.retry.jitter_seconds,
            settings,
        ),
    )
    attachment_transfer_semaphore = providers.Singleton(
        asyncio.Semaphore,
        value=providers.Callable(
            lambda app_settings: app_settings.worker.attachment_transfer_concurrency,
            settings,
        ),
    )
    message_checksum_service = providers.Singleton(
        MessageChecksumService,
        secret=providers.Callable(
            lambda app_settings: (
                app_settings.security.message_hmac_secret
                or "extg-local-dev-message-hmac-secret"
            ),
            settings,
        ),
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
    integration_event_publisher = providers.Selector(
        command_transport_mode,
        shadow=providers.Singleton(
            LoggingIntegrationEventPublisher,
            logger=logger,
        ),
        kafka=providers.Singleton(
            ConfluentKafkaIntegrationEventPublisher,
            bootstrap_servers=providers.Callable(
                lambda app_settings: app_settings.kafka.bootstrap_servers,
                settings,
            ),
            command_topic=providers.Callable(
                lambda app_settings: app_settings.kafka.command_topic,
                settings,
            ),
            producer_client_id=providers.Callable(
                lambda app_settings: app_settings.kafka.producer_client_id,
                settings,
            ),
            producer_flush_timeout_seconds=providers.Callable(
                lambda app_settings: app_settings.kafka.producer_flush_timeout_seconds,
                settings,
            ),
            logger=logger,
        ),
    )
    integration_outbox_publisher = providers.Singleton(
        IntegrationOutboxPublisher,
        outbox_repository=outbox_repository,
        event_publisher=integration_event_publisher,
        service_watermark_repository=service_watermark_repository,
        logger=logger,
        enabled=providers.Callable(
            lambda app_settings: app_settings.worker.outbox_publisher_enabled,
            settings,
        ),
        batch_size=providers.Callable(
            lambda app_settings: app_settings.worker.outbox_publisher_batch_size,
            settings,
        ),
        poll_interval_seconds=providers.Callable(
            lambda app_settings: app_settings.worker.outbox_publisher_poll_interval_seconds,
            settings,
        ),
        lease_duration_seconds=providers.Callable(
            lambda app_settings: app_settings.worker.outbox_publisher_lease_duration_seconds,
            settings,
        ),
        publisher_name=providers.Callable(
            lambda app_settings: f"{app_settings.kafka.producer_client_id}.outbox",
            settings,
        ),
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
    remote_telegram_session_service = providers.Singleton(
        RemoteTelegramSessionService,
        client=remote_telethon_service_client,
    )
    telegram_session_service = providers.Selector(
        telegram_runtime_mode,
        local=local_telegram_session_service,
        remote=remote_telegram_session_service,
    )
    message_delivery_service = providers.Factory(
        MessageDeliveryService,
        telegram_gateway=telegram_gateway,
        express_gateway=express_gateway,
        express_file_store=express_file_store,
        attachment_mapping_repository=attachment_mapping_repository,
        attachment_stage_repository=attachment_stage_repository,
        audit_repository=audit_repository,
        renderer=renderer,
        retry_policy=retry_policy,
        logger=logger,
        max_attachment_upload_size_bytes=providers.Callable(
            lambda app_settings: app_settings.worker.attachment_max_upload_size_bytes,
            settings,
        ),
        attachment_transfer_semaphore=attachment_transfer_semaphore,
    )
    target_chat_provisioning_service = providers.Factory(
        TargetChatProvisioningService,
        telegram_gateway=telegram_gateway,
        express_gateway=express_gateway,
        chat_mapping_repository=chat_mapping_repository,
        express_bot_huid_binding_repository=express_bot_huid_binding_repository,
        identity_directory=identity_directory,
        audit_repository=audit_repository,
        retry_policy=retry_policy,
        logger=logger,
    )
    inventory_use_case = providers.Factory(
        InventoryUseCase,
        telegram_gateway=telegram_gateway,
        inventory_snapshot_repository=inventory_snapshot_repository,
        audit_repository=audit_repository,
        retry_policy=retry_policy,
    )
    backfill_chat_use_case = providers.Factory(
        BackfillChatUseCase,
        telegram_gateway=telegram_gateway,
        express_gateway=express_gateway,
        chat_mapping_repository=chat_mapping_repository,
        message_mapping_repository=message_mapping_repository,
        checkpoint_repository=checkpoint_repository,
        audit_repository=audit_repository,
        normalizer=normalizer,
        message_delivery_service=message_delivery_service,
        target_chat_provisioning_service=target_chat_provisioning_service,
        retry_policy=retry_policy,
        checksum_service=message_checksum_service,
    )
    replay_failed_use_case = providers.Factory(
        ReplayFailedUseCase,
        telegram_gateway=telegram_gateway,
        chat_mapping_repository=chat_mapping_repository,
        message_mapping_repository=message_mapping_repository,
        audit_repository=audit_repository,
        normalizer=normalizer,
        message_delivery_service=message_delivery_service,
        retry_policy=retry_policy,
    )
    reconcile_migration_use_case = providers.Factory(
        ReconcileMigrationUseCase,
        inventory_snapshot_repository=inventory_snapshot_repository,
        message_mapping_repository=message_mapping_repository,
        attachment_mapping_repository=attachment_mapping_repository,
        audit_repository=audit_repository,
    )
    finalize_cutover_use_case = providers.Factory(
        FinalizeCutoverUseCase,
        reconcile_migration_use_case=reconcile_migration_use_case,
        migration_state_repository=migration_state_repository,
        audit_repository=audit_repository,
    )
    manage_migration_state_use_case = providers.Factory(
        ManageMigrationStateUseCase,
        migration_state_repository=migration_state_repository,
        audit_repository=audit_repository,
    )
    pause_delta_use_case = providers.Factory(
        PauseDeltaUseCase,
        manage_migration_state_use_case=manage_migration_state_use_case,
    )
    resume_delta_use_case = providers.Factory(
        ResumeDeltaUseCase,
        manage_migration_state_use_case=manage_migration_state_use_case,
    )
    freeze_cutover_use_case = providers.Factory(
        FreezeCutoverUseCase,
        manage_migration_state_use_case=manage_migration_state_use_case,
    )
    delta_sync_use_case = providers.Factory(
        DeltaSyncUseCase,
        telegram_gateway=telegram_gateway,
        express_gateway=express_gateway,
        chat_mapping_repository=chat_mapping_repository,
        message_mapping_repository=message_mapping_repository,
        checkpoint_repository=checkpoint_repository,
        migration_state_repository=migration_state_repository,
        audit_repository=audit_repository,
        normalizer=normalizer,
        message_delivery_service=message_delivery_service,
        target_chat_provisioning_service=target_chat_provisioning_service,
        retry_policy=retry_policy,
        checksum_service=message_checksum_service,
    )
    bot_control_service = providers.Singleton(
        MigrationBotControlService,
        settings=settings,
        telegram_gateway=telegram_gateway,
        express_gateway=express_gateway,
        inventory_use_case=inventory_use_case,
        backfill_chat_use_case=backfill_chat_use_case,
        delta_sync_use_case=delta_sync_use_case,
        replay_failed_use_case=replay_failed_use_case,
        reconcile_migration_use_case=reconcile_migration_use_case,
        pause_delta_use_case=pause_delta_use_case,
        resume_delta_use_case=resume_delta_use_case,
        chat_mapping_repository=chat_mapping_repository,
        chat_migration_config_repository=chat_migration_config_repository,
        operator_migration_defaults_repository=operator_migration_defaults_repository,
        checkpoint_repository=checkpoint_repository,
        message_mapping_repository=message_mapping_repository,
        migration_state_repository=migration_state_repository,
        identity_mapping_repository=identity_mapping_repository,
        telegram_export_snapshot_repository=telegram_export_snapshot_repository,
        telegram_export_archive_stage_store=telegram_export_archive_stage_store,
        operator_telegram_session_repository=operator_telegram_session_repository,
        telegram_session_context=telegram_session_context,
        telegram_session_service=telegram_session_service,
        telegram_export_archive_parser=telegram_export_archive_parser,
        identity_directory=identity_directory,
        target_chat_provisioning_service=target_chat_provisioning_service,
        audit_repository=audit_repository,
        migration_job_repository=migration_job_repository,
        logger=logger,
    )
    migration_job_worker = providers.Singleton(
        MigrationJobWorker,
        migration_job_repository=migration_job_repository,
        bot_control_service=bot_control_service,
        audit_repository=audit_repository,
        logger=logger,
        enabled=providers.Callable(
            lambda app_settings: app_settings.worker.enabled,
            settings,
        ),
        concurrency=providers.Callable(
            lambda app_settings: app_settings.worker.concurrency,
            settings,
        ),
        poll_interval_seconds=providers.Callable(
            lambda app_settings: app_settings.worker.poll_interval_seconds,
            settings,
        ),
        lease_duration_seconds=providers.Callable(
            lambda app_settings: app_settings.worker.lease_duration_seconds,
            settings,
        ),
        heartbeat_interval_seconds=providers.Callable(
            lambda app_settings: app_settings.worker.heartbeat_interval_seconds,
            settings,
        ),
    )
    migration_command_handler = providers.Singleton(
        MigrationCommandHandler,
        inbox_repository=inbox_repository,
        migration_job_repository=migration_job_repository,
        migration_job_worker=migration_job_worker,
        logger=logger,
        consumer_name=providers.Callable(
            lambda app_settings: app_settings.kafka.consumer_name,
            settings,
        ),
        lease_duration_seconds=providers.Callable(
            lambda app_settings: app_settings.worker.lease_duration_seconds,
            settings,
        ),
    )
    migration_command_consumer = providers.Singleton(
        ConfluentKafkaMigrationCommandConsumer,
        bootstrap_servers=providers.Callable(
            lambda app_settings: app_settings.kafka.bootstrap_servers,
            settings,
        ),
        topic=providers.Callable(
            lambda app_settings: app_settings.kafka.command_topic,
            settings,
        ),
        consumer_group=providers.Callable(
            lambda app_settings: app_settings.kafka.consumer_group,
            settings,
        ),
        consumer_client_id=providers.Callable(
            lambda app_settings: app_settings.kafka.consumer_client_id,
            settings,
        ),
        handler=migration_command_handler,
        service_watermark_repository=service_watermark_repository,
        logger=logger,
        enabled=providers.Callable(
            lambda app_settings: app_settings.kafka.enabled,
            settings,
        ),
        poll_timeout_seconds=providers.Callable(
            lambda app_settings: app_settings.kafka.poll_timeout_seconds,
            settings,
        ),
        consumer_name=providers.Callable(
            lambda app_settings: app_settings.kafka.consumer_name,
            settings,
        ),
    )
    worker_runtime_metrics_reporter = providers.Singleton(
        WorkerRuntimeMetricsReporter,
        migration_job_repository=migration_job_repository,
        outbox_repository=outbox_repository,
        service_watermark_repository=service_watermark_repository,
        logger=logger,
        enabled=providers.Callable(
            lambda app_settings: app_settings.worker.runtime_metrics_enabled,
            settings,
        ),
        poll_interval_seconds=providers.Callable(
            lambda app_settings: app_settings.worker.runtime_metrics_poll_interval_seconds,
            settings,
        ),
    )


def create_container() -> MigrationRuntimeContainer:
    return MigrationRuntimeContainer()


__all__ = ["MigrationRuntimeContainer", "create_container"]
