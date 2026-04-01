from __future__ import annotations

import asyncio
import hashlib
from collections import defaultdict
from dataclasses import dataclass, field, replace
from datetime import UTC, datetime
from typing import Any, Protocol
from uuid import uuid4

from extg_shared.contracts.ports import (
    AuditRepository,
    ChatMappingRepository,
    ChatMigrationConfigRepository,
    CheckpointRepository,
    ExpressGateway,
    IdentityMappingRepository,
    MigrationJobRepository,
    MessageMappingRepository,
    MessageStatusSummary,
    OperatorTelegramSessionRepository,
    TelegramExportArchiveStageStore,
    TelegramExportSnapshotRepository,
    TelegramGateway,
    MigrationStateRepository,
)
from extg_telethon_service.application.telegram_session_context import TelegramSessionContext
from extg_telethon_service.application.telegram_session_service import TelegramSessionService
from extg_migration_runtime.application.use_cases.backfill_chat import (
    BackfillChatCommand,
    BackfillChatUseCase,
)
from extg_migration_runtime.application.use_cases.delta_sync import (
    DeltaSyncCommand,
    DeltaSyncUseCase,
)
from extg_migration_runtime.application.use_cases.inventory import (
    InventoryCommand,
    InventoryUseCase,
)
from extg_migration_runtime.application.use_cases.migration_state_control import (
    MigrationStateControlResult,
    PauseDeltaCommand,
    PauseDeltaUseCase,
    ResumeDeltaCommand,
    ResumeDeltaUseCase,
)
from extg_migration_runtime.application.use_cases.reconcile_migration import (
    ReconcileMigrationCommand,
    ReconcileMigrationResult,
    ReconcileMigrationUseCase,
)
from extg_migration_runtime.application.use_cases.replay_failed import (
    ReplayFailedCommand,
    ReplayFailedResult,
    ReplayFailedUseCase,
)
from extg_migration_runtime.infrastructure.archive.telegram_export_archive_parser import (
    TelegramExportArchiveParser,
)
from extg_migration_runtime.application.identity import UsernameEmailIdentityDirectory
from extg_migration_runtime.application.target_chat_provisioning import (
    ResolvedParticipantTarget,
    TargetChatProvisioningService,
)
from extg_bot_ui.application.identity_matrix_workbook import (
    IdentityMatrixWorkbookRow,
    IdentityMatrixWorkbookService,
)
from extg_shared.contracts.errors import ConfigurationError, FatalItemError
from extg_shared.contracts.manifest import (
    ManifestDefaults,
    ManifestDialog,
    MigrationManifest,
)
from extg_shared.contracts.models import (
    AuditEvent,
    AuditSeverity,
    ChatMappingRecord,
    ChatMigrationConfigRecord,
    IdentityMappingRecord,
    IntegrationOutboxEventRecord,
    MigrationJobRecord,
    MigrationJobStatus,
    MIGRATION_JOB_CANCELLED_ERROR_CODE,
    MIGRATION_JOB_CANCEL_REQUESTED_ERROR_CODE,
    MessageImportStatus,
    MigrationLifecycleStatus,
    SourceDialog,
)
from extg_shared.utils import (
    get_current_express_cts_host,
    normalize_express_cts_host,
    use_express_cts_host,
)


class _BotSettingsLike(Protocol):
    migration_id: str | None
    operator_huids: list[str]


class _BackfillSettingsLike(Protocol):
    batch_size: int


class _WorkerSettingsLike(Protocol):
    backpressure_max_queued_jobs: int
    backpressure_max_running_jobs: int


class BotControlSettings(Protocol):
    bot: _BotSettingsLike
    backfill: _BackfillSettingsLike
    worker: _WorkerSettingsLike


@dataclass(frozen=True, slots=True)
class BotOperatorContext:
    huid: str
    chat_id: str
    username: str | None = None
    ad_login: str | None = None
    ad_domain: str | None = None
    display_name: str | None = None
    current_cts_host: str | None = None
    current_bot_id: str | None = None


@dataclass(frozen=True, slots=True)
class MigrationRunOptions:
    include_from: str | None = None
    include_to: str | None = None
    migrate_media: bool | None = None
    reply_mode: str | None = None
    source_backend: str | None = None
    target_strategy: str | None = None
    target_title: str | None = None
    target_chat_id: str | None = None
    identity_policy: str | None = None
    access_strategy: str | None = None
    topic_strategy: str | None = None
    skip_in_all: bool | None = None
    progress_policy: str = "resume"
    batch_size: int | None = None


@dataclass(frozen=True, slots=True)
class BotProgressHint:
    source_chat_id: str
    mapped_total: int
    imported_count: int
    last_source_message_id: str | None


@dataclass(frozen=True, slots=True)
class BotActiveJob:
    job_key: str
    operation: str
    migration_id: str
    source_chat_ids: tuple[str, ...]
    started_at: datetime


@dataclass(frozen=True, slots=True)
class BotAvailableChat:
    source_chat_id: str
    source_chat_type: str
    source_chat_title: str
    message_count: int
    media_count: int
    approximate_bytes: int
    configured: bool
    has_progress: bool
    imported_count: int
    mapped_total: int
    last_source_message_id: str | None


@dataclass(frozen=True, slots=True)
class BotChatConfigurationResult:
    migration_id: str
    source_chat_id: str
    source_chat_type: str
    source_chat_title: str
    source_backend: str
    target_strategy: str
    target_title: str | None
    target_chat_id: str | None
    include_from: str | None
    include_to: str | None
    migrate_media: bool
    reply_mode: str
    identity_policy: str
    access_strategy: str
    topic_strategy: str
    skip_in_all: bool
    telegram_chat_id: str | None
    source_thread_id: str | None
    source_thread_title: str | None
    configured_via: str
    progress_hint: BotProgressHint | None = None


@dataclass(frozen=True, slots=True)
class BotIdentityMappingResult:
    telegram_user_id: str | None
    telegram_username: str | None
    telegram_display_name: str | None
    corporate_email: str | None
    target_huid: str | None
    resolution_source: str
    reason: str | None = None


@dataclass(frozen=True, slots=True)
class BotPrivateChatResolutionRequest:
    source_chat_id: str
    source_chat_title: str
    peer_telegram_user_id: str | None
    peer_telegram_username: str | None
    peer_display_name: str
    corporate_email: str | None = None
    target_huid: str | None = None


@dataclass(frozen=True, slots=True)
class BotGroupChatResolutionRequest:
    source_chat_id: str
    source_chat_title: str
    source_chat_type: str
    action: str
    unresolved_count: int = 0
    resolved_count: int = 0
    workbook_content: bytes | None = None
    workbook_filename: str | None = None
    topic_titles: tuple[str, ...] = ()


@dataclass(frozen=True, slots=True)
class BotChannelChatResolutionRequest:
    source_chat_id: str
    source_chat_title: str
    action: str
    unresolved_count: int = 0
    resolved_count: int = 0
    workbook_content: bytes | None = None
    workbook_filename: str | None = None
    can_list_participants: bool = False


@dataclass(frozen=True, slots=True)
class BotGroupIdentityImportResult:
    source_chat_id: str
    imported_count: int
    skipped_count: int


@dataclass(frozen=True, slots=True)
class BotChatUserMatrixEntry:
    telegram_user_id: str | None
    telegram_username: str | None
    telegram_display_name: str
    is_self: bool
    corporate_email: str | None
    target_huid: str | None
    resolution_source: str
    reason: str | None = None


@dataclass(frozen=True, slots=True)
class BotChatUserMatrixResult:
    source_chat_id: str
    source_chat_type: str
    source_chat_title: str
    entries: tuple[BotChatUserMatrixEntry, ...]
    note: str | None = None


@dataclass(frozen=True, slots=True)
class BotMigratedChat:
    source_chat_id: str
    source_chat_type: str
    source_chat_title: str
    target_chat_id: str
    target_chat_title: str
    anchor_cts_host: str | None = None


@dataclass(frozen=True, slots=True)
class BotChatMembersWorkbookRequest:
    source_chat_id: str
    source_chat_type: str
    source_chat_title: str
    target_chat_id: str
    target_chat_title: str
    access_strategy: str
    workbook_content: bytes
    workbook_filename: str
    mapped_rows: int
    unresolved_rows: int
    note: str | None = None


@dataclass(frozen=True, slots=True)
class BotChatMembersAddResult:
    source_chat_id: str
    target_chat_id: str
    target_chat_title: str
    access_strategy: str
    effective_result: str
    invite_fallback_used: bool
    processed_rows: int
    imported_identity_mappings: int
    mapping_skipped_rows: int
    resolved_targets: int
    direct_added: int
    invited: int
    skipped_rows: int
    failed_targets: tuple[str, ...] = ()


@dataclass(frozen=True, slots=True)
class _MigratedChatBinding:
    config: ChatMigrationConfigRecord
    mapping: ChatMappingRecord


@dataclass(frozen=True, slots=True)
class BotOperationAcceptedResult:
    status: str
    operation: str
    migration_id: str
    source_chat_ids: tuple[str, ...]
    job_key: str | None = None
    job_keys: tuple[str, ...] = ()
    reason: str | None = None
    active_jobs: tuple[BotActiveJob, ...] = ()
    progress_hints: tuple[BotProgressHint, ...] = ()
    accepted_source_chat_ids: tuple[str, ...] = ()
    blocked_source_chat_ids: tuple[str, ...] = ()
    already_running_source_chat_ids: tuple[str, ...] = ()
    target_chat_id: str | None = None
    target_chat_title: str | None = None
    target_chat_link: str | None = None
    topic_targets: tuple["BotTopicTarget", ...] = ()


@dataclass(frozen=True, slots=True)
class BotTopicTarget:
    source_chat_id: str
    topic_id: str | None
    topic_title: str
    target_chat_id: str
    target_chat_title: str
    target_chat_link: str | None = None


@dataclass(frozen=True, slots=True)
class BotChatCheckpointStatus:
    source_chat_id: str
    backfill_last_source_message_id: str | None
    delta_last_source_message_id: str | None


@dataclass(frozen=True, slots=True)
class MigrationBotStatusResult:
    migration_id: str
    migration_state: MigrationLifecycleStatus | None
    active_jobs: tuple[BotActiveJob, ...]
    reconcile: ReconcileMigrationResult
    chat_checkpoints: tuple[BotChatCheckpointStatus, ...]


@dataclass(frozen=True, slots=True)
class BotMigrationStatsResult:
    migration_id: str
    configured_chats: int
    foreign_managed_chats: int
    skipped_in_all_chats: int
    active_jobs: int
    chats_with_progress: int
    attention_chats: int
    imported_messages: int
    failed_messages: int
    ambiguous_messages: int
    imported_attachments: int
    failed_attachments: int


@dataclass(frozen=True, slots=True)
class BotCancelResult:
    migration_id: str
    source_chat_id: str | None = None
    cancelled_queued_jobs: tuple[BotActiveJob, ...] = ()
    cancellation_requested_jobs: tuple[BotActiveJob, ...] = ()


class InMemoryBotJobRegistry:
    def __init__(self) -> None:
        self._jobs: dict[str, BotActiveJob] = {}
        self._lock = asyncio.Lock()

    async def add(self, job: BotActiveJob) -> bool:
        async with self._lock:
            if job.job_key in self._jobs:
                return False
            self._jobs[job.job_key] = job
            return True

    async def remove(self, job_key: str) -> None:
        async with self._lock:
            self._jobs.pop(job_key, None)

    async def list_for_migration(self, migration_id: str) -> list[BotActiveJob]:
        async with self._lock:
            return sorted(
                [
                    job
                    for job in self._jobs.values()
                    if job.migration_id == migration_id
                ],
                key=lambda job: job.started_at,
            )


class MigrationBotControlService:
    def __init__(
        self,
        *,
        settings: BotControlSettings,
        telegram_gateway: TelegramGateway,
        express_gateway: ExpressGateway,
        inventory_use_case: InventoryUseCase,
        backfill_chat_use_case: BackfillChatUseCase,
        delta_sync_use_case: DeltaSyncUseCase,
        replay_failed_use_case: ReplayFailedUseCase,
        reconcile_migration_use_case: ReconcileMigrationUseCase,
        pause_delta_use_case: PauseDeltaUseCase,
        resume_delta_use_case: ResumeDeltaUseCase,
        chat_mapping_repository: ChatMappingRepository,
        chat_migration_config_repository: ChatMigrationConfigRepository,
        checkpoint_repository: CheckpointRepository,
        message_mapping_repository: MessageMappingRepository,
        migration_state_repository: MigrationStateRepository,
        identity_mapping_repository: IdentityMappingRepository,
        telegram_export_snapshot_repository: TelegramExportSnapshotRepository,
        telegram_export_archive_stage_store: TelegramExportArchiveStageStore,
        operator_telegram_session_repository: OperatorTelegramSessionRepository,
        telegram_session_context: TelegramSessionContext,
        telegram_session_service: TelegramSessionService,
        telegram_export_archive_parser: TelegramExportArchiveParser,
        identity_directory: UsernameEmailIdentityDirectory,
        target_chat_provisioning_service: TargetChatProvisioningService,
        audit_repository: AuditRepository,
        migration_job_repository: MigrationJobRepository,
        logger: Any,
    ) -> None:
        self._settings = settings
        self._telegram_gateway = telegram_gateway
        self._express_gateway = express_gateway
        self._inventory_use_case = inventory_use_case
        self._backfill_chat_use_case = backfill_chat_use_case
        self._delta_sync_use_case = delta_sync_use_case
        self._replay_failed_use_case = replay_failed_use_case
        self._reconcile_migration_use_case = reconcile_migration_use_case
        self._pause_delta_use_case = pause_delta_use_case
        self._resume_delta_use_case = resume_delta_use_case
        self._chat_mapping_repository = chat_mapping_repository
        self._chat_migration_config_repository = chat_migration_config_repository
        self._checkpoint_repository = checkpoint_repository
        self._message_mapping_repository = message_mapping_repository
        self._migration_state_repository = migration_state_repository
        self._identity_mapping_repository = identity_mapping_repository
        self._telegram_export_snapshot_repository = telegram_export_snapshot_repository
        self._telegram_export_archive_stage_store = telegram_export_archive_stage_store
        self._operator_telegram_session_repository = operator_telegram_session_repository
        self._telegram_session_context = telegram_session_context
        self._telegram_session_service = telegram_session_service
        self._telegram_export_archive_parser = telegram_export_archive_parser
        self._identity_directory = identity_directory
        self._target_chat_provisioning_service = target_chat_provisioning_service
        self._audit_repository = audit_repository
        self._migration_job_repository = migration_job_repository
        self._logger = logger
        self._identity_matrix_workbook_service = IdentityMatrixWorkbookService()

    async def list_available_chats(
        self,
        *,
        operator: BotOperatorContext,
        limit: int = 20,
        query: str | None = None,
        source_backend: str = "telethon_user_session",
    ) -> tuple[BotAvailableChat, ...]:
        await self._authorize(operator)
        async def run() -> tuple[BotAvailableChat, ...]:
            migration_id = self._migration_id_for_tracking()
            configured_chat_ids = await self._configured_chat_ids(operator_huid=operator.huid)
            progress_hints = {
                hint.source_chat_id: hint
                for hint in await self._progress_hints_for_query(
                    migration_id,
                    source_chat_ids=tuple(sorted(configured_chat_ids)) if configured_chat_ids else (),
                )
            }
            available_dialogs = await self._telegram_gateway.list_available_dialogs(
                limit=limit,
                query=query,
                source_backend=source_backend,
            )
            return tuple(
                BotAvailableChat(
                    source_chat_id=dialog.dialog_id,
                    source_chat_type=dialog.chat_type,
                    source_chat_title=dialog.title,
                    message_count=dialog.message_count,
                    media_count=dialog.media_count,
                    approximate_bytes=dialog.approximate_bytes,
                    configured=dialog.dialog_id in configured_chat_ids,
                    has_progress=dialog.dialog_id in progress_hints,
                    imported_count=progress_hints.get(dialog.dialog_id).imported_count
                    if dialog.dialog_id in progress_hints
                    else 0,
                    mapped_total=progress_hints.get(dialog.dialog_id).mapped_total
                    if dialog.dialog_id in progress_hints
                    else 0,
                    last_source_message_id=(
                        progress_hints.get(dialog.dialog_id).last_source_message_id
                        if dialog.dialog_id in progress_hints
                        else None
                    ),
                )
                for dialog in available_dialogs
            )

        return await self._execute_for_source_backends(
            operator=operator,
            source_backends={source_backend},
            runner=run,
        )

    async def configure_chat(
        self,
        *,
        operator: BotOperatorContext,
        source_chat_id: str,
        options: MigrationRunOptions,
    ) -> BotChatConfigurationResult:
        await self._authorize(operator)
        source_backend = await self._source_backend_for_chat(
            source_chat_id=source_chat_id,
            requested=options.source_backend,
        )
        record = await self._execute_for_source_backends(
            operator=operator,
            source_backends={source_backend},
            runner=lambda: self._upsert_chat_config(
                operator=operator,
                source_chat_id=source_chat_id,
                options=options,
            ),
        )
        return await self._chat_configuration_result(record)

    async def show_chat(
        self,
        *,
        operator: BotOperatorContext,
        source_chat_id: str,
    ) -> BotChatConfigurationResult:
        await self._authorize(operator)
        migration_id = self._require_configured_migration_id()
        record = await self._chat_migration_config_repository.get(migration_id, source_chat_id)
        if record is None:
            raise ConfigurationError(
                f"source_chat_id={source_chat_id!r} is not configured; use /configure first",
            )
        self._ensure_operator_owns_chat_config(
            record=record,
            operator_huid=operator.huid,
        )
        return await self._chat_configuration_result(record)

    async def show_or_configure_chat(
        self,
        *,
        operator: BotOperatorContext,
        source_chat_id: str,
    ) -> BotChatConfigurationResult:
        await self._authorize(operator)
        migration_id = self._require_configured_migration_id()
        existing = await self._chat_migration_config_repository.get(migration_id, source_chat_id)
        if existing is not None:
            self._ensure_operator_owns_chat_config(
                record=existing,
                operator_huid=operator.huid,
            )
            return await self._chat_configuration_result(existing)
        return await self.configure_chat(
            operator=operator,
            source_chat_id=source_chat_id,
            options=MigrationRunOptions(),
        )

    async def list_chat_users(
        self,
        *,
        operator: BotOperatorContext,
        source_chat_id: str,
    ) -> BotChatUserMatrixResult:
        await self._authorize(operator)
        source_backend = await self._source_backend_for_chat(source_chat_id=source_chat_id)
        migration_id = self._require_configured_migration_id()
        record = await self._chat_migration_config_repository.get(migration_id, source_chat_id)
        if record is not None:
            self._ensure_operator_owns_chat_config(
                record=record,
                operator_huid=operator.huid,
            )
        participant_source_chat_id = (
            record.telegram_chat_id if record is not None else None
        )

        async def run() -> BotChatUserMatrixResult:
            dialog = await self._discover_dialog(
                source_chat_id,
                source_backend=source_backend,
            )
            note: str | None = None
            try:
                participants = await self._target_chat_provisioning_service.list_participant_matrix(
                    source_chat_id=source_chat_id,
                    participant_source_chat_id=participant_source_chat_id,
                    source_backend=source_backend,
                )
            except FatalItemError as error:
                if dialog.chat_type != "channel":
                    raise
                participants = []
                note = (
                    "Telegram не дал доступный список подписчиков канала. "
                    "Для channel это допустимый best-effort режим; маппинг можно делать "
                    "по наблюдаемым авторам сообщений и администраторам.\n"
                    f"details={error}"
                )
            entries = tuple(
                BotChatUserMatrixEntry(
                    telegram_user_id=item.telegram_user_id,
                    telegram_username=item.telegram_username,
                    telegram_display_name=item.telegram_display_name,
                    is_self=item.is_self,
                    corporate_email=item.corporate_email,
                    target_huid=item.target_huid,
                    resolution_source=item.resolution_source,
                    reason=item.reason,
                )
                for item in participants
            )
            return BotChatUserMatrixResult(
                source_chat_id=dialog.dialog_id,
                source_chat_type=dialog.chat_type,
                source_chat_title=dialog.title,
                entries=entries,
                note=(
                    note
                    or (
                        "Список пользователей канала best-effort и может быть неполным."
                        if dialog.chat_type == "channel"
                        else None
                    )
                ),
            )

        return await self._execute_for_source_backends(
            operator=operator,
            source_backends={source_backend},
            runner=run,
        )

    async def list_migrated_chats_for_user_addition(
        self,
        *,
        operator: BotOperatorContext,
    ) -> tuple[BotMigratedChat, ...]:
        await self._authorize(operator)
        migration_id = self._require_configured_migration_id()
        bindings = await self._migrated_chat_bindings(
            migration_id=migration_id,
            operator_huid=operator.huid,
        )
        chats = tuple(
            BotMigratedChat(
                source_chat_id=binding.config.source_chat_id,
                source_chat_type=binding.config.source_chat_type,
                source_chat_title=binding.config.source_chat_title,
                target_chat_id=binding.mapping.target_chat_id,
                target_chat_title=binding.mapping.target_chat_title,
                anchor_cts_host=binding.mapping.anchor_cts_host or binding.config.anchor_cts_host,
            )
            for binding in bindings
        )
        return chats

    async def prepare_chat_members_workbook(
        self,
        *,
        operator: BotOperatorContext,
        chat_selector: str,
    ) -> BotChatMembersWorkbookRequest:
        await self._authorize(operator)
        migration_id = self._require_configured_migration_id()
        binding = await self._resolve_migrated_chat_binding(
            migration_id=migration_id,
            operator_huid=operator.huid,
            chat_selector=chat_selector,
        )
        source_backend = self._ensure_supported_source_backend(binding.config.source_backend)

        async def run() -> BotChatMembersWorkbookRequest:
            note: str | None = None
            try:
                participants = await self._target_chat_provisioning_service.list_participant_matrix(
                    source_chat_id=binding.config.source_chat_id,
                    participant_source_chat_id=binding.config.telegram_chat_id,
                    source_backend=source_backend,
                )
            except FatalItemError as error:
                if binding.config.source_chat_type != "channel":
                    raise
                participants = []
                note = (
                    "Telegram не дал полный список подписчиков канала. "
                    "Матрица создана только для ручного добавления пользователей; "
                    "можно заполнить `email`, `huid`, `ad_login` или `other_id`.\n"
                    f"details={error}"
                )
            workbook_rows = [
                IdentityMatrixWorkbookRow(
                    telegram_user_id=item.telegram_user_id,
                    telegram_username=item.telegram_username,
                    telegram_display_name=item.telegram_display_name,
                    corporate_email=item.corporate_email,
                    target_huid=item.target_huid,
                    is_self=item.is_self,
                    resolution_source=item.resolution_source,
                    reason=item.reason,
                )
                for item in participants
            ]
            workbook_content = self._identity_matrix_workbook_service.build_workbook(
                rows=workbook_rows,
                manual_template_rows=10,
            )
            safe_chat_id = binding.config.source_chat_id.replace("/", "_")
            mapped_rows = sum(
                1
                for item in participants
                if not item.is_self and item.target_huid is not None
            )
            unresolved_rows = sum(
                1
                for item in participants
                if not item.is_self and item.target_huid is None
            )
            await self._audit_operator_event(
                migration_id,
                operator,
                event_type="bot_chat_members_workbook_prepared",
                payload={
                    "source_chat_id": binding.config.source_chat_id,
                    "target_chat_id": binding.mapping.target_chat_id,
                    "mapped_rows": mapped_rows,
                    "unresolved_rows": unresolved_rows,
                    "manual_template_rows": 10,
                },
            )
            return BotChatMembersWorkbookRequest(
                source_chat_id=binding.config.source_chat_id,
                source_chat_type=binding.config.source_chat_type,
                source_chat_title=binding.config.source_chat_title,
                target_chat_id=binding.mapping.target_chat_id,
                target_chat_title=binding.mapping.target_chat_title,
                access_strategy=binding.config.access_strategy or "direct_add",
                workbook_content=workbook_content,
                workbook_filename=f"chat_members_{safe_chat_id}.xlsx",
                mapped_rows=mapped_rows,
                unresolved_rows=unresolved_rows,
                note=note,
            )

        return await self._execute_for_source_backends(
            operator=operator,
            source_backends={source_backend},
            runner=run,
        )

    async def add_chat_members_from_workbook(
        self,
        *,
        operator: BotOperatorContext,
        chat_selector: str,
        workbook_content: bytes,
    ) -> BotChatMembersAddResult:
        await self._authorize(operator)
        migration_id = self._require_configured_migration_id()
        binding = await self._resolve_migrated_chat_binding(
            migration_id=migration_id,
            operator_huid=operator.huid,
            chat_selector=chat_selector,
        )
        source_backend = self._ensure_supported_source_backend(binding.config.source_backend)

        async def run() -> BotChatMembersAddResult:
            rows = self._identity_matrix_workbook_service.parse_workbook(workbook_content)
            participant_targets: list[ResolvedParticipantTarget] = []
            target_labels_by_huid: dict[str, str] = {}
            processed_rows = 0
            imported_identity_mappings = 0
            mapping_skipped_rows = 0
            skipped_rows = 0
            for row_index, row in enumerate(rows, start=2):
                if row.is_self:
                    skipped_rows += 1
                    continue
                selector_count = self._row_selector_count(row)
                if selector_count == 0:
                    skipped_rows += 1
                    continue
                selector_kind, selector_kwargs = self._preferred_row_selector(row)
                lookup = await self._identity_directory.resolve_corporate_selector(
                    corporate_email=selector_kwargs["corporate_email"],
                    target_huid=selector_kwargs["target_huid"],
                    ad_login=selector_kwargs["ad_login"],
                    other_id=selector_kwargs["other_id"],
                    default_ad_domain=operator.ad_domain,
                )
                if lookup.target_huid is None:
                    raise ConfigurationError(
                        f"workbook row {row_index} could not be resolved in eXpress; "
                        f"reason={lookup.reason or 'identity_not_found'}",
                    )
                if row.telegram_user_id is not None or row.telegram_username is not None:
                    if selector_kind == "corporate_email":
                        await self._identity_directory.upsert_mapping(
                            telegram_user_id=row.telegram_user_id,
                            telegram_username=row.telegram_username,
                            telegram_display_name=row.telegram_display_name or None,
                            corporate_email=selector_kwargs["corporate_email"] or "",
                        )
                    else:
                        await self._identity_directory.upsert_direct_mapping(
                            telegram_user_id=row.telegram_user_id,
                            telegram_username=row.telegram_username,
                            telegram_display_name=row.telegram_display_name or None,
                            target_huid=lookup.target_huid,
                            corporate_email=row.corporate_email,
                        )
                    imported_identity_mappings += 1
                else:
                    mapping_skipped_rows += 1
                participant_targets.append(
                    ResolvedParticipantTarget(
                        target_huid=lookup.target_huid,
                        cts_host=normalize_express_cts_host(lookup.cts_host),
                    ),
                )
                target_labels_by_huid.setdefault(
                    lookup.target_huid,
                    self._workbook_row_label(row, fallback_huid=lookup.target_huid),
                )
                processed_rows += 1
            if not participant_targets:
                raise ConfigurationError("workbook does not contain any resolvable users to add")
            access_result = await self._target_chat_provisioning_service.add_resolved_members_to_existing_chat(
                migration_id=migration_id,
                source_chat_id=binding.config.source_chat_id,
                source_chat_type=binding.config.source_chat_type,
                source_backend=source_backend,
                target_chat_id=binding.mapping.target_chat_id,
                target_chat_title=binding.mapping.target_chat_title,
                participant_targets=participant_targets,
                initiator_huid=operator.huid,
                access_strategy=binding.config.access_strategy or "direct_add",
                anchor_cts_host=binding.mapping.anchor_cts_host or binding.config.anchor_cts_host,
            )
            await self._audit_operator_event(
                migration_id,
                operator,
                event_type="bot_chat_members_added_from_workbook",
                payload={
                    "source_chat_id": binding.config.source_chat_id,
                    "target_chat_id": binding.mapping.target_chat_id,
                    "processed_rows": processed_rows,
                    "imported_identity_mappings": imported_identity_mappings,
                    "mapping_skipped_rows": mapping_skipped_rows,
                    "direct_added": len(access_result.added_huids),
                    "invited": len(access_result.invited_huids),
                    "failed_target_huids": [
                        item.target_huid for item in access_result.failed_targets
                    ],
                },
            )
            requested_access_strategy = binding.config.access_strategy or "direct_add"
            direct_added_count = len(access_result.added_huids)
            invited_count = len(access_result.invited_huids)
            failed_target_labels = tuple(
                target_labels_by_huid.get(item.target_huid, item.target_huid)
                for item in access_result.failed_targets
            )
            return BotChatMembersAddResult(
                source_chat_id=binding.config.source_chat_id,
                target_chat_id=binding.mapping.target_chat_id,
                target_chat_title=binding.mapping.target_chat_title,
                access_strategy=requested_access_strategy,
                effective_result=self._member_add_effective_result(
                    requested_access_strategy=requested_access_strategy,
                    direct_added=direct_added_count,
                    invited=invited_count,
                    failed=len(access_result.failed_targets),
                ),
                invite_fallback_used=(
                    requested_access_strategy == "direct_add" and invited_count > 0
                ),
                processed_rows=processed_rows,
                imported_identity_mappings=imported_identity_mappings,
                mapping_skipped_rows=mapping_skipped_rows,
                resolved_targets=len(participant_targets),
                direct_added=direct_added_count,
                invited=invited_count,
                skipped_rows=skipped_rows,
                failed_targets=failed_target_labels,
            )

        return await self._execute_for_source_backends(
            operator=operator,
            source_backends={source_backend},
            runner=run,
        )

    async def start_archive_import(
        self,
        *,
        operator: BotOperatorContext,
        archive_content: bytes,
        archive_filename: str | None,
    ) -> BotOperationAcceptedResult:
        await self._authorize(operator)

        async def run() -> BotOperationAcceptedResult:
            parsed = await self._telegram_export_archive_parser.parse_upload(
                filename=archive_filename,
                content=archive_content,
            )
            migration_id = self._require_configured_migration_id()
            existing = await self._chat_migration_config_repository.get(
                migration_id,
                parsed.source_chat_id,
            )
            if existing is not None:
                self._ensure_operator_owns_chat_config(
                    record=existing,
                    operator_huid=operator.huid,
                )
            dialog = parsed.to_source_dialog()
            already_migrated = await self._completed_chat_already_migrated_result(
                migration_id=migration_id,
                dialog=dialog,
                operator_huid=operator.huid,
            )
            if already_migrated is not None:
                return replace(
                    already_migrated,
                    operation="import_archive_chat",
                )

            options = MigrationRunOptions(
                source_backend="telegram_export_archive",
                target_strategy="create",
                target_title=parsed.source_chat_title,
                migrate_media=False,
                reply_mode=self._manifest_defaults().reply_mode,
                identity_policy="display_only",
                access_strategy="direct_add",
                skip_in_all=True,
                progress_policy="resume",
            )
            archive_locator = await self._telegram_export_archive_stage_store.stage_upload(
                filename=archive_filename,
                content=archive_content,
            )
            saved = await self._save_chat_config(
                operator=operator,
                dialog=dialog,
                options=options,
                existing=existing,
            )
            snapshot_record = parsed.to_snapshot_record(
                archive_locator=archive_locator,
                uploaded_by_huid=operator.huid,
                created_at=self._now(),
            )
            await self._telegram_export_snapshot_repository.save(snapshot_record)
            await self._audit_operator_event(
                migration_id,
                operator,
                event_type="telegram_export_archive_staged",
                payload={
                    "source_chat_id": saved.source_chat_id,
                    "source_chat_type": saved.source_chat_type,
                    "source_chat_title": saved.source_chat_title,
                    "archive_filename": archive_filename,
                    "message_count": parsed.message_count,
                    "media_count": parsed.media_count,
                    "approximate_bytes": parsed.approximate_bytes,
                    "export_chat_id": parsed.export_chat_id,
                },
            )
            manifest = await self._load_config_manifest(
                source_chat_ids=(saved.source_chat_id,),
                operator_huid=operator.huid,
            )
            await self._validate_manifest_preconditions(manifest)
            return await self._start_background_operation(
                operator=operator,
                operation="import_archive_chat",
                manifest=manifest,
                source_chat_ids=(saved.source_chat_id,),
                options=options,
            )

        return await self._execute_for_source_backends(
            operator=operator,
            source_backends={"telegram_export_archive"},
            runner=run,
        )

    async def map_identity(
        self,
        *,
        operator: BotOperatorContext,
        telegram_user_id: str | None,
        telegram_username: str | None,
        telegram_display_name: str | None,
        corporate_email: str,
    ) -> BotIdentityMappingResult:
        await self._authorize(operator)
        lookup = await self._identity_directory.upsert_mapping(
            telegram_user_id=telegram_user_id,
            telegram_username=telegram_username,
            telegram_display_name=telegram_display_name,
            corporate_email=corporate_email,
        )
        await self._audit_operator_event(
            self._migration_id_for_tracking(),
            operator,
            event_type="bot_identity_mapping_upserted",
            payload={
                "telegram_user_id": lookup.telegram_user_id,
                "telegram_username": lookup.telegram_username,
                "telegram_display_name": lookup.telegram_display_name,
                "corporate_email": lookup.corporate_email,
                "target_huid": lookup.target_huid,
                "resolution_source": lookup.resolution_source,
                "reason": lookup.reason,
            },
        )
        return BotIdentityMappingResult(
            telegram_user_id=lookup.telegram_user_id,
            telegram_username=lookup.telegram_username,
            telegram_display_name=lookup.telegram_display_name,
            corporate_email=lookup.corporate_email or corporate_email.strip().lower(),
            target_huid=lookup.target_huid,
            resolution_source=lookup.resolution_source,
            reason=lookup.reason,
        )

    async def prepare_private_chat_resolution(
        self,
        *,
        operator: BotOperatorContext,
        source_chat_id: str,
        options: MigrationRunOptions,
    ) -> BotPrivateChatResolutionRequest | None:
        await self._authorize(operator)

        async def run() -> BotPrivateChatResolutionRequest | None:
            if options.target_strategy not in {None, "create"}:
                return None
            migration_id = self._require_configured_migration_id()
            dialog = await self._discover_dialog(source_chat_id)
            existing = await self._chat_migration_config_repository.get(
                migration_id,
                source_chat_id,
            )
            if existing is not None:
                self._ensure_operator_owns_chat_config(
                    record=existing,
                    operator_huid=operator.huid,
                )
            if dialog.chat_type != "private":
                return None
            if (
                await self._completed_chat_already_migrated_result(
                    migration_id=migration_id,
                    dialog=dialog,
                    operator_huid=operator.huid,
                )
            ) is not None:
                return None
            peer = await self._private_chat_peer_entry(source_chat_id)
            if peer is None or peer.target_huid is not None:
                return None
            return BotPrivateChatResolutionRequest(
                source_chat_id=dialog.dialog_id,
                source_chat_title=dialog.title,
                peer_telegram_user_id=peer.telegram_user_id,
                peer_telegram_username=peer.telegram_username,
                peer_display_name=peer.telegram_display_name,
                corporate_email=peer.corporate_email,
                target_huid=peer.target_huid,
            )

        return await self._execute_with_operator_session(operator=operator, runner=run)

    async def prepare_group_chat_resolution(
        self,
        *,
        operator: BotOperatorContext,
        source_chat_id: str,
        options: MigrationRunOptions,
    ) -> BotGroupChatResolutionRequest | None:
        await self._authorize(operator)
        source_backend = await self._source_backend_for_chat(
            source_chat_id=source_chat_id,
            requested=options.source_backend,
        )

        async def run() -> BotGroupChatResolutionRequest | None:
            migration_id = self._require_configured_migration_id()
            dialog = await self._discover_dialog(
                source_chat_id,
                source_backend=source_backend,
            )
            existing = await self._chat_migration_config_repository.get(
                migration_id,
                source_chat_id,
            )
            if existing is not None:
                self._ensure_operator_owns_chat_config(
                    record=existing,
                    operator_huid=operator.huid,
                )
            if dialog.chat_type not in {"group", "supergroup"}:
                return None
            if (
                await self._completed_chat_already_migrated_result(
                    migration_id=migration_id,
                    dialog=dialog,
                    operator_huid=operator.huid,
                )
            ) is not None:
                return None
            if options.target_strategy not in {None, "create"}:
                return None

            topic_strategy = await self._effective_topic_strategy(
                migration_id=migration_id,
                source_chat_id=source_chat_id,
                requested=options.topic_strategy,
            )
            if dialog.has_topics:
                if topic_strategy is None:
                    topics = await self._telegram_gateway.list_topics(
                        source_chat_id,
                        source_backend=source_backend,
                    )
                    return BotGroupChatResolutionRequest(
                        source_chat_id=dialog.dialog_id,
                        source_chat_title=dialog.title,
                        source_chat_type=dialog.chat_type,
                        action="choose_topic_strategy",
                        topic_titles=tuple(topic.title for topic in topics),
                    )

            effective_identity_policy = await self._effective_identity_policy(
                migration_id=migration_id,
                source_chat_id=source_chat_id,
                requested=options.identity_policy,
            )
            if effective_identity_policy in {"skip_unresolved", "matrix_uploaded"}:
                return None

            try:
                participants = await self._target_chat_provisioning_service.list_participant_matrix(
                    source_chat_id=source_chat_id,
                    source_backend=source_backend,
                )
            except FatalItemError:
                return None

            unresolved_rows = [
                item
                for item in participants
                if not item.is_self and item.target_huid is None
            ]
            if not unresolved_rows:
                return None
            resolved_count = sum(
                1
                for item in participants
                if not item.is_self and item.target_huid is not None
            )
            workbook_rows = [
                IdentityMatrixWorkbookRow(
                    telegram_user_id=item.telegram_user_id,
                    telegram_username=item.telegram_username,
                    telegram_display_name=item.telegram_display_name,
                    corporate_email=item.corporate_email,
                    target_huid=item.target_huid,
                    is_self=item.is_self,
                    resolution_source=item.resolution_source,
                    reason=item.reason,
                )
                for item in participants
            ]
            workbook_content = self._identity_matrix_workbook_service.build_workbook(
                rows=workbook_rows,
            )
            safe_chat_id = source_chat_id.replace("/", "_")
            return BotGroupChatResolutionRequest(
                source_chat_id=dialog.dialog_id,
                source_chat_title=dialog.title,
                source_chat_type=dialog.chat_type,
                action="upload_identity_matrix",
                unresolved_count=len(unresolved_rows),
                resolved_count=resolved_count,
                workbook_content=workbook_content,
                workbook_filename=f"identity_matrix_{safe_chat_id}.xlsx",
            )

        return await self._execute_for_source_backends(
            operator=operator,
            source_backends={source_backend},
            runner=run,
        )

    async def prepare_channel_chat_resolution(
        self,
        *,
        operator: BotOperatorContext,
        source_chat_id: str,
        options: MigrationRunOptions,
    ) -> BotChannelChatResolutionRequest | None:
        await self._authorize(operator)
        source_backend = await self._source_backend_for_chat(
            source_chat_id=source_chat_id,
            requested=options.source_backend,
        )

        async def run() -> BotChannelChatResolutionRequest | None:
            migration_id = self._require_configured_migration_id()
            dialog = await self._discover_dialog(
                source_chat_id,
                source_backend=source_backend,
            )
            existing = await self._chat_migration_config_repository.get(
                migration_id,
                source_chat_id,
            )
            if existing is not None:
                self._ensure_operator_owns_chat_config(
                    record=existing,
                    operator_huid=operator.huid,
                )
            if dialog.chat_type != "channel":
                return None
            if (
                await self._completed_chat_already_migrated_result(
                    migration_id=migration_id,
                    dialog=dialog,
                    operator_huid=operator.huid,
                )
            ) is not None:
                return None
            if options.target_strategy not in {None, "create"}:
                return None

            access_profile = await self._target_chat_provisioning_service.get_channel_access_profile(
                source_chat_id=source_chat_id,
                source_backend=source_backend,
            )

            effective_identity_policy = await self._effective_identity_policy(
                migration_id=migration_id,
                source_chat_id=source_chat_id,
                requested=options.identity_policy,
            )
            if effective_identity_policy not in {"skip_unresolved", "matrix_uploaded"}:
                try:
                    participants = await self._target_chat_provisioning_service.list_participant_matrix(
                        source_chat_id=source_chat_id,
                        source_backend=source_backend,
                    )
                except FatalItemError:
                    participants = []
                unresolved_rows = [
                    item
                    for item in participants
                    if not item.is_self and item.target_huid is None
                ]
                if unresolved_rows:
                    resolved_count = sum(
                        1
                        for item in participants
                        if not item.is_self and item.target_huid is not None
                    )
                    workbook_rows = [
                        IdentityMatrixWorkbookRow(
                            telegram_user_id=item.telegram_user_id,
                            telegram_username=item.telegram_username,
                            telegram_display_name=item.telegram_display_name,
                            corporate_email=item.corporate_email,
                            target_huid=item.target_huid,
                            is_self=item.is_self,
                            resolution_source=item.resolution_source,
                            reason=item.reason,
                        )
                        for item in participants
                    ]
                    workbook_content = self._identity_matrix_workbook_service.build_workbook(
                        rows=workbook_rows,
                    )
                    safe_chat_id = source_chat_id.replace("/", "_")
                    return BotChannelChatResolutionRequest(
                        source_chat_id=dialog.dialog_id,
                        source_chat_title=dialog.title,
                        action="upload_identity_matrix",
                        unresolved_count=len(unresolved_rows),
                        resolved_count=resolved_count,
                        workbook_content=workbook_content,
                        workbook_filename=f"channel_identity_matrix_{safe_chat_id}.xlsx",
                        can_list_participants=access_profile.can_list_participants,
                    )

            effective_access_strategy = await self._effective_access_strategy(
                migration_id=migration_id,
                source_chat_id=source_chat_id,
                requested=options.access_strategy,
            )
            if effective_access_strategy is None:
                return BotChannelChatResolutionRequest(
                    source_chat_id=dialog.dialog_id,
                    source_chat_title=dialog.title,
                    action="choose_access_strategy",
                    can_list_participants=access_profile.can_list_participants,
                )
            return None

        return await self._execute_for_source_backends(
            operator=operator,
            source_backends={source_backend},
            runner=run,
        )

    async def apply_group_identity_matrix(
        self,
        *,
        operator: BotOperatorContext,
        source_chat_id: str,
        workbook_content: bytes,
    ) -> BotGroupIdentityImportResult:
        await self._authorize(operator)

        async def run() -> BotGroupIdentityImportResult:
            imported_count, skipped_count = await self._apply_identity_matrix_workbook(
                workbook_content=workbook_content,
                default_ad_domain=operator.ad_domain,
            )

            await self._audit_operator_event(
                self._migration_id_for_tracking(),
                operator,
                event_type="bot_group_identity_matrix_imported",
                payload={
                    "source_chat_id": source_chat_id,
                    "imported_count": imported_count,
                    "skipped_count": skipped_count,
                },
            )
            return BotGroupIdentityImportResult(
                source_chat_id=source_chat_id,
                imported_count=imported_count,
                skipped_count=skipped_count,
            )

        return await self._execute_with_operator_session(operator=operator, runner=run)

    async def apply_channel_identity_matrix(
        self,
        *,
        operator: BotOperatorContext,
        source_chat_id: str,
        workbook_content: bytes,
    ) -> BotGroupIdentityImportResult:
        await self._authorize(operator)

        async def run() -> BotGroupIdentityImportResult:
            imported_count, skipped_count = await self._apply_identity_matrix_workbook(
                workbook_content=workbook_content,
                default_ad_domain=operator.ad_domain,
            )
            await self._audit_operator_event(
                self._migration_id_for_tracking(),
                operator,
                event_type="bot_channel_identity_matrix_imported",
                payload={
                    "source_chat_id": source_chat_id,
                    "imported_count": imported_count,
                    "skipped_count": skipped_count,
                },
            )
            return BotGroupIdentityImportResult(
                source_chat_id=source_chat_id,
                imported_count=imported_count,
                skipped_count=skipped_count,
            )

        return await self._execute_with_operator_session(operator=operator, runner=run)

    async def map_private_chat_peer_identity(
        self,
        *,
        operator: BotOperatorContext,
        source_chat_id: str,
        corporate_email: str | None = None,
        target_huid: str | None = None,
    ) -> BotIdentityMappingResult:
        await self._authorize(operator)

        async def run() -> BotIdentityMappingResult:
            dialog = await self._discover_dialog(source_chat_id)
            if dialog.chat_type != "private":
                raise ConfigurationError(
                    f"source_chat_id={source_chat_id!r} is not a personal Telegram chat",
                )
            peer = await self._private_chat_peer_entry(source_chat_id)
            if peer is None:
                raise ConfigurationError(
                    f"second participant was not discovered for source_chat_id={source_chat_id}",
                )
            if bool(corporate_email) == bool(target_huid):
                raise ConfigurationError(
                    "exactly one of corporate_email or target_huid is required",
                )
            if corporate_email is not None:
                lookup = await self._identity_directory.upsert_mapping(
                    telegram_user_id=peer.telegram_user_id,
                    telegram_username=peer.telegram_username,
                    telegram_display_name=peer.telegram_display_name,
                    corporate_email=corporate_email,
                )
            else:
                lookup = await self._identity_directory.upsert_direct_mapping(
                    telegram_user_id=peer.telegram_user_id,
                    telegram_username=peer.telegram_username,
                    telegram_display_name=peer.telegram_display_name,
                    target_huid=target_huid or "",
                )
            await self._audit_operator_event(
                self._migration_id_for_tracking(),
                operator,
                event_type="bot_private_chat_peer_identity_upserted",
                payload={
                    "source_chat_id": source_chat_id,
                    "telegram_user_id": lookup.telegram_user_id,
                    "telegram_username": lookup.telegram_username,
                    "telegram_display_name": lookup.telegram_display_name,
                    "corporate_email": lookup.corporate_email,
                    "target_huid": lookup.target_huid,
                    "resolution_source": lookup.resolution_source,
                    "reason": lookup.reason,
                },
            )
            return BotIdentityMappingResult(
                telegram_user_id=lookup.telegram_user_id,
                telegram_username=lookup.telegram_username,
                telegram_display_name=lookup.telegram_display_name,
                corporate_email=lookup.corporate_email,
                target_huid=lookup.target_huid,
                resolution_source=lookup.resolution_source,
                reason=lookup.reason,
            )

        return await self._execute_with_operator_session(operator=operator, runner=run)

    async def _apply_identity_matrix_workbook(
        self,
        *,
        workbook_content: bytes,
        default_ad_domain: str | None = None,
    ) -> tuple[int, int]:
        rows = self._identity_matrix_workbook_service.parse_workbook(workbook_content)
        imported_count = 0
        skipped_count = 0
        for row in rows:
            if row.is_self:
                skipped_count += 1
                continue
            selector_count = self._row_selector_count(row)
            if selector_count == 0:
                skipped_count += 1
                continue
            if row.telegram_user_id is None and row.telegram_username is None:
                skipped_count += 1
                continue
            selector_kind, selector_kwargs = self._preferred_row_selector(row)
            if selector_kind == "corporate_email":
                await self._identity_directory.upsert_mapping(
                    telegram_user_id=row.telegram_user_id,
                    telegram_username=row.telegram_username,
                    telegram_display_name=row.telegram_display_name,
                    corporate_email=selector_kwargs["corporate_email"] or "",
                )
            else:
                lookup = await self._identity_directory.resolve_corporate_selector(
                    corporate_email=selector_kwargs["corporate_email"],
                    target_huid=selector_kwargs["target_huid"],
                    ad_login=selector_kwargs["ad_login"],
                    other_id=selector_kwargs["other_id"],
                    default_ad_domain=default_ad_domain,
                )
                if lookup.target_huid is None:
                    raise ConfigurationError(
                        f"workbook row for {row.telegram_display_name or row.telegram_username or row.telegram_user_id or '-'} "
                        f"could not be resolved in eXpress; reason={lookup.reason or 'identity_not_found'}",
                    )
                await self._identity_directory.upsert_direct_mapping(
                    telegram_user_id=row.telegram_user_id,
                    telegram_username=row.telegram_username,
                    telegram_display_name=row.telegram_display_name,
                    target_huid=lookup.target_huid,
                    corporate_email=row.corporate_email,
                )
            imported_count += 1
        return imported_count, skipped_count

    async def start_migrate_all(
        self,
        *,
        operator: BotOperatorContext,
        options: MigrationRunOptions,
    ) -> BotOperationAcceptedResult:
        await self._authorize(operator)
        source_backends = {self._supported_source_backend()}
        async def run() -> BotOperationAcceptedResult:
            manifest = await self._load_or_bootstrap_config_manifest(
                operator=operator,
                options=options,
            )
            await self._validate_manifest_preconditions(manifest)
            source_chat_ids = tuple(dialog.source_chat_id for dialog in manifest.dialogs)
            if not source_chat_ids:
                return BotOperationAcceptedResult(
                    status="blocked",
                    operation="migrate_all",
                    migration_id=manifest.migration_id,
                    source_chat_ids=(),
                    reason="no Telegram chats are configured or available for migration",
                )
            await self._inventory_use_case.execute(InventoryCommand(manifest=manifest))
            return await self._start_fan_out_background_operation(
                operator=operator,
                aggregate_operation="migrate_all",
                job_operation="migrate_chat",
                manifest=manifest,
                source_chat_ids=source_chat_ids,
                options=options,
            )

        return await self._execute_for_source_backends(
            operator=operator,
            source_backends=source_backends,
            runner=run,
        )

    async def start_delta_sync(
        self,
        *,
        operator: BotOperatorContext,
        source_chat_id: str | None = None,
        batch_size: int | None = None,
    ) -> BotOperationAcceptedResult:
        await self._authorize(operator)
        manifest = await self._load_runtime_manifest(
            operator_huid=operator.huid,
            source_chat_id=source_chat_id,
            require_configured=source_chat_id is not None,
        )

        async def run() -> BotOperationAcceptedResult:
            await self._validate_manifest_preconditions(manifest)
            source_chat_ids = tuple(dialog.source_chat_id for dialog in manifest.dialogs)
            if not source_chat_ids:
                return BotOperationAcceptedResult(
                    status="blocked",
                    operation="delta_sync",
                    migration_id=manifest.migration_id,
                    source_chat_ids=(),
                    reason="no Telegram chats are configured or available for delta sync",
                )
            return await self._start_fan_out_background_operation(
                operator=operator,
                aggregate_operation="delta_sync",
                job_operation="delta_sync",
                manifest=manifest,
                source_chat_ids=source_chat_ids,
                options=MigrationRunOptions(
                    progress_policy="resume",
                    batch_size=batch_size or self._settings.backfill.batch_size,
                ),
            )

        return await self._execute_for_source_backends(
            operator=operator,
            source_backends={dialog.source_backend for dialog in manifest.dialogs},
            runner=run,
        )

    async def start_migrate_chat(
        self,
        *,
        operator: BotOperatorContext,
        source_chat_id: str,
        options: MigrationRunOptions,
    ) -> BotOperationAcceptedResult:
        return await self._start_migrate_chat(
            operator=operator,
            source_chat_id=source_chat_id,
            options=options,
            return_existing_if_completed=True,
            require_completed_existing=False,
        )

    async def remigrate_chat(
        self,
        *,
        operator: BotOperatorContext,
        source_chat_id: str,
        options: MigrationRunOptions,
    ) -> BotOperationAcceptedResult:
        return await self._start_migrate_chat(
            operator=operator,
            source_chat_id=source_chat_id,
            options=options,
            return_existing_if_completed=False,
            require_completed_existing=True,
        )

    async def _start_migrate_chat(
        self,
        *,
        operator: BotOperatorContext,
        source_chat_id: str,
        options: MigrationRunOptions,
        return_existing_if_completed: bool,
        require_completed_existing: bool,
    ) -> BotOperationAcceptedResult:
        await self._authorize(operator)
        source_backend = await self._source_backend_for_chat(
            source_chat_id=source_chat_id,
            requested=options.source_backend,
        )
        async def run() -> BotOperationAcceptedResult:
            migration_id = self._require_configured_migration_id()
            existing = await self._chat_migration_config_repository.get(
                migration_id,
                source_chat_id,
            )
            if existing is not None:
                self._ensure_operator_owns_chat_config(
                    record=existing,
                    operator_huid=operator.huid,
                )
            dialog = await self._discover_dialog(
                source_chat_id,
                source_backend=source_backend,
            )
            already_migrated = await self._completed_chat_already_migrated_result(
                migration_id=migration_id,
                dialog=dialog,
                operator_huid=operator.huid,
            )
            if already_migrated is not None and return_existing_if_completed:
                await self._audit_operator_event(
                    migration_id,
                    operator,
                    event_type="bot_existing_chat_migration_returned",
                    payload={
                        "source_chat_id": source_chat_id,
                        "target_chat_id": already_migrated.target_chat_id,
                        "target_chat_link": already_migrated.target_chat_link,
                        "source_chat_type": dialog.chat_type,
                    },
                )
                return already_migrated
            if already_migrated is None and require_completed_existing:
                raise ConfigurationError(
                    f"source_chat_id={source_chat_id!r} has not been migrated yet; use /migrate first",
                )

            requested_topic_strategy = await self._effective_topic_strategy(
                migration_id=migration_id,
                source_chat_id=source_chat_id,
                requested=options.topic_strategy,
            )
            if (
                dialog.has_topics
                and requested_topic_strategy == "split_by_topic"
            ):
                await self._save_split_topic_chat_configs(
                    operator=operator,
                    dialog=dialog,
                    options=options,
                    existing=existing,
                )
            else:
                await self._save_chat_config(
                    operator=operator,
                    dialog=dialog,
                    options=options,
                    existing=existing,
                )
            base_manifest = await self._load_config_manifest(
                source_chat_ids=(source_chat_id,),
                operator_huid=operator.huid,
            )
            manifest_source_chat_ids = tuple(
                dialog.source_chat_id for dialog in base_manifest.dialogs
            )
            manifest = self._apply_run_overrides(
                base_manifest,
                manifest_source_chat_ids,
                options,
            )
            await self._validate_manifest_preconditions(manifest)
            source_chat_ids = tuple(dialog.source_chat_id for dialog in manifest.dialogs)
            if (
                dialog.has_topics
                and requested_topic_strategy == "split_by_topic"
                and len(source_chat_ids) > 1
            ):
                return await self._start_fan_out_background_operation(
                    operator=operator,
                    aggregate_operation="migrate_chat",
                    job_operation="migrate_chat",
                    manifest=manifest,
                    source_chat_ids=source_chat_ids,
                    options=options,
                )
            return await self._start_background_operation(
                operator=operator,
                operation="migrate_chat",
                manifest=manifest,
                source_chat_ids=source_chat_ids,
                options=options,
            )

        return await self._execute_for_source_backends(
            operator=operator,
            source_backends={source_backend},
            runner=run,
        )

    async def replay_failed(
        self,
        *,
        operator: BotOperatorContext,
        source_chat_id: str | None = None,
        limit: int = 100,
        batch_size: int | None = None,
    ) -> ReplayFailedResult:
        await self._authorize(operator)
        manifest = await self._load_runtime_manifest(
            operator_huid=operator.huid,
            source_chat_id=source_chat_id,
            require_configured=source_chat_id is not None,
        )
        async def run() -> ReplayFailedResult:
            await self._audit_operator_event(
                manifest.migration_id,
                operator,
                event_type="bot_replay_failed_requested",
                payload={
                    "source_chat_id": source_chat_id,
                    "limit": limit,
                    "batch_size": batch_size or self._settings.backfill.batch_size,
                },
            )
            per_dialog_commands = (
                source_chat_id is not None
                and all(dialog.source_chat_id != source_chat_id for dialog in manifest.dialogs)
            )
            aggregated = self._empty_replay_failed_result(manifest.migration_id)
            if per_dialog_commands:
                for anchor_cts_host, dialogs in self._group_manifest_dialogs_by_anchor(manifest):
                    with use_express_cts_host(anchor_cts_host):
                        for dialog in dialogs:
                            partial = await self._replay_failed_use_case.execute(
                                ReplayFailedCommand(
                                    manifest=manifest.model_copy(update={"dialogs": [dialog]}),
                                    source_chat_id=dialog.source_chat_id,
                                    limit=limit,
                                    batch_size=batch_size or self._settings.backfill.batch_size,
                                ),
                            )
                            aggregated = self._merge_replay_failed_results(
                                aggregated,
                                partial,
                            )
                return aggregated
            for anchor_cts_host, dialogs in self._group_manifest_dialogs_by_anchor(manifest):
                with use_express_cts_host(anchor_cts_host):
                    partial = await self._replay_failed_use_case.execute(
                        ReplayFailedCommand(
                            manifest=manifest.model_copy(update={"dialogs": dialogs}),
                            source_chat_id=source_chat_id,
                            limit=limit,
                            batch_size=batch_size or self._settings.backfill.batch_size,
                        ),
                    )
                    aggregated = self._merge_replay_failed_results(aggregated, partial)
            return aggregated

        return await self._execute_for_source_backends(
            operator=operator,
            source_backends={dialog.source_backend for dialog in manifest.dialogs},
            runner=run,
        )

    async def reconcile(
        self,
        *,
        operator: BotOperatorContext,
        source_chat_id: str | None = None,
    ) -> ReconcileMigrationResult:
        await self._authorize(operator)
        manifest = await self._load_runtime_manifest(
            operator_huid=operator.huid,
            source_chat_id=source_chat_id,
            require_configured=source_chat_id is not None,
        )
        return await self._reconcile_migration_use_case.execute(
            ReconcileMigrationCommand(
                manifest=manifest,
                source_chat_id=source_chat_id,
            ),
        )

    async def status(
        self,
        *,
        operator: BotOperatorContext,
        source_chat_id: str | None = None,
    ) -> MigrationBotStatusResult:
        await self._authorize(operator)
        manifest = await self._load_runtime_manifest(
            operator_huid=operator.huid,
            source_chat_id=source_chat_id,
            require_configured=source_chat_id is not None,
            include_skipped_in_all=source_chat_id is None,
        )
        relevant_chat_ids = [
            dialog.source_chat_id
            for dialog in manifest.dialogs
            if source_chat_id is None
            or dialog.source_chat_id == source_chat_id
            or dialog.telegram_chat_id == source_chat_id
        ]

        reconcile_result = await self._reconcile_migration_use_case.execute(
            ReconcileMigrationCommand(
                manifest=manifest,
                source_chat_id=source_chat_id,
            ),
        )
        state = await self._migration_state_repository.get(manifest.migration_id)
        checkpoints: list[BotChatCheckpointStatus] = []
        for chat_id in relevant_chat_ids:
            backfill = await self._checkpoint_repository.get(
                manifest.migration_id,
                chat_id,
                "backfill",
            )
            delta = await self._checkpoint_repository.get(
                manifest.migration_id,
                chat_id,
                "delta",
            )
            checkpoints.append(
                BotChatCheckpointStatus(
                    source_chat_id=chat_id,
                    backfill_last_source_message_id=(
                        backfill.cursor.last_source_message_id if backfill else None
                    ),
                    delta_last_source_message_id=(
                        delta.cursor.last_source_message_id if delta else None
                    ),
                ),
            )

        return MigrationBotStatusResult(
            migration_id=manifest.migration_id,
            migration_state=state.status if state is not None else None,
            active_jobs=tuple(
                await self._list_active_jobs(
                    manifest.migration_id,
                    operator_huid=operator.huid,
                ),
            ),
            reconcile=reconcile_result,
            chat_checkpoints=tuple(checkpoints),
        )

    async def stats(
        self,
        *,
        operator: BotOperatorContext,
    ) -> BotMigrationStatsResult:
        await self._authorize(operator)
        all_configs = await self._chat_migration_config_repository.list_by_migration(
            self._require_configured_migration_id(),
        )
        configs = [
            record
            for record in all_configs
            if record.updated_by_huid == operator.huid
        ]
        foreign_managed_chats = sum(
            1
            for record in all_configs
            if record.updated_by_huid != operator.huid
        )
        included_configs = [
            config
            for config in configs
            if not config.skip_in_all and not self._is_split_root_config(config)
        ]
        if not included_configs:
            return BotMigrationStatsResult(
                migration_id=self._require_configured_migration_id(),
                configured_chats=len(configs),
                foreign_managed_chats=foreign_managed_chats,
                skipped_in_all_chats=sum(1 for config in configs if config.skip_in_all),
                active_jobs=0,
                chats_with_progress=0,
                attention_chats=0,
                imported_messages=0,
                failed_messages=0,
                ambiguous_messages=0,
                imported_attachments=0,
                failed_attachments=0,
            )
        status_result = await self.status(operator=operator)
        chats_with_progress = sum(
            1
            for chat in status_result.reconcile.chats
            if (
                chat.imported_count
                or chat.failed_count
                or chat.ambiguous_count
                or chat.processing_count
                or chat.attachment_imported_count
                or chat.attachment_failed_count
                or chat.attachment_ambiguous_count
                or chat.attachment_processing_count
            )
        )
        return BotMigrationStatsResult(
            migration_id=status_result.migration_id,
            configured_chats=len(configs),
            foreign_managed_chats=foreign_managed_chats,
            skipped_in_all_chats=sum(1 for config in configs if config.skip_in_all),
            active_jobs=len(status_result.active_jobs),
            chats_with_progress=chats_with_progress,
            attention_chats=status_result.reconcile.attention_chats,
            imported_messages=status_result.reconcile.imported_count,
            failed_messages=status_result.reconcile.failed_count,
            ambiguous_messages=status_result.reconcile.ambiguous_count,
            imported_attachments=status_result.reconcile.attachment_imported_count,
            failed_attachments=status_result.reconcile.attachment_failed_count,
        )

    async def cancel_active_jobs(
        self,
        *,
        operator: BotOperatorContext,
        source_chat_id: str | None = None,
    ) -> BotCancelResult:
        await self._authorize(operator)
        migration_id = self._require_configured_migration_id()
        normalized_source_chat_id = (source_chat_id or "").strip() or None
        active_jobs = [
            job
            for job in await self._migration_job_repository.list_active(migration_id)
            if (
                job.operator_huid == operator.huid
                and self._is_effectively_active_job(job)
                and (
                    normalized_source_chat_id is None
                    or normalized_source_chat_id in job.source_chat_ids
                )
            )
        ]
        cancelled_queued_jobs: list[BotActiveJob] = []
        cancellation_requested_jobs: list[BotActiveJob] = []
        for job in active_jobs:
            updated = await self._migration_job_repository.request_cancel(
                job_key=job.job_key,
                operator_huid=operator.huid,
            )
            if updated is None:
                continue
            active_job = self._bot_active_job_from_record(updated)
            if job.status is MigrationJobStatus.QUEUED:
                cancelled_queued_jobs.append(active_job)
            else:
                cancellation_requested_jobs.append(active_job)
        await self._audit_operator_event(
            migration_id,
            operator,
            event_type="bot_migration_jobs_cancel_requested",
            payload={
                "source_chat_id": normalized_source_chat_id,
                "cancelled_queued_job_keys": [job.job_key for job in cancelled_queued_jobs],
                "running_job_keys": [job.job_key for job in cancellation_requested_jobs],
            },
        )
        return BotCancelResult(
            migration_id=migration_id,
            source_chat_id=normalized_source_chat_id,
            cancelled_queued_jobs=tuple(cancelled_queued_jobs),
            cancellation_requested_jobs=tuple(cancellation_requested_jobs),
        )

    async def pause_delta(
        self,
        *,
        operator: BotOperatorContext,
    ) -> MigrationStateControlResult:
        await self._authorize(operator)
        return await self._pause_delta_use_case.execute(
            PauseDeltaCommand(manifest=self._migration_stub_manifest()),
        )

    async def resume_delta(
        self,
        *,
        operator: BotOperatorContext,
    ) -> MigrationStateControlResult:
        await self._authorize(operator)
        return await self._resume_delta_use_case.execute(
            ResumeDeltaCommand(manifest=self._migration_stub_manifest()),
        )

    async def execute_migration_job(
        self,
        job: MigrationJobRecord,
    ) -> None:
        operator = BotOperatorContext(
            huid=job.operator_huid,
            chat_id="migration-worker",
            display_name="migration-worker",
            current_cts_host=job.anchor_cts_host,
            current_bot_id=job.anchor_bot_id,
        )
        manifest = await self._load_config_manifest(
            source_chat_ids=job.source_chat_ids or None,
            require_configured=True,
            operator_huid=job.operator_huid,
        )

        async def run() -> None:
            await self._validate_manifest_preconditions(manifest)
            if job.operation == "migrate_all":
                await self._run_migrate_all_job(
                    manifest=manifest,
                    batch_size=job.batch_size,
                )
                return
            if job.operation == "migrate_chat":
                await self._run_migrate_chat_job(
                    manifest=manifest,
                    source_chat_ids=job.source_chat_ids,
                    batch_size=job.batch_size,
                )
                return
            if job.operation == "import_archive_chat":
                await self._run_migrate_chat_job(
                    manifest=manifest,
                    source_chat_ids=job.source_chat_ids,
                    batch_size=job.batch_size,
                )
                return
            if job.operation == "delta_sync":
                await self._run_delta_sync_job(
                    manifest=manifest,
                    source_chat_ids=job.source_chat_ids,
                    batch_size=job.batch_size,
                )
                return
            raise ConfigurationError(f"unsupported migration job operation={job.operation!r}")
        anchor_cts_host = job.anchor_cts_host or self._resolve_manifest_anchor_cts_host(
            manifest,
            source_chat_ids=job.source_chat_ids,
        )
        with use_express_cts_host(anchor_cts_host):
            await self._execute_for_source_backends(
                operator=operator,
                source_backends={dialog.source_backend for dialog in manifest.dialogs},
                runner=run,
            )

    async def _list_active_jobs(
        self,
        migration_id: str,
        *,
        operator_huid: str | None = None,
    ) -> list[BotActiveJob]:
        jobs = [
            job
            for job in await self._migration_job_repository.list_active(migration_id)
            if self._is_effectively_active_job(job)
            and (operator_huid is None or job.operator_huid == operator_huid)
        ]
        return [self._bot_active_job_from_record(job) for job in jobs]

    async def _start_background_operation(
        self,
        *,
        operator: BotOperatorContext,
        operation: str,
        manifest: MigrationManifest,
        source_chat_ids: tuple[str, ...],
        options: MigrationRunOptions,
    ) -> BotOperationAcceptedResult:
        active_jobs = tuple(
            job
            for job in await self._list_active_jobs(manifest.migration_id)
            if self._jobs_conflict(job.source_chat_ids, source_chat_ids)
        )
        if active_jobs:
            return BotOperationAcceptedResult(
                status="already_running",
                operation=operation,
                migration_id=manifest.migration_id,
                source_chat_ids=source_chat_ids,
                reason="another bot-managed migration job is already running",
                active_jobs=active_jobs,
                already_running_source_chat_ids=source_chat_ids,
            )

        backpressure_reason = await self._backpressure_reason()
        if backpressure_reason is not None:
            await self._audit_operator_event(
                manifest.migration_id,
                operator,
                event_type="bot_migration_backpressure_blocked",
                payload={
                    "operation": operation,
                    "source_chat_ids": source_chat_ids,
                    "reason": backpressure_reason,
                },
            )
            return BotOperationAcceptedResult(
                status="blocked",
                operation=operation,
                migration_id=manifest.migration_id,
                source_chat_ids=source_chat_ids,
                blocked_source_chat_ids=source_chat_ids,
                reason=backpressure_reason,
            )

        progress_hints = tuple(
            await self._progress_hints(manifest.migration_id, source_chat_ids),
        )
        inconsistent_progress_reason = self._detect_inconsistent_progress(progress_hints)
        if inconsistent_progress_reason is not None:
            await self._audit_operator_event(
                manifest.migration_id,
                operator,
                event_type="bot_migration_inconsistent_progress_blocked",
                payload={
                    "operation": operation,
                    "source_chat_ids": source_chat_ids,
                    "progress_hints": [
                        {
                            "source_chat_id": hint.source_chat_id,
                            "mapped_total": hint.mapped_total,
                            "imported_count": hint.imported_count,
                            "last_source_message_id": hint.last_source_message_id,
                        }
                        for hint in progress_hints
                    ],
                    "reason": inconsistent_progress_reason,
                },
            )
            return BotOperationAcceptedResult(
                status="blocked",
                operation=operation,
                migration_id=manifest.migration_id,
                source_chat_ids=source_chat_ids,
                blocked_source_chat_ids=source_chat_ids,
                reason=inconsistent_progress_reason,
            )
        if options.progress_policy == "ask" and progress_hints:
            await self._audit_operator_event(
                manifest.migration_id,
                operator,
                event_type="bot_migration_progress_confirmation_required",
                payload={
                    "operation": operation,
                    "source_chat_ids": source_chat_ids,
                    "progress_hints": [
                        {
                            "source_chat_id": hint.source_chat_id,
                            "mapped_total": hint.mapped_total,
                            "imported_count": hint.imported_count,
                            "last_source_message_id": hint.last_source_message_id,
                        }
                        for hint in progress_hints
                    ],
                },
            )
            return BotOperationAcceptedResult(
                status="blocked",
                operation=operation,
                migration_id=manifest.migration_id,
                source_chat_ids=source_chat_ids,
                blocked_source_chat_ids=source_chat_ids,
                reason="existing progress detected; explicit resume confirmation required",
                progress_hints=progress_hints,
            )

        job_key = self._job_key(operation, manifest.migration_id, source_chat_ids)
        job = BotActiveJob(
            job_key=job_key,
            operation=operation,
            migration_id=manifest.migration_id,
            source_chat_ids=source_chat_ids,
            started_at=self._now(),
        )
        anchor_cts_host = self._resolve_manifest_anchor_cts_host(
            manifest,
            source_chat_ids=source_chat_ids,
        ) or self._default_anchor_cts_host(
            operator=operator,
        )
        anchor_bot_id = self._resolve_manifest_anchor_bot_id(
            manifest,
            source_chat_ids=source_chat_ids,
        ) or self._configured_bot_id_for_cts_host(anchor_cts_host) or (
            (operator.current_bot_id or "").strip() or None
        )
        registered = await self._migration_job_repository.enqueue(
            MigrationJobRecord(
                job_key=job.job_key,
                migration_id=job.migration_id,
                operation=job.operation,
                operator_huid=operator.huid,
                source_chat_ids=job.source_chat_ids,
                anchor_cts_host=anchor_cts_host,
                anchor_bot_id=anchor_bot_id,
                batch_size=options.batch_size or self._settings.backfill.batch_size,
                status=MigrationJobStatus.QUEUED,
                requested_at=job.started_at,
            ),
            outbox_events=(
                self._build_job_requested_outbox_event(
                    job=job,
                    operator=operator,
                    options=options,
                    anchor_cts_host=anchor_cts_host,
                    anchor_bot_id=anchor_bot_id,
                ),
            ),
        )
        if not registered:
            return BotOperationAcceptedResult(
                status="already_running",
                operation=operation,
                migration_id=manifest.migration_id,
                source_chat_ids=source_chat_ids,
                already_running_source_chat_ids=source_chat_ids,
                reason="job with the same key is already running",
            )

        await self._audit_operator_event(
            manifest.migration_id,
            operator,
            event_type="bot_migration_job_accepted",
            payload={
                "operation": operation,
                "job_key": job_key,
                "source_chat_ids": source_chat_ids,
                "options": {
                    "include_from": options.include_from,
                    "include_to": options.include_to,
                    "migrate_media": options.migrate_media,
                    "reply_mode": options.reply_mode,
                    "progress_policy": options.progress_policy,
                    "batch_size": options.batch_size,
                },
            },
        )

        return BotOperationAcceptedResult(
            status="accepted",
            operation=operation,
            migration_id=manifest.migration_id,
            source_chat_ids=source_chat_ids,
            job_key=job_key,
            job_keys=(job_key,),
            accepted_source_chat_ids=source_chat_ids,
        )

    async def _backpressure_reason(self) -> str | None:
        counts = await self._migration_job_repository.count_by_status()
        queued = counts.get(MigrationJobStatus.QUEUED, 0)
        running = counts.get(MigrationJobStatus.RUNNING, 0)
        max_queued = max(0, self._settings.worker.backpressure_max_queued_jobs)
        max_running = max(0, self._settings.worker.backpressure_max_running_jobs)
        if max_queued and queued >= max_queued:
            return (
                "runtime backpressure is active: queued migration jobs reached "
                f"{queued}, threshold={max_queued}"
            )
        if max_running and running >= max_running:
            return (
                "runtime backpressure is active: running migration jobs reached "
                f"{running}, threshold={max_running}"
            )
        return None

    async def _start_fan_out_background_operation(
        self,
        *,
        operator: BotOperatorContext,
        aggregate_operation: str,
        job_operation: str,
        manifest: MigrationManifest,
        source_chat_ids: tuple[str, ...],
        options: MigrationRunOptions,
    ) -> BotOperationAcceptedResult:
        accepted_source_chat_ids: list[str] = []
        accepted_job_keys: list[str] = []
        blocked_source_chat_ids: list[str] = []
        already_running_source_chat_ids: list[str] = []
        progress_hints_by_chat: dict[str, BotProgressHint] = {}
        active_jobs_by_key: dict[str, BotActiveJob] = {}

        for source_chat_id in source_chat_ids:
            result = await self._start_background_operation(
                operator=operator,
                operation=job_operation,
                manifest=manifest,
                source_chat_ids=(source_chat_id,),
                options=options,
            )
            if result.status == "accepted":
                accepted_source_chat_ids.extend(result.accepted_source_chat_ids or result.source_chat_ids)
                if result.job_key is not None:
                    accepted_job_keys.append(result.job_key)
                continue
            if result.status == "already_running":
                already_running_source_chat_ids.extend(
                    result.already_running_source_chat_ids or result.source_chat_ids,
                )
                for job in result.active_jobs:
                    active_jobs_by_key[job.job_key] = job
                continue
            blocked_source_chat_ids.extend(
                result.blocked_source_chat_ids or result.source_chat_ids,
            )
            for hint in result.progress_hints:
                progress_hints_by_chat[hint.source_chat_id] = hint

        if accepted_job_keys:
            return BotOperationAcceptedResult(
                status="accepted",
                operation=aggregate_operation,
                migration_id=manifest.migration_id,
                source_chat_ids=source_chat_ids,
                job_key=accepted_job_keys[0] if len(accepted_job_keys) == 1 else None,
                job_keys=tuple(accepted_job_keys),
                active_jobs=tuple(active_jobs_by_key.values()),
                progress_hints=tuple(progress_hints_by_chat.values()),
                accepted_source_chat_ids=tuple(accepted_source_chat_ids),
                blocked_source_chat_ids=tuple(blocked_source_chat_ids),
                already_running_source_chat_ids=tuple(already_running_source_chat_ids),
                reason=(
                    "some chats were not enqueued"
                    if blocked_source_chat_ids or already_running_source_chat_ids
                    else None
                ),
            )
        if already_running_source_chat_ids:
            return BotOperationAcceptedResult(
                status="already_running",
                operation=aggregate_operation,
                migration_id=manifest.migration_id,
                source_chat_ids=source_chat_ids,
                active_jobs=tuple(active_jobs_by_key.values()),
                progress_hints=tuple(progress_hints_by_chat.values()),
                blocked_source_chat_ids=tuple(blocked_source_chat_ids),
                already_running_source_chat_ids=tuple(already_running_source_chat_ids),
                reason="all selected chats already have active bot-managed jobs",
            )
        return BotOperationAcceptedResult(
            status="blocked",
            operation=aggregate_operation,
            migration_id=manifest.migration_id,
            source_chat_ids=source_chat_ids,
            progress_hints=tuple(progress_hints_by_chat.values()),
            blocked_source_chat_ids=tuple(blocked_source_chat_ids),
            reason="all selected chats are blocked",
        )

    def _build_job_requested_outbox_event(
        self,
        *,
        job: BotActiveJob,
        operator: BotOperatorContext,
        options: MigrationRunOptions,
        anchor_cts_host: str | None,
        anchor_bot_id: str | None,
    ) -> IntegrationOutboxEventRecord:
        now = self._now()
        return IntegrationOutboxEventRecord(
            event_id=uuid4().hex,
            aggregate_type="migration_job",
            aggregate_id=job.job_key,
            event_type="migration.job.requested",
            payload_json={
                "job_key": job.job_key,
                "migration_id": job.migration_id,
                "operation": job.operation,
                "operator_huid": operator.huid,
                "source_chat_ids": list(job.source_chat_ids),
                "anchor_cts_host": anchor_cts_host,
                "anchor_bot_id": anchor_bot_id,
                "options": {
                    "include_from": options.include_from,
                    "include_to": options.include_to,
                    "migrate_media": options.migrate_media,
                    "reply_mode": options.reply_mode,
                    "progress_policy": options.progress_policy,
                    "batch_size": options.batch_size,
                },
            },
            headers_json={
                "schema_version": 1,
                "source": "bot_control",
                "shadow_topic": "migration.commands",
            },
            created_at=now,
        )

    async def _run_migrate_all_job(
        self,
        *,
        manifest: MigrationManifest,
        batch_size: int,
    ) -> None:
        await self._inventory_use_case.execute(InventoryCommand(manifest=manifest))
        failures: dict[str, str] = {}
        for dialog in manifest.dialogs:
            try:
                await self._backfill_chat_use_case.execute(
                    BackfillChatCommand(
                        manifest=manifest,
                        source_chat_id=dialog.source_chat_id,
                        batch_size=batch_size,
                    ),
                )
            except Exception as error:
                failures[dialog.source_chat_id] = str(error)
                self._logger.exception(
                    "bot-managed migrate-all chat failed",
                    migration_id=manifest.migration_id,
                    source_chat_id=dialog.source_chat_id,
                )
                await self._audit_repository.add(
                    AuditEvent(
                        migration_id=manifest.migration_id,
                        source_chat_id=dialog.source_chat_id,
                        event_type="bot_migrate_all_chat_failed",
                        severity=AuditSeverity.ERROR,
                        payload_json={"error": str(error)},
                        created_at=self._now(),
                    ),
                )
        if failures:
            await self._audit_repository.add(
                AuditEvent(
                    migration_id=manifest.migration_id,
                    event_type="bot_migrate_all_completed_with_failures",
                    severity=AuditSeverity.WARNING,
                    payload_json={"failed_chats": failures},
                    created_at=self._now(),
                ),
            )

    async def _run_migrate_chat_job(
        self,
        *,
        manifest: MigrationManifest,
        source_chat_ids: tuple[str, ...],
        batch_size: int,
    ) -> None:
        selected_ids = set(source_chat_ids)
        failures: dict[str, str] = {}
        for dialog in manifest.dialogs:
            if dialog.source_chat_id not in selected_ids:
                continue
            try:
                await self._backfill_chat_use_case.execute(
                    BackfillChatCommand(
                        manifest=manifest,
                        source_chat_id=dialog.source_chat_id,
                        batch_size=batch_size,
                    ),
                )
            except Exception as error:
                failures[dialog.source_chat_id] = str(error)
                self._logger.exception(
                    "bot-managed migrate-chat dialog failed",
                    migration_id=manifest.migration_id,
                    source_chat_id=dialog.source_chat_id,
                )
                await self._audit_repository.add(
                    AuditEvent(
                        migration_id=manifest.migration_id,
                        source_chat_id=dialog.source_chat_id,
                        event_type="bot_migrate_chat_dialog_failed",
                        severity=AuditSeverity.ERROR,
                        payload_json={"error": str(error)},
                        created_at=self._now(),
                    ),
                )
        if failures:
            await self._audit_repository.add(
                AuditEvent(
                    migration_id=manifest.migration_id,
                    event_type="bot_migrate_chat_completed_with_failures",
                    severity=AuditSeverity.WARNING,
                    payload_json={"failed_chats": failures},
                    created_at=self._now(),
                ),
            )

    async def _run_delta_sync_job(
        self,
        *,
        manifest: MigrationManifest,
        source_chat_ids: tuple[str, ...],
        batch_size: int,
    ) -> None:
        await self._delta_sync_use_case.execute(
            DeltaSyncCommand(
                manifest=manifest,
                source_chat_ids=list(source_chat_ids) if source_chat_ids else None,
                batch_size=batch_size,
            ),
        )

    async def _progress_hints(
        self,
        migration_id: str,
        source_chat_ids: tuple[str, ...],
    ) -> list[BotProgressHint]:
        return await self._progress_hints_for_query(
            migration_id,
            source_chat_ids=source_chat_ids,
        )

    async def _progress_hints_for_query(
        self,
        migration_id: str,
        *,
        source_chat_ids: tuple[str, ...] | None,
    ) -> list[BotProgressHint]:
        summaries = await self._message_mapping_repository.summarize_by_chat(migration_id)
        by_chat: dict[str, list[MessageStatusSummary]] = defaultdict(list)
        for summary in summaries:
            if source_chat_ids is None or summary.source_chat_id in source_chat_ids:
                by_chat[summary.source_chat_id].append(summary)

        checkpoint_ids: list[str]
        if source_chat_ids is None:
            checkpoint_ids = sorted(by_chat)
        else:
            checkpoint_ids = list(source_chat_ids)

        hints: list[BotProgressHint] = []
        for source_chat_id in checkpoint_ids:
            checkpoint = await self._checkpoint_repository.get(
                migration_id,
                source_chat_id,
                "backfill",
            )
            chat_summaries = by_chat.get(source_chat_id, [])
            mapped_total = sum(summary.count for summary in chat_summaries)
            imported_count = sum(
                summary.count
                for summary in chat_summaries
                if summary.import_status is MessageImportStatus.IMPORTED
            )
            if checkpoint is None and mapped_total == 0:
                continue
            hints.append(
                BotProgressHint(
                    source_chat_id=source_chat_id,
                    mapped_total=mapped_total,
                    imported_count=imported_count,
                    last_source_message_id=(
                        checkpoint.cursor.last_source_message_id if checkpoint else None
                    ),
                ),
            )
        return hints

    def _detect_inconsistent_progress(
        self,
        progress_hints: tuple[BotProgressHint, ...],
    ) -> str | None:
        inconsistent_hints = [
            hint
            for hint in progress_hints
            if hint.last_source_message_id is not None and hint.mapped_total == 0
        ]
        if not inconsistent_hints:
            return None
        rendered = ", ".join(
            (
                f"{hint.source_chat_id}"
                f"(last_source_message_id={hint.last_source_message_id})"
            )
            for hint in inconsistent_hints
        )
        return (
            "inconsistent migration progress detected: backfill checkpoint exists, "
            "but message registry rows are missing for "
            f"{rendered}. Automatic resume is blocked because it can create a fresh "
            "target chat with no historical messages or duplicate an already migrated chat. "
            "Re-bind the original target chat or explicitly reset checkpoint/chat mapping "
            "before rerunning /migrate."
        )

    async def _completed_chat_already_migrated_result(
        self,
        *,
        migration_id: str,
        dialog: SourceDialog,
        operator_huid: str | None = None,
    ) -> BotOperationAcceptedResult | None:
        config = await self._chat_migration_config_repository.get(
            migration_id,
            dialog.dialog_id,
        )
        if config is not None:
            self._ensure_operator_owns_chat_config(
                record=config,
                operator_huid=operator_huid,
            )
        existing = await self._chat_mapping_repository.get(migration_id, dialog.dialog_id)
        if existing is not None and existing.status == "completed":
            progress_hints = tuple(
                await self._progress_hints_for_query(
                    migration_id,
                    source_chat_ids=(dialog.dialog_id,),
                ),
            )
            with use_express_cts_host(existing.anchor_cts_host):
                target_chat_link = await self._express_gateway.create_chat_link(
                    existing.target_chat_id,
                )
            return BotOperationAcceptedResult(
                status="already_migrated",
                operation="migrate_chat",
                migration_id=migration_id,
                source_chat_ids=(dialog.dialog_id,),
                reason=(
                    f"{dialog.chat_type} chat has already been migrated; returning the existing target chat"
                ),
                progress_hints=progress_hints,
                target_chat_id=existing.target_chat_id,
                target_chat_title=existing.target_chat_title,
                target_chat_link=target_chat_link,
            )
        split_configs = await self._split_topic_configs_for_root(
            migration_id=migration_id,
            source_chat_id=dialog.dialog_id,
        )
        if not split_configs:
            return None
        topic_targets: list[BotTopicTarget] = []
        for config in split_configs:
            topic_mapping = await self._chat_mapping_repository.get(
                migration_id,
                config.source_chat_id,
            )
            if topic_mapping is None or topic_mapping.status != "completed":
                return None
            with use_express_cts_host(topic_mapping.anchor_cts_host):
                target_chat_link = await self._express_gateway.create_chat_link(
                    topic_mapping.target_chat_id,
                )
            topic_targets.append(
                BotTopicTarget(
                    source_chat_id=config.source_chat_id,
                    topic_id=config.source_topic_id,
                    topic_title=config.source_thread_title or config.target_chat_title or config.source_chat_title,
                    target_chat_id=topic_mapping.target_chat_id,
                    target_chat_title=topic_mapping.target_chat_title,
                    target_chat_link=target_chat_link,
                ),
            )
        progress_hints = tuple(
            await self._progress_hints_for_query(
                migration_id,
                source_chat_ids=tuple(config.source_chat_id for config in split_configs),
            ),
        )
        return BotOperationAcceptedResult(
            status="already_migrated",
            operation="migrate_chat",
            migration_id=migration_id,
            source_chat_ids=(dialog.dialog_id,),
            reason=(
                "forum supergroup has already been migrated by topic; returning the existing target chats"
            ),
            progress_hints=progress_hints,
            topic_targets=tuple(topic_targets),
        )

    async def _effective_identity_policy(
        self,
        *,
        migration_id: str,
        source_chat_id: str,
        requested: str | None,
    ) -> str | None:
        if requested is not None:
            normalized = requested.strip().lower()
            return normalized or None
        existing = await self._chat_migration_config_repository.get(migration_id, source_chat_id)
        if existing is None:
            return None
        normalized = existing.identity_policy.strip().lower()
        return normalized or None

    async def _effective_access_strategy(
        self,
        *,
        migration_id: str,
        source_chat_id: str,
        requested: str | None,
    ) -> str | None:
        if requested is not None:
            normalized = requested.strip().lower()
            return normalized or None
        existing = await self._chat_migration_config_repository.get(migration_id, source_chat_id)
        if existing is None:
            return None
        normalized = existing.access_strategy.strip().lower()
        return normalized or None

    async def _effective_topic_strategy(
        self,
        *,
        migration_id: str,
        source_chat_id: str,
        requested: str | None,
    ) -> str | None:
        if requested is not None:
            normalized = requested.strip().lower()
            return normalized or None
        existing = await self._chat_migration_config_repository.get(migration_id, source_chat_id)
        if existing is not None:
            normalized = existing.topic_strategy.strip().lower()
            if normalized:
                return normalized
        split_configs = await self._split_topic_configs_for_root(
            migration_id=migration_id,
            source_chat_id=source_chat_id,
        )
        if split_configs:
            return "split_by_topic"
        return None

    async def _migrated_chat_bindings(
        self,
        *,
        migration_id: str,
        operator_huid: str,
    ) -> tuple[_MigratedChatBinding, ...]:
        records = await self._chat_migration_config_repository.list_by_migration(migration_id)
        bindings: list[_MigratedChatBinding] = []
        for record in records:
            if record.updated_by_huid != operator_huid:
                continue
            if record.source_chat_type == "private":
                continue
            mapping = await self._chat_mapping_repository.get(
                migration_id,
                record.source_chat_id,
            )
            if mapping is None:
                continue
            bindings.append(_MigratedChatBinding(config=record, mapping=mapping))
        bindings.sort(
            key=lambda item: (
                item.config.source_chat_title.casefold(),
                item.config.source_chat_id,
            ),
        )
        return tuple(bindings)

    async def _owned_chat_configs(
        self,
        *,
        operator_huid: str,
    ) -> list[ChatMigrationConfigRecord]:
        records = await self._chat_migration_config_repository.list_by_migration(
            self._require_configured_migration_id(),
        )
        return [
            record
            for record in records
            if record.updated_by_huid == operator_huid
        ]

    async def _resolve_migrated_chat_binding(
        self,
        *,
        migration_id: str,
        operator_huid: str,
        chat_selector: str,
    ) -> _MigratedChatBinding:
        normalized_selector = chat_selector.strip()
        if not normalized_selector:
            raise ConfigurationError("chat selection is required")
        bindings = await self._migrated_chat_bindings(
            migration_id=migration_id,
            operator_huid=operator_huid,
        )
        for binding in bindings:
            if normalized_selector in {
                binding.config.source_chat_id,
                binding.mapping.target_chat_id,
            }:
                return binding
        raise ConfigurationError(
            "selected chat is not in the migrated chat list; send source_chat_id or mention the target chat",
        )

    def _row_selector_count(self, row: IdentityMatrixWorkbookRow) -> int:
        return sum(
            1
            for value in (
                row.corporate_email,
                row.target_huid,
                row.ad_login,
                row.other_id,
            )
            if value is not None and value.strip()
        )

    def _preferred_row_selector(
        self,
        row: IdentityMatrixWorkbookRow,
    ) -> tuple[str, dict[str, str | None]]:
        if row.target_huid is not None and row.target_huid.strip():
            return "target_huid", {
                "corporate_email": None,
                "target_huid": row.target_huid,
                "ad_login": None,
                "other_id": None,
            }
        if row.corporate_email is not None and row.corporate_email.strip():
            return "corporate_email", {
                "corporate_email": row.corporate_email,
                "target_huid": None,
                "ad_login": None,
                "other_id": None,
            }
        if row.ad_login is not None and row.ad_login.strip():
            return "ad_login", {
                "corporate_email": None,
                "target_huid": None,
                "ad_login": row.ad_login,
                "other_id": None,
            }
        if row.other_id is not None and row.other_id.strip():
            return "other_id", {
                "corporate_email": None,
                "target_huid": None,
                "ad_login": None,
                "other_id": row.other_id,
            }
        raise ConfigurationError("workbook row does not contain a supported corporate selector")

    def _workbook_row_label(
        self,
        row: IdentityMatrixWorkbookRow,
        *,
        fallback_huid: str,
    ) -> str:
        if row.target_huid is not None and row.target_huid.strip():
            return f"huid={row.target_huid.strip()}"
        if row.corporate_email is not None and row.corporate_email.strip():
            return f"email={row.corporate_email.strip().lower()}"
        if row.ad_login is not None and row.ad_login.strip():
            return f"ad_login={row.ad_login.strip()}"
        if row.other_id is not None and row.other_id.strip():
            return f"other_id={row.other_id.strip()}"
        if row.telegram_username is not None and row.telegram_username.strip():
            return f"telegram_username={row.telegram_username.strip()}"
        if row.telegram_user_id is not None and row.telegram_user_id.strip():
            return f"telegram_user_id={row.telegram_user_id.strip()}"
        if row.telegram_display_name.strip():
            return f"display_name={row.telegram_display_name.strip()}"
        return f"huid={fallback_huid}"

    async def _private_chat_peer_entry(
        self,
        source_chat_id: str,
    ) -> BotChatUserMatrixEntry | None:
        participants = await self._target_chat_provisioning_service.list_participant_matrix(
            source_chat_id=source_chat_id,
        )
        for item in participants:
            if item.is_self:
                continue
            return BotChatUserMatrixEntry(
                telegram_user_id=item.telegram_user_id,
                telegram_username=item.telegram_username,
                telegram_display_name=item.telegram_display_name,
                is_self=item.is_self,
                corporate_email=item.corporate_email,
                target_huid=item.target_huid,
                resolution_source=item.resolution_source,
                reason=item.reason,
            )
        return None

    async def _authorize(self, operator: BotOperatorContext) -> None:
        allowed_huids = set(self._settings.bot.operator_huids)
        if not allowed_huids:
            return
        if operator.huid not in allowed_huids:
            raise FatalItemError(
                f"user huid={operator.huid} is not allowed to manage migrations",
            )

    def _ensure_operator_owns_chat_config(
        self,
        *,
        record: ChatMigrationConfigRecord,
        operator_huid: str | None,
    ) -> None:
        if operator_huid is None:
            return
        if record.updated_by_huid == operator_huid:
            return
        raise ConfigurationError(
            f"source_chat_id={record.source_chat_id!r} is already managed by another operator",
        )

    async def _execute_with_operator_session(
        self,
        *,
        operator: BotOperatorContext,
        runner,
    ):
        session_string = await self._telegram_session_service.resolve_session_string(
            operator_huid=operator.huid,
            mark_used=True,
        )
        with self._telegram_session_context.use(
            session_string,
            operator_huid=operator.huid,
        ):
            return await runner()

    async def _execute_for_source_backends(
        self,
        *,
        operator: BotOperatorContext,
        source_backends: set[str],
        runner,
    ):
        for source_backend in source_backends:
            self._ensure_supported_source_backend(source_backend)
        if any(self._source_backend_requires_operator_session(item) for item in source_backends):
            return await self._execute_with_operator_session(operator=operator, runner=runner)
        return await runner()

    async def _source_backend_for_chat(
        self,
        *,
        source_chat_id: str,
        requested: str | None = None,
    ) -> str:
        if requested is not None:
            return self._ensure_supported_source_backend(requested)
        migration_id = self._require_configured_migration_id()
        existing = await self._chat_migration_config_repository.get(migration_id, source_chat_id)
        if existing is not None:
            return self._ensure_supported_source_backend(existing.source_backend)
        return self._supported_source_backend()

    def _supported_source_backend(self) -> str:
        return "telethon_user_session"

    def _supported_source_backends(self) -> tuple[str, ...]:
        return (
            "telethon_user_session",
            "telegram_export_archive",
        )

    def _source_backend_requires_operator_session(
        self,
        source_backend: str | None,
    ) -> bool:
        normalized = (source_backend or self._supported_source_backend()).strip()
        return normalized != "telegram_export_archive"

    def _ensure_supported_source_backend(self, source_backend: str | None) -> str:
        normalized = (source_backend or self._supported_source_backend()).strip()
        if normalized not in set(self._supported_source_backends()):
            raise ConfigurationError(
                "Unsupported source backend for bot-managed runtime. "
                "Supported values: telethon_user_session, telegram_export_archive.",
            )
        return normalized

    def _is_effectively_active_job(
        self,
        job: MigrationJobRecord,
    ) -> bool:
        if job.status is MigrationJobStatus.QUEUED:
            return True
        if job.status is not MigrationJobStatus.RUNNING:
            return False
        if job.lease_expires_at is None:
            return True
        return job.lease_expires_at >= datetime.now(tz=UTC)

    def _bot_active_job_from_record(
        self,
        job: MigrationJobRecord,
    ) -> BotActiveJob:
        return BotActiveJob(
            job_key=job.job_key,
            operation=job.operation,
            migration_id=job.migration_id,
            source_chat_ids=job.source_chat_ids,
            started_at=job.started_at or job.requested_at,
        )

    def _jobs_conflict(
        self,
        active_source_chat_ids: tuple[str, ...],
        requested_source_chat_ids: tuple[str, ...],
    ) -> bool:
        if not active_source_chat_ids or not requested_source_chat_ids:
            return True
        return bool(set(active_source_chat_ids) & set(requested_source_chat_ids))

    async def _load_runtime_manifest(
        self,
        *,
        operator_huid: str | None = None,
        source_chat_id: str | None = None,
        require_configured: bool = False,
        include_skipped_in_all: bool = False,
    ) -> MigrationManifest:
        source_chat_ids = (source_chat_id,) if source_chat_id is not None else None
        return await self._load_config_manifest(
            operator_huid=operator_huid,
            source_chat_ids=source_chat_ids,
            require_configured=require_configured,
            include_skipped_in_all=include_skipped_in_all,
        )

    async def _load_config_manifest(
        self,
        *,
        operator_huid: str | None = None,
        source_chat_ids: tuple[str, ...] | None = None,
        require_configured: bool = True,
        include_skipped_in_all: bool = False,
    ) -> MigrationManifest:
        migration_id = self._require_configured_migration_id()
        configs = await self._chat_migration_config_repository.list_by_migration(migration_id)
        owned_configs = [
            record
            for record in configs
            if operator_huid is None or record.updated_by_huid == operator_huid
        ]
        config_by_chat = {record.source_chat_id: record for record in configs}
        owned_config_by_chat = {
            record.source_chat_id: record
            for record in owned_configs
        }
        configs_by_telegram_chat: dict[str, list[ChatMigrationConfigRecord]] = defaultdict(list)
        for record in configs:
            physical_chat_id = record.telegram_chat_id or record.source_chat_id
            configs_by_telegram_chat[physical_chat_id].append(record)
        owned_configs_by_telegram_chat: dict[str, list[ChatMigrationConfigRecord]] = defaultdict(list)
        for record in owned_configs:
            physical_chat_id = record.telegram_chat_id or record.source_chat_id
            owned_configs_by_telegram_chat[physical_chat_id].append(record)
        if source_chat_ids is None:
            selected = [
                record
                for record in owned_configs
                if not self._is_split_root_config(record)
                and (include_skipped_in_all or not record.skip_in_all)
            ]
        else:
            selected = []
            missing: list[str] = []
            for source_chat_id in source_chat_ids:
                record = owned_config_by_chat.get(source_chat_id)
                if record is not None:
                    if self._is_split_root_config(record):
                        topic_records = self._topic_child_configs_for_records(
                            owned_configs_by_telegram_chat.get(source_chat_id, []),
                        )
                        if topic_records:
                            selected.extend(topic_records)
                            continue
                        missing.append(source_chat_id)
                        continue
                    selected.append(record)
                    continue
                foreign_record = config_by_chat.get(source_chat_id)
                if foreign_record is not None:
                    self._ensure_operator_owns_chat_config(
                        record=foreign_record,
                        operator_huid=operator_huid,
                    )
                topic_records = self._topic_child_configs_for_records(
                    owned_configs_by_telegram_chat.get(source_chat_id, []),
                )
                if topic_records:
                    selected.extend(topic_records)
                    continue
                foreign_topic_records = self._topic_child_configs_for_records(
                    configs_by_telegram_chat.get(source_chat_id, []),
                )
                if foreign_topic_records:
                    self._ensure_operator_owns_chat_config(
                        record=foreign_topic_records[0],
                        operator_huid=operator_huid,
                    )
                missing.append(source_chat_id)
            if missing and require_configured:
                missing_ids = ", ".join(missing)
                raise ConfigurationError(
                    f"source_chat_id={missing_ids} is not configured; use /configure first",
                )
        if not selected:
            if operator_huid is not None and configs and not owned_configs:
                raise ConfigurationError(
                    "no configured chats found for this operator; existing chats are managed by another operator",
                )
            raise ConfigurationError(
                "no configured chats found for bot-managed migration; use /configure or /migrate first",
            )
        for record in selected:
            self._ensure_supported_source_backend(record.source_backend)
        dialogs = [self._manifest_dialog_from_config(record) for record in selected]
        return MigrationManifest(
            migration_id=migration_id,
            defaults=self._manifest_defaults(),
            dialogs=dialogs,
        )

    async def _load_or_bootstrap_config_manifest(
        self,
        *,
        operator: BotOperatorContext,
        options: MigrationRunOptions,
    ) -> MigrationManifest:
        migration_id = self._require_configured_migration_id()
        configs = await self._chat_migration_config_repository.list_by_migration(migration_id)
        bootstrap_options = replace(
            options,
            access_strategy=options.access_strategy or "invite_link",
            source_backend=self._supported_source_backend(),
        )
        discovered = await self._telegram_gateway.list_available_dialogs(
            limit=1000,
            source_backend=self._supported_source_backend(),
        )
        configured_source_chat_ids = {record.source_chat_id for record in configs}
        created_any = False
        for dialog in discovered:
            if dialog.dialog_id in configured_source_chat_ids:
                continue
            try:
                await self._save_chat_config(
                    operator=operator,
                    dialog=dialog,
                    options=bootstrap_options,
                    existing=None,
                )
            except ConfigurationError:
                existing = await self._chat_migration_config_repository.get(
                    migration_id,
                    dialog.dialog_id,
                )
                if existing is None or existing.updated_by_huid == operator.huid:
                    raise
            configured_source_chat_ids.add(dialog.dialog_id)
            created_any = True
        if created_any:
            configs = await self._chat_migration_config_repository.list_by_migration(migration_id)

        manifest = await self._load_config_manifest(operator_huid=operator.huid)
        return self._apply_run_overrides(
            manifest,
            tuple(dialog.source_chat_id for dialog in manifest.dialogs),
            options,
        )

    async def _upsert_chat_config(
        self,
        *,
        operator: BotOperatorContext,
        source_chat_id: str,
        options: MigrationRunOptions,
    ) -> ChatMigrationConfigRecord:
        migration_id = self._require_configured_migration_id()
        existing = await self._chat_migration_config_repository.get(migration_id, source_chat_id)
        dialog = await self._discover_dialog(
            source_chat_id,
            source_backend=(
                options.source_backend
                or (existing.source_backend if existing is not None else self._supported_source_backend())
            ),
        )
        return await self._save_chat_config(
            operator=operator,
            dialog=dialog,
            options=options,
            existing=existing,
        )

    async def _save_chat_config(
        self,
        *,
        operator: BotOperatorContext,
        dialog: SourceDialog,
        options: MigrationRunOptions,
        existing: ChatMigrationConfigRecord | None,
    ) -> ChatMigrationConfigRecord:
        migration_id = self._require_configured_migration_id()
        defaults = self._manifest_defaults()
        if existing is not None:
            self._ensure_operator_owns_chat_config(
                record=existing,
                operator_huid=operator.huid,
            )
        target_strategy = options.target_strategy or (
            existing.target_strategy if existing is not None else "create"
        )
        source_backend = options.source_backend or (
            existing.source_backend if existing is not None else self._supported_source_backend()
        )
        source_backend = self._ensure_supported_source_backend(source_backend)
        target_title = (
            options.target_title
            if options.target_title is not None
            else (
                existing.target_title
                if existing is not None
                else (dialog.title if target_strategy == "create" else None)
            )
        )
        target_chat_id = (
            options.target_chat_id
            if options.target_chat_id is not None
            else (existing.target_chat_id if existing is not None else None)
        )
        access_strategy = options.access_strategy or (
            existing.access_strategy if existing is not None else "direct_add"
        )
        if target_strategy == "create" and not target_title:
            target_title = dialog.title or dialog.dialog_id
        if target_strategy == "bind" and not target_chat_id:
            raise ConfigurationError("target_chat_id is required when target=bind")
        anchor_cts_host = self._resolve_operator_anchor_cts_host(
            operator=operator,
            existing=existing,
        )
        anchor_bot_id = self._resolve_operator_anchor_bot_id(
            operator=operator,
            existing=existing,
        )

        record = ChatMigrationConfigRecord(
            migration_id=migration_id,
            source_chat_id=dialog.dialog_id,
            source_chat_type=dialog.chat_type,
            source_chat_title=dialog.title,
            target_strategy=target_strategy,
            target_title=target_title,
            target_chat_id=target_chat_id,
            source_backend=source_backend,
            include_from=self._parse_datetime_or_none(
                options.include_from,
                existing.include_from if existing is not None else None,
            ),
            include_to=self._parse_datetime_or_none(
                options.include_to,
                existing.include_to if existing is not None else None,
            ),
            migrate_media=(
                options.migrate_media
                if options.migrate_media is not None
                else (
                    existing.migrate_media
                    if existing is not None
                    else defaults.migrate_media
                )
            ),
            reply_mode=(
                options.reply_mode
                if options.reply_mode is not None
                else (
                    existing.reply_mode
                    if existing is not None
                    else defaults.reply_mode
                )
            ),
            identity_policy=(
                options.identity_policy
                if options.identity_policy is not None
                else (
                    existing.identity_policy
                    if existing is not None
                    else defaults.identity_policy
                )
            ),
            access_strategy=access_strategy,
            updated_by_huid=operator.huid,
            created_at=existing.created_at if existing is not None else self._now(),
            updated_at=self._now(),
            anchor_cts_host=anchor_cts_host,
            anchor_bot_id=anchor_bot_id,
            topic_strategy=(
                options.topic_strategy
                if options.topic_strategy is not None
                else (
                    existing.topic_strategy
                    if existing is not None
                    else "single_chat"
                )
            ),
            skip_in_all=(
                options.skip_in_all
                if options.skip_in_all is not None
                else (existing.skip_in_all if existing is not None else False)
            ),
            telegram_chat_id=existing.telegram_chat_id if existing is not None else None,
            source_topic_id=existing.source_topic_id if existing is not None else None,
            source_thread_id=existing.source_thread_id if existing is not None else None,
            source_thread_title=existing.source_thread_title if existing is not None else None,
        )
        saved = await self._chat_migration_config_repository.save(record)
        self._ensure_operator_owns_chat_config(
            record=saved,
            operator_huid=operator.huid,
        )
        return saved

    async def _discover_dialog(
        self,
        source_chat_id: str,
        *,
        source_backend: str = "telethon_user_session",
    ) -> SourceDialog:
        source_backend = self._ensure_supported_source_backend(source_backend)
        if source_backend == "telegram_export_archive":
            snapshot = await self._telegram_export_snapshot_repository.get(source_chat_id)
            if snapshot is not None:
                return SourceDialog(
                    dialog_id=snapshot.source_chat_id,
                    chat_type=snapshot.source_chat_type,
                    title=snapshot.source_chat_title,
                    message_count=snapshot.message_count,
                    media_count=snapshot.media_count,
                    approximate_bytes=snapshot.approximate_bytes,
                    has_topics=False,
                )
            migration_id = self._require_configured_migration_id()
            existing = await self._chat_migration_config_repository.get(migration_id, source_chat_id)
            if existing is not None:
                return SourceDialog(
                    dialog_id=existing.source_chat_id,
                    chat_type=existing.source_chat_type,
                    title=existing.source_chat_title,
                    has_topics=False,
                )
            raise ConfigurationError(
                f"source_chat_id={source_chat_id!r} was not found in staged Telegram export archives",
            )
        dialog = await self._telegram_gateway.get_source_dialog(
            source_chat_id,
            source_backend=source_backend,
        )
        if dialog is not None:
            return dialog
        raise ConfigurationError(f"source_chat_id={source_chat_id!r} was not found in Telegram dialogs")

    async def _configured_chat_ids(
        self,
        *,
        operator_huid: str | None = None,
    ) -> set[str]:
        configured: set[str] = set()
        migration_id = self._settings.bot.migration_id
        records = await self._chat_migration_config_repository.list_by_migration(migration_id)
        if operator_huid is not None:
            records = [
                record
                for record in records
                if record.updated_by_huid == operator_huid
            ]
        configured.update(record.source_chat_id for record in records)
        configured.update(
            record.telegram_chat_id
            for record in records
            if record.telegram_chat_id is not None
        )
        return configured

    async def _chat_configuration_result(
        self,
        record: ChatMigrationConfigRecord,
    ) -> BotChatConfigurationResult:
        progress_hint = next(
            iter(
                await self._progress_hints_for_query(
                    record.migration_id,
                    source_chat_ids=(record.source_chat_id,),
                ),
            ),
            None,
        )
        return BotChatConfigurationResult(
            migration_id=record.migration_id,
            source_chat_id=record.source_chat_id,
            source_chat_type=record.source_chat_type,
            source_chat_title=record.source_chat_title,
            source_backend=record.source_backend,
            target_strategy=record.target_strategy,
            target_title=record.target_title,
            target_chat_id=record.target_chat_id,
            include_from=self._format_datetime(record.include_from),
            include_to=self._format_datetime(record.include_to),
            migrate_media=record.migrate_media,
            reply_mode=record.reply_mode,
            identity_policy=record.identity_policy,
            access_strategy=record.access_strategy,
            topic_strategy=record.topic_strategy,
            skip_in_all=record.skip_in_all,
            telegram_chat_id=record.telegram_chat_id,
            source_thread_id=record.source_thread_id,
            source_thread_title=record.source_thread_title,
            configured_via="db",
            progress_hint=progress_hint,
        )

    def _manifest_dialog_from_config(
        self,
        record: ChatMigrationConfigRecord,
    ) -> ManifestDialog:
        source_thread_id = record.source_thread_id
        if record.topic_strategy == "split_by_topic" and record.source_topic_id:
            # Older configs stored Telegram `top_message`, but forum history routing
            # uses topic id. Prefer `source_topic_id` for split-by-topic manifests.
            source_thread_id = record.source_topic_id
        return ManifestDialog(
            source_chat_id=record.source_chat_id,
            source_chat_type=record.source_chat_type,
            source_backend=record.source_backend,
            target_strategy=record.target_strategy,
            target_title=record.target_title,
            target_chat_id=record.target_chat_id,
            initiator_huid=record.updated_by_huid,
            anchor_cts_host=record.anchor_cts_host,
            anchor_bot_id=record.anchor_bot_id,
            include_from=self._format_datetime(record.include_from),
            include_to=self._format_datetime(record.include_to),
            migrate_media=record.migrate_media,
            reply_mode=record.reply_mode,
            identity_policy=record.identity_policy,
            access_strategy=record.access_strategy,
            topic_strategy=record.topic_strategy,
            skip_in_all=record.skip_in_all,
            telegram_chat_id=record.telegram_chat_id,
            source_topic_id=record.source_topic_id,
            source_thread_id=source_thread_id,
            source_thread_title=record.source_thread_title,
        )

    def _resolve_operator_anchor_cts_host(
        self,
        *,
        operator: BotOperatorContext,
        existing: ChatMigrationConfigRecord | None,
    ) -> str | None:
        if existing is not None and existing.anchor_cts_host:
            return normalize_express_cts_host(existing.anchor_cts_host)
        return self._default_anchor_cts_host(
            operator=operator,
        ) or normalize_express_cts_host(
            operator.current_cts_host or get_current_express_cts_host(),
        )

    def _resolve_operator_anchor_bot_id(
        self,
        *,
        operator: BotOperatorContext,
        existing: ChatMigrationConfigRecord | None,
    ) -> str | None:
        if existing is not None and existing.anchor_bot_id:
            return existing.anchor_bot_id
        if existing is not None and existing.anchor_cts_host:
            configured = self._configured_bot_id_for_cts_host(existing.anchor_cts_host)
            if configured is not None:
                return configured
        configured = self._configured_bot_id_for_cts_host(
            self._default_anchor_cts_host(operator=operator),
        )
        if configured is not None:
            return configured
        raw_bot_id = operator.current_bot_id
        normalized = str(raw_bot_id).strip() if raw_bot_id is not None else ""
        return normalized or None

    def _default_anchor_cts_host(
        self,
        *,
        operator: BotOperatorContext,
    ) -> str | None:
        primary_cts_host = normalize_express_cts_host(
            getattr(self._express_gateway, "primary_cts_host", None),
        )
        if primary_cts_host is not None:
            return primary_cts_host
        return normalize_express_cts_host(
            operator.current_cts_host or get_current_express_cts_host(),
        )

    def _configured_bot_id_for_cts_host(
        self,
        cts_host: str | None,
    ) -> str | None:
        getter = getattr(self._express_gateway, "bot_id_for_cts_host", None)
        if not callable(getter):
            return None
        normalized = normalize_express_cts_host(cts_host)
        value = getter(normalized)
        normalized_bot_id = str(value).strip() if value is not None else ""
        return normalized_bot_id or None

    def _member_add_effective_result(
        self,
        *,
        requested_access_strategy: str,
        direct_added: int,
        invited: int,
        failed: int = 0,
    ) -> str:
        normalized_requested = (requested_access_strategy or "direct_add").strip().lower()
        if direct_added > 0 and invited > 0:
            return "mixed"
        if direct_added > 0 and failed > 0:
            return "partial_direct_add"
        if direct_added > 0:
            return "direct_add"
        if invited > 0:
            if normalized_requested == "direct_add":
                return "invite_link_fallback"
            return "invite_link"
        if failed > 0:
            return "failed"
        return "no_change"

    def _resolve_manifest_anchor_cts_host(
        self,
        manifest: MigrationManifest,
        *,
        source_chat_ids: tuple[str, ...] | None = None,
    ) -> str | None:
        selected_ids = set(source_chat_ids or ())
        hosts = {
            normalize_express_cts_host(dialog.anchor_cts_host)
            for dialog in manifest.dialogs
            if dialog.anchor_cts_host
            and (not selected_ids or dialog.source_chat_id in selected_ids)
        }
        hosts.discard(None)
        if len(hosts) == 1:
            return next(iter(hosts))
        return None

    def _resolve_manifest_anchor_bot_id(
        self,
        manifest: MigrationManifest,
        *,
        source_chat_ids: tuple[str, ...] | None = None,
    ) -> str | None:
        selected_ids = set(source_chat_ids or ())
        bot_ids = {
            dialog.anchor_bot_id
            for dialog in manifest.dialogs
            if dialog.anchor_bot_id
            and (not selected_ids or dialog.source_chat_id in selected_ids)
        }
        if len(bot_ids) == 1:
            return next(iter(bot_ids))
        return None

    def _group_manifest_dialogs_by_anchor(
        self,
        manifest: MigrationManifest,
    ) -> list[tuple[str | None, list[ManifestDialog]]]:
        grouped: dict[str | None, list[ManifestDialog]] = defaultdict(list)
        for dialog in manifest.dialogs:
            grouped[normalize_express_cts_host(dialog.anchor_cts_host)].append(dialog)
        return list(grouped.items())

    def _empty_replay_failed_result(self, migration_id: str) -> ReplayFailedResult:
        return ReplayFailedResult(
            migration_id=migration_id,
            requested_count=0,
            recovered_count=0,
            failed_count=0,
            ambiguous_count=0,
            missing_count=0,
            skipped_count=0,
            source_chat_ids=[],
        )

    def _merge_replay_failed_results(
        self,
        aggregated: ReplayFailedResult,
        partial: ReplayFailedResult,
    ) -> ReplayFailedResult:
        return ReplayFailedResult(
            migration_id=partial.migration_id,
            requested_count=aggregated.requested_count + partial.requested_count,
            recovered_count=aggregated.recovered_count + partial.recovered_count,
            failed_count=aggregated.failed_count + partial.failed_count,
            ambiguous_count=aggregated.ambiguous_count + partial.ambiguous_count,
            missing_count=aggregated.missing_count + partial.missing_count,
            skipped_count=aggregated.skipped_count + partial.skipped_count,
            source_chat_ids=sorted(
                set(aggregated.source_chat_ids).union(partial.source_chat_ids),
            ),
        )

    def _migration_stub_manifest(self) -> MigrationManifest:
        return MigrationManifest(migration_id=self._require_configured_migration_id(), dialogs=[])

    def _migration_id_for_tracking(self) -> str:
        return self._require_configured_migration_id()

    def _require_configured_migration_id(self) -> str:
        migration_id = self._settings.bot.migration_id
        if migration_id:
            return migration_id
        raise ConfigurationError(
            "EXTG_BOT__MIGRATION_ID is required for bot-managed runtime",
        )

    def _manifest_defaults(self) -> ManifestDefaults:
        return ManifestDefaults()

    async def _validate_manifest_preconditions(
        self,
        manifest: MigrationManifest,
    ) -> None:
        for dialog in manifest.dialogs:
            if dialog.source_backend == "telegram_bot_api_live":
                if dialog.source_chat_type not in {"group", "supergroup"}:
                    raise ConfigurationError(
                        "source_backend=telegram_bot_api_live currently supports only "
                        "group/supergroup chats with observed capture",
                    )
            await self._target_chat_provisioning_service.validate_dialog_preconditions(
                migration_id=manifest.migration_id,
                dialog=dialog,
            )

    def _apply_run_overrides(
        self,
        manifest: MigrationManifest,
        source_chat_ids: tuple[str, ...],
        options: MigrationRunOptions,
    ) -> MigrationManifest:
        selected_ids = set(source_chat_ids)
        dialogs = [
            dialog.model_copy(
                update={
                    "include_from": (
                        options.include_from
                        if options.include_from is not None
                        else dialog.include_from
                    ),
                    "include_to": (
                        options.include_to
                        if options.include_to is not None
                        else dialog.include_to
                    ),
                    "migrate_media": (
                        options.migrate_media
                        if options.migrate_media is not None
                        else dialog.migrate_media
                    ),
                    "reply_mode": (
                        options.reply_mode
                        if options.reply_mode is not None
                        else dialog.reply_mode
                    ),
                    "target_strategy": (
                        options.target_strategy
                        if options.target_strategy is not None
                        else dialog.target_strategy
                    ),
                    "target_title": (
                        options.target_title
                        if options.target_title is not None
                        else dialog.target_title
                    ),
                    "target_chat_id": (
                        options.target_chat_id
                        if options.target_chat_id is not None
                        else dialog.target_chat_id
                    ),
                    "source_backend": (
                        options.source_backend
                        if options.source_backend is not None
                        else dialog.source_backend
                    ),
                    "identity_policy": (
                        options.identity_policy
                        if options.identity_policy is not None
                        else dialog.identity_policy
                    ),
                    "access_strategy": (
                        options.access_strategy
                        if options.access_strategy is not None
                        else dialog.access_strategy
                    ),
                    "topic_strategy": (
                        options.topic_strategy
                        if options.topic_strategy is not None
                        else dialog.topic_strategy
                    ),
                },
            )
            for dialog in manifest.dialogs
            if dialog.source_chat_id in selected_ids
        ]
        defaults = manifest.defaults.model_copy(
            update={
                "migrate_media": (
                    options.migrate_media
                    if options.migrate_media is not None
                    else manifest.defaults.migrate_media
                ),
                "reply_mode": (
                    options.reply_mode
                    if options.reply_mode is not None
                    else manifest.defaults.reply_mode
                ),
                "identity_policy": (
                    options.identity_policy
                    if options.identity_policy is not None
                    else manifest.defaults.identity_policy
                ),
                "access_strategy": (
                    options.access_strategy
                    if options.access_strategy is not None
                    else manifest.defaults.access_strategy
                ),
            },
        )
        return manifest.model_copy(
            update={
                "defaults": defaults,
                "dialogs": dialogs,
            },
        )

    async def _audit_operator_event(
        self,
        migration_id: str,
        operator: BotOperatorContext,
        *,
        event_type: str,
        payload: dict[str, object],
    ) -> None:
        await self._audit_repository.add(
            AuditEvent(
                migration_id=migration_id,
                event_type=event_type,
                severity=AuditSeverity.INFO,
                payload_json={
                    "operator": {
                        "huid": operator.huid,
                        "chat_id": operator.chat_id,
                        "username": operator.username,
                        "display_name": operator.display_name,
                        "current_cts_host": operator.current_cts_host,
                        "current_bot_id": operator.current_bot_id,
                    },
                    **payload,
                },
                created_at=self._now(),
            ),
        )

    def _job_key(
        self,
        operation: str,
        migration_id: str,
        source_chat_ids: tuple[str, ...],
    ) -> str:
        if not source_chat_ids:
            suffix = "all"
        elif len(source_chat_ids) == 1:
            suffix = source_chat_ids[0]
        else:
            joined_source_chat_ids = ",".join(source_chat_ids)
            digest = hashlib.sha1(
                joined_source_chat_ids.encode("utf-8"),
                usedforsecurity=False,
            ).hexdigest()[:12]
            suffix = f"{source_chat_ids[0]}+{len(source_chat_ids) - 1}:{digest}"
        return f"{operation}:{migration_id}:{suffix}:{uuid4().hex[:12]}"

    def _format_datetime(self, value: datetime | None) -> str | None:
        if value is None:
            return None
        normalized = value.astimezone(UTC)
        return normalized.isoformat().replace("+00:00", "Z")

    def _parse_datetime_or_none(
        self,
        raw_value: str | None,
        default: datetime | None,
    ) -> datetime | None:
        if raw_value is None:
            return default
        candidate = raw_value.strip()
        if not candidate:
            return None
        normalized = candidate.replace("Z", "+00:00")
        try:
            parsed = datetime.fromisoformat(normalized)
        except ValueError as error:
            raise ConfigurationError(f"invalid datetime value: {raw_value!r}") from error
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=UTC)
        return parsed.astimezone(UTC)

    def _now(self) -> datetime:
        return datetime.now(tz=UTC)

    async def _save_split_topic_chat_configs(
        self,
        *,
        operator: BotOperatorContext,
        dialog: SourceDialog,
        options: MigrationRunOptions,
        existing: ChatMigrationConfigRecord | None,
    ) -> None:
        target_strategy = options.target_strategy or (
            existing.target_strategy if existing is not None else "create"
        )
        if target_strategy != "create":
            raise ConfigurationError(
                "split_by_topic supports only target=create; per-topic bind is not implemented",
            )
        topics = await self._telegram_gateway.list_topics(
            dialog.dialog_id,
            source_backend=(
                options.source_backend
                or (existing.source_backend if existing is not None else self._supported_source_backend())
            ),
        )
        if not topics:
            raise ConfigurationError(
                f"forum topics were not found for source_chat_id={dialog.dialog_id}",
            )
        root_record = await self._save_chat_config(
            operator=operator,
            dialog=dialog,
            options=MigrationRunOptions(
                include_from=options.include_from,
                include_to=options.include_to,
                migrate_media=options.migrate_media,
                reply_mode=options.reply_mode,
                target_strategy="create",
                target_title=options.target_title,
                source_backend=options.source_backend,
                identity_policy=options.identity_policy,
                access_strategy=options.access_strategy,
                topic_strategy="split_by_topic",
                skip_in_all=options.skip_in_all,
                progress_policy=options.progress_policy,
                batch_size=options.batch_size,
            ),
            existing=existing,
        )
        migration_id = self._require_configured_migration_id()
        base_target_title = (options.target_title or dialog.title or dialog.dialog_id).strip()
        existing_records = await self._chat_migration_config_repository.list_by_migration(
            migration_id,
        )
        existing_by_source_chat_id = {
            record.source_chat_id: record
            for record in existing_records
        }
        for topic in topics:
            if not topic.topic_id:
                raise ConfigurationError(
                    f"topic in source_chat_id={dialog.dialog_id} has no topic_id",
                )
            logical_source_chat_id = self._topic_source_chat_id(
                dialog.dialog_id,
                topic.topic_id,
            )
            child_existing = existing_by_source_chat_id.get(logical_source_chat_id)
            if child_existing is not None:
                self._ensure_operator_owns_chat_config(
                    record=child_existing,
                    operator_huid=operator.huid,
                )
            target_title = f"{base_target_title} / {topic.title}"
            child_record = ChatMigrationConfigRecord(
                migration_id=migration_id,
                source_chat_id=logical_source_chat_id,
                source_chat_type=dialog.chat_type,
                source_chat_title=target_title,
                target_strategy="create",
                target_title=target_title,
                target_chat_id=None,
                source_backend=(
                    self._ensure_supported_source_backend(options.source_backend)
                    if options.source_backend is not None
                    else (
                        self._ensure_supported_source_backend(child_existing.source_backend)
                        if child_existing is not None
                        else self._supported_source_backend()
                    )
                ),
                include_from=self._parse_datetime_or_none(
                    options.include_from,
                    child_existing.include_from if child_existing is not None else None,
                ),
                include_to=self._parse_datetime_or_none(
                    options.include_to,
                    child_existing.include_to if child_existing is not None else None,
                ),
                migrate_media=(
                    options.migrate_media
                    if options.migrate_media is not None
                    else (
                        child_existing.migrate_media
                        if child_existing is not None
                        else self._manifest_defaults().migrate_media
                    )
                ),
                reply_mode=(
                    options.reply_mode
                    if options.reply_mode is not None
                    else (
                        child_existing.reply_mode
                        if child_existing is not None
                        else self._manifest_defaults().reply_mode
                    )
                ),
                identity_policy=(
                    options.identity_policy
                    if options.identity_policy is not None
                    else (
                        child_existing.identity_policy
                        if child_existing is not None
                        else self._manifest_defaults().identity_policy
                    )
                ),
                access_strategy=(
                    options.access_strategy
                    if options.access_strategy is not None
                    else (
                        child_existing.access_strategy
                        if child_existing is not None
                        else "direct_add"
                    )
                ),
                updated_by_huid=operator.huid,
                created_at=child_existing.created_at if child_existing is not None else self._now(),
                updated_at=self._now(),
                anchor_cts_host=(
                    child_existing.anchor_cts_host
                    if child_existing is not None
                    else root_record.anchor_cts_host
                ),
                anchor_bot_id=(
                    child_existing.anchor_bot_id
                    if child_existing is not None
                    else root_record.anchor_bot_id
                ),
                topic_strategy="split_by_topic",
                skip_in_all=(
                    options.skip_in_all
                    if options.skip_in_all is not None
                    else (child_existing.skip_in_all if child_existing is not None else False)
                ),
                telegram_chat_id=dialog.dialog_id,
                source_topic_id=topic.topic_id,
                source_thread_id=topic.topic_id,
                source_thread_title=topic.title,
            )
            saved_child = await self._chat_migration_config_repository.save(child_record)
            self._ensure_operator_owns_chat_config(
                record=saved_child,
                operator_huid=operator.huid,
            )

    async def _split_topic_configs_for_root(
        self,
        *,
        migration_id: str,
        source_chat_id: str,
    ) -> list[ChatMigrationConfigRecord]:
        records = await self._chat_migration_config_repository.list_by_migration(migration_id)
        topic_records = [
            record
            for record in records
            if record.telegram_chat_id == source_chat_id and record.source_thread_id is not None
        ]
        topic_records.sort(key=lambda item: (item.source_thread_title or item.source_chat_id))
        return topic_records

    def _is_split_root_config(
        self,
        record: ChatMigrationConfigRecord,
    ) -> bool:
        return record.topic_strategy == "split_by_topic" and record.source_thread_id is None

    def _topic_child_configs_for_records(
        self,
        records: list[ChatMigrationConfigRecord],
    ) -> list[ChatMigrationConfigRecord]:
        topic_records = [
            record
            for record in records
            if record.source_thread_id is not None
        ]
        topic_records.sort(key=lambda item: (item.source_thread_title or item.source_chat_id))
        return topic_records

    def _topic_source_chat_id(self, source_chat_id: str, topic_id: str) -> str:
        return f"{source_chat_id}#topic:{topic_id}"
