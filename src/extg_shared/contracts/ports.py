from __future__ import annotations

from collections.abc import AsyncIterator
from dataclasses import dataclass
from typing import Protocol

from extg_shared.contracts.manifest import MigrationManifest
from extg_shared.contracts.models import (
    AttachmentClaimResult,
    AttachmentImportStatus,
    AttachmentMappingRecord,
    AttachmentStageClaimResult,
    AttachmentStageRecord,
    AuditEvent,
    CanonicalAttachment,
    CanonicalMessage,
    ChatMigrationConfigRecord,
    ChatMappingRecord,
    ConsumerInboxRecord,
    DownloadedAttachment,
    ExpressStagedFile,
    ExpressBotHuidBindingRecord,
    ExpressUserCtsBindingRecord,
    FilePayload,
    HistoryBatch,
    HistoryCursor,
    IdentityMappingRecord,
    IntegrationOutboxEventRecord,
    IntegrationOutboxStatus,
    InventorySnapshotRecord,
    MessageClaimResult,
    MessageImportStatus,
    MessageMappingRecord,
    MigrationCheckpoint,
    MigrationJobRecord,
    MigrationJobStatus,
    MigrationStateRecord,
    OperatorMigrationDefaultsRecord,
    ObservedTelegramCapture,
    ObservedTelegramChatRecord,
    ObservedTelegramMessageRecord,
    OperatorTelegramSessionRecord,
    PublishedIntegrationEvent,
    SentMessageRef,
    ServiceWatermarkRecord,
    SourceChannelAccessProfile,
    SourceDialog,
    SourceParticipant,
    SourceTopic,
    TelegramExportSnapshotRecord,
    TelegramDeltaEvent,
)


class TelegramGateway(Protocol):
    async def list_available_dialogs(
        self,
        *,
        limit: int = 100,
        query: str | None = None,
        source_backend: str = "telethon_user_session",
    ) -> list[SourceDialog]:
        """Discover available Telegram dialogs without requiring a manifest."""

    async def get_source_dialog(
        self,
        dialog_id: str,
        *,
        source_backend: str = "telethon_user_session",
    ) -> SourceDialog | None:
        """Resolve one Telegram dialog directly by id."""

    async def list_dialogs(self, manifest: MigrationManifest) -> list[SourceDialog]:
        """List dialogs included in the migration manifest."""

    async def fetch_history(
        self,
        dialog_id: str,
        cursor: HistoryCursor | None,
        limit: int,
        *,
        source_backend: str = "telethon_user_session",
        thread_id: str | None = None,
    ) -> HistoryBatch:
        """Fetch an ordered batch of Telegram messages for a dialog."""

    async def download_attachment(
        self,
        attachment: CanonicalAttachment,
        *,
        source_backend: str = "telethon_user_session",
    ) -> DownloadedAttachment:
        """Download Telegram attachment content for later upload."""

    async def list_participants(
        self,
        dialog_id: str,
        *,
        source_backend: str = "telethon_user_session",
    ) -> list[SourceParticipant]:
        """List participants for one Telegram dialog."""

    async def list_topics(
        self,
        dialog_id: str,
        *,
        source_backend: str = "telethon_user_session",
    ) -> list[SourceTopic]:
        """List topics for one Telegram forum supergroup."""

    async def get_channel_access_profile(
        self,
        dialog_id: str,
        *,
        source_backend: str = "telethon_user_session",
    ) -> SourceChannelAccessProfile:
        """Describe channel-specific access capabilities for one Telegram dialog."""

    async def subscribe_delta(
        self,
        dialog_ids: list[str],
        *,
        source_backend: str = "telethon_user_session",
    ) -> AsyncIterator[TelegramDeltaEvent]:
        """Subscribe to new Telegram messages for the selected dialogs."""


class TelegramBotApiCaptureStore(Protocol):
    async def capture(self, capture: ObservedTelegramCapture) -> bool:
        """Persist one observed Telegram Bot API update. Returns True for a new message."""

    async def get_chat(self, chat_id: str) -> ObservedTelegramChatRecord | None:
        """Fetch one observed Telegram chat."""

    async def list_chats(
        self,
        *,
        limit: int = 100,
        query: str | None = None,
    ) -> list[ObservedTelegramChatRecord]:
        """List observed Telegram chats known to the Bot API capture plane."""

    async def get_source_dialog(
        self,
        *,
        chat_id: str,
        thread_id: str | None = None,
        title_override: str | None = None,
    ) -> SourceDialog | None:
        """Resolve one observed chat or thread into SourceDialog inventory metadata."""

    async def list_participants(self, chat_id: str) -> list[SourceParticipant]:
        """List observed participants for one Telegram chat."""

    async def list_topics(self, chat_id: str) -> list[SourceTopic]:
        """List observed topics for one Telegram forum chat."""

    async def fetch_messages(
        self,
        *,
        chat_id: str,
        cursor: HistoryCursor | None,
        limit: int,
        thread_id: str | None = None,
    ) -> tuple[list[ObservedTelegramMessageRecord], bool]:
        """Fetch observed messages for one chat or thread."""

    async def list_messages_since(
        self,
        *,
        chat_ids: list[str],
        after_capture_event_id: int | None,
        limit: int,
    ) -> list[ObservedTelegramMessageRecord]:
        """List observed messages strictly after one capture event id."""


class ExpressGateway(Protocol):
    async def create_chat(
        self,
        title: str,
        participant_huids: list[str] | None = None,
        *,
        chat_type: str | None = None,
    ) -> str:
        """Create a target eXpress chat or return an existing one.

        `chat_type` is an adapter-facing override such as
        `PERSONAL_CHAT|GROUP_CHAT|CHANNEL|THREAD`. When omitted, adapters may
        use their configured default.
        """

    async def ensure_chat_members(
        self,
        target_chat_id: str,
        participant_huids: list[str],
    ) -> tuple[str, ...]:
        """Ensure target chat contains the provided members and return newly added HUIDs."""

    async def promote_chat_admins(
        self,
        target_chat_id: str,
        participant_huids: list[str],
    ) -> tuple[str, ...]:
        """Ensure target chat admins include the provided members and return newly promoted HUIDs."""

    async def list_chat_admin_huids(
        self,
        target_chat_id: str,
    ) -> tuple[str, ...]:
        """List current target chat admin HUIDs visible to the active bot account."""

    async def ensure_personal_chat(
        self,
        user_huid: str,
        *,
        title: str | None = None,
    ) -> str:
        """Return or create a personal chat with one resolved eXpress user."""

    async def create_chat_link(
        self,
        target_chat_id: str,
    ) -> str | None:
        """Create an access link for one existing eXpress chat when possible."""

    async def search_user_by_email(self, email: str) -> str | None:
        """Resolve one corporate email to an eXpress user HUID."""

    async def search_user_by_huid(self, huid: str) -> str | None:
        """Validate one eXpress user HUID and return it when it exists."""

    async def search_user_by_ad_login(
        self,
        ad_login: str,
        *,
        ad_domain: str | None = None,
    ) -> str | None:
        """Resolve one AD login to an eXpress user HUID."""

    async def search_user_by_other_id(self, other_id: str) -> str | None:
        """Resolve one external other_id to an eXpress user HUID."""

    async def send_message(
        self,
        target_chat_id: str,
        body: str,
        *,
        idempotency_key: str | None = None,
        file: FilePayload | None = None,
        staged_file: ExpressStagedFile | None = None,
        fallback_file: FilePayload | None = None,
    ) -> SentMessageRef:
        """Send a message to eXpress.

        The optional idempotency key is used by fakes and future adapters to
        prevent duplicate visible effects across retries.
        """


class ExpressFileStore(Protocol):
    async def upload_file(
        self,
        target_chat_id: str,
        *,
        file: FilePayload,
    ) -> ExpressStagedFile:
        """Upload one attachment to Files API for the concrete target chat."""


class ResolvedIdentity(Protocol):
    display_name: str
    username: str | None
    target_huid: str | None
    resolution_source: str


class IdentityResolver(Protocol):
    async def resolve(self, message: CanonicalMessage) -> ResolvedIdentity:
        """Resolve a sender into eXpress-aware display metadata."""


class ChatMappingRepository(Protocol):
    async def get(
        self,
        migration_id: str,
        source_chat_id: str,
    ) -> ChatMappingRecord | None:
        """Fetch a chat mapping."""

    async def save(self, record: ChatMappingRecord) -> None:
        """Persist a chat mapping."""

    async def list_by_migration(
        self,
        migration_id: str,
    ) -> list[ChatMappingRecord]:
        """List persisted chat mappings for one migration."""


class ChatMigrationConfigRepository(Protocol):
    async def get(
        self,
        migration_id: str,
        source_chat_id: str,
    ) -> ChatMigrationConfigRecord | None:
        """Fetch one persisted bot-managed chat migration config."""

    async def list_by_migration(
        self,
        migration_id: str,
    ) -> list[ChatMigrationConfigRecord]:
        """List persisted chat migration configs for one migration."""

    async def save(
        self,
        record: ChatMigrationConfigRecord,
    ) -> ChatMigrationConfigRecord:
        """Create or update one persisted chat migration config."""

    async def delete(
        self,
        migration_id: str,
        source_chat_id: str,
    ) -> bool:
        """Delete one persisted chat migration config if it exists."""


class OperatorMigrationDefaultsRepository(Protocol):
    async def get(
        self,
        migration_id: str,
        operator_huid: str,
    ) -> OperatorMigrationDefaultsRecord | None:
        """Fetch one persisted operator-level defaults profile."""

    async def save(
        self,
        record: OperatorMigrationDefaultsRecord,
    ) -> OperatorMigrationDefaultsRecord:
        """Create or update one persisted operator-level defaults profile."""


class MigrationJobRepository(Protocol):
    async def enqueue(
        self,
        record: MigrationJobRecord,
        *,
        outbox_events: tuple[IntegrationOutboxEventRecord, ...] = (),
    ) -> bool:
        """Persist one queued migration job. Returns False when job_key already exists."""

    async def get(self, job_key: str) -> MigrationJobRecord | None:
        """Load one migration job by job_key."""

    async def list_active(
        self,
        migration_id: str,
    ) -> list[MigrationJobRecord]:
        """List queued or running jobs for one migration."""

    async def claim(
        self,
        *,
        job_key: str,
        worker_id: str,
        lease_duration_seconds: float,
    ) -> MigrationJobRecord | None:
        """Lease one specific queued or stale running job for execution."""

    async def acquire_next(
        self,
        *,
        worker_id: str,
        lease_duration_seconds: float,
    ) -> MigrationJobRecord | None:
        """Lease one queued or stale job for execution."""

    async def heartbeat(
        self,
        *,
        job_key: str,
        worker_id: str,
        lease_duration_seconds: float,
    ) -> bool:
        """Refresh the lease for one running job."""

    async def mark_completed(
        self,
        *,
        job_key: str,
        worker_id: str,
    ) -> MigrationJobRecord | None:
        """Mark one running job as completed."""

    async def mark_failed(
        self,
        *,
        job_key: str,
        worker_id: str,
        error_code: str,
        error_payload: dict[str, object],
    ) -> MigrationJobRecord | None:
        """Mark one running job as failed."""

    async def request_cancel(
        self,
        *,
        job_key: str,
        operator_huid: str,
    ) -> MigrationJobRecord | None:
        """Request cancellation for one queued or running job owned by the operator."""

    async def count_by_status(self) -> dict[MigrationJobStatus, int]:
        """Return job counts grouped by status across the whole runtime."""


class OutboxRepository(Protocol):
    async def append_many(
        self,
        events: tuple[IntegrationOutboxEventRecord, ...],
    ) -> None:
        """Append one or more outbox events."""

    async def get(self, event_id: str) -> IntegrationOutboxEventRecord | None:
        """Load one outbox event by id."""

    async def lease_batch(
        self,
        *,
        publisher_id: str,
        limit: int,
        lease_duration_seconds: float,
    ) -> list[IntegrationOutboxEventRecord]:
        """Lease a batch of pending or stale outbox events for publishing."""

    async def mark_published(
        self,
        *,
        event_id: str,
        publisher_id: str,
        result: PublishedIntegrationEvent,
    ) -> IntegrationOutboxEventRecord | None:
        """Mark one leased outbox event as published."""

    async def mark_failed(
        self,
        *,
        event_id: str,
        publisher_id: str,
        error_code: str,
        error_payload: dict[str, object],
    ) -> IntegrationOutboxEventRecord | None:
        """Mark one leased outbox event as failed."""

    async def count_by_status(self) -> dict[IntegrationOutboxStatus, int]:
        """Return outbox counts grouped by status across the whole runtime."""


class InboxRepository(Protocol):
    async def is_processed(self, *, consumer_name: str, message_id: str) -> bool:
        """Return True when one consumer already processed one message."""

    async def mark_processed(self, record: ConsumerInboxRecord) -> bool:
        """Persist one consumer inbox receipt. Returns False when duplicate."""


class ServiceWatermarkRepository(Protocol):
    async def get(
        self,
        *,
        consumer_name: str,
        logical_stream_key: str,
    ) -> ServiceWatermarkRecord | None:
        """Load one watermark by consumer and logical stream key."""

    async def upsert(
        self,
        record: ServiceWatermarkRecord,
    ) -> ServiceWatermarkRecord:
        """Create or update one service watermark."""

    async def list_all(self) -> list[ServiceWatermarkRecord]:
        """List all persisted service watermarks."""


class IntegrationEventPublisher(Protocol):
    async def publish(
        self,
        event: IntegrationOutboxEventRecord,
    ) -> PublishedIntegrationEvent:
        """Publish one leased outbox event and return broker metadata."""


@dataclass(frozen=True)
class MessageStatusSummary:
    migration_id: str
    source_chat_id: str
    import_status: MessageImportStatus
    count: int


@dataclass(frozen=True)
class AttachmentStatusSummary:
    migration_id: str
    source_chat_id: str
    import_status: AttachmentImportStatus
    count: int


class MessageMappingRepository(Protocol):
    async def claim(
        self,
        record: MessageMappingRecord,
    ) -> MessageClaimResult:
        """Create a processing record or return an existing one."""

    async def get(
        self,
        migration_id: str,
        source_chat_id: str,
        source_message_id: str,
    ) -> MessageMappingRecord | None:
        """Fetch message mapping by idempotency key."""

    async def mark_imported(
        self,
        migration_id: str,
        source_chat_id: str,
        source_message_id: str,
        *,
        target_sync_id: str,
        rendered_body: str,
    ) -> MessageMappingRecord:
        """Persist an imported state for a message."""

    async def mark_failed(
        self,
        migration_id: str,
        source_chat_id: str,
        source_message_id: str,
        *,
        error_code: str,
        error_payload: dict[str, object],
    ) -> MessageMappingRecord:
        """Persist a failed state for a message."""

    async def mark_ambiguous(
        self,
        migration_id: str,
        source_chat_id: str,
        source_message_id: str,
        *,
        error_code: str,
        error_payload: dict[str, object],
    ) -> MessageMappingRecord:
        """Persist an ambiguous delivery state for a message."""

    async def list_failed(
        self,
        migration_id: str,
        *,
        source_chat_id: str | None = None,
        limit: int = 100,
    ) -> list[MessageMappingRecord]:
        """List failed messages for reconciliation or replay."""

    async def requeue_failed(
        self,
        migration_id: str,
        *,
        source_chat_id: str | None = None,
        limit: int = 100,
    ) -> list[MessageMappingRecord]:
        """Prepare failed messages for replay."""

    async def summarize_by_chat(
        self,
        migration_id: str,
    ) -> list[MessageStatusSummary]:
        """Return counts grouped by chat and status for reconciliation."""


class AttachmentMappingRepository(Protocol):
    async def claim(
        self,
        record: AttachmentMappingRecord,
        *,
        retry_failed: bool = False,
    ) -> AttachmentClaimResult:
        """Create or re-enter processing state for one attachment delivery item."""

    async def get(
        self,
        migration_id: str,
        source_chat_id: str,
        source_message_id: str,
        attachment_index: int,
    ) -> AttachmentMappingRecord | None:
        """Fetch one attachment mapping record."""

    async def list_by_message(
        self,
        migration_id: str,
        source_chat_id: str,
        source_message_id: str,
    ) -> list[AttachmentMappingRecord]:
        """List attachment mappings for one source message."""

    async def mark_imported(
        self,
        migration_id: str,
        source_chat_id: str,
        source_message_id: str,
        attachment_index: int,
        *,
        attachment: CanonicalAttachment,
        target_sync_id: str,
    ) -> AttachmentMappingRecord:
        """Persist an imported state for one attachment delivery item."""

    async def mark_failed(
        self,
        migration_id: str,
        source_chat_id: str,
        source_message_id: str,
        attachment_index: int,
        *,
        attachment: CanonicalAttachment | None,
        error_code: str,
        error_payload: dict[str, object],
    ) -> AttachmentMappingRecord:
        """Persist a failed state for one attachment delivery item."""

    async def mark_ambiguous(
        self,
        migration_id: str,
        source_chat_id: str,
        source_message_id: str,
        attachment_index: int,
        *,
        attachment: CanonicalAttachment | None,
        error_code: str,
        error_payload: dict[str, object],
    ) -> AttachmentMappingRecord:
        """Persist an ambiguous delivery state for one attachment delivery item."""

    async def mark_skipped(
        self,
        migration_id: str,
        source_chat_id: str,
        source_message_id: str,
        attachment_index: int,
        *,
        attachment: CanonicalAttachment,
        reason: str,
    ) -> AttachmentMappingRecord:
        """Persist an intentionally skipped attachment item."""

    async def summarize_by_chat(
        self,
        migration_id: str,
    ) -> list[AttachmentStatusSummary]:
        """Return counts grouped by chat and status for attachment reconciliation."""


class AttachmentStageRepository(Protocol):
    async def claim(
        self,
        record: AttachmentStageRecord,
        *,
        retry_failed: bool = False,
    ) -> AttachmentStageClaimResult:
        """Create or re-enter processing state for one attachment upload item."""

    async def get(
        self,
        migration_id: str,
        source_chat_id: str,
        source_message_id: str,
        attachment_index: int,
        target_chat_id: str,
    ) -> AttachmentStageRecord | None:
        """Fetch one attachment stage record."""

    async def mark_uploaded(
        self,
        migration_id: str,
        source_chat_id: str,
        source_message_id: str,
        attachment_index: int,
        target_chat_id: str,
        *,
        staged_file: ExpressStagedFile,
    ) -> AttachmentStageRecord:
        """Persist uploaded state for one attachment upload item."""

    async def mark_attached(
        self,
        migration_id: str,
        source_chat_id: str,
        source_message_id: str,
        attachment_index: int,
        target_chat_id: str,
        *,
        target_sync_id: str,
    ) -> AttachmentStageRecord:
        """Persist successful attachment linkage to one target message."""

    async def mark_failed(
        self,
        migration_id: str,
        source_chat_id: str,
        source_message_id: str,
        attachment_index: int,
        target_chat_id: str,
        *,
        error_code: str,
        error_payload: dict[str, object],
    ) -> AttachmentStageRecord:
        """Persist a failed upload state."""


class CheckpointRepository(Protocol):
    async def get(
        self,
        migration_id: str,
        source_chat_id: str,
        mode: str,
    ) -> MigrationCheckpoint | None:
        """Read the checkpoint for the chat."""

    async def save(self, checkpoint: MigrationCheckpoint) -> None:
        """Persist the latest checkpoint."""


class AuditRepository(Protocol):
    async def add(self, event: AuditEvent) -> None:
        """Persist an audit event."""


class InventorySnapshotRepository(Protocol):
    async def replace_for_migration(
        self,
        migration_id: str,
        snapshots: list[InventorySnapshotRecord],
    ) -> None:
        """Replace the current inventory snapshot for the migration."""

    async def list_by_migration(
        self,
        migration_id: str,
    ) -> list[InventorySnapshotRecord]:
        """Read inventory snapshots for a migration."""


class TelegramExportSnapshotRepository(Protocol):
    async def get(
        self,
        source_chat_id: str,
    ) -> TelegramExportSnapshotRecord | None:
        """Read one staged Telegram export snapshot."""

    async def save(
        self,
        record: TelegramExportSnapshotRecord,
    ) -> TelegramExportSnapshotRecord:
        """Persist one staged Telegram export snapshot."""

    async def delete(
        self,
        source_chat_id: str,
    ) -> bool:
        """Delete one staged Telegram export snapshot."""


class TelegramExportArchiveStageStore(Protocol):
    async def stage_upload(
        self,
        *,
        filename: str | None,
        content: bytes,
    ) -> str:
        """Persist one uploaded archive to ephemeral staging and return a locator."""

    async def read(
        self,
        locator: str,
    ) -> bytes:
        """Read one staged archive by locator."""

    async def delete(
        self,
        locator: str,
    ) -> bool:
        """Delete one staged archive by locator."""


class MigrationStateRepository(Protocol):
    async def get(
        self,
        migration_id: str,
    ) -> MigrationStateRecord | None:
        """Read the current migration lifecycle state."""

    async def save(
        self,
        record: MigrationStateRecord,
    ) -> MigrationStateRecord:
        """Persist or update the current migration lifecycle state."""


class IdentityMappingRepository(Protocol):
    async def get_by_user_id(
        self,
        telegram_user_id: str,
        *,
        express_host: str | None = None,
    ) -> IdentityMappingRecord | None:
        """Read one Telegram user-id to corporate identity mapping."""

    async def get_by_username(
        self,
        telegram_username: str,
        *,
        express_host: str | None = None,
    ) -> IdentityMappingRecord | None:
        """Read one Telegram username to corporate identity mapping."""

    async def save(
        self,
        record: IdentityMappingRecord,
    ) -> IdentityMappingRecord:
        """Persist one Telegram username to corporate identity mapping."""


class ExpressUserCtsBindingRepository(Protocol):
    async def get_by_target_huid(
        self,
        target_huid: str,
    ) -> ExpressUserCtsBindingRecord | None:
        """Read one cached eXpress user HUID to CTS binding."""

    async def get_by_email(
        self,
        corporate_email: str,
    ) -> ExpressUserCtsBindingRecord | None:
        """Read one cached corporate email to CTS binding."""

    async def save(
        self,
        record: ExpressUserCtsBindingRecord,
    ) -> ExpressUserCtsBindingRecord:
        """Persist one eXpress user to CTS binding."""


class ExpressBotHuidBindingRepository(Protocol):
    async def get_by_bot_id(
        self,
        bot_id: str,
    ) -> ExpressBotHuidBindingRecord | None:
        """Read one cached bot_id to bot_huid binding."""

    async def save(
        self,
        record: ExpressBotHuidBindingRecord,
    ) -> ExpressBotHuidBindingRecord:
        """Persist one eXpress bot identity binding."""


class OperatorTelegramSessionRepository(Protocol):
    async def get_by_operator(
        self,
        operator_huid: str,
    ) -> OperatorTelegramSessionRecord | None:
        """Read one persisted Telegram session binding for an operator."""

    async def save(
        self,
        record: OperatorTelegramSessionRecord,
    ) -> OperatorTelegramSessionRecord:
        """Persist or update one operator Telegram session binding."""

    async def delete(
        self,
        operator_huid: str,
    ) -> bool:
        """Delete one operator Telegram session binding if it exists."""
