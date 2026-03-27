from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any


class ContentType(str, Enum):
    TEXT = "text"
    PHOTO = "photo"
    DOCUMENT = "document"
    VIDEO = "video"
    AUDIO = "audio"
    VOICE = "voice"
    STICKER = "sticker"
    SERVICE = "service"
    POLL = "poll"
    UNSUPPORTED = "unsupported"


class MessageImportStatus(str, Enum):
    PROCESSING = "processing"
    IMPORTED = "imported"
    FAILED = "failed"
    AMBIGUOUS = "ambiguous"


class AttachmentImportStatus(str, Enum):
    PROCESSING = "processing"
    IMPORTED = "imported"
    FAILED = "failed"
    AMBIGUOUS = "ambiguous"
    SKIPPED = "skipped"


class AttachmentStageStatus(str, Enum):
    PROCESSING = "processing"
    UPLOADED = "uploaded"
    ATTACHED = "attached"
    FAILED = "failed"


class MigrationLifecycleStatus(str, Enum):
    ACTIVE = "active"
    PAUSED = "paused"
    NEEDS_MANUAL_RECONCILE = "needs_manual_reconcile"
    COMPLETED = "completed"


class MigrationJobStatus(str, Enum):
    QUEUED = "queued"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"


class IntegrationOutboxStatus(str, Enum):
    PENDING = "pending"
    LEASED = "leased"
    PUBLISHED = "published"
    FAILED = "failed"


MIGRATION_JOB_CANCEL_REQUESTED_ERROR_CODE = "cancel_requested_by_operator"
MIGRATION_JOB_CANCELLED_ERROR_CODE = "cancelled_by_operator"


class ClaimState(str, Enum):
    CLAIMED = "claimed"
    IMPORTED = "imported"
    FAILED = "failed"
    AMBIGUOUS = "ambiguous"


class AuditSeverity(str, Enum):
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"


@dataclass(frozen=True, slots=True)
class CanonicalAttachment:
    source_file_id: str | None
    filename: str | None
    mime_type: str | None
    size_bytes: int | None
    media_kind: str
    duration_seconds: int | None = None
    download_url: str | None = None
    temp_local_path: str | None = None
    sha256: str | None = None


@dataclass(frozen=True, slots=True)
class CanonicalEntity:
    kind: str
    offset: int
    length: int
    text: str | None = None
    telegram_user_id: str | None = None
    telegram_username: str | None = None
    target_huid: str | None = None
    corporate_email: str | None = None
    resolution_source: str = "display_only"
    reason: str | None = None


@dataclass(frozen=True, slots=True)
class CanonicalMessage:
    source_platform: str
    source_chat_id: str
    source_message_id: str
    source_thread_id: str | None
    source_reply_to_message_id: str | None
    sender_external_id: str | None
    sender_display_name: str
    sender_username: str | None
    sent_at_utc: datetime
    edited_at_utc: datetime | None
    deleted_in_source: bool
    content_type: ContentType
    body_plain: str | None
    body_rendered: str | None
    attachments: list[CanonicalAttachment] = field(default_factory=list)
    entities: list[CanonicalEntity] = field(default_factory=list)
    service_payload: dict[str, Any] | None = None
    raw_snapshot: dict[str, Any] = field(default_factory=dict)
    resolved_target_huid: str | None = None
    identity_resolution_source: str = "display_only"
    attachment_failures: list[str] = field(default_factory=list)

    @property
    def idempotency_key(self) -> str:
        return ":".join(
            (self.source_platform, self.source_chat_id, self.source_message_id),
        )


@dataclass(frozen=True, slots=True)
class TelegramAuthor:
    external_id: str | None
    display_name: str
    username: str | None = None


@dataclass(frozen=True, slots=True)
class TelegramSourceMessage:
    chat_id: str
    message_id: str
    sent_at_utc: datetime
    author: TelegramAuthor
    body: str | None
    chat_title: str | None = None
    thread_id: str | None = None
    reply_to_message_id: str | None = None
    edited_at_utc: datetime | None = None
    deleted_in_source: bool = False
    content_type: ContentType = ContentType.TEXT
    attachments: list[CanonicalAttachment] = field(default_factory=list)
    entities: list[CanonicalEntity] = field(default_factory=list)
    raw_payload: dict[str, Any] = field(default_factory=dict)
    service_payload: dict[str, Any] | None = None
    attachment_failures: list[str] = field(default_factory=list)


@dataclass(frozen=True, slots=True)
class SourceDialog:
    dialog_id: str
    chat_type: str
    title: str
    message_count: int = 0
    media_count: int = 0
    approximate_bytes: int = 0
    has_topics: bool = False


@dataclass(frozen=True, slots=True)
class SourceChannelAccessProfile:
    dialog_id: str
    is_admin: bool
    can_list_participants: bool


@dataclass(frozen=True, slots=True)
class SourceTopic:
    topic_id: str
    title: str
    top_message_id: str | None = None


@dataclass(frozen=True, slots=True)
class ObservedTelegramChatRecord:
    chat_id: str
    chat_type: str
    title: str
    has_topics: bool
    message_count: int
    media_count: int
    approximate_bytes: int
    last_captured_at: datetime


@dataclass(frozen=True, slots=True)
class ObservedTelegramMessageRecord:
    chat_id: str
    chat_type: str
    chat_title: str
    message_id: str
    message_id_numeric: int
    sent_at_utc: datetime
    observed_at: datetime
    raw_message: dict[str, Any]
    capture_event_id: int | None = None
    thread_id: str | None = None
    reply_to_message_id: str | None = None
    edited_at_utc: datetime | None = None
    has_attachments: bool = False
    approximate_bytes: int = 0


@dataclass(frozen=True, slots=True)
class SourceParticipant:
    external_id: str | None
    username: str | None
    display_name: str
    is_self: bool = False


@dataclass(frozen=True, slots=True)
class ObservedTelegramCapture:
    chat: ObservedTelegramChatRecord
    participants: tuple[SourceParticipant, ...] = ()
    topics: tuple[SourceTopic, ...] = ()
    message: ObservedTelegramMessageRecord | None = None


@dataclass(frozen=True, slots=True)
class HistoryCursor:
    last_source_message_id: str | None = None
    last_source_sent_at: datetime | None = None


@dataclass(frozen=True, slots=True)
class HistoryBatch:
    dialog_id: str
    messages: list[TelegramSourceMessage]
    next_cursor: HistoryCursor | None
    has_more: bool
    dialog_title: str | None = None


@dataclass(frozen=True, slots=True)
class ReplyPreview:
    sent_at_utc: datetime | None
    author_display_name: str
    excerpt: str | None


@dataclass(frozen=True, slots=True)
class RenderedAttachment:
    filename: str | None
    media_kind: str


@dataclass(frozen=True, slots=True)
class RenderedMessage:
    display_header: str
    display_body: str
    footer: str | None
    attachments: list[RenderedAttachment] = field(default_factory=list)

    def render_text(self) -> str:
        parts = [self.display_header, self.display_body]
        if self.footer:
            parts.append(self.footer)
        return "\n".join(part for part in parts if part)


@dataclass(frozen=True, slots=True)
class SentMessageRef:
    target_chat_id: str
    target_sync_id: str
    deduplicated: bool = False


@dataclass(frozen=True, slots=True)
class FilePayload:
    filename: str
    content: bytes | None = None
    local_path: str | None = None
    mime_type: str | None = None
    media_kind: str | None = None
    duration_seconds: int | None = None


@dataclass(frozen=True, slots=True)
class ExpressStagedFile:
    attachment_type: str
    file_id: str
    file_url: str
    filename: str
    size_bytes: int
    mime_type: str
    file_hash: str
    duration_seconds: int | None = None
    preview_url: str | None = None
    preview_height: int | None = None
    preview_width: int | None = None
    encryption_algo: str | None = None
    chunk_size: int | None = None
    caption: str | None = None


@dataclass(frozen=True, slots=True)
class DownloadedAttachment:
    attachment: CanonicalAttachment
    local_path: str | None = None
    content: bytes | None = None
    size_bytes: int | None = None


@dataclass(frozen=True, slots=True)
class TelegramDeltaEvent:
    source_chat_id: str
    source_message: TelegramSourceMessage
    occurred_at: datetime


@dataclass(frozen=True, slots=True)
class ChatMappingRecord:
    migration_id: str
    source_chat_id: str
    source_chat_type: str
    source_chat_title: str
    target_chat_id: str
    target_chat_title: str
    status: str
    created_at: datetime
    updated_at: datetime
    anchor_cts_host: str | None = None
    anchor_bot_id: str | None = None


@dataclass(frozen=True, slots=True)
class ChatMigrationConfigRecord:
    migration_id: str
    source_chat_id: str
    source_chat_type: str
    source_chat_title: str
    target_strategy: str
    target_title: str | None
    target_chat_id: str | None
    include_from: datetime | None
    include_to: datetime | None
    migrate_media: bool
    reply_mode: str
    identity_policy: str
    updated_by_huid: str
    created_at: datetime
    updated_at: datetime
    anchor_cts_host: str | None = None
    anchor_bot_id: str | None = None
    topic_strategy: str = "single_chat"
    source_backend: str = "telethon_user_session"
    access_strategy: str = "direct_add"
    telegram_chat_id: str | None = None
    source_topic_id: str | None = None
    source_thread_id: str | None = None
    source_thread_title: str | None = None
    skip_in_all: bool = False

    def __post_init__(self) -> None:
        if (
            self.include_from is not None
            and self.include_to is not None
            and self.include_from > self.include_to
        ):
            raise ValueError("include_from must be less than or equal to include_to")


@dataclass(frozen=True, slots=True)
class MessageMappingRecord:
    migration_id: str
    source_chat_id: str
    source_message_id: str
    source_sent_at: datetime
    target_chat_id: str
    checksum: str
    import_status: MessageImportStatus
    imported_at: datetime | None = None
    target_sync_id: str | None = None
    rendered_body: str | None = None
    last_error_code: str | None = None
    last_error_payload: dict[str, Any] | None = None


@dataclass(frozen=True, slots=True)
class MessageClaimResult:
    state: ClaimState
    record: MessageMappingRecord


@dataclass(frozen=True, slots=True)
class IdentityMappingRecord:
    telegram_user_id: str | None
    telegram_username: str | None
    telegram_display_name: str | None
    corporate_email: str | None
    target_huid: str | None = None
    express_host: str | None = None
    created_at: datetime | None = None
    updated_at: datetime | None = None
    last_resolved_at: datetime | None = None


@dataclass(frozen=True, slots=True)
class ExpressUserCtsBindingRecord:
    cts_host: str
    target_huid: str | None = None
    corporate_email: str | None = None
    created_at: datetime | None = None
    updated_at: datetime | None = None
    last_verified_at: datetime | None = None


@dataclass(frozen=True, slots=True)
class ExpressBotHuidBindingRecord:
    bot_id: str
    cts_host: str
    bot_huid: str
    created_at: datetime | None = None
    updated_at: datetime | None = None
    last_learned_at: datetime | None = None


@dataclass(frozen=True, slots=True)
class OperatorTelegramSessionRecord:
    operator_huid: str
    phone_number: str
    session_path: str
    telegram_user_id: str
    telegram_username: str | None
    telegram_display_name: str
    created_at: datetime
    updated_at: datetime
    last_used_at: datetime | None = None


@dataclass(frozen=True, slots=True)
class AttachmentMappingRecord:
    migration_id: str
    source_chat_id: str
    source_message_id: str
    attachment_index: int
    source_file_id: str | None
    source_filename: str | None
    media_kind: str
    checksum: str | None
    size_bytes: int | None
    target_chat_id: str
    import_status: AttachmentImportStatus
    imported_at: datetime | None = None
    target_sync_id: str | None = None
    last_error_code: str | None = None
    last_error_payload: dict[str, Any] | None = None


@dataclass(frozen=True, slots=True)
class AttachmentClaimResult:
    state: ClaimState
    record: AttachmentMappingRecord


@dataclass(frozen=True, slots=True)
class AttachmentStageRecord:
    migration_id: str
    source_chat_id: str
    source_message_id: str
    attachment_index: int
    target_chat_id: str
    source_locator: str | None
    media_kind: str
    checksum: str | None
    size_bytes: int | None
    status: AttachmentStageStatus
    express_file_id: str | None = None
    express_file_payload: dict[str, Any] | None = None
    target_sync_id: str | None = None
    last_error_code: str | None = None
    last_error_payload: dict[str, Any] | None = None
    created_at: datetime | None = None
    updated_at: datetime | None = None


@dataclass(frozen=True, slots=True)
class AttachmentStageClaimResult:
    state: ClaimState
    record: AttachmentStageRecord


@dataclass(frozen=True, slots=True)
class MigrationCheckpoint:
    migration_id: str
    source_chat_id: str
    mode: str
    cursor: HistoryCursor
    updated_at: datetime


@dataclass(frozen=True, slots=True)
class InventorySnapshotRecord:
    migration_id: str
    source_chat_id: str
    source_chat_type: str
    source_chat_title: str
    message_count: int
    media_count: int
    approximate_bytes: int
    captured_at: datetime


@dataclass(frozen=True, slots=True)
class TelegramExportSnapshotRecord:
    source_chat_id: str
    source_chat_title: str
    source_chat_type: str
    original_filename: str | None
    export_chat_id: str | None
    archive_locator: str
    payload_sha256: str
    message_count: int
    media_count: int
    approximate_bytes: int
    uploaded_by_huid: str
    created_at: datetime
    updated_at: datetime


@dataclass(frozen=True, slots=True)
class AuditEvent:
    migration_id: str
    event_type: str
    severity: AuditSeverity
    created_at: datetime
    source_chat_id: str | None = None
    source_message_id: str | None = None
    payload_json: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class MigrationStateRecord:
    migration_id: str
    status: MigrationLifecycleStatus
    updated_at: datetime
    created_at: datetime
    freeze_started_at: datetime | None = None
    finalized_at: datetime | None = None
    last_reconcile_at: datetime | None = None
    last_blockers: list[str] = field(default_factory=list)


@dataclass(frozen=True, slots=True)
class MigrationJobRecord:
    job_key: str
    migration_id: str
    operation: str
    operator_huid: str
    source_chat_ids: tuple[str, ...]
    batch_size: int
    status: MigrationJobStatus
    requested_at: datetime
    anchor_cts_host: str | None = None
    anchor_bot_id: str | None = None
    started_at: datetime | None = None
    finished_at: datetime | None = None
    heartbeat_at: datetime | None = None
    lease_expires_at: datetime | None = None
    worker_id: str | None = None
    attempts: int = 0
    last_error_code: str | None = None
    last_error_payload: dict[str, Any] | None = None


@dataclass(frozen=True, slots=True)
class IntegrationOutboxEventRecord:
    event_id: str
    aggregate_type: str
    aggregate_id: str
    event_type: str
    payload_json: dict[str, Any]
    created_at: datetime
    headers_json: dict[str, Any] = field(default_factory=dict)
    status: IntegrationOutboxStatus = IntegrationOutboxStatus.PENDING
    leased_by: str | None = None
    leased_at: datetime | None = None
    lease_expires_at: datetime | None = None
    published_at: datetime | None = None
    broker_topic: str | None = None
    broker_partition: int | None = None
    broker_offset: int | None = None
    attempts: int = 0
    last_error_code: str | None = None
    last_error_payload: dict[str, Any] | None = None


@dataclass(frozen=True, slots=True)
class PublishedIntegrationEvent:
    broker_topic: str
    published_at: datetime
    broker_partition: int | None = None
    broker_offset: int | None = None


@dataclass(frozen=True, slots=True)
class ConsumerInboxRecord:
    consumer_name: str
    message_id: str
    processed_at: datetime
    result_code: str
    result_payload: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class ServiceWatermarkRecord:
    consumer_name: str
    logical_stream_key: str
    watermark_value: str
    updated_at: datetime
