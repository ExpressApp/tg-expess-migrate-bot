from __future__ import annotations

from datetime import UTC, datetime

from sqlalchemy import (
    BigInteger,
    Boolean,
    CheckConstraint,
    DateTime,
    Index,
    Integer,
    String,
    Text,
    UniqueConstraint,
    func,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from extg_shared.persistence.base import Base


class MigrationChatMapModel(Base):
    __tablename__ = "migration_chat_map"
    __table_args__ = (
        UniqueConstraint(
            "migration_id",
            "source_chat_id",
            name="uq_migration_chat_map_migration_source_chat",
        ),
    )

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    migration_id: Mapped[str] = mapped_column(String(128), nullable=False)
    source_chat_id: Mapped[str] = mapped_column(String(128), nullable=False)
    source_chat_type: Mapped[str] = mapped_column(String(32), nullable=False)
    source_chat_title: Mapped[str] = mapped_column(String(512), nullable=False)
    anchor_cts_host: Mapped[str | None] = mapped_column(String(255), nullable=True)
    anchor_bot_id: Mapped[str | None] = mapped_column(String(128), nullable=True)
    target_chat_id: Mapped[str] = mapped_column(String(128), nullable=False)
    target_chat_title: Mapped[str] = mapped_column(String(512), nullable=False)
    status: Mapped[str] = mapped_column(String(32), nullable=False)
    member_success_count: Mapped[int | None] = mapped_column(Integer, nullable=True)
    member_total_count: Mapped[int | None] = mapped_column(Integer, nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        default=lambda: datetime.now(tz=UTC),
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        default=lambda: datetime.now(tz=UTC),
    )


class MigrationChatConfigModel(Base):
    __tablename__ = "migration_chat_config"
    __table_args__ = (
        UniqueConstraint(
            "migration_id",
            "source_chat_id",
            name="uq_migration_chat_config_key",
        ),
        Index(
            "ix_migration_chat_config_migration_id",
            "migration_id",
        ),
        CheckConstraint(
            "include_from IS NULL OR include_to IS NULL OR include_from <= include_to",
            name="ck_migration_chat_config_include_range",
        ),
    )

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    migration_id: Mapped[str] = mapped_column(String(128), nullable=False)
    source_chat_id: Mapped[str] = mapped_column(String(128), nullable=False)
    source_chat_type: Mapped[str] = mapped_column(String(32), nullable=False)
    source_chat_title: Mapped[str] = mapped_column(String(512), nullable=False)
    anchor_cts_host: Mapped[str | None] = mapped_column(String(255), nullable=True)
    anchor_bot_id: Mapped[str | None] = mapped_column(String(128), nullable=True)
    source_backend: Mapped[str] = mapped_column(
        String(64),
        nullable=False,
        default="telethon_user_session",
    )
    target_strategy: Mapped[str] = mapped_column(String(64), nullable=False)
    target_title: Mapped[str | None] = mapped_column(String(512), nullable=True)
    target_chat_id: Mapped[str | None] = mapped_column(String(128), nullable=True)
    include_from: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    include_to: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    migrate_media: Mapped[bool] = mapped_column(Boolean, nullable=False)
    media_kinds_json: Mapped[list[str] | None] = mapped_column(JSONB, nullable=True)
    service_messages: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    reply_mode: Mapped[str] = mapped_column(String(64), nullable=False)
    output_template: Mapped[str | None] = mapped_column(Text, nullable=True)
    identity_policy: Mapped[str] = mapped_column(String(64), nullable=False)
    access_strategy: Mapped[str] = mapped_column(String(32), nullable=False, default="direct_add")
    topic_strategy: Mapped[str] = mapped_column(String(32), nullable=False, default="single_chat")
    skip_in_all: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    telegram_chat_id: Mapped[str | None] = mapped_column(String(128), nullable=True)
    source_topic_id: Mapped[str | None] = mapped_column(String(128), nullable=True)
    source_thread_id: Mapped[str | None] = mapped_column(String(128), nullable=True)
    source_thread_title: Mapped[str | None] = mapped_column(String(512), nullable=True)
    updated_by_huid: Mapped[str] = mapped_column(String(128), nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        default=lambda: datetime.now(tz=UTC),
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        default=lambda: datetime.now(tz=UTC),
    )


class MigrationOperatorDefaultsModel(Base):
    __tablename__ = "migration_operator_defaults"
    __table_args__ = (
        UniqueConstraint(
            "migration_id",
            "operator_huid",
            name="uq_migration_operator_defaults_key",
        ),
        Index(
            "ix_migration_operator_defaults_migration_id",
            "migration_id",
        ),
        CheckConstraint(
            "include_from IS NULL OR include_to IS NULL OR include_from <= include_to",
            name="ck_migration_operator_defaults_include_range",
        ),
    )

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    migration_id: Mapped[str] = mapped_column(String(128), nullable=False)
    operator_huid: Mapped[str] = mapped_column(String(128), nullable=False)
    include_from: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    include_to: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    migrate_media: Mapped[bool] = mapped_column(Boolean, nullable=False)
    media_kinds_json: Mapped[list[str] | None] = mapped_column(JSONB, nullable=True)
    service_messages: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    reply_mode: Mapped[str] = mapped_column(String(64), nullable=False)
    output_template: Mapped[str | None] = mapped_column(Text, nullable=True)
    access_strategy: Mapped[str] = mapped_column(String(32), nullable=False, default="direct_add")
    topic_strategy: Mapped[str] = mapped_column(String(32), nullable=False, default="single_chat")
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        default=lambda: datetime.now(tz=UTC),
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        default=lambda: datetime.now(tz=UTC),
    )

class MigrationMessageMapModel(Base):
    __tablename__ = "migration_message_map"
    __table_args__ = (
        UniqueConstraint(
            "migration_id",
            "source_chat_id",
            "source_message_id",
            name="uq_migration_message_map_dedup_key",
        ),
        Index(
            "ix_migration_message_map_status",
            "migration_id",
            "source_chat_id",
            "import_status",
        ),
        CheckConstraint(
            "import_status IN ('processing', 'imported', 'failed', 'ambiguous')",
            name="ck_migration_message_map_status",
        ),
    )

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    migration_id: Mapped[str] = mapped_column(String(128), nullable=False)
    source_chat_id: Mapped[str] = mapped_column(String(128), nullable=False)
    source_message_id: Mapped[str] = mapped_column(String(128), nullable=False)
    source_sent_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    target_chat_id: Mapped[str] = mapped_column(String(128), nullable=False)
    target_sync_id: Mapped[str | None] = mapped_column(String(128), nullable=True)
    checksum: Mapped[str] = mapped_column(String(64), nullable=False)
    import_status: Mapped[str] = mapped_column(String(32), nullable=False)
    imported_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    rendered_body: Mapped[str | None] = mapped_column(Text, nullable=True)
    last_error_code: Mapped[str | None] = mapped_column(String(128), nullable=True)
    last_error_payload: Mapped[dict | None] = mapped_column(JSONB, nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
    )


class MigrationAttachmentMapModel(Base):
    __tablename__ = "migration_attachment_map"
    __table_args__ = (
        UniqueConstraint(
            "migration_id",
            "source_chat_id",
            "source_message_id",
            "attachment_index",
            name="uq_migration_attachment_map_key",
        ),
        Index(
            "ix_migration_attachment_map_status",
            "migration_id",
            "source_chat_id",
            "import_status",
        ),
        Index(
            "ix_migration_attachment_map_message",
            "migration_id",
            "source_chat_id",
            "source_message_id",
        ),
        CheckConstraint(
            "import_status IN ('processing', 'imported', 'failed', 'ambiguous', 'skipped')",
            name="ck_migration_attachment_map_status",
        ),
    )

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    migration_id: Mapped[str] = mapped_column(String(128), nullable=False)
    source_chat_id: Mapped[str] = mapped_column(String(128), nullable=False)
    source_message_id: Mapped[str] = mapped_column(String(128), nullable=False)
    attachment_index: Mapped[int] = mapped_column(Integer, nullable=False)
    source_file_id: Mapped[str | None] = mapped_column(String(256), nullable=True)
    source_filename: Mapped[str | None] = mapped_column(String(512), nullable=True)
    media_kind: Mapped[str] = mapped_column(String(64), nullable=False)
    checksum: Mapped[str | None] = mapped_column(String(64), nullable=True)
    size_bytes: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    target_chat_id: Mapped[str] = mapped_column(String(128), nullable=False)
    target_sync_id: Mapped[str | None] = mapped_column(String(128), nullable=True)
    import_status: Mapped[str] = mapped_column(String(32), nullable=False)
    imported_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    last_error_code: Mapped[str | None] = mapped_column(String(128), nullable=True)
    last_error_payload: Mapped[dict | None] = mapped_column(JSONB, nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
    )


class AttachmentStageModel(Base):
    __tablename__ = "attachment_stage"
    __table_args__ = (
        UniqueConstraint(
            "migration_id",
            "source_chat_id",
            "source_message_id",
            "attachment_index",
            "target_chat_id",
            name="uq_attachment_stage_key",
        ),
        Index(
            "ix_attachment_stage_migration_chat_message",
            "migration_id",
            "source_chat_id",
            "source_message_id",
        ),
        CheckConstraint(
            "status IN ('processing', 'uploaded', 'attached', 'failed')",
            name="ck_attachment_stage_status",
        ),
    )

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    migration_id: Mapped[str] = mapped_column(String(128), nullable=False)
    source_chat_id: Mapped[str] = mapped_column(String(128), nullable=False)
    source_message_id: Mapped[str] = mapped_column(String(128), nullable=False)
    attachment_index: Mapped[int] = mapped_column(Integer, nullable=False)
    target_chat_id: Mapped[str] = mapped_column(String(128), nullable=False)
    source_locator: Mapped[str | None] = mapped_column(String(1024), nullable=True)
    media_kind: Mapped[str] = mapped_column(String(64), nullable=False)
    checksum: Mapped[str | None] = mapped_column(String(64), nullable=True)
    size_bytes: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    status: Mapped[str] = mapped_column(String(32), nullable=False)
    express_file_id: Mapped[str | None] = mapped_column(String(128), nullable=True)
    express_file_payload: Mapped[dict | None] = mapped_column(JSONB, nullable=True)
    target_sync_id: Mapped[str | None] = mapped_column(String(128), nullable=True)
    last_error_code: Mapped[str | None] = mapped_column(String(128), nullable=True)
    last_error_payload: Mapped[dict | None] = mapped_column(JSONB, nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
    )


class MigrationCheckpointModel(Base):
    __tablename__ = "migration_checkpoint"
    __table_args__ = (
        UniqueConstraint(
            "migration_id",
            "source_chat_id",
            "mode",
            name="uq_migration_checkpoint_key",
        ),
    )

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    migration_id: Mapped[str] = mapped_column(String(128), nullable=False)
    source_chat_id: Mapped[str] = mapped_column(String(128), nullable=False)
    mode: Mapped[str] = mapped_column(String(32), nullable=False)
    cursor_type: Mapped[str] = mapped_column(String(32), nullable=False, default="source_message_id")
    cursor_value: Mapped[str | None] = mapped_column(String(128), nullable=True)
    last_source_message_id: Mapped[str | None] = mapped_column(String(128), nullable=True)
    last_source_sent_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        default=lambda: datetime.now(tz=UTC),
    )

class MigrationInventorySnapshotModel(Base):
    __tablename__ = "migration_inventory_snapshot"
    __table_args__ = (
        UniqueConstraint(
            "migration_id",
            "source_chat_id",
            name="uq_migration_inventory_snapshot_key",
        ),
        Index(
            "ix_migration_inventory_snapshot_migration_id",
            "migration_id",
        ),
    )

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    migration_id: Mapped[str] = mapped_column(String(128), nullable=False)
    source_chat_id: Mapped[str] = mapped_column(String(128), nullable=False)
    source_chat_type: Mapped[str] = mapped_column(String(32), nullable=False)
    source_chat_title: Mapped[str] = mapped_column(String(512), nullable=False)
    messages_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    media_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    approximate_bytes: Mapped[int] = mapped_column(BigInteger, nullable=False, default=0)
    snapshot_payload: Mapped[dict] = mapped_column(JSONB, nullable=False, default=dict)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        default=lambda: datetime.now(tz=UTC),
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        default=lambda: datetime.now(tz=UTC),
    )


class TelegramExportSnapshotModel(Base):
    __tablename__ = "telegram_export_snapshot"
    __table_args__ = (
        UniqueConstraint(
            "source_chat_id",
            name="uq_telegram_export_snapshot_source_chat",
        ),
    )

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    source_chat_id: Mapped[str] = mapped_column(String(128), nullable=False)
    source_chat_title: Mapped[str] = mapped_column(String(512), nullable=False)
    source_chat_type: Mapped[str] = mapped_column(String(32), nullable=False)
    original_filename: Mapped[str | None] = mapped_column(String(512), nullable=True)
    export_chat_id: Mapped[str | None] = mapped_column(String(128), nullable=True)
    archive_locator: Mapped[str] = mapped_column(String(1024), nullable=False)
    payload_sha256: Mapped[str] = mapped_column(String(64), nullable=False)
    message_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    media_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    approximate_bytes: Mapped[int] = mapped_column(BigInteger, nullable=False, default=0)
    uploaded_by_huid: Mapped[str] = mapped_column(String(128), nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        default=lambda: datetime.now(tz=UTC),
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        default=lambda: datetime.now(tz=UTC),
    )


class IdentityMappingModel(Base):
    __tablename__ = "identity_mapping"
    __table_args__ = (
        UniqueConstraint(
            "express_host",
            "telegram_user_id",
            name="uq_identity_mapping_host_telegram_user_id",
        ),
        UniqueConstraint(
            "express_host",
            "telegram_username",
            name="uq_identity_mapping_host_telegram_username",
        ),
    )

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    express_host: Mapped[str | None] = mapped_column(String(255), nullable=True)
    telegram_user_id: Mapped[str | None] = mapped_column(String(128), nullable=True)
    telegram_username: Mapped[str | None] = mapped_column(String(256), nullable=True)
    telegram_display_name: Mapped[str | None] = mapped_column(String(512), nullable=True)
    corporate_email: Mapped[str | None] = mapped_column(String(320), nullable=True)
    target_huid: Mapped[str | None] = mapped_column(String(128), nullable=True)
    last_resolved_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        default=lambda: datetime.now(tz=UTC),
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        default=lambda: datetime.now(tz=UTC),
    )


class ExpressUserCtsBindingModel(Base):
    __tablename__ = "express_user_cts_binding"
    __table_args__ = (
        UniqueConstraint(
            "target_huid",
            name="uq_express_user_cts_binding_target_huid",
        ),
        UniqueConstraint(
            "corporate_email",
            name="uq_express_user_cts_binding_corporate_email",
        ),
        CheckConstraint(
            "target_huid IS NOT NULL OR corporate_email IS NOT NULL",
            name="ck_express_user_cts_binding_identifier",
        ),
    )

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    target_huid: Mapped[str | None] = mapped_column(String(128), nullable=True)
    corporate_email: Mapped[str | None] = mapped_column(String(320), nullable=True)
    cts_host: Mapped[str] = mapped_column(String(255), nullable=False)
    last_verified_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        default=lambda: datetime.now(tz=UTC),
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        default=lambda: datetime.now(tz=UTC),
    )


class ExpressBotHuidBindingModel(Base):
    __tablename__ = "express_bot_huid_binding"
    __table_args__ = (
        UniqueConstraint(
            "bot_id",
            name="uq_express_bot_huid_binding_bot_id",
        ),
        Index(
            "ix_express_bot_huid_binding_cts_host",
            "cts_host",
        ),
    )

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    bot_id: Mapped[str] = mapped_column(String(128), nullable=False)
    cts_host: Mapped[str] = mapped_column(String(255), nullable=False)
    bot_huid: Mapped[str] = mapped_column(String(128), nullable=False)
    last_learned_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        default=lambda: datetime.now(tz=UTC),
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        default=lambda: datetime.now(tz=UTC),
    )


class MigrationStateModel(Base):
    __tablename__ = "migration_state"
    __table_args__ = (
        CheckConstraint(
            "status IN ('active', 'paused', 'needs_manual_reconcile', 'completed')",
            name="ck_migration_state_status",
        ),
    )

    migration_id: Mapped[str] = mapped_column(String(128), primary_key=True)
    status: Mapped[str] = mapped_column(String(64), nullable=False)
    freeze_started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    finalized_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    last_reconcile_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    last_blockers_json: Mapped[dict] = mapped_column(JSONB, nullable=False, default=dict)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        default=lambda: datetime.now(tz=UTC),
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        default=lambda: datetime.now(tz=UTC),
    )


class MigrationJobModel(Base):
    __tablename__ = "migration_job"
    __table_args__ = (
        CheckConstraint(
            "status IN ('queued', 'running', 'completed', 'failed')",
            name="ck_migration_job_status",
        ),
        Index(
            "ix_migration_job_migration_status",
            "migration_id",
            "status",
        ),
        Index(
            "ix_migration_job_status_lease",
            "status",
            "lease_expires_at",
        ),
    )

    job_key: Mapped[str] = mapped_column(String(255), primary_key=True)
    migration_id: Mapped[str] = mapped_column(String(128), nullable=False)
    operation: Mapped[str] = mapped_column(String(64), nullable=False)
    operator_huid: Mapped[str] = mapped_column(String(128), nullable=False)
    source_chat_ids: Mapped[list[str]] = mapped_column(JSONB, nullable=False, default=list)
    anchor_cts_host: Mapped[str | None] = mapped_column(String(255), nullable=True)
    anchor_bot_id: Mapped[str | None] = mapped_column(String(128), nullable=True)
    batch_size: Mapped[int] = mapped_column(Integer, nullable=False)
    status: Mapped[str] = mapped_column(String(32), nullable=False)
    worker_id: Mapped[str | None] = mapped_column(String(128), nullable=True)
    attempts: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    last_error_code: Mapped[str | None] = mapped_column(String(128), nullable=True)
    last_error_payload: Mapped[dict | None] = mapped_column(JSONB, nullable=True)
    requested_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        default=lambda: datetime.now(tz=UTC),
    )
    started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    heartbeat_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    lease_expires_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        default=lambda: datetime.now(tz=UTC),
    )


class IntegrationOutboxModel(Base):
    __tablename__ = "integration_outbox"
    __table_args__ = (
        CheckConstraint(
            "status IN ('pending', 'leased', 'published', 'failed')",
            name="ck_integration_outbox_status",
        ),
        Index(
            "ix_integration_outbox_status_lease",
            "status",
            "lease_expires_at",
        ),
        Index(
            "ix_integration_outbox_aggregate",
            "aggregate_type",
            "aggregate_id",
        ),
    )

    event_id: Mapped[str] = mapped_column(String(128), primary_key=True)
    aggregate_type: Mapped[str] = mapped_column(String(64), nullable=False)
    aggregate_id: Mapped[str] = mapped_column(String(255), nullable=False)
    event_type: Mapped[str] = mapped_column(String(128), nullable=False)
    payload_json: Mapped[dict] = mapped_column(JSONB, nullable=False, default=dict)
    headers_json: Mapped[dict] = mapped_column(JSONB, nullable=False, default=dict)
    status: Mapped[str] = mapped_column(String(32), nullable=False)
    leased_by: Mapped[str | None] = mapped_column(String(128), nullable=True)
    leased_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    lease_expires_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    published_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    broker_topic: Mapped[str | None] = mapped_column(String(255), nullable=True)
    broker_partition: Mapped[int | None] = mapped_column(Integer, nullable=True)
    broker_offset: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    attempts: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    last_error_code: Mapped[str | None] = mapped_column(String(128), nullable=True)
    last_error_payload: Mapped[dict | None] = mapped_column(JSONB, nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        default=lambda: datetime.now(tz=UTC),
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        default=lambda: datetime.now(tz=UTC),
    )


class ConsumerInboxModel(Base):
    __tablename__ = "consumer_inbox"
    __table_args__ = (
        Index(
            "ix_consumer_inbox_processed_at",
            "processed_at",
        ),
    )

    consumer_name: Mapped[str] = mapped_column(String(128), primary_key=True)
    message_id: Mapped[str] = mapped_column(String(255), primary_key=True)
    processed_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    result_code: Mapped[str] = mapped_column(String(128), nullable=False)
    result_payload: Mapped[dict] = mapped_column(JSONB, nullable=False, default=dict)


class ServiceWatermarkModel(Base):
    __tablename__ = "service_watermark"
    __table_args__ = (
        Index("ix_service_watermark_updated_at", "updated_at"),
    )

    consumer_name: Mapped[str] = mapped_column(String(128), primary_key=True)
    logical_stream_key: Mapped[str] = mapped_column(String(255), primary_key=True)
    watermark_value: Mapped[str] = mapped_column(String(255), nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
