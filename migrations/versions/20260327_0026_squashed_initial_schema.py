"""Squashed current schema baseline.

Revision ID: 20260327_0026
Revises:
Create Date: 2026-03-27 17:44:07.219895

"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = "20260327_0026"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "attachment_stage",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("migration_id", sa.String(length=128), nullable=False),
        sa.Column("source_chat_id", sa.String(length=128), nullable=False),
        sa.Column("source_message_id", sa.String(length=128), nullable=False),
        sa.Column("attachment_index", sa.Integer(), nullable=False),
        sa.Column("target_chat_id", sa.String(length=128), nullable=False),
        sa.Column("source_locator", sa.String(length=1024), nullable=True),
        sa.Column("media_kind", sa.String(length=64), nullable=False),
        sa.Column("checksum", sa.String(length=64), nullable=True),
        sa.Column("size_bytes", sa.Integer(), nullable=True),
        sa.Column("status", sa.String(length=32), nullable=False),
        sa.Column("express_file_id", sa.String(length=128), nullable=True),
        sa.Column(
            "express_file_payload",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=True,
        ),
        sa.Column("target_sync_id", sa.String(length=128), nullable=True),
        sa.Column("last_error_code", sa.String(length=128), nullable=True),
        sa.Column(
            "last_error_payload",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=True,
        ),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.CheckConstraint(
            "status IN ('processing', 'uploaded', 'attached', 'failed')",
            name="ck_attachment_stage_status",
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "migration_id",
            "source_chat_id",
            "source_message_id",
            "attachment_index",
            "target_chat_id",
            name="uq_attachment_stage_key",
        ),
    )
    op.create_index(
        "ix_attachment_stage_migration_chat_message",
        "attachment_stage",
        ["migration_id", "source_chat_id", "source_message_id"],
        unique=False,
    )

    op.create_table(
        "bot_fsm_state",
        sa.Column("storage_key", sa.String(length=512), nullable=False),
        sa.Column("state_module", sa.String(length=255), nullable=False),
        sa.Column("state_class", sa.String(length=255), nullable=False),
        sa.Column("state_name", sa.String(length=128), nullable=False),
        sa.Column(
            "storage_payload",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=False,
        ),
        sa.Column("expires_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("storage_key"),
    )
    op.create_index(
        "ix_bot_fsm_state_expires_at",
        "bot_fsm_state",
        ["expires_at"],
        unique=False,
    )

    op.create_table(
        "consumer_inbox",
        sa.Column("consumer_name", sa.String(length=128), nullable=False),
        sa.Column("message_id", sa.String(length=255), nullable=False),
        sa.Column("processed_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("result_code", sa.String(length=128), nullable=False),
        sa.Column(
            "result_payload",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=False,
        ),
        sa.PrimaryKeyConstraint("consumer_name", "message_id"),
    )
    op.create_index(
        "ix_consumer_inbox_processed_at",
        "consumer_inbox",
        ["processed_at"],
        unique=False,
    )

    op.create_table(
        "express_bot_huid_binding",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("bot_id", sa.String(length=128), nullable=False),
        sa.Column("cts_host", sa.String(length=255), nullable=False),
        sa.Column("bot_huid", sa.String(length=128), nullable=False),
        sa.Column("last_learned_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("bot_id", name="uq_express_bot_huid_binding_bot_id"),
    )
    op.create_index(
        "ix_express_bot_huid_binding_cts_host",
        "express_bot_huid_binding",
        ["cts_host"],
        unique=False,
    )

    op.create_table(
        "express_user_cts_binding",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("target_huid", sa.String(length=128), nullable=True),
        sa.Column("corporate_email", sa.String(length=320), nullable=True),
        sa.Column("cts_host", sa.String(length=255), nullable=False),
        sa.Column("last_verified_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.CheckConstraint(
            "target_huid IS NOT NULL OR corporate_email IS NOT NULL",
            name="ck_express_user_cts_binding_identifier",
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "corporate_email",
            name="uq_express_user_cts_binding_corporate_email",
        ),
        sa.UniqueConstraint(
            "target_huid",
            name="uq_express_user_cts_binding_target_huid",
        ),
    )

    op.create_table(
        "identity_mapping",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("express_host", sa.String(length=255), nullable=True),
        sa.Column("telegram_user_id", sa.String(length=128), nullable=True),
        sa.Column("telegram_username", sa.String(length=256), nullable=True),
        sa.Column("telegram_display_name", sa.String(length=512), nullable=True),
        sa.Column("corporate_email", sa.String(length=320), nullable=True),
        sa.Column("target_huid", sa.String(length=128), nullable=True),
        sa.Column("last_resolved_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "express_host",
            "telegram_user_id",
            name="uq_identity_mapping_host_telegram_user_id",
        ),
        sa.UniqueConstraint(
            "express_host",
            "telegram_username",
            name="uq_identity_mapping_host_telegram_username",
        ),
    )

    op.create_table(
        "integration_outbox",
        sa.Column("event_id", sa.String(length=128), nullable=False),
        sa.Column("aggregate_type", sa.String(length=64), nullable=False),
        sa.Column("aggregate_id", sa.String(length=255), nullable=False),
        sa.Column("event_type", sa.String(length=128), nullable=False),
        sa.Column(
            "payload_json",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=False,
        ),
        sa.Column(
            "headers_json",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=False,
        ),
        sa.Column("status", sa.String(length=32), nullable=False),
        sa.Column("leased_by", sa.String(length=128), nullable=True),
        sa.Column("leased_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("lease_expires_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("published_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("broker_topic", sa.String(length=255), nullable=True),
        sa.Column("broker_partition", sa.Integer(), nullable=True),
        sa.Column("broker_offset", sa.BigInteger(), nullable=True),
        sa.Column("attempts", sa.Integer(), nullable=False),
        sa.Column("last_error_code", sa.String(length=128), nullable=True),
        sa.Column(
            "last_error_payload",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=True,
        ),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.CheckConstraint(
            "status IN ('pending', 'leased', 'published', 'failed')",
            name="ck_integration_outbox_status",
        ),
        sa.PrimaryKeyConstraint("event_id"),
    )
    op.create_index(
        "ix_integration_outbox_aggregate",
        "integration_outbox",
        ["aggregate_type", "aggregate_id"],
        unique=False,
    )
    op.create_index(
        "ix_integration_outbox_status_lease",
        "integration_outbox",
        ["status", "lease_expires_at"],
        unique=False,
    )

    op.create_table(
        "migration_attachment_map",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("migration_id", sa.String(length=128), nullable=False),
        sa.Column("source_chat_id", sa.String(length=128), nullable=False),
        sa.Column("source_message_id", sa.String(length=128), nullable=False),
        sa.Column("attachment_index", sa.Integer(), nullable=False),
        sa.Column("source_file_id", sa.String(length=256), nullable=True),
        sa.Column("source_filename", sa.String(length=512), nullable=True),
        sa.Column("media_kind", sa.String(length=64), nullable=False),
        sa.Column("checksum", sa.String(length=64), nullable=True),
        sa.Column("size_bytes", sa.Integer(), nullable=True),
        sa.Column("target_chat_id", sa.String(length=128), nullable=False),
        sa.Column("target_sync_id", sa.String(length=128), nullable=True),
        sa.Column("import_status", sa.String(length=32), nullable=False),
        sa.Column("imported_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_error_code", sa.String(length=128), nullable=True),
        sa.Column(
            "last_error_payload",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=True,
        ),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.CheckConstraint(
            "import_status IN ('processing', 'imported', 'failed', 'ambiguous', 'skipped')",
            name="ck_migration_attachment_map_status",
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "migration_id",
            "source_chat_id",
            "source_message_id",
            "attachment_index",
            name="uq_migration_attachment_map_key",
        ),
    )
    op.create_index(
        "ix_migration_attachment_map_message",
        "migration_attachment_map",
        ["migration_id", "source_chat_id", "source_message_id"],
        unique=False,
    )
    op.create_index(
        "ix_migration_attachment_map_status",
        "migration_attachment_map",
        ["migration_id", "source_chat_id", "import_status"],
        unique=False,
    )

    op.create_table(
        "migration_audit_event",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("migration_id", sa.String(length=128), nullable=False),
        sa.Column("event_type", sa.String(length=64), nullable=False),
        sa.Column("source_chat_id", sa.String(length=128), nullable=True),
        sa.Column("source_message_id", sa.String(length=128), nullable=True),
        sa.Column("severity", sa.String(length=16), nullable=False),
        sa.Column(
            "payload_json",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=False,
        ),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        "ix_migration_audit_event_migration_id",
        "migration_audit_event",
        ["migration_id"],
        unique=False,
    )
    op.create_index(
        "ix_migration_audit_event_source_message",
        "migration_audit_event",
        ["migration_id", "source_chat_id", "source_message_id"],
        unique=False,
    )

    op.create_table(
        "migration_chat_config",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("migration_id", sa.String(length=128), nullable=False),
        sa.Column("source_chat_id", sa.String(length=128), nullable=False),
        sa.Column("source_chat_type", sa.String(length=32), nullable=False),
        sa.Column("source_chat_title", sa.String(length=512), nullable=False),
        sa.Column("anchor_cts_host", sa.String(length=255), nullable=True),
        sa.Column("anchor_bot_id", sa.String(length=128), nullable=True),
        sa.Column("source_backend", sa.String(length=64), nullable=False),
        sa.Column("target_strategy", sa.String(length=64), nullable=False),
        sa.Column("target_title", sa.String(length=512), nullable=True),
        sa.Column("target_chat_id", sa.String(length=128), nullable=True),
        sa.Column("include_from", sa.DateTime(timezone=True), nullable=True),
        sa.Column("include_to", sa.DateTime(timezone=True), nullable=True),
        sa.Column("migrate_media", sa.Boolean(), nullable=False),
        sa.Column("reply_mode", sa.String(length=64), nullable=False),
        sa.Column("identity_policy", sa.String(length=64), nullable=False),
        sa.Column("access_strategy", sa.String(length=32), nullable=False),
        sa.Column("topic_strategy", sa.String(length=32), nullable=False),
        sa.Column("skip_in_all", sa.Boolean(), nullable=False),
        sa.Column("telegram_chat_id", sa.String(length=128), nullable=True),
        sa.Column("source_topic_id", sa.String(length=128), nullable=True),
        sa.Column("source_thread_id", sa.String(length=128), nullable=True),
        sa.Column("source_thread_title", sa.String(length=512), nullable=True),
        sa.Column("updated_by_huid", sa.String(length=128), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.CheckConstraint(
            "include_from IS NULL OR include_to IS NULL OR include_from <= include_to",
            name="ck_migration_chat_config_include_range",
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "migration_id",
            "source_chat_id",
            name="uq_migration_chat_config_key",
        ),
    )
    op.create_index(
        "ix_migration_chat_config_migration_id",
        "migration_chat_config",
        ["migration_id"],
        unique=False,
    )

    op.create_table(
        "migration_chat_map",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("migration_id", sa.String(length=128), nullable=False),
        sa.Column("source_chat_id", sa.String(length=128), nullable=False),
        sa.Column("source_chat_type", sa.String(length=32), nullable=False),
        sa.Column("source_chat_title", sa.String(length=512), nullable=False),
        sa.Column("anchor_cts_host", sa.String(length=255), nullable=True),
        sa.Column("anchor_bot_id", sa.String(length=128), nullable=True),
        sa.Column("target_chat_id", sa.String(length=128), nullable=False),
        sa.Column("target_chat_title", sa.String(length=512), nullable=False),
        sa.Column("status", sa.String(length=32), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "migration_id",
            "source_chat_id",
            name="uq_migration_chat_map_migration_source_chat",
        ),
    )

    op.create_table(
        "migration_checkpoint",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("migration_id", sa.String(length=128), nullable=False),
        sa.Column("source_chat_id", sa.String(length=128), nullable=False),
        sa.Column("mode", sa.String(length=32), nullable=False),
        sa.Column("cursor_type", sa.String(length=32), nullable=False),
        sa.Column("cursor_value", sa.String(length=128), nullable=True),
        sa.Column("last_source_message_id", sa.String(length=128), nullable=True),
        sa.Column("last_source_sent_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "migration_id",
            "source_chat_id",
            "mode",
            name="uq_migration_checkpoint_key",
        ),
    )

    op.create_table(
        "migration_inventory_snapshot",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("migration_id", sa.String(length=128), nullable=False),
        sa.Column("source_chat_id", sa.String(length=128), nullable=False),
        sa.Column("source_chat_type", sa.String(length=32), nullable=False),
        sa.Column("source_chat_title", sa.String(length=512), nullable=False),
        sa.Column("messages_count", sa.Integer(), nullable=False),
        sa.Column("media_count", sa.Integer(), nullable=False),
        sa.Column("approximate_bytes", sa.BigInteger(), nullable=False),
        sa.Column(
            "snapshot_payload",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=False,
        ),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "migration_id",
            "source_chat_id",
            name="uq_migration_inventory_snapshot_key",
        ),
    )
    op.create_index(
        "ix_migration_inventory_snapshot_migration_id",
        "migration_inventory_snapshot",
        ["migration_id"],
        unique=False,
    )

    op.create_table(
        "migration_job",
        sa.Column("job_key", sa.String(length=255), nullable=False),
        sa.Column("migration_id", sa.String(length=128), nullable=False),
        sa.Column("operation", sa.String(length=64), nullable=False),
        sa.Column("operator_huid", sa.String(length=128), nullable=False),
        sa.Column(
            "source_chat_ids",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=False,
        ),
        sa.Column("anchor_cts_host", sa.String(length=255), nullable=True),
        sa.Column("anchor_bot_id", sa.String(length=128), nullable=True),
        sa.Column("batch_size", sa.Integer(), nullable=False),
        sa.Column("status", sa.String(length=32), nullable=False),
        sa.Column("worker_id", sa.String(length=128), nullable=True),
        sa.Column("attempts", sa.Integer(), nullable=False),
        sa.Column("last_error_code", sa.String(length=128), nullable=True),
        sa.Column(
            "last_error_payload",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=True,
        ),
        sa.Column("requested_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("finished_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("heartbeat_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("lease_expires_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.CheckConstraint(
            "status IN ('queued', 'running', 'completed', 'failed')",
            name="ck_migration_job_status",
        ),
        sa.PrimaryKeyConstraint("job_key"),
    )
    op.create_index(
        "ix_migration_job_migration_status",
        "migration_job",
        ["migration_id", "status"],
        unique=False,
    )
    op.create_index(
        "ix_migration_job_status_lease",
        "migration_job",
        ["status", "lease_expires_at"],
        unique=False,
    )

    op.create_table(
        "migration_message_map",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("migration_id", sa.String(length=128), nullable=False),
        sa.Column("source_chat_id", sa.String(length=128), nullable=False),
        sa.Column("source_message_id", sa.String(length=128), nullable=False),
        sa.Column("source_sent_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("target_chat_id", sa.String(length=128), nullable=False),
        sa.Column("target_sync_id", sa.String(length=128), nullable=True),
        sa.Column("checksum", sa.String(length=64), nullable=False),
        sa.Column("import_status", sa.String(length=32), nullable=False),
        sa.Column("imported_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("rendered_body", sa.Text(), nullable=True),
        sa.Column("last_error_code", sa.String(length=128), nullable=True),
        sa.Column(
            "last_error_payload",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=True,
        ),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.CheckConstraint(
            "import_status IN ('processing', 'imported', 'failed', 'ambiguous')",
            name="ck_migration_message_map_status",
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "migration_id",
            "source_chat_id",
            "source_message_id",
            name="uq_migration_message_map_dedup_key",
        ),
    )
    op.create_index(
        "ix_migration_message_map_status",
        "migration_message_map",
        ["migration_id", "source_chat_id", "import_status"],
        unique=False,
    )

    op.create_table(
        "migration_state",
        sa.Column("migration_id", sa.String(length=128), nullable=False),
        sa.Column("status", sa.String(length=64), nullable=False),
        sa.Column("freeze_started_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("finalized_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_reconcile_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column(
            "last_blockers_json",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=False,
        ),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.CheckConstraint(
            "status IN ('active', 'paused', 'needs_manual_reconcile', 'completed')",
            name="ck_migration_state_status",
        ),
        sa.PrimaryKeyConstraint("migration_id"),
    )

    op.create_table(
        "operator_telegram_session",
        sa.Column("operator_huid", sa.String(length=128), nullable=False),
        sa.Column("phone_number", sa.String(length=32), nullable=False),
        sa.Column("session_path", sa.String(length=1024), nullable=False),
        sa.Column("telegram_user_id", sa.String(length=128), nullable=False),
        sa.Column("telegram_username", sa.String(length=256), nullable=True),
        sa.Column("telegram_display_name", sa.String(length=512), nullable=False),
        sa.Column("last_used_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("operator_huid"),
    )

    op.create_table(
        "service_watermark",
        sa.Column("consumer_name", sa.String(length=128), nullable=False),
        sa.Column("logical_stream_key", sa.String(length=255), nullable=False),
        sa.Column("watermark_value", sa.String(length=255), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("consumer_name", "logical_stream_key"),
    )
    op.create_index(
        "ix_service_watermark_updated_at",
        "service_watermark",
        ["updated_at"],
        unique=False,
    )

    op.create_table(
        "telegram_export_snapshot",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("source_chat_id", sa.String(length=128), nullable=False),
        sa.Column("source_chat_title", sa.String(length=512), nullable=False),
        sa.Column("source_chat_type", sa.String(length=32), nullable=False),
        sa.Column("original_filename", sa.String(length=512), nullable=True),
        sa.Column("export_chat_id", sa.String(length=128), nullable=True),
        sa.Column("archive_locator", sa.String(length=1024), nullable=False),
        sa.Column("payload_sha256", sa.String(length=64), nullable=False),
        sa.Column("message_count", sa.Integer(), nullable=False),
        sa.Column("media_count", sa.Integer(), nullable=False),
        sa.Column("approximate_bytes", sa.BigInteger(), nullable=False),
        sa.Column("uploaded_by_huid", sa.String(length=128), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "source_chat_id",
            name="uq_telegram_export_snapshot_source_chat",
        ),
    )

    op.create_table(
        "telegram_observed_chat",
        sa.Column("chat_id", sa.String(length=128), nullable=False),
        sa.Column("chat_type", sa.String(length=32), nullable=False),
        sa.Column("title", sa.String(length=512), nullable=False),
        sa.Column("has_topics", sa.Boolean(), nullable=False),
        sa.Column("message_count", sa.Integer(), nullable=False),
        sa.Column("media_count", sa.Integer(), nullable=False),
        sa.Column("approximate_bytes", sa.BigInteger(), nullable=False),
        sa.Column("last_captured_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.PrimaryKeyConstraint("chat_id"),
    )
    op.create_index(
        "ix_telegram_observed_chat_title",
        "telegram_observed_chat",
        ["title"],
        unique=False,
    )

    op.create_table(
        "telegram_observed_message",
        sa.Column("capture_event_id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("chat_id", sa.String(length=128), nullable=False),
        sa.Column("chat_type", sa.String(length=32), nullable=False),
        sa.Column("chat_title", sa.String(length=512), nullable=False),
        sa.Column("message_id", sa.String(length=128), nullable=False),
        sa.Column("message_id_numeric", sa.BigInteger(), nullable=False),
        sa.Column("thread_id", sa.String(length=128), nullable=True),
        sa.Column("reply_to_message_id", sa.String(length=128), nullable=True),
        sa.Column("sent_at_utc", sa.DateTime(timezone=True), nullable=False),
        sa.Column("edited_at_utc", sa.DateTime(timezone=True), nullable=True),
        sa.Column("has_attachments", sa.Boolean(), nullable=False),
        sa.Column("approximate_bytes", sa.BigInteger(), nullable=False),
        sa.Column(
            "raw_message_json",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=False,
        ),
        sa.Column("observed_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.PrimaryKeyConstraint("capture_event_id"),
        sa.UniqueConstraint(
            "chat_id",
            "message_id",
            name="uq_telegram_observed_message_chat_message",
        ),
    )
    op.create_index(
        "ix_telegram_observed_message_capture_event",
        "telegram_observed_message",
        ["capture_event_id"],
        unique=False,
    )
    op.create_index(
        "ix_telegram_observed_message_chat_sequence",
        "telegram_observed_message",
        ["chat_id", "message_id_numeric"],
        unique=False,
    )
    op.create_index(
        "ix_telegram_observed_message_chat_thread",
        "telegram_observed_message",
        ["chat_id", "thread_id"],
        unique=False,
    )

    op.create_table(
        "telegram_observed_participant",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("chat_id", sa.String(length=128), nullable=False),
        sa.Column("external_id", sa.String(length=128), nullable=False),
        sa.Column("username", sa.String(length=255), nullable=True),
        sa.Column("display_name", sa.String(length=512), nullable=False),
        sa.Column("is_self", sa.Boolean(), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "chat_id",
            "external_id",
            name="uq_telegram_observed_participant_chat_user",
        ),
    )
    op.create_index(
        "ix_telegram_observed_participant_chat",
        "telegram_observed_participant",
        ["chat_id"],
        unique=False,
    )

    op.create_table(
        "telegram_observed_topic",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("chat_id", sa.String(length=128), nullable=False),
        sa.Column("topic_id", sa.String(length=128), nullable=False),
        sa.Column("title", sa.String(length=512), nullable=False),
        sa.Column("top_message_id", sa.String(length=128), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "chat_id",
            "topic_id",
            name="uq_telegram_observed_topic_chat_topic",
        ),
    )
    op.create_index(
        "ix_telegram_observed_topic_chat",
        "telegram_observed_topic",
        ["chat_id"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index(
        "ix_telegram_observed_topic_chat",
        table_name="telegram_observed_topic",
    )
    op.drop_table("telegram_observed_topic")
    op.drop_index(
        "ix_telegram_observed_participant_chat",
        table_name="telegram_observed_participant",
    )
    op.drop_table("telegram_observed_participant")
    op.drop_index(
        "ix_telegram_observed_message_chat_thread",
        table_name="telegram_observed_message",
    )
    op.drop_index(
        "ix_telegram_observed_message_chat_sequence",
        table_name="telegram_observed_message",
    )
    op.drop_index(
        "ix_telegram_observed_message_capture_event",
        table_name="telegram_observed_message",
    )
    op.drop_table("telegram_observed_message")
    op.drop_index(
        "ix_telegram_observed_chat_title",
        table_name="telegram_observed_chat",
    )
    op.drop_table("telegram_observed_chat")
    op.drop_table("telegram_export_snapshot")
    op.drop_index("ix_service_watermark_updated_at", table_name="service_watermark")
    op.drop_table("service_watermark")
    op.drop_table("operator_telegram_session")
    op.drop_table("migration_state")
    op.drop_index(
        "ix_migration_message_map_status",
        table_name="migration_message_map",
    )
    op.drop_table("migration_message_map")
    op.drop_index("ix_migration_job_status_lease", table_name="migration_job")
    op.drop_index("ix_migration_job_migration_status", table_name="migration_job")
    op.drop_table("migration_job")
    op.drop_index(
        "ix_migration_inventory_snapshot_migration_id",
        table_name="migration_inventory_snapshot",
    )
    op.drop_table("migration_inventory_snapshot")
    op.drop_table("migration_checkpoint")
    op.drop_table("migration_chat_map")
    op.drop_index(
        "ix_migration_chat_config_migration_id",
        table_name="migration_chat_config",
    )
    op.drop_table("migration_chat_config")
    op.drop_index(
        "ix_migration_audit_event_source_message",
        table_name="migration_audit_event",
    )
    op.drop_index(
        "ix_migration_audit_event_migration_id",
        table_name="migration_audit_event",
    )
    op.drop_table("migration_audit_event")
    op.drop_index(
        "ix_migration_attachment_map_status",
        table_name="migration_attachment_map",
    )
    op.drop_index(
        "ix_migration_attachment_map_message",
        table_name="migration_attachment_map",
    )
    op.drop_table("migration_attachment_map")
    op.drop_index(
        "ix_integration_outbox_status_lease",
        table_name="integration_outbox",
    )
    op.drop_index(
        "ix_integration_outbox_aggregate",
        table_name="integration_outbox",
    )
    op.drop_table("integration_outbox")
    op.drop_table("identity_mapping")
    op.drop_table("express_user_cts_binding")
    op.drop_index(
        "ix_express_bot_huid_binding_cts_host",
        table_name="express_bot_huid_binding",
    )
    op.drop_table("express_bot_huid_binding")
    op.drop_index("ix_consumer_inbox_processed_at", table_name="consumer_inbox")
    op.drop_table("consumer_inbox")
    op.drop_index("ix_bot_fsm_state_expires_at", table_name="bot_fsm_state")
    op.drop_table("bot_fsm_state")
    op.drop_index(
        "ix_attachment_stage_migration_chat_message",
        table_name="attachment_stage",
    )
    op.drop_table("attachment_stage")
