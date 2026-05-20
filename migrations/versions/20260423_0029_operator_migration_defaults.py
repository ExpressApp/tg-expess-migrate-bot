"""Add operator-level migration defaults storage.

Revision ID: 20260423_0029
Revises: 20260423_0028
Create Date: 2026-04-23 18:30:00.000000

"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = "20260423_0029"
down_revision = "20260423_0028"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "migration_operator_defaults",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("migration_id", sa.String(length=128), nullable=False),
        sa.Column("operator_huid", sa.String(length=128), nullable=False),
        sa.Column("include_from", sa.DateTime(timezone=True), nullable=True),
        sa.Column("include_to", sa.DateTime(timezone=True), nullable=True),
        sa.Column("migrate_media", sa.Boolean(), nullable=False),
        sa.Column(
            "media_kinds_json",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=True,
        ),
        sa.Column(
            "service_messages",
            sa.Boolean(),
            nullable=False,
            server_default=sa.true(),
        ),
        sa.Column("reply_mode", sa.String(length=64), nullable=False),
        sa.Column(
            "access_strategy",
            sa.String(length=32),
            nullable=False,
            server_default="direct_add",
        ),
        sa.Column(
            "topic_strategy",
            sa.String(length=32),
            nullable=False,
            server_default="single_chat",
        ),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.CheckConstraint(
            "include_from IS NULL OR include_to IS NULL OR include_from <= include_to",
            name="ck_migration_operator_defaults_include_range",
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "migration_id",
            "operator_huid",
            name="uq_migration_operator_defaults_key",
        ),
    )
    op.create_index(
        "ix_migration_operator_defaults_migration_id",
        "migration_operator_defaults",
        ["migration_id"],
        unique=False,
    )
    op.alter_column(
        "migration_operator_defaults",
        "service_messages",
        server_default=None,
    )
    op.alter_column(
        "migration_operator_defaults",
        "access_strategy",
        server_default=None,
    )
    op.alter_column(
        "migration_operator_defaults",
        "topic_strategy",
        server_default=None,
    )


def downgrade() -> None:
    op.drop_index(
        "ix_migration_operator_defaults_migration_id",
        table_name="migration_operator_defaults",
    )
    op.drop_table("migration_operator_defaults")
