"""Add service message and media selection fields to chat config.

Revision ID: 20260423_0028
Revises: 20260401_0027
Create Date: 2026-04-23 13:30:00.000000

"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = "20260423_0028"
down_revision = "20260401_0027"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "migration_chat_config",
        sa.Column(
            "media_kinds_json",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=True,
        ),
    )
    op.add_column(
        "migration_chat_config",
        sa.Column(
            "service_messages",
            sa.Boolean(),
            nullable=False,
            server_default=sa.true(),
        ),
    )
    op.alter_column(
        "migration_chat_config",
        "service_messages",
        server_default=None,
    )


def downgrade() -> None:
    op.drop_column("migration_chat_config", "service_messages")
    op.drop_column("migration_chat_config", "media_kinds_json")
