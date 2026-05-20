"""Add member sync counters to chat mappings.

Revision ID: 20260423_0030
Revises: 20260423_0029
Create Date: 2026-04-23 20:10:00.000000

"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "20260423_0030"
down_revision = "20260423_0029"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "migration_chat_map",
        sa.Column("member_success_count", sa.Integer(), nullable=True),
    )
    op.add_column(
        "migration_chat_map",
        sa.Column("member_total_count", sa.Integer(), nullable=True),
    )


def downgrade() -> None:
    op.drop_column("migration_chat_map", "member_total_count")
    op.drop_column("migration_chat_map", "member_success_count")
