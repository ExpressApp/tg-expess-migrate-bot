"""Add output template fields for bot-managed migration configs.

Revision ID: 20260423_0031
Revises: 20260423_0030
Create Date: 2026-04-23 21:30:00.000000

"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "20260423_0031"
down_revision = "20260423_0030"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "migration_chat_config",
        sa.Column("output_template", sa.Text(), nullable=True),
    )
    op.add_column(
        "migration_operator_defaults",
        sa.Column("output_template", sa.Text(), nullable=True),
    )


def downgrade() -> None:
    op.drop_column("migration_operator_defaults", "output_template")
    op.drop_column("migration_chat_config", "output_template")
