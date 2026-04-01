"""Promote attachment size columns to bigint.

Revision ID: 20260401_0027
Revises: 20260327_0026
Create Date: 2026-04-01 18:45:00.000000

"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "20260401_0027"
down_revision = "20260327_0026"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.alter_column(
        "migration_attachment_map",
        "size_bytes",
        existing_type=sa.Integer(),
        type_=sa.BigInteger(),
        existing_nullable=True,
    )
    op.alter_column(
        "attachment_stage",
        "size_bytes",
        existing_type=sa.Integer(),
        type_=sa.BigInteger(),
        existing_nullable=True,
    )


def downgrade() -> None:
    op.alter_column(
        "attachment_stage",
        "size_bytes",
        existing_type=sa.BigInteger(),
        type_=sa.Integer(),
        existing_nullable=True,
    )
    op.alter_column(
        "migration_attachment_map",
        "size_bytes",
        existing_type=sa.BigInteger(),
        type_=sa.Integer(),
        existing_nullable=True,
    )
