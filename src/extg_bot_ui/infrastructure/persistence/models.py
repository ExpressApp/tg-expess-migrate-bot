from __future__ import annotations

from datetime import UTC, datetime

from sqlalchemy import DateTime, Index, String
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from extg_shared.persistence.base import Base


class BotFSMStateModel(Base):
    __tablename__ = "bot_fsm_state"
    __table_args__ = (
        Index("ix_bot_fsm_state_expires_at", "expires_at"),
    )

    storage_key: Mapped[str] = mapped_column(String(512), primary_key=True)
    state_module: Mapped[str] = mapped_column(String(255), nullable=False)
    state_class: Mapped[str] = mapped_column(String(255), nullable=False)
    state_name: Mapped[str] = mapped_column(String(128), nullable=False)
    storage_payload: Mapped[dict] = mapped_column(JSONB, nullable=False, default=dict)
    expires_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
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


__all__ = ["BotFSMStateModel"]
