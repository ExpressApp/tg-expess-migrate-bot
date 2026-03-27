from __future__ import annotations

from datetime import UTC, datetime

from sqlalchemy import (
    BigInteger,
    Boolean,
    DateTime,
    Index,
    Integer,
    String,
    UniqueConstraint,
    func,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from extg_shared.persistence.base import Base


class TelegramObservedChatModel(Base):
    __tablename__ = "telegram_observed_chat"
    __table_args__ = (
        Index("ix_telegram_observed_chat_title", "title"),
    )

    chat_id: Mapped[str] = mapped_column(String(128), primary_key=True)
    chat_type: Mapped[str] = mapped_column(String(32), nullable=False)
    title: Mapped[str] = mapped_column(String(512), nullable=False)
    has_topics: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    message_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    media_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    approximate_bytes: Mapped[int] = mapped_column(BigInteger, nullable=False, default=0)
    last_captured_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
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


class TelegramObservedParticipantModel(Base):
    __tablename__ = "telegram_observed_participant"
    __table_args__ = (
        UniqueConstraint(
            "chat_id",
            "external_id",
            name="uq_telegram_observed_participant_chat_user",
        ),
        Index("ix_telegram_observed_participant_chat", "chat_id"),
    )

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    chat_id: Mapped[str] = mapped_column(String(128), nullable=False)
    external_id: Mapped[str] = mapped_column(String(128), nullable=False)
    username: Mapped[str | None] = mapped_column(String(255), nullable=True)
    display_name: Mapped[str] = mapped_column(String(512), nullable=False)
    is_self: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
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


class TelegramObservedTopicModel(Base):
    __tablename__ = "telegram_observed_topic"
    __table_args__ = (
        UniqueConstraint(
            "chat_id",
            "topic_id",
            name="uq_telegram_observed_topic_chat_topic",
        ),
        Index("ix_telegram_observed_topic_chat", "chat_id"),
    )

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    chat_id: Mapped[str] = mapped_column(String(128), nullable=False)
    topic_id: Mapped[str] = mapped_column(String(128), nullable=False)
    title: Mapped[str] = mapped_column(String(512), nullable=False)
    top_message_id: Mapped[str | None] = mapped_column(String(128), nullable=True)
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


class TelegramObservedMessageModel(Base):
    __tablename__ = "telegram_observed_message"
    __table_args__ = (
        UniqueConstraint(
            "chat_id",
            "message_id",
            name="uq_telegram_observed_message_chat_message",
        ),
        Index("ix_telegram_observed_message_chat_sequence", "chat_id", "message_id_numeric"),
        Index("ix_telegram_observed_message_capture_event", "capture_event_id"),
        Index("ix_telegram_observed_message_chat_thread", "chat_id", "thread_id"),
    )

    capture_event_id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    chat_id: Mapped[str] = mapped_column(String(128), nullable=False)
    chat_type: Mapped[str] = mapped_column(String(32), nullable=False)
    chat_title: Mapped[str] = mapped_column(String(512), nullable=False)
    message_id: Mapped[str] = mapped_column(String(128), nullable=False)
    message_id_numeric: Mapped[int] = mapped_column(BigInteger, nullable=False)
    thread_id: Mapped[str | None] = mapped_column(String(128), nullable=True)
    reply_to_message_id: Mapped[str | None] = mapped_column(String(128), nullable=True)
    sent_at_utc: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    edited_at_utc: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    has_attachments: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    approximate_bytes: Mapped[int] = mapped_column(BigInteger, nullable=False, default=0)
    raw_message_json: Mapped[dict] = mapped_column(JSONB, nullable=False)
    observed_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
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


class MigrationAuditEventModel(Base):
    __tablename__ = "migration_audit_event"
    __table_args__ = (
        Index("ix_migration_audit_event_migration_id", "migration_id"),
        Index(
            "ix_migration_audit_event_source_message",
            "migration_id",
            "source_chat_id",
            "source_message_id",
        ),
    )

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    migration_id: Mapped[str] = mapped_column(String(128), nullable=False)
    event_type: Mapped[str] = mapped_column(String(64), nullable=False)
    source_chat_id: Mapped[str | None] = mapped_column(String(128), nullable=True)
    source_message_id: Mapped[str | None] = mapped_column(String(128), nullable=True)
    severity: Mapped[str] = mapped_column(String(16), nullable=False)
    payload_json: Mapped[dict] = mapped_column(JSONB, nullable=False, default=dict)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        default=lambda: datetime.now(tz=UTC),
    )


class OperatorTelegramSessionModel(Base):
    __tablename__ = "operator_telegram_session"

    operator_huid: Mapped[str] = mapped_column(String(128), primary_key=True)
    phone_number: Mapped[str] = mapped_column(String(32), nullable=False)
    session_path: Mapped[str] = mapped_column(String(1024), nullable=False)
    telegram_user_id: Mapped[str] = mapped_column(String(128), nullable=False)
    telegram_username: Mapped[str | None] = mapped_column(String(256), nullable=True)
    telegram_display_name: Mapped[str] = mapped_column(String(512), nullable=False)
    last_used_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
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


__all__ = [
    "MigrationAuditEventModel",
    "OperatorTelegramSessionModel",
    "TelegramObservedChatModel",
    "TelegramObservedMessageModel",
    "TelegramObservedParticipantModel",
    "TelegramObservedTopicModel",
]
