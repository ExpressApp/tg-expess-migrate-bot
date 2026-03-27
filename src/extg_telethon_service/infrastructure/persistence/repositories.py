from __future__ import annotations

from datetime import UTC, datetime

from sqlalchemy import delete, select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from extg_shared.contracts.models import AuditEvent, OperatorTelegramSessionRecord
from extg_telethon_service.infrastructure.persistence.models import (
    MigrationAuditEventModel,
    OperatorTelegramSessionModel,
)

_FULL_PAYLOAD_STORAGE_MODE = "full"
_REDACTED_PAYLOAD_STORAGE_MODE = "redacted"
_NONE_PAYLOAD_STORAGE_MODE = "none"
_REDACTED_VALUE = "[redacted]"
_TRUNCATED_VALUE = "[truncated]"
_SAFE_STRING_KEYS = frozenset(
    {
        "migration_id",
        "source_chat_id",
        "source_message_id",
        "target_chat_id",
        "operation",
        "job_key",
        "phase",
        "status",
        "mode",
        "source",
        "event_type",
        "severity",
        "media_kind",
        "resolution_source",
        "reply_mode",
        "identity_policy",
        "cursor_type",
        "cursor_value",
        "source_chat_type",
        "target_strategy",
    },
)
_SAFE_STRING_SUFFIXES = ("_id", "_ids", "_type", "_mode", "_status", "_policy")
_SENSITIVE_STRING_KEYS = frozenset(
    {
        "body",
        "rendered_body",
        "message",
        "error",
        "reason",
        "excerpt",
        "content",
        "phone_number",
        "corporate_email",
        "source_chat_title",
        "target_chat_title",
        "filename",
        "download_url",
        "session_path",
        "phone_code_hash",
        "password",
    },
)
_SENSITIVE_KEY_SUFFIXES = (
    "_username",
    "_display_name",
    "_email",
    "_phone_number",
    "_title",
    "_filename",
    "_download_url",
    "_session_path",
    "_huid",
    "_huids",
)


def _sanitize_payload(payload: dict[str, object] | None, *, mode: str) -> dict[str, object]:
    if mode == _FULL_PAYLOAD_STORAGE_MODE:
        return dict(payload or {})
    if mode == _NONE_PAYLOAD_STORAGE_MODE:
        return {}
    sanitized = _sanitize_payload_value(payload or {}, key=None, depth=0)
    return sanitized if isinstance(sanitized, dict) else {}


def _sanitize_payload_value(
    value: object,
    *,
    key: str | None,
    depth: int,
) -> object:
    if depth >= 6:
        return _TRUNCATED_VALUE
    if key is not None and _is_sensitive_key(key):
        return _REDACTED_VALUE
    if value is None or isinstance(value, bool | int | float):
        return value
    if isinstance(value, str):
        return value if _is_safe_string_key(key) else _REDACTED_VALUE
    if isinstance(value, dict):
        return {
            str(child_key): _sanitize_payload_value(
                child_value,
                key=str(child_key),
                depth=depth + 1,
            )
            for child_key, child_value in value.items()
        }
    if isinstance(value, (list, tuple, set)):
        items = list(value)
        return [
            _sanitize_payload_value(item, key=key, depth=depth + 1)
            for item in items[:50]
        ]
    return _REDACTED_VALUE


def _is_safe_string_key(key: str | None) -> bool:
    if key is None:
        return False
    normalized = key.strip().lower()
    if _is_sensitive_key(normalized):
        return False
    return normalized in _SAFE_STRING_KEYS or normalized.endswith(_SAFE_STRING_SUFFIXES)


def _is_sensitive_key(key: str) -> bool:
    normalized = key.strip().lower()
    return normalized in _SENSITIVE_STRING_KEYS or normalized.endswith(_SENSITIVE_KEY_SUFFIXES)


class PostgresAuditRepository:
    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        *,
        payload_storage_mode: str = _REDACTED_PAYLOAD_STORAGE_MODE,
    ) -> None:
        self._session_factory = session_factory
        self._payload_storage_mode = payload_storage_mode

    async def add(self, event: AuditEvent) -> None:
        statement = insert(MigrationAuditEventModel).values(
            migration_id=event.migration_id,
            event_type=event.event_type,
            source_chat_id=event.source_chat_id,
            source_message_id=event.source_message_id,
            severity=event.severity.value,
            payload_json=_sanitize_payload(
                event.payload_json,
                mode=self._payload_storage_mode,
            ),
            created_at=event.created_at,
        )
        async with self._session_factory() as session:
            await session.execute(statement)
            await session.commit()


class PostgresOperatorTelegramSessionRepository:
    def __init__(self, session_factory: async_sessionmaker[AsyncSession]) -> None:
        self._session_factory = session_factory

    async def get_by_operator(
        self,
        operator_huid: str,
    ) -> OperatorTelegramSessionRecord | None:
        statement = select(OperatorTelegramSessionModel).where(
            OperatorTelegramSessionModel.operator_huid == operator_huid,
        )
        async with self._session_factory() as session:
            model = (await session.execute(statement)).scalar_one_or_none()
            return _operator_telegram_session_to_domain(model) if model else None

    async def save(
        self,
        record: OperatorTelegramSessionRecord,
    ) -> OperatorTelegramSessionRecord:
        statement = (
            insert(OperatorTelegramSessionModel)
            .values(
                operator_huid=record.operator_huid,
                phone_number=record.phone_number,
                session_path=record.session_path,
                telegram_user_id=record.telegram_user_id,
                telegram_username=record.telegram_username,
                telegram_display_name=record.telegram_display_name,
                last_used_at=record.last_used_at,
                created_at=record.created_at,
                updated_at=record.updated_at,
            )
            .on_conflict_do_update(
                index_elements=[OperatorTelegramSessionModel.operator_huid],
                set_={
                    "phone_number": record.phone_number,
                    "session_path": record.session_path,
                    "telegram_user_id": record.telegram_user_id,
                    "telegram_username": record.telegram_username,
                    "telegram_display_name": record.telegram_display_name,
                    "last_used_at": record.last_used_at,
                    "updated_at": record.updated_at,
                },
            )
            .returning(OperatorTelegramSessionModel)
        )
        async with self._session_factory() as session:
            model = (await session.execute(statement)).scalar_one()
            await session.commit()
            return _operator_telegram_session_to_domain(model)

    async def delete(
        self,
        operator_huid: str,
    ) -> bool:
        statement = (
            delete(OperatorTelegramSessionModel)
            .where(OperatorTelegramSessionModel.operator_huid == operator_huid)
            .returning(OperatorTelegramSessionModel.operator_huid)
        )
        async with self._session_factory() as session:
            deleted_operator_huid = (await session.execute(statement)).scalar_one_or_none()
            await session.commit()
            return deleted_operator_huid is not None


def _operator_telegram_session_to_domain(
    model: OperatorTelegramSessionModel,
) -> OperatorTelegramSessionRecord:
    return OperatorTelegramSessionRecord(
        operator_huid=model.operator_huid,
        phone_number=model.phone_number,
        session_path=model.session_path,
        telegram_user_id=model.telegram_user_id,
        telegram_username=model.telegram_username,
        telegram_display_name=model.telegram_display_name,
        created_at=model.created_at,
        updated_at=model.updated_at,
        last_used_at=model.last_used_at,
    )


def _now() -> datetime:
    return datetime.now(tz=UTC)


__all__ = [
    "PostgresAuditRepository",
    "PostgresOperatorTelegramSessionRepository",
]
