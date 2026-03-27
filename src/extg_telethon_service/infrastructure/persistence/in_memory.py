from __future__ import annotations

import asyncio

from extg_shared.contracts.models import AuditEvent, OperatorTelegramSessionRecord


class InMemoryOperatorTelegramSessionRepository:
    def __init__(self) -> None:
        self._items: dict[str, OperatorTelegramSessionRecord] = {}
        self._lock = asyncio.Lock()

    async def get_by_operator(
        self,
        operator_huid: str,
    ) -> OperatorTelegramSessionRecord | None:
        async with self._lock:
            return self._items.get(operator_huid)

    async def save(
        self,
        record: OperatorTelegramSessionRecord,
    ) -> OperatorTelegramSessionRecord:
        async with self._lock:
            existing = self._items.get(record.operator_huid)
            updated = OperatorTelegramSessionRecord(
                operator_huid=record.operator_huid,
                phone_number=record.phone_number,
                session_path=record.session_path,
                telegram_user_id=record.telegram_user_id,
                telegram_username=record.telegram_username,
                telegram_display_name=record.telegram_display_name,
                created_at=existing.created_at if existing is not None else record.created_at,
                updated_at=record.updated_at,
                last_used_at=record.last_used_at,
            )
            self._items[record.operator_huid] = updated
            return updated

    async def delete(self, operator_huid: str) -> bool:
        async with self._lock:
            return self._items.pop(operator_huid, None) is not None


class InMemoryAuditRepository:
    def __init__(self) -> None:
        self._items: list[AuditEvent] = []
        self._lock = asyncio.Lock()

    async def add(self, event: AuditEvent) -> None:
        async with self._lock:
            self._items.append(event)

    async def list_all(self) -> list[AuditEvent]:
        async with self._lock:
            return list(self._items)


__all__ = [
    "InMemoryAuditRepository",
    "InMemoryOperatorTelegramSessionRepository",
]
