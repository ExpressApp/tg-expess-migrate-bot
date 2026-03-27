from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta
from typing import Any, Hashable


class InMemoryFSMStateRepository:
    def __init__(self) -> None:
        self._items: dict[str, tuple[Any, datetime | None]] = {}
        self._lock = asyncio.Lock()

    async def get(self, key: Hashable, default: Any = None) -> Any:
        storage_key = str(key)
        async with self._lock:
            entry = self._items.get(storage_key)
            if entry is None:
                return default
            value, expires_at = entry
            if expires_at is not None and expires_at <= _now():
                self._items.pop(storage_key, None)
                return default
            return value

    async def set(
        self,
        key: Hashable,
        value: Any,
        expire: int | None = None,
    ) -> None:
        expires_at = _now() + timedelta(seconds=expire) if expire is not None else None
        async with self._lock:
            self._items[str(key)] = (value, expires_at)

    async def delete(self, key: Hashable) -> None:
        async with self._lock:
            self._items.pop(str(key), None)


def _now() -> datetime:
    return datetime.now(tz=UTC)
