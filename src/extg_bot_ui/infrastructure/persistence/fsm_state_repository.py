from __future__ import annotations

from datetime import UTC, datetime, timedelta
from enum import Enum
from importlib import import_module
from types import SimpleNamespace
from typing import Any, Hashable

from sqlalchemy import delete, select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from pybotx_fsm.fsm import FSMStateData

from extg_bot_ui.infrastructure.persistence.in_memory_fsm_state import (
    InMemoryFSMStateRepository,
)
from extg_bot_ui.infrastructure.persistence.models import BotFSMStateModel


class PostgresFSMStateRepository:
    def __init__(self, session_factory: async_sessionmaker[AsyncSession]) -> None:
        self._session_factory = session_factory
        self._fallback_repository = InMemoryFSMStateRepository()
        self._schema_unavailable = False

    async def get(self, key: Hashable, default: Any = None) -> Any:
        if self._schema_unavailable:
            return await self._fallback_repository.get(key, default)
        storage_key = str(key)
        try:
            async with self._session_factory() as session:
                model = (
                    await session.execute(
                        select(BotFSMStateModel).where(BotFSMStateModel.storage_key == storage_key),
                    )
                ).scalar_one_or_none()
                if model is None:
                    return default
                if model.expires_at is not None and model.expires_at <= _now():
                    await session.execute(
                        delete(BotFSMStateModel).where(BotFSMStateModel.storage_key == storage_key),
                    )
                    await session.commit()
                    return default
                state = _resolve_state(
                    model.state_module,
                    model.state_class,
                    model.state_name,
                )
                if state is None:
                    await session.execute(
                        delete(BotFSMStateModel).where(BotFSMStateModel.storage_key == storage_key),
                    )
                    await session.commit()
                    return default
                return FSMStateData(
                    state=state,
                    storage=SimpleNamespace(**(model.storage_payload or {})),
                )
        except Exception as error:
            if self._should_fallback_to_memory(error):
                self._schema_unavailable = True
                return await self._fallback_repository.get(key, default)
            raise

    async def set(
        self,
        key: Hashable,
        value: Any,
        expire: int | None = None,
    ) -> None:
        if self._schema_unavailable:
            await self._fallback_repository.set(key, value, expire=expire)
            return
        if not isinstance(value, FSMStateData):
            raise TypeError("PostgresFSMStateRepository only supports pybotx_fsm.FSMStateData")
        payload = dict(vars(value.storage))
        expires_at = _now() + timedelta(seconds=expire) if expire is not None else None
        statement = (
            insert(BotFSMStateModel)
            .values(
                storage_key=str(key),
                state_module=value.state.__class__.__module__,
                state_class=value.state.__class__.__name__,
                state_name=value.state.name,
                storage_payload=payload,
                expires_at=expires_at,
                created_at=_now(),
                updated_at=_now(),
            )
            .on_conflict_do_update(
                index_elements=[BotFSMStateModel.storage_key],
                set_={
                    "state_module": value.state.__class__.__module__,
                    "state_class": value.state.__class__.__name__,
                    "state_name": value.state.name,
                    "storage_payload": payload,
                    "expires_at": expires_at,
                    "updated_at": _now(),
                },
            )
        )
        try:
            async with self._session_factory() as session:
                await session.execute(statement)
                await session.commit()
        except Exception as error:
            if self._should_fallback_to_memory(error):
                self._schema_unavailable = True
                await self._fallback_repository.set(key, value, expire=expire)
                return
            raise

    async def delete(self, key: Hashable) -> None:
        if self._schema_unavailable:
            await self._fallback_repository.delete(key)
            return
        try:
            async with self._session_factory() as session:
                await session.execute(
                    delete(BotFSMStateModel).where(BotFSMStateModel.storage_key == str(key)),
                )
                await session.commit()
        except Exception as error:
            if self._should_fallback_to_memory(error):
                self._schema_unavailable = True
                await self._fallback_repository.delete(key)
                return
            raise

    def _should_fallback_to_memory(self, error: Exception) -> bool:
        texts = [str(error)]
        original = getattr(error, "orig", None)
        if original is not None:
            texts.append(str(original))
            texts.append(original.__class__.__name__)
        combined = " ".join(texts).lower()
        return "bot_fsm_state" in combined and (
            "does not exist" in combined
            or "undefinedtable" in combined
            or "no such table" in combined
        )


def _resolve_state(module_name: str, class_name: str, state_name: str) -> Enum | None:
    try:
        module = import_module(module_name)
        enum_cls = getattr(module, class_name)
        return enum_cls[state_name]
    except Exception:
        return None


def _now() -> datetime:
    return datetime.now(tz=UTC)
