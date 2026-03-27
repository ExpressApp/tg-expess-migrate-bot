from __future__ import annotations

from contextlib import contextmanager
from contextvars import ContextVar
from typing import Iterator


class TelegramSessionContext:
    def __init__(self) -> None:
        self._session_path: ContextVar[str | None] = ContextVar(
            "extg_telegram_session_path",
            default=None,
        )
        self._operator_huid: ContextVar[str | None] = ContextVar(
            "extg_telegram_operator_huid",
            default=None,
        )

    def current_session_path(self) -> str | None:
        return self._session_path.get()

    def current_operator_huid(self) -> str | None:
        return self._operator_huid.get()

    @contextmanager
    def use(
        self,
        session_path: str | None,
        *,
        operator_huid: str | None = None,
    ) -> Iterator[None]:
        token = self._session_path.set(session_path)
        operator_token = self._operator_huid.set(operator_huid)
        try:
            yield
        finally:
            self._operator_huid.reset(operator_token)
            self._session_path.reset(token)
