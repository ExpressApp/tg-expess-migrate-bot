from __future__ import annotations

import asyncio
import random
from typing import Awaitable, Callable, TypeVar

from extg_shared.contracts.errors import RecoverableItemError

T = TypeVar("T")


class AsyncRetryPolicy:
    def __init__(
        self,
        *,
        max_attempts: int = 3,
        base_delay_seconds: float = 0.2,
        max_delay_seconds: float = 3.0,
        jitter_seconds: float = 0.2,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
        randomizer: Callable[[], float] = random.random,
        retry_exceptions: tuple[type[Exception], ...] = (RecoverableItemError,),
    ) -> None:
        self._max_attempts = max_attempts
        self._base_delay_seconds = base_delay_seconds
        self._max_delay_seconds = max_delay_seconds
        self._jitter_seconds = jitter_seconds
        self._sleep = sleep
        self._randomizer = randomizer
        self._retry_exceptions = retry_exceptions

    async def run(self, operation: Callable[[], Awaitable[T]]) -> T:
        attempt = 1
        while True:
            try:
                return await operation()
            except self._retry_exceptions:
                if attempt >= self._max_attempts:
                    raise
                delay = min(
                    self._max_delay_seconds,
                    self._base_delay_seconds * (2 ** (attempt - 1)),
                )
                delay += self._randomizer() * self._jitter_seconds
                attempt += 1
                await self._sleep(delay)
