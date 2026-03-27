from __future__ import annotations

import asyncio
from contextlib import suppress
from pathlib import Path
import time
from typing import Any


class AttachmentTempFileJanitor:
    def __init__(
        self,
        *,
        directory: str | None,
        enabled: bool = True,
        ttl_seconds: float = 3600.0,
        cleanup_interval_seconds: float = 600.0,
        filename_prefix: str = "extg-",
        logger: Any | None = None,
    ) -> None:
        self._directory = Path(directory).expanduser() if directory else None
        self._enabled = enabled and self._directory is not None
        self._ttl_seconds = max(ttl_seconds, 0.0)
        self._cleanup_interval_seconds = max(cleanup_interval_seconds, 1.0)
        self._filename_prefix = filename_prefix
        self._logger = logger
        self._task: asyncio.Task[None] | None = None

    async def start(self) -> None:
        if not self._enabled or self._task is not None:
            return
        await self.cleanup_once()
        self._task = asyncio.create_task(self._run_loop())

    async def stop(self) -> None:
        if self._task is None:
            return
        self._task.cancel()
        with suppress(asyncio.CancelledError):
            await self._task
        self._task = None

    async def close(self) -> None:
        await self.stop()

    async def cleanup_once(self) -> int:
        if not self._enabled or self._directory is None:
            return 0
        deleted_count = await asyncio.to_thread(self._cleanup_sync)
        if deleted_count:
            self._log(
                "info",
                "attachment temp janitor removed stale files",
                deleted_count=deleted_count,
                directory=str(self._directory),
            )
        return deleted_count

    async def _run_loop(self) -> None:
        while True:
            await asyncio.sleep(self._cleanup_interval_seconds)
            try:
                await self.cleanup_once()
            except Exception as error:
                self._log(
                    "warning",
                    "attachment temp janitor cleanup failed",
                    directory=str(self._directory) if self._directory is not None else None,
                    error_type=type(error).__name__,
                    error=str(error),
                )

    def _cleanup_sync(self) -> int:
        if self._directory is None or not self._directory.exists():
            return 0
        cutoff = time.time() - self._ttl_seconds
        deleted_count = 0
        for candidate in self._directory.iterdir():
            if not candidate.is_file():
                continue
            if not candidate.name.startswith(self._filename_prefix):
                continue
            try:
                modified_at = candidate.stat().st_mtime
            except OSError:
                continue
            if modified_at > cutoff:
                continue
            try:
                candidate.unlink(missing_ok=True)
            except OSError:
                continue
            deleted_count += 1
        return deleted_count

    def _log(self, level: str, event: str, **payload: object) -> None:
        if self._logger is None:
            return
        handler = getattr(self._logger, level, None)
        if handler is None:
            return
        handler(event, **payload)


__all__ = ["AttachmentTempFileJanitor"]
