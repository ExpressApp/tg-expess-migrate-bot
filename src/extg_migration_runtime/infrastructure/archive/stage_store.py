from __future__ import annotations

import asyncio
import os
import tempfile
from pathlib import Path

from extg_shared.contracts.errors import ConfigurationError, FatalItemError


class LocalTelegramExportArchiveStageStore:
    def __init__(
        self,
        *,
        directory: str | None,
        filename_prefix: str = "extg-archive-",
    ) -> None:
        self._directory = Path(directory).expanduser() if directory else None
        self._filename_prefix = filename_prefix

    async def stage_upload(
        self,
        *,
        filename: str | None,
        content: bytes,
    ) -> str:
        return await asyncio.to_thread(
            self._stage_upload_sync,
            filename,
            content,
        )

    async def read(self, locator: str) -> bytes:
        path = self._resolve_locator(locator)
        try:
            return await asyncio.to_thread(path.read_bytes)
        except FileNotFoundError as error:
            raise FatalItemError(
                "staged Telegram export archive is missing; re-upload the archive",
            ) from error

    async def delete(self, locator: str) -> bool:
        path = self._resolve_locator(locator)

        def _delete() -> bool:
            try:
                path.unlink()
            except FileNotFoundError:
                return False
            return True

        return await asyncio.to_thread(_delete)

    def _stage_upload_sync(
        self,
        filename: str | None,
        content: bytes,
    ) -> str:
        target_dir = self._ensure_directory()
        suffix = Path(filename or "").suffix
        descriptor, raw_path = tempfile.mkstemp(
            prefix=self._filename_prefix,
            suffix=suffix,
            dir=str(target_dir),
        )
        try:
            with os.fdopen(descriptor, "wb") as handle:
                handle.write(content)
            try:
                os.chmod(raw_path, 0o600)
            except OSError:
                pass
            return raw_path
        except Exception:
            try:
                Path(raw_path).unlink(missing_ok=True)
            except OSError:
                pass
            raise

    def _ensure_directory(self) -> Path:
        if self._directory is None:
            raise ConfigurationError(
                "Telegram export archive staging directory is not configured",
            )
        self._directory.mkdir(parents=True, exist_ok=True, mode=0o700)
        try:
            self._directory.chmod(0o700)
        except OSError:
            pass
        return self._directory

    def _resolve_locator(self, locator: str) -> Path:
        target_dir = self._ensure_directory().resolve()
        candidate = Path(locator).expanduser().resolve()
        try:
            candidate.relative_to(target_dir)
        except ValueError as error:
            raise FatalItemError("invalid staged Telegram export archive locator") from error
        return candidate


class InMemoryTelegramExportArchiveStageStore:
    def __init__(self) -> None:
        self._items: dict[str, bytes] = {}
        self._lock = asyncio.Lock()
        self._sequence = 0

    async def stage_upload(
        self,
        *,
        filename: str | None,
        content: bytes,
    ) -> str:
        async with self._lock:
            self._sequence += 1
            locator = f"in-memory-archive-{self._sequence}"
            self._items[locator] = bytes(content)
            return locator

    async def read(self, locator: str) -> bytes:
        async with self._lock:
            content = self._items.get(locator)
            if content is None:
                raise FatalItemError(
                    "staged Telegram export archive is missing; re-upload the archive",
                )
            return content

    async def delete(self, locator: str) -> bool:
        async with self._lock:
            return self._items.pop(locator, None) is not None
