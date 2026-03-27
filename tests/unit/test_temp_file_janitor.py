from __future__ import annotations

import asyncio
from pathlib import Path
import time

import pytest

from extg_shared.utils.temp_files import AttachmentTempFileJanitor


@pytest.mark.asyncio
async def test_attachment_temp_file_janitor_removes_only_stale_prefixed_files(
    tmp_path: Path,
) -> None:
    stale = tmp_path / "extg-old.bin"
    fresh = tmp_path / "extg-new.bin"
    foreign = tmp_path / "foreign.bin"
    stale.write_bytes(b"old")
    fresh.write_bytes(b"new")
    foreign.write_bytes(b"foreign")
    old_timestamp = time.time() - 3600
    stale.touch()
    fresh.touch()
    foreign.touch()
    stale_epoch = (old_timestamp, old_timestamp)
    import os

    os.utime(stale, stale_epoch)

    janitor = AttachmentTempFileJanitor(
        directory=str(tmp_path),
        ttl_seconds=60,
        cleanup_interval_seconds=3600,
    )

    deleted_count = await janitor.cleanup_once()

    assert deleted_count == 1
    assert not stale.exists()
    assert fresh.exists()
    assert foreign.exists()


@pytest.mark.asyncio
async def test_attachment_temp_file_janitor_start_and_stop_are_idempotent(
    tmp_path: Path,
) -> None:
    janitor = AttachmentTempFileJanitor(
        directory=str(tmp_path),
        ttl_seconds=60,
        cleanup_interval_seconds=3600,
    )

    await janitor.start()
    await janitor.start()
    await asyncio.sleep(0)
    await janitor.stop()
    await janitor.stop()
