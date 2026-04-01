from types import SimpleNamespace
from pathlib import Path

import pytest

from extg_shared.contracts.errors import FatalItemError
from extg_shared.contracts.models import FilePayload
from extg_migration_runtime.infrastructure.express.pybotx_file_store import (
    PybotxExpressFileStore,
)


class StubBot:
    def __init__(self) -> None:
        self.calls = []

    async def upload_file(self, **kwargs):
        self.calls.append(kwargs)
        return SimpleNamespace(
            type=SimpleNamespace(value="voice"),
            _file_id="file-1",
            file_url="https://files.example/file-1",
            filename=kwargs["filename"],
            size=9,
            file_mimetype="audio/ogg",
            file_hash="hash-1",
            duration=7,
            file_preview=None,
            file_preview_height=None,
            file_preview_width=None,
            file_encryption_algo=None,
            chunk_size=1024,
            caption=None,
        )


@pytest.mark.asyncio
async def test_upload_file_maps_async_voice_payload_to_domain_staged_file():
    bot = StubBot()
    store = PybotxExpressFileStore(
        bot_id="043a8472-0ec8-5f35-a5a4-3f3ef3ae4aa9",
        cts_url="https://cts11dev.ccsteam.ru/",
        secret_key="secret",
        bot=bot,
    )

    staged_file = await store.upload_file(
        "6367c7c9-6dec-5960-8aa8-6b6c6b57048e",
        file=FilePayload(
            content=b"voice-data",
            filename="voice.ogg",
            mime_type="audio/ogg",
            media_kind="voice",
            duration_seconds=7,
        ),
    )

    assert bot.calls
    assert staged_file.attachment_type == "voice"
    assert staged_file.file_id == "file-1"
    assert staged_file.filename == "voice.ogg"
    assert staged_file.duration_seconds == 7
    assert "async_buffer" in bot.calls[0]


@pytest.mark.asyncio
async def test_upload_file_reads_from_local_path_without_materializing_content(
    tmp_path: Path,
) -> None:
    payload_path = tmp_path / "voice.ogg"
    payload_path.write_bytes(b"voice-data")

    bot = StubBot()
    store = PybotxExpressFileStore(
        bot_id="043a8472-0ec8-5f35-a5a4-3f3ef3ae4aa9",
        cts_url="https://cts11dev.ccsteam.ru/",
        secret_key="secret",
        bot=bot,
    )

    staged_file = await store.upload_file(
        "6367c7c9-6dec-5960-8aa8-6b6c6b57048e",
        file=FilePayload(
            filename="voice.ogg",
            local_path=str(payload_path),
            mime_type="audio/ogg",
            media_kind="voice",
            duration_seconds=7,
        ),
    )

    assert bot.calls
    assert staged_file.filename == "voice.ogg"
    assert bot.calls[0]["filename"] == "voice.ogg"


@pytest.mark.asyncio
async def test_upload_file_rejects_payload_over_size_limit():
    bot = StubBot()
    store = PybotxExpressFileStore(
        bot_id="043a8472-0ec8-5f35-a5a4-3f3ef3ae4aa9",
        cts_url="https://cts11dev.ccsteam.ru/",
        secret_key="secret",
        bot=bot,
        max_upload_size_bytes=4,
    )

    with pytest.raises(FatalItemError, match="size limit exceeded"):
        await store.upload_file(
            "6367c7c9-6dec-5960-8aa8-6b6c6b57048e",
            file=FilePayload(
                content=b"voice-data",
                filename="voice.ogg",
                mime_type="audio/ogg",
                media_kind="voice",
                duration_seconds=7,
            ),
        )

    assert bot.calls == []


@pytest.mark.asyncio
async def test_file_store_uses_extended_write_timeout_for_attachment_requests():
    store = PybotxExpressFileStore(
        bot_id="043a8472-0ec8-5f35-a5a4-3f3ef3ae4aa9",
        cts_url="https://cts11dev.ccsteam.ru/",
        secret_key="secret",
        request_timeout_seconds=20.0,
        attachment_request_timeout_seconds=300.0,
    )

    try:
        timeout = store._httpx_client.timeout
        assert timeout.connect == 20.0
        assert timeout.read == 20.0
        assert timeout.write == 300.0
        assert timeout.pool == 20.0
    finally:
        await store.close()
