from types import SimpleNamespace

import httpx
import pytest
from pybotx import ChatTypes
from pybotx.constants import MAX_NOTIFICATION_BODY_LENGTH
from pybotx.models.attachments import AttachmentDocument, AttachmentImage, AttachmentVoice

from extg_shared.contracts.models import ExpressStagedFile, FilePayload
from extg_migration_runtime.infrastructure.express.pybotx_gateway import (
    PybotxExpressGateway,
)


class StubBot:
    def __init__(self) -> None:
        self.calls = []

    async def create_chat(self, **kwargs):
        self.calls.append(kwargs)
        return "created-chat-id"

    async def ensure_personal_chat(self, *, bot_id, user_huid, name=None):
        self.calls.append(
            {
                "bot_id": bot_id,
                "user_huid": user_huid,
                "name": name,
            },
        )
        return SimpleNamespace(chat_id="personal-chat-id")

    async def send_message(self, **kwargs):
        self.calls.append(kwargs)
        return "sync-id"

    async def search_user_by_ad(self, *, bot_id, ad_login, ad_domain):
        self.calls.append(
            {
                "bot_id": bot_id,
                "ad_login": ad_login,
                "ad_domain": ad_domain,
            },
        )
        return SimpleNamespace(huid="ad-user-huid")

    async def search_user_by_other_id(self, *, bot_id, other_id):
        self.calls.append(
            {
                "bot_id": bot_id,
                "other_id": other_id,
            },
        )
        return SimpleNamespace(huid="other-user-huid")


def _malformed_staged_file_transport(request: httpx.Request) -> httpx.Response:
    return httpx.Response(
        400,
        json={
            "reason": "malformed_request",
            "status": "error",
            "errors": [],
            "error_data": {"file": {"data": "invalid"}},
        },
        request=request,
    )


@pytest.mark.asyncio
async def test_ensure_personal_chat_returns_chat_info_chat_id():
    gateway = PybotxExpressGateway(
        bot_id="043a8472-0ec8-5f35-a5a4-3f3ef3ae4aa9",
        cts_url="https://cts11dev.ccsteam.ru/",
        secret_key="secret",
        bot=StubBot(),
    )

    chat_id = await gateway.ensure_personal_chat(
        "6367c7c9-6dec-5960-8aa8-6b6c6b57048e",
        title="Operator Chat",
    )

    assert chat_id == "personal-chat-id"


@pytest.mark.asyncio
async def test_create_chat_accepts_explicit_channel_type():
    bot = StubBot()
    gateway = PybotxExpressGateway(
        bot_id="043a8472-0ec8-5f35-a5a4-3f3ef3ae4aa9",
        cts_url="https://cts11dev.ccsteam.ru/",
        secret_key="secret",
        bot=bot,
    )

    chat_id = await gateway.create_chat(
        "Imported Channel",
        participant_huids=["6367c7c9-6dec-5960-8aa8-6b6c6b57048e"],
        chat_type="CHANNEL",
    )

    assert chat_id == "created-chat-id"
    assert bot.calls[-1]["chat_type"] == ChatTypes.CHANNEL


@pytest.mark.asyncio
async def test_send_message_with_voice_uses_attachment_voice():
    bot = StubBot()
    gateway = PybotxExpressGateway(
        bot_id="043a8472-0ec8-5f35-a5a4-3f3ef3ae4aa9",
        cts_url="https://cts11dev.ccsteam.ru/",
        secret_key="secret",
        bot=bot,
    )

    result = await gateway.send_message(
        "6367c7c9-6dec-5960-8aa8-6b6c6b57048e",
        "[voice] voice.ogg",
        file=FilePayload(
            content=b"voice-data",
            filename="voice.ogg",
            mime_type="audio/ogg",
            media_kind="voice",
            duration_seconds=7,
        ),
    )

    assert result.target_sync_id == "sync-id"
    assert isinstance(bot.calls[-1]["file"], AttachmentVoice)
    assert bot.calls[-1]["file"].duration == 7


@pytest.mark.asyncio
async def test_send_message_with_photo_uses_attachment_image():
    bot = StubBot()
    gateway = PybotxExpressGateway(
        bot_id="043a8472-0ec8-5f35-a5a4-3f3ef3ae4aa9",
        cts_url="https://cts11dev.ccsteam.ru/",
        secret_key="secret",
        bot=bot,
    )

    await gateway.send_message(
        "6367c7c9-6dec-5960-8aa8-6b6c6b57048e",
        "[photo] photo.jpg",
        file=FilePayload(
            content=b"jpeg-data",
            filename="photo.jpg",
            mime_type="image/jpeg",
            media_kind="photo",
        ),
    )

    assert isinstance(bot.calls[-1]["file"], AttachmentImage)


@pytest.mark.asyncio
async def test_send_message_with_document_uses_attachment_document():
    bot = StubBot()
    gateway = PybotxExpressGateway(
        bot_id="043a8472-0ec8-5f35-a5a4-3f3ef3ae4aa9",
        cts_url="https://cts11dev.ccsteam.ru/",
        secret_key="secret",
        bot=bot,
    )

    await gateway.send_message(
        "6367c7c9-6dec-5960-8aa8-6b6c6b57048e",
        "[document] doc.txt",
        file=FilePayload(
            content=b"hello",
            filename="doc.txt",
            mime_type="text/plain",
            media_kind="document",
        ),
    )

    assert isinstance(bot.calls[-1]["file"], AttachmentDocument)


@pytest.mark.asyncio
async def test_send_message_with_staged_file_falls_back_to_regular_file_payload():
    bot = StubBot()
    httpx_client = httpx.AsyncClient(
        transport=httpx.MockTransport(_malformed_staged_file_transport),
    )
    gateway = PybotxExpressGateway(
        bot_id="043a8472-0ec8-5f35-a5a4-3f3ef3ae4aa9",
        cts_url="https://cts11dev.ccsteam.ru/",
        secret_key="secret",
        bot=bot,
        httpx_client=httpx_client,
    )

    result = await gateway.send_message(
        "6367c7c9-6dec-5960-8aa8-6b6c6b57048e",
        "[document] doc.txt",
        staged_file=ExpressStagedFile(
            attachment_type="document",
            file_id="file-1",
            file_url="https://files.example/file-1",
            filename="doc.txt",
            size_bytes=5,
            mime_type="text/plain",
            file_hash="hash-1",
        ),
        fallback_file=FilePayload(
            content=b"hello",
            filename="doc.txt",
            mime_type="text/plain",
            media_kind="document",
        ),
    )

    assert result.target_sync_id == "sync-id"
    assert isinstance(bot.calls[-1]["file"], AttachmentDocument)
    await httpx_client.aclose()


@pytest.mark.asyncio
async def test_send_message_chunks_long_body_into_multiple_notifications():
    bot = StubBot()
    gateway = PybotxExpressGateway(
        bot_id="043a8472-0ec8-5f35-a5a4-3f3ef3ae4aa9",
        cts_url="https://cts11dev.ccsteam.ru/",
        secret_key="secret",
        bot=bot,
    )

    long_body = ("a" * (MAX_NOTIFICATION_BODY_LENGTH - 16)) + "\n\n" + ("b" * 64)

    result = await gateway.send_message(
        "6367c7c9-6dec-5960-8aa8-6b6c6b57048e",
        long_body,
        idempotency_key="message-1",
    )

    assert result.target_sync_id == "sync-id"
    assert len(bot.calls) == 2
    assert all(len(call["body"]) <= MAX_NOTIFICATION_BODY_LENGTH for call in bot.calls)
    assert bot.calls[0]["body"].endswith("a" * 16)
    assert bot.calls[1]["body"] == "b" * 64


@pytest.mark.asyncio
async def test_send_message_with_file_chunks_followup_text_without_duplicate_file():
    bot = StubBot()
    gateway = PybotxExpressGateway(
        bot_id="043a8472-0ec8-5f35-a5a4-3f3ef3ae4aa9",
        cts_url="https://cts11dev.ccsteam.ru/",
        secret_key="secret",
        bot=bot,
    )

    long_body = ("x" * (MAX_NOTIFICATION_BODY_LENGTH - 8)) + " " + ("y" * 32)

    await gateway.send_message(
        "6367c7c9-6dec-5960-8aa8-6b6c6b57048e",
        long_body,
        file=FilePayload(
            content=b"hello",
            filename="doc.txt",
            mime_type="text/plain",
            media_kind="document",
        ),
        idempotency_key="message-with-file-1",
    )

    assert len(bot.calls) == 2
    assert isinstance(bot.calls[0]["file"], AttachmentDocument)
    assert "file" not in bot.calls[1]
    assert all(len(call["body"]) <= MAX_NOTIFICATION_BODY_LENGTH for call in bot.calls)


@pytest.mark.asyncio
async def test_search_user_by_ad_login_uses_pybotx_search_user_by_ad() -> None:
    bot = StubBot()
    gateway = PybotxExpressGateway(
        bot_id="043a8472-0ec8-5f35-a5a4-3f3ef3ae4aa9",
        cts_url="https://cts11dev.ccsteam.ru/",
        secret_key="secret",
        bot=bot,
    )

    result = await gateway.search_user_by_ad_login("alice", ad_domain="corp.example")

    assert result == "ad-user-huid"
    assert bot.calls[-1]["ad_login"] == "alice"
    assert bot.calls[-1]["ad_domain"] == "corp.example"


@pytest.mark.asyncio
async def test_search_user_by_other_id_uses_pybotx_search_user_by_other_id() -> None:
    bot = StubBot()
    gateway = PybotxExpressGateway(
        bot_id="043a8472-0ec8-5f35-a5a4-3f3ef3ae4aa9",
        cts_url="https://cts11dev.ccsteam.ru/",
        secret_key="secret",
        bot=bot,
    )

    result = await gateway.search_user_by_other_id("hr-123")

    assert result == "other-user-huid"
    assert bot.calls[-1]["other_id"] == "hr-123"
