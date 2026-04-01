from types import SimpleNamespace
from uuid import UUID

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


class MembershipStubBot:
    def __init__(
        self,
        *,
        existing_member_huids: list[str] | None = None,
        confirm_added_huids: bool = True,
    ) -> None:
        self._members = [
            SimpleNamespace(huid=UUID(huid), is_admin=False)
            for huid in (existing_member_huids or [])
        ]
        self._confirm_added_huids = confirm_added_huids
        self.add_calls: list[dict[str, object]] = []

    async def chat_info(self, *, bot_id, chat_id):
        return SimpleNamespace(members=list(self._members))

    async def add_users_to_chat(self, *, bot_id, chat_id, huids):
        self.add_calls.append(
            {
                "bot_id": bot_id,
                "chat_id": chat_id,
                "huids": list(huids),
            },
        )
        if not self._confirm_added_huids:
            return
        existing = {str(member.huid) for member in self._members}
        for huid in huids:
            normalized = str(huid)
            if normalized in existing:
                continue
            self._members.append(
                SimpleNamespace(huid=UUID(normalized), is_admin=False)
            )
            existing.add(normalized)


class AdminStubBot:
    def __init__(
        self,
        *,
        existing_admin_huids: list[str] | None = None,
        confirm_promoted_admin_huids: bool = True,
    ) -> None:
        self._members = [
            SimpleNamespace(huid=UUID(huid), is_admin=True)
            for huid in (existing_admin_huids or [])
        ]
        self._confirm_promoted_admin_huids = confirm_promoted_admin_huids
        self.promote_calls: list[dict[str, object]] = []

    async def chat_info(self, *, bot_id, chat_id):
        return SimpleNamespace(members=list(self._members))

    async def promote_to_chat_admins(self, *, bot_id, chat_id, huids):
        self.promote_calls.append(
            {
                "bot_id": bot_id,
                "chat_id": chat_id,
                "huids": list(huids),
            },
        )
        if not self._confirm_promoted_admin_huids:
            return
        existing = {str(member.huid) for member in self._members if member.is_admin}
        for huid in huids:
            normalized = str(huid)
            if normalized in existing:
                continue
            self._members.append(
                SimpleNamespace(huid=UUID(normalized), is_admin=True)
            )
            existing.add(normalized)


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
async def test_gateway_uses_extended_write_timeout_for_attachment_requests():
    gateway = PybotxExpressGateway(
        bot_id="043a8472-0ec8-5f35-a5a4-3f3ef3ae4aa9",
        cts_url="https://cts11dev.ccsteam.ru/",
        secret_key="secret",
        request_timeout_seconds=20.0,
        attachment_request_timeout_seconds=300.0,
    )

    try:
        timeout = gateway._httpx_client.timeout
        assert timeout.connect == 20.0
        assert timeout.read == 20.0
        assert timeout.write == 300.0
        assert timeout.pool == 20.0
    finally:
        await gateway.close()


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


@pytest.mark.asyncio
async def test_ensure_chat_members_returns_only_confirmed_members() -> None:
    bot = MembershipStubBot(
        existing_member_huids=["6367c7c9-6dec-5960-8aa8-6b6c6b57048e"],
    )
    gateway = PybotxExpressGateway(
        bot_id="043a8472-0ec8-5f35-a5a4-3f3ef3ae4aa9",
        cts_url="https://cts11dev.ccsteam.ru/",
        secret_key="secret",
        bot=bot,
    )

    added = await gateway.ensure_chat_members(
        "043a8472-0ec8-5f35-a5a4-3f3ef3ae4aa9",
        [
            "6367c7c9-6dec-5960-8aa8-6b6c6b57048e",
            "b7ceffbf-6f4f-4f23-87d0-8a61d4d2ea61",
        ],
    )

    assert added == ("b7ceffbf-6f4f-4f23-87d0-8a61d4d2ea61",)
    assert bot.add_calls == [
        {
            "bot_id": UUID("043a8472-0ec8-5f35-a5a4-3f3ef3ae4aa9"),
            "chat_id": UUID("043a8472-0ec8-5f35-a5a4-3f3ef3ae4aa9"),
            "huids": [UUID("b7ceffbf-6f4f-4f23-87d0-8a61d4d2ea61")],
        },
    ]


@pytest.mark.asyncio
async def test_ensure_chat_members_raises_when_add_is_not_confirmed() -> None:
    bot = MembershipStubBot(confirm_added_huids=False)
    gateway = PybotxExpressGateway(
        bot_id="043a8472-0ec8-5f35-a5a4-3f3ef3ae4aa9",
        cts_url="https://cts11dev.ccsteam.ru/",
        secret_key="secret",
        bot=bot,
    )

    with pytest.raises(Exception, match="members not visible after add_users_to_chat"):
        await gateway.ensure_chat_members(
            "043a8472-0ec8-5f35-a5a4-3f3ef3ae4aa9",
            ["b7ceffbf-6f4f-4f23-87d0-8a61d4d2ea61"],
        )


@pytest.mark.asyncio
async def test_promote_chat_admins_raises_when_promotion_is_not_confirmed() -> None:
    bot = AdminStubBot(confirm_promoted_admin_huids=False)
    gateway = PybotxExpressGateway(
        bot_id="043a8472-0ec8-5f35-a5a4-3f3ef3ae4aa9",
        cts_url="https://cts11dev.ccsteam.ru/",
        secret_key="secret",
        bot=bot,
    )

    with pytest.raises(
        Exception,
        match="members not visible as admins after promote_to_chat_admins",
    ):
        await gateway.promote_chat_admins(
            "043a8472-0ec8-5f35-a5a4-3f3ef3ae4aa9",
            ["b7ceffbf-6f4f-4f23-87d0-8a61d4d2ea61"],
        )
