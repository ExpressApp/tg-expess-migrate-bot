from __future__ import annotations

import pytest

from extg_migration_runtime.infrastructure.express.router import (
    ExpressFileStoreRouter,
    ExpressGatewayRouter,
)
from extg_shared.config.common import (
    ExpressBotAccountRegistry,
    ExpressBotAccountSettings,
)
from extg_shared.contracts.errors import FatalItemError
from extg_shared.contracts.models import ExpressStagedFile, FilePayload, SentMessageRef


class RecordingGateway:
    def __init__(self, host: str) -> None:
        self.host = host
        self.calls: list[str] = []

    async def create_chat(self, title: str, participant_huids=None, *, chat_type=None) -> str:
        self.calls.append("create_chat")
        return f"{self.host}:{title}"

    async def ensure_chat_members(self, target_chat_id: str, participant_huids: list[str]) -> tuple[str, ...]:
        self.calls.append("ensure_chat_members")
        return tuple(participant_huids)

    async def promote_chat_admins(
        self,
        target_chat_id: str,
        participant_huids: list[str],
    ) -> tuple[str, ...]:
        self.calls.append("promote_chat_admins")
        return tuple(participant_huids)

    async def ensure_personal_chat(self, user_huid: str, *, title: str | None = None) -> str:
        self.calls.append("ensure_personal_chat")
        return f"{self.host}:personal:{user_huid}"

    async def create_chat_link(self, target_chat_id: str) -> str | None:
        self.calls.append("create_chat_link")
        return f"https://{self.host}/{target_chat_id}"

    async def search_user_by_email(self, email: str) -> str | None:
        self.calls.append("search_user_by_email")
        return f"{self.host}:{email}"

    async def search_user_by_huid(self, huid: str) -> str | None:
        self.calls.append("search_user_by_huid")
        return f"{self.host}:{huid}"

    async def search_user_by_ad_login(
        self,
        ad_login: str,
        *,
        ad_domain: str | None = None,
    ) -> str | None:
        self.calls.append("search_user_by_ad_login")
        suffix = f"@{ad_domain}" if ad_domain else ""
        return f"{self.host}:{ad_login}{suffix}"

    async def search_user_by_other_id(self, other_id: str) -> str | None:
        self.calls.append("search_user_by_other_id")
        return f"{self.host}:{other_id}"

    async def send_message(
        self,
        target_chat_id: str,
        body: str,
        *,
        idempotency_key: str | None = None,
        file: FilePayload | None = None,
        staged_file: ExpressStagedFile | None = None,
        fallback_file: FilePayload | None = None,
    ) -> SentMessageRef:
        self.calls.append("send_message")
        return SentMessageRef(target_chat_id=target_chat_id, target_sync_id=f"{self.host}:sync")


class RecordingFileStore:
    def __init__(self, host: str) -> None:
        self.host = host
        self.calls: list[str] = []

    async def upload_file(self, target_chat_id: str, *, file: FilePayload) -> ExpressStagedFile:
        self.calls.append(target_chat_id)
        return ExpressStagedFile(
            attachment_type="document",
            file_id=f"{self.host}:file",
            file_url=f"https://{self.host}/file",
            filename=file.filename,
            size_bytes=len(file.content),
            mime_type=file.mime_type,
            file_hash=f"{self.host}:hash",
        )


def _registry() -> ExpressBotAccountRegistry:
    return ExpressBotAccountRegistry(
        (
            ExpressBotAccountSettings(
                role="primary",
                bot_id="00000000-0000-0000-0000-000000000001",
                bot_huid="main-bot-huid",
                cts_url="https://cts-main.example.test",
                secret_key="primary-secret",
            ),
            ExpressBotAccountSettings(
                role="technical",
                bot_id="00000000-0000-0000-0000-000000000002",
                bot_huid="helper-bot-huid",
                cts_url="https://cts-helper.example.test",
                secret_key="helper-secret",
                visible=False,
            ),
        ),
    )


@pytest.mark.asyncio
async def test_express_gateway_router_uses_primary_account_by_default() -> None:
    gateways: dict[str, RecordingGateway] = {}
    router = ExpressGatewayRouter(
        account_registry=_registry(),
        gateway_factory=lambda account: gateways.setdefault(account.cts_host, RecordingGateway(account.cts_host)),
    )

    result = await router.search_user_by_email("user@example.test")

    assert result == "cts-main.example.test:user@example.test"
    assert gateways["cts-main.example.test"].calls == ["search_user_by_email"]
    assert gateways["cts-helper.example.test"].calls == []


@pytest.mark.asyncio
async def test_express_gateway_router_routes_by_context_cts_host() -> None:
    gateways: dict[str, RecordingGateway] = {}
    router = ExpressGatewayRouter(
        account_registry=_registry(),
        gateway_factory=lambda account: gateways.setdefault(account.cts_host, RecordingGateway(account.cts_host)),
    )

    with router.use_cts_host("cts-helper.example.test"):
        result = await router.search_user_by_huid("user-huid")

    assert result == "cts-helper.example.test:user-huid"
    assert gateways["cts-helper.example.test"].calls == ["search_user_by_huid"]


@pytest.mark.asyncio
async def test_express_gateway_router_routes_ad_login_search_by_context_cts_host() -> None:
    gateways: dict[str, RecordingGateway] = {}
    router = ExpressGatewayRouter(
        account_registry=_registry(),
        gateway_factory=lambda account: gateways.setdefault(account.cts_host, RecordingGateway(account.cts_host)),
    )

    with router.use_cts_host("cts-helper.example.test"):
        result = await router.search_user_by_ad_login("alice", ad_domain="corp.example")

    assert result == "cts-helper.example.test:alice@corp.example"
    assert gateways["cts-helper.example.test"].calls == ["search_user_by_ad_login"]


def test_express_gateway_router_exposes_configured_bot_huid_by_cts_host() -> None:
    router = ExpressGatewayRouter(
        account_registry=_registry(),
        gateway_factory=lambda account: RecordingGateway(account.cts_host),
    )

    assert router.bot_huid_for_cts_host("cts-helper.example.test") == "helper-bot-huid"
    assert router.bot_huid_for_cts_host(None) == "main-bot-huid"


def test_express_gateway_router_falls_back_to_bot_id_when_bot_huid_is_not_configured() -> None:
    router = ExpressGatewayRouter(
        account_registry=ExpressBotAccountRegistry(
            (
                ExpressBotAccountSettings(
                    role="primary",
                    bot_id="00000000-0000-0000-0000-000000000001",
                    cts_url="https://cts-main.example.test",
                    secret_key="primary-secret",
                ),
                ExpressBotAccountSettings(
                    role="technical",
                    bot_id="00000000-0000-0000-0000-000000000002",
                    cts_url="https://cts-helper.example.test",
                    secret_key="helper-secret",
                    visible=False,
                ),
            ),
        ),
        gateway_factory=lambda account: RecordingGateway(account.cts_host),
    )

    assert (
        router.bot_huid_for_cts_host("cts-helper.example.test")
        == "00000000-0000-0000-0000-000000000002"
    )
    assert router.bot_huid_for_cts_host(None) == "00000000-0000-0000-0000-000000000001"


@pytest.mark.asyncio
async def test_express_gateway_router_rejects_unknown_context_cts_host() -> None:
    router = ExpressGatewayRouter(
        account_registry=_registry(),
        gateway_factory=lambda account: RecordingGateway(account.cts_host),
    )

    with pytest.raises(FatalItemError, match="No eXpress bot account configured for CTS host"):
        with router.use_cts_host("cts-unknown.example.test"):
            await router.create_chat("chat")


@pytest.mark.asyncio
async def test_express_file_store_router_routes_by_context_cts_host() -> None:
    file_stores: dict[str, RecordingFileStore] = {}
    router = ExpressFileStoreRouter(
        account_registry=_registry(),
        file_store_factory=lambda account: file_stores.setdefault(account.cts_host, RecordingFileStore(account.cts_host)),
    )

    with router.use_cts_host("https://cts-helper.example.test/"):
        staged = await router.upload_file(
            "target-chat",
            file=FilePayload(
                filename="doc.txt",
                content=b"payload",
                mime_type="text/plain",
                media_kind="document",
            ),
        )

    assert staged.file_id == "cts-helper.example.test:file"
    assert file_stores["cts-helper.example.test"].calls == ["target-chat"]
