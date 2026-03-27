from __future__ import annotations

import pytest

from extg_telethon_service.application.telegram_session_context import TelegramSessionContext
from extg_shared.contracts.errors import ConfigurationError
from extg_shared.contracts.models import SourceChannelAccessProfile, SourceDialog
from extg_telethon_service.infrastructure.telegram import session_aware_gateway as gateway_module
from extg_telethon_service.infrastructure.telegram.session_aware_gateway import (
    SessionAwareTelegramGateway,
)


class FakeTelethonGateway:
    created: list[dict[str, object]] = []
    closed: list[str | None] = []

    def __init__(self, **kwargs) -> None:
        self.kwargs = kwargs
        self.session_string = kwargs["session_string"]
        self.list_calls: list[tuple[int, str | None]] = []
        FakeTelethonGateway.created.append(kwargs)

    async def list_available_dialogs(
        self,
        *,
        limit: int = 100,
        query: str | None = None,
        source_backend: str = "telethon_user_session",
    ):
        self.list_calls.append((limit, query))
        return [
            SourceDialog(
                dialog_id="chat-1",
                chat_type="supergroup",
                title="Project Chat",
            ),
        ]

    async def close(self) -> None:
        FakeTelethonGateway.closed.append(self.session_string)

    async def get_channel_access_profile(
        self,
        dialog_id: str,
        *,
        source_backend: str = "telethon_user_session",
    ):
        return SourceChannelAccessProfile(
            dialog_id=dialog_id,
            is_admin=True,
            can_list_participants=True,
        )


@pytest.mark.asyncio
async def test_session_aware_gateway_requires_operator_session(monkeypatch):
    FakeTelethonGateway.created.clear()
    monkeypatch.setattr(gateway_module, "TelethonTelegramGateway", FakeTelethonGateway)
    context = TelegramSessionContext()
    gateway = SessionAwareTelegramGateway(
        session_context=context,
        api_id=20225351,
        api_hash="api-hash",
    )

    with pytest.raises(ConfigurationError, match="/connect"):
        await gateway.list_available_dialogs(limit=5, query="proj")


@pytest.mark.asyncio
async def test_session_aware_gateway_uses_operator_session(monkeypatch):
    FakeTelethonGateway.created.clear()
    FakeTelethonGateway.closed.clear()
    monkeypatch.setattr(gateway_module, "TelethonTelegramGateway", FakeTelethonGateway)
    context = TelegramSessionContext()
    gateway = SessionAwareTelegramGateway(
        session_context=context,
        api_id=20225351,
        api_hash="api-hash",
    )

    with context.use("session-string-1"):
        await gateway.list_available_dialogs(limit=10, query=None)

    assert FakeTelethonGateway.created[0]["session_string"] == "session-string-1"

    await gateway.close_session("session-string-1")

    assert FakeTelethonGateway.closed == ["session-string-1"]


@pytest.mark.asyncio
async def test_session_aware_gateway_delegates_channel_access_profile(monkeypatch):
    FakeTelethonGateway.created.clear()
    monkeypatch.setattr(gateway_module, "TelethonTelegramGateway", FakeTelethonGateway)
    context = TelegramSessionContext()
    gateway = SessionAwareTelegramGateway(
        session_context=context,
        api_id=20225351,
        api_hash="api-hash",
    )

    with context.use("session-string-1"):
        profile = await gateway.get_channel_access_profile("-100123")

    assert profile.dialog_id == "-100123"
    assert profile.is_admin is True
    assert profile.can_list_participants is True
