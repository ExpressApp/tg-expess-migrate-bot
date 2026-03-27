from __future__ import annotations

import asyncio
from fastapi.testclient import TestClient
from types import SimpleNamespace

import extg_bot_ui.presentation.bot.app as bot_app_module
from dependency_injector import providers
from extg_bot_ui.bootstrap.container import BotUiContainer
from extg_bot_ui.bootstrap.config import BotUiSettings
from extg_bot_ui.presentation.bot.app import create_bot_app
from extg_shared.config.common import BotSettings, ExpressSettings, PostgresSettings
from extg_shared.utils.express_routing import get_current_express_cts_host


class FakeBot:
    def __init__(self) -> None:
        self.events = []
        self.callbacks = []
        self.started = False
        self.stopped = False
        self.state = SimpleNamespace()

    def async_execute_raw_bot_command(
        self,
        raw_bot_command,
        *,
        verify_request=True,
        request_headers=None,
    ):
        self.events.append((raw_bot_command, verify_request, dict(request_headers or {})))

    async def raw_get_status(self, query_params, *, verify_request=True, request_headers=None):
        return {"ok": True, "query": dict(query_params), "verify_request": verify_request}

    async def set_raw_botx_method_result(
        self,
        payload,
        *,
        verify_request=True,
        request_headers=None,
    ):
        self.callbacks.append((payload, verify_request, dict(request_headers or {})))

    async def startup(self, *, fetch_tokens=True):
        self.started = True

    async def shutdown(self):
        self.stopped = True


class CapturingBot(FakeBot):
    def __init__(self, *args, **kwargs) -> None:
        super().__init__()
        self.args = args
        self.kwargs = kwargs


class ContextCapturingBot(FakeBot):
    def __init__(self) -> None:
        super().__init__()
        self.observed_cts_host: str | None = None

    def async_execute_raw_bot_command(
        self,
        raw_bot_command,
        *,
        verify_request=True,
        request_headers=None,
    ):
        self.observed_cts_host = get_current_express_cts_host()
        super().async_execute_raw_bot_command(
            raw_bot_command,
            verify_request=verify_request,
            request_headers=request_headers,
        )


def test_create_bot_app_wires_webhook_routes(monkeypatch, tmp_path):
    monkeypatch.setenv("EXTG_ENVIRONMENT", "test")
    monkeypatch.setenv("EXTG_BOT__MIGRATION_ID", "migration-bot")

    app = create_bot_app(BotUiContainer(), bot=FakeBot())

    with TestClient(app) as client:
        health_response = client.get("/health")
        webhook_response = client.post("/command", json={"command": {"body": "/help"}})
        status_response = client.get("/status")
        callback_response = client.post("/notification/callback", json={"sync_id": "123"})
        legacy_webhook_response = client.post(
            "/api/bot/events",
            json={"command": {"body": "/help"}},
        )

    assert health_response.status_code == 200
    assert health_response.json() == {"status": "ok"}
    assert webhook_response.status_code == 200
    assert webhook_response.json() == {"result": "accepted"}
    assert status_response.status_code == 200
    assert status_response.json()["ok"] is True
    assert callback_response.status_code == 204
    assert legacy_webhook_response.status_code == 200


def test_create_bot_app_does_not_start_worker_runtime(monkeypatch):
    monkeypatch.setenv("EXTG_ENVIRONMENT", "test")
    monkeypatch.setenv("EXTG_BOT__MIGRATION_ID", "migration-bot")
    monkeypatch.setenv("EXTG_EXPRESS__BOT_ID", "00000000-0000-0000-0000-000000000001")
    monkeypatch.setenv("EXTG_EXPRESS__CTS_URL", "https://cts.example.test")
    monkeypatch.setenv("EXTG_EXPRESS__SECRET_KEY", "secret")

    fake_runtime_bot = FakeBot()
    container = BotUiContainer()
    monkeypatch.setattr(bot_app_module, "Bot", lambda *args, **kwargs: fake_runtime_bot)

    app = create_bot_app(container)

    with TestClient(app):
        pass

    assert fake_runtime_bot.started is True
    assert fake_runtime_bot.stopped is True


def test_create_bot_app_registers_all_configured_bot_accounts(monkeypatch):
    monkeypatch.setenv("EXTG_ENVIRONMENT", "test")
    monkeypatch.setenv("EXTG_BOT__MIGRATION_ID", "migration-bot")
    monkeypatch.delenv("EXTG_EXPRESS__BOT_ID", raising=False)
    monkeypatch.delenv("EXTG_EXPRESS__CTS_URL", raising=False)
    monkeypatch.delenv("EXTG_EXPRESS__SECRET_KEY", raising=False)
    monkeypatch.setenv(
        "EXTG_EXPRESS__ACCOUNTS",
        (
            '[{"role":"primary","bot_id":"00000000-0000-0000-0000-000000000001",'
            '"cts_url":"https://cts-main.example.test","secret_key":"primary-secret"},'
            '{"role":"technical","bot_id":"00000000-0000-0000-0000-000000000002",'
            '"cts_url":"https://cts-helper.example.test","secret_key":"helper-secret",'
            '"visible":false}]'
        ),
    )

    captured: dict[str, object] = {}

    def build_bot(*args, **kwargs):
        bot = CapturingBot(*args, **kwargs)
        captured["bot"] = bot
        return bot

    monkeypatch.setattr(bot_app_module, "Bot", build_bot)

    app = create_bot_app(BotUiContainer())

    with TestClient(app):
        pass

    runtime_bot = captured["bot"]
    assert len(runtime_bot.kwargs["bot_accounts"]) == 2
    assert {account.host for account in runtime_bot.kwargs["bot_accounts"]} == {
        "cts-main.example.test",
        "cts-helper.example.test",
    }


def test_create_bot_app_sets_express_routing_context_from_incoming_bot_host(monkeypatch):
    monkeypatch.setenv("EXTG_ENVIRONMENT", "test")
    monkeypatch.setenv("EXTG_BOT__MIGRATION_ID", "migration-bot")

    runtime_bot = ContextCapturingBot()
    app = create_bot_app(BotUiContainer(), bot=runtime_bot)

    with TestClient(app) as client:
        response = client.post(
            "/command",
            json={
                "bot": {"host": "cts-helper.example.test"},
                "command": {"body": "/status"},
            },
        )

    assert response.status_code == 200
    assert runtime_bot.observed_cts_host == "cts-helper.example.test"


def test_create_bot_app_ignores_events_from_unconfigured_bot_id(monkeypatch):
    monkeypatch.setenv("EXTG_ENVIRONMENT", "test")
    monkeypatch.setenv("EXTG_BOT__MIGRATION_ID", "migration-bot")
    monkeypatch.setenv(
        "EXTG_EXPRESS__ACCOUNTS",
        (
            '[{"role":"primary","bot_id":"00000000-0000-0000-0000-000000000001",'
            '"cts_url":"https://cts-main.example.test","secret_key":"primary-secret"}]'
        ),
    )

    runtime_bot = FakeBot()
    app = create_bot_app(BotUiContainer(), bot=runtime_bot)

    with TestClient(app) as client:
        response = client.post(
            "/command",
            json={
                "bot_id": "00000000-0000-0000-0000-000000000099",
                "command": {"body": "/status"},
            },
        )

    assert response.status_code == 200
    assert runtime_bot.events == []


def test_create_bot_app_learns_bot_huid_from_bot_originated_system_event(monkeypatch):
    monkeypatch.setenv("EXTG_ENVIRONMENT", "test")
    monkeypatch.setenv("EXTG_BOT__MIGRATION_ID", "migration-bot")

    container = BotUiContainer()
    container.settings.override(
        providers.Object(
            BotUiSettings(
                environment="test",
                postgres=PostgresSettings(),
                bot=BotSettings(migration_id="migration-bot"),
                express=ExpressSettings(
                    accounts=[
                        {
                            "role": "primary",
                            "bot_id": "00000000-0000-0000-0000-000000000001",
                            "cts_url": "https://cts-main.example.test",
                            "secret_key": "primary-secret",
                        },
                    ],
                ),
            ),
        ),
    )
    repository = container.express_bot_huid_binding_repository()
    app = create_bot_app(container, bot=FakeBot())

    with TestClient(app) as client:
        response = client.post(
            "/command",
            json={
                "bot_id": "00000000-0000-0000-0000-000000000001",
                "sync_id": "00000000-0000-0000-0000-000000000123",
                "proto_version": 1,
                "from": {
                    "host": "cts-main.example.test",
                    "user_huid": "00000000-0000-0000-0000-000000000777",
                    "group_chat_id": "00000000-0000-0000-0000-000000000555",
                    "chat_type": "group",
                    "is_admin": True,
                    "is_creator": False,
                },
                "command": {
                    "body": "system:internal_bot_notification",
                    "command_type": "system",
                    "data": {"data": {}, "opts": {}},
                },
            },
        )

    learned = asyncio.run(
        repository.get_by_bot_id("00000000-0000-0000-0000-000000000001"),
    )

    assert response.status_code == 200
    assert learned is not None
    assert learned.bot_huid == "00000000-0000-0000-0000-000000000777"
    assert learned.cts_host == "cts-main.example.test"
