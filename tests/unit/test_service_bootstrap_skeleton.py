from __future__ import annotations

import extg_bot_ui.bootstrap.container as bot_container_module
import extg_bot_ui.bootstrap.main as bot_main_module
import extg_migration_runtime.bootstrap.container as runtime_container_module
import extg_migration_runtime.bootstrap.main as runtime_main_module
import extg_telethon_service.bootstrap.container as telethon_container_module
import extg_telethon_service.bootstrap.main as telethon_main_module


def test_bot_ui_bootstrap_main_delegates_to_bot_ui_package(monkeypatch) -> None:
    called = {"value": False}
    monkeypatch.setattr(
        bot_main_module,
        "canonical_main",
        lambda: called.__setitem__("value", True),
    )

    bot_main_module.main()

    assert called["value"] is True


def test_telethon_service_bootstrap_main_delegates_to_telethon_service_package(monkeypatch) -> None:
    called = {"value": False}
    monkeypatch.setattr(
        telethon_main_module,
        "canonical_main",
        lambda: called.__setitem__("value", True),
    )

    telethon_main_module.main()

    assert called["value"] is True


def test_migration_runtime_bootstrap_main_delegates_to_migration_runtime_package(monkeypatch) -> None:
    called = {"value": False}
    monkeypatch.setattr(
        runtime_main_module,
        "canonical_main",
        lambda: called.__setitem__("value", True),
    )

    runtime_main_module.main()

    assert called["value"] is True


def test_bot_ui_bootstrap_container_builds_service_container() -> None:
    container = bot_container_module.create_container()

    assert hasattr(container, "bot_control_service")
    assert hasattr(container, "fsm_state_repository")
    assert not hasattr(container, "migration_command_consumer")


def test_telethon_service_bootstrap_container_builds_service_container() -> None:
    container = telethon_container_module.create_container()

    assert hasattr(container, "telethon_integration_service")
    assert hasattr(container, "local_telegram_session_service")
    assert not hasattr(container, "express_gateway")


def test_migration_runtime_bootstrap_container_builds_service_container() -> None:
    container = runtime_container_module.create_container()

    assert hasattr(container, "migration_job_worker")
    assert hasattr(container, "migration_command_consumer")
    assert not hasattr(container, "fsm_state_repository")
