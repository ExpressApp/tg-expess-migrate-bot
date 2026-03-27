from __future__ import annotations

from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
SERVICE_SCAN_ROOTS = (
    REPO_ROOT / "src" / "extg_bot_ui",
    REPO_ROOT / "src" / "extg_telethon_service",
    REPO_ROOT / "src" / "extg_migration_runtime",
)
TEST_SCAN_ROOTS = (REPO_ROOT / "tests",)
EXCLUDED_FILES = {Path(__file__).resolve()}
LEGACY_PACKAGE = "extg" + "_" + "migrator"
FORBIDDEN_SERVICE_WRAPPER_FRAGMENTS = (
    f"{LEGACY_PACKAGE}.app.config",
    f"{LEGACY_PACKAGE}.app.container",
    f"{LEGACY_PACKAGE}.application.bot_control",
    f"{LEGACY_PACKAGE}.application.telegram_session_service",
    f"{LEGACY_PACKAGE}.application.telethon_integration_service",
    f"{LEGACY_PACKAGE}.application.message_delivery",
    f"{LEGACY_PACKAGE}.application.migration_job_worker",
    f"{LEGACY_PACKAGE}.application.migration_command_handler",
    f"{LEGACY_PACKAGE}.application.integration_outbox_publisher",
    f"{LEGACY_PACKAGE}.application.worker_runtime_metrics",
    f"{LEGACY_PACKAGE}.application.use_cases.",
    f"{LEGACY_PACKAGE}.presentation.bot",
    f"{LEGACY_PACKAGE}.presentation.telethon_service",
    f"{LEGACY_PACKAGE}.infrastructure.events.",
    f"{LEGACY_PACKAGE}.infrastructure.express.pybotx_gateway",
    f"{LEGACY_PACKAGE}.infrastructure.telegram.remote_service_client",
    f"{LEGACY_PACKAGE}.infrastructure.telegram.session_aware_gateway",
    f"{LEGACY_PACKAGE}.infrastructure.telegram.telethon_gateway",
    f"{LEGACY_PACKAGE}.runtime.worker",
    f"{LEGACY_PACKAGE}.runtime.telethon_service",
)
FORBIDDEN_PERSISTENCE_FRAGMENTS = (
    f"{LEGACY_PACKAGE}.infrastructure.persistence.db",
    f"{LEGACY_PACKAGE}.infrastructure.persistence.models",
    f"{LEGACY_PACKAGE}.infrastructure.persistence.repositories",
    f"{LEGACY_PACKAGE}.infrastructure.persistence.fsm_state_repository",
    f"{LEGACY_PACKAGE}.infrastructure.persistence.types",
    f"{LEGACY_PACKAGE}.infrastructure.repositories.in_memory",
    f"{LEGACY_PACKAGE}.infrastructure.repositories.in_memory_fsm_state",
    "extg_shared.persistence.repositories",
    "extg_shared.persistence.in_memory",
    "extg_shared.persistence.fsm_state_repository",
    "extg_shared.persistence.in_memory_fsm_state",
    "extg_shared.persistence.models",
)
FORBIDDEN_LEGACY_CONTRACT_FRAGMENTS = (
    f"{LEGACY_PACKAGE}.application.ports",
    f"{LEGACY_PACKAGE}.domain.models",
    f"{LEGACY_PACKAGE}.domain.errors",
    f"{LEGACY_PACKAGE}.domain.manifest",
)
FORBIDDEN_LEGACY_UTILITY_FRAGMENTS = (
    f"{LEGACY_PACKAGE}.application.retry",
    f"{LEGACY_PACKAGE}.application.message_checksum",
    f"{LEGACY_PACKAGE}.application.renderer",
    f"{LEGACY_PACKAGE}.application.normalizer",
)
FORBIDDEN_LEGACY_BUSINESS_SERVICE_FRAGMENTS = (
    f"{LEGACY_PACKAGE}.application.identity",
    f"{LEGACY_PACKAGE}.application.target_chat_provisioning",
    f"{LEGACY_PACKAGE}.application.identity_matrix_workbook",
)
FORBIDDEN_TEST_LEGACY_IMPORT_FRAGMENTS = (f"{LEGACY_PACKAGE}.",)


def test_service_packages_and_tests_do_not_depend_on_legacy_service_wrapper_paths() -> None:
    violations: list[str] = []
    for root in SERVICE_SCAN_ROOTS:
        for path in sorted(root.rglob("*.py")):
            if path in EXCLUDED_FILES:
                continue
            text = path.read_text()
            for fragment in (
                FORBIDDEN_SERVICE_WRAPPER_FRAGMENTS
                + FORBIDDEN_PERSISTENCE_FRAGMENTS
                + FORBIDDEN_LEGACY_CONTRACT_FRAGMENTS
                + FORBIDDEN_LEGACY_UTILITY_FRAGMENTS
                + FORBIDDEN_LEGACY_BUSINESS_SERVICE_FRAGMENTS
            ):
                if fragment in text:
                    violations.append(f"{path.relative_to(REPO_ROOT)} -> {fragment}")
    for root in TEST_SCAN_ROOTS:
        for path in sorted(root.rglob("*.py")):
            if path in EXCLUDED_FILES:
                continue
            text = path.read_text()
            for fragment in FORBIDDEN_SERVICE_WRAPPER_FRAGMENTS:
                if fragment in text:
                    violations.append(f"{path.relative_to(REPO_ROOT)} -> {fragment}")
            for fragment in FORBIDDEN_TEST_LEGACY_IMPORT_FRAGMENTS:
                if fragment in text:
                    violations.append(f"{path.relative_to(REPO_ROOT)} -> {fragment}")

    assert violations == []
