import inspect

import pytest

from extg_shared.contracts.manifest import MigrationManifest
from extg_telethon_service.infrastructure.telegram.fake_gateway import (
    FakeTelegramGateway,
)
from extg_migration_runtime.infrastructure.persistence.in_memory import (
    InMemoryInventorySnapshotRepository,
)
from extg_telethon_service.infrastructure.persistence.in_memory import (
    InMemoryAuditRepository,
)
from extg_shared.utils.retry import AsyncRetryPolicy
from tests.helpers import load_sample_manifest, sample_dialogs_and_messages


def _build_use_case(use_case_cls, overrides: dict[str, object]):
    parameters = inspect.signature(use_case_cls.__init__).parameters
    kwargs: dict[str, object] = {}
    for name, parameter in parameters.items():
        if name == "self":
            continue
        if name in overrides:
            kwargs[name] = overrides[name]
        elif parameter.default is not inspect._empty:
            continue
        else:
            raise RuntimeError(
                f"Unable to build {use_case_cls.__name__}: missing dependency {name}",
            )
    return use_case_cls(**kwargs)


@pytest.mark.asyncio
async def test_inventory_use_case_records_manifest_dialogs():
    inventory_module = pytest.importorskip(
        "extg_migration_runtime.application.use_cases.inventory",
    )
    InventoryUseCase = inventory_module.InventoryUseCase
    InventoryCommand = inventory_module.InventoryCommand

    manifest = load_sample_manifest()
    dialog_id = manifest.dialogs[0].source_chat_id
    dialogs, messages_by_dialog = sample_dialogs_and_messages(dialog_ids=(dialog_id,))
    fake_gateway = FakeTelegramGateway(dialogs=dialogs, messages_by_dialog=messages_by_dialog)

    use_case = _build_use_case(
        InventoryUseCase,
        {
            "telegram_gateway": fake_gateway,
            "inventory_snapshot_repository": InMemoryInventorySnapshotRepository(),
            "audit_repository": InMemoryAuditRepository(),
            "retry_policy": AsyncRetryPolicy(
                max_attempts=1,
                base_delay_seconds=0,
                max_delay_seconds=0,
                jitter_seconds=0,
            ),
        },
    )
    command = InventoryCommand(manifest=manifest)
    result = await use_case.execute(command)
    reported_dialogs = getattr(result, "dialogs", None) or getattr(
        result,
        "dialog_summaries",
        None,
    )

    assert reported_dialogs, "inventory result must expose dialog list"
    assert all(
        hasattr(dialog, "chat_type") or hasattr(dialog, "source_chat_id") for dialog in reported_dialogs
    )
