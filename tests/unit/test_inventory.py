from datetime import UTC, datetime

import pytest

from extg_migration_runtime.application.use_cases.inventory import (
    InventoryCommand,
    InventoryUseCase,
)
from extg_shared.contracts.errors import RecoverableItemError
from extg_shared.contracts.models import InventorySnapshotRecord, SourceDialog
from extg_migration_runtime.infrastructure.persistence.in_memory import (
    InMemoryInventorySnapshotRepository,
)
from extg_telethon_service.infrastructure.persistence.in_memory import (
    InMemoryAuditRepository,
)
from extg_shared.utils.retry import AsyncRetryPolicy

from tests.helpers import load_sample_manifest


class _FakeTelegramGateway:
    def __init__(self, dialogs: list[SourceDialog]) -> None:
        self._dialogs = dialogs
        self.received_manifest = None

    async def list_dialogs(self, manifest):
        self.received_manifest = manifest
        return list(self._dialogs)


class _FlakyTelegramGateway(_FakeTelegramGateway):
    def __init__(self, dialogs: list[SourceDialog], *, failures_before_success: int) -> None:
        super().__init__(dialogs)
        self.failures_before_success = failures_before_success
        self.attempts = 0

    async def list_dialogs(self, manifest):
        self.attempts += 1
        if self.failures_before_success > 0:
            self.failures_before_success -= 1
            raise RecoverableItemError("temporary telethon-service timeout")
        return await super().list_dialogs(manifest)


@pytest.mark.asyncio
async def test_inventory_use_case_aggregates_dialog_stats():
    manifest = load_sample_manifest()
    gateway = _FakeTelegramGateway(
        dialogs=[
            SourceDialog(
                dialog_id="123456789",
                chat_type="supergroup",
                title="Chat A",
                message_count=10,
                media_count=3,
                approximate_bytes=512,
            ),
            SourceDialog(
                dialog_id="987654321",
                chat_type="channel",
                title="Chat B",
                message_count=5,
                media_count=1,
                approximate_bytes=128,
            ),
        ],
    )
    inventory_repo = InMemoryInventorySnapshotRepository()
    audit_repo = InMemoryAuditRepository()

    result = await InventoryUseCase(
        telegram_gateway=gateway,
        inventory_snapshot_repository=inventory_repo,
        audit_repository=audit_repo,
        retry_policy=AsyncRetryPolicy(
            max_attempts=1,
            base_delay_seconds=0,
            max_delay_seconds=0,
            jitter_seconds=0,
        ),
    ).execute(
        InventoryCommand(manifest=manifest),
    )
    persisted = await inventory_repo.list_by_migration(manifest.migration_id)
    audit_events = await audit_repo.list_all()

    assert gateway.received_manifest == manifest
    assert result.migration_id == manifest.migration_id
    assert result.dialogs_total == 2
    assert result.messages_total == 15
    assert result.media_total == 4
    assert result.approximate_bytes_total == 640
    assert [dialog.source_chat_id for dialog in result.dialogs] == [
        "123456789",
        "987654321",
    ]
    assert len(persisted) == 2
    assert persisted[0].message_count == 10
    assert len(audit_events) == 1
    assert audit_events[0].event_type == "inventory_snapshot_persisted"


@pytest.mark.asyncio
async def test_inventory_use_case_replaces_existing_snapshots_for_same_migration():
    manifest = load_sample_manifest()
    source_chat_id = manifest.dialogs[0].source_chat_id
    gateway = _FakeTelegramGateway(
        dialogs=[
            SourceDialog(
                dialog_id=source_chat_id,
                chat_type="supergroup",
                title="Chat A",
                message_count=7,
                media_count=2,
                approximate_bytes=256,
            ),
        ],
    )
    inventory_repo = InMemoryInventorySnapshotRepository()
    await inventory_repo.save(
        InventorySnapshotRecord(
            migration_id=manifest.migration_id,
            source_chat_id="legacy-chat",
            source_chat_type="group",
            source_chat_title="Legacy Chat",
            message_count=999,
            media_count=111,
            approximate_bytes=222,
            captured_at=datetime(2026, 3, 24, 10, 0, tzinfo=UTC),
        ),
    )
    audit_repo = InMemoryAuditRepository()

    await InventoryUseCase(
        telegram_gateway=gateway,
        inventory_snapshot_repository=inventory_repo,
        audit_repository=audit_repo,
        retry_policy=AsyncRetryPolicy(
            max_attempts=1,
            base_delay_seconds=0,
            max_delay_seconds=0,
            jitter_seconds=0,
        ),
    ).execute(
        InventoryCommand(manifest=manifest),
    )
    persisted = await inventory_repo.list_by_migration(manifest.migration_id)
    audit_events = await audit_repo.list_all()

    assert [snapshot.source_chat_id for snapshot in persisted] == [source_chat_id]
    assert persisted[0].message_count == 7
    assert persisted[0].media_count == 2
    assert persisted[0].approximate_bytes == 256
    assert persisted[0].captured_at is not None
    assert audit_events[0].payload_json == {
        "dialogs_total": 1,
        "messages_total": 7,
        "media_total": 2,
        "approximate_bytes_total": 256,
    }


@pytest.mark.asyncio
async def test_inventory_use_case_retries_transient_gateway_failures():
    manifest = load_sample_manifest()
    gateway = _FlakyTelegramGateway(
        dialogs=[
            SourceDialog(
                dialog_id="123456789",
                chat_type="supergroup",
                title="Chat A",
                message_count=10,
                media_count=3,
                approximate_bytes=512,
            ),
        ],
        failures_before_success=1,
    )
    result = await InventoryUseCase(
        telegram_gateway=gateway,
        inventory_snapshot_repository=InMemoryInventorySnapshotRepository(),
        audit_repository=InMemoryAuditRepository(),
        retry_policy=AsyncRetryPolicy(
            max_attempts=2,
            base_delay_seconds=0,
            max_delay_seconds=0,
            jitter_seconds=0,
        ),
    ).execute(
        InventoryCommand(manifest=manifest),
    )

    assert gateway.attempts == 2
    assert result.dialogs_total == 1
