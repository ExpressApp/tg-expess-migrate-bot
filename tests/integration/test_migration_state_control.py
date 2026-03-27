from __future__ import annotations

from datetime import UTC, datetime

import pytest

from extg_migration_runtime.application.use_cases.manage_migration_state import (
    ManageMigrationStateUseCase,
)
from extg_migration_runtime.application.use_cases.migration_state_control import (
    FreezeCutoverCommand,
    FreezeCutoverUseCase,
    PauseDeltaCommand,
    PauseDeltaUseCase,
    ResumeDeltaCommand,
    ResumeDeltaUseCase,
)
from extg_shared.contracts.errors import FatalItemError
from extg_shared.contracts.manifest import MigrationManifest
from extg_shared.contracts.models import MigrationLifecycleStatus, MigrationStateRecord
from extg_migration_runtime.infrastructure.persistence.in_memory import (
    InMemoryMigrationStateRepository,
)
from extg_telethon_service.infrastructure.persistence.in_memory import (
    InMemoryAuditRepository,
)


def build_manifest() -> MigrationManifest:
    return MigrationManifest.model_validate(
        {
            "migration_id": "migration-1",
            "mode": "backfill_delta_cutover",
            "dialogs": [
                {
                    "source_chat_id": "chat-1",
                    "source_chat_type": "supergroup",
                    "target_strategy": "bind",
                    "target_chat_id": "express-chat-1",
                },
            ],
        },
    )


def build_use_cases():
    migration_state_repo = InMemoryMigrationStateRepository()
    audit_repo = InMemoryAuditRepository()
    manage_use_case = ManageMigrationStateUseCase(
        migration_state_repository=migration_state_repo,
        audit_repository=audit_repo,
    )
    return (
        migration_state_repo,
        audit_repo,
        PauseDeltaUseCase(manage_migration_state_use_case=manage_use_case),
        ResumeDeltaUseCase(manage_migration_state_use_case=manage_use_case),
        FreezeCutoverUseCase(manage_migration_state_use_case=manage_use_case),
    )


@pytest.mark.asyncio
async def test_pause_and_resume_delta_update_migration_state():
    manifest = build_manifest()
    migration_state_repo, audit_repo, pause_use_case, resume_use_case, _ = build_use_cases()

    paused = await pause_use_case.execute(PauseDeltaCommand(manifest=manifest))
    resumed = await resume_use_case.execute(ResumeDeltaCommand(manifest=manifest))
    saved_state = await migration_state_repo.get(manifest.migration_id)
    audit_events = await audit_repo.list_all()

    assert paused.migration_state is MigrationLifecycleStatus.PAUSED
    assert paused.previous_migration_state is MigrationLifecycleStatus.ACTIVE
    assert paused.changed is True

    assert resumed.migration_state is MigrationLifecycleStatus.ACTIVE
    assert resumed.previous_migration_state is MigrationLifecycleStatus.PAUSED
    assert resumed.changed is True

    assert saved_state is not None
    assert saved_state.status is MigrationLifecycleStatus.ACTIVE
    assert saved_state.freeze_started_at is None
    assert [event.event_type for event in audit_events] == [
        "migration_delta_paused",
        "migration_delta_resumed",
    ]


@pytest.mark.asyncio
async def test_freeze_cutover_sets_pause_and_records_freeze_timestamp():
    manifest = build_manifest()
    migration_state_repo, audit_repo, _, _, freeze_use_case = build_use_cases()

    result = await freeze_use_case.execute(FreezeCutoverCommand(manifest=manifest))
    saved_state = await migration_state_repo.get(manifest.migration_id)
    audit_events = await audit_repo.list_all()

    assert result.migration_state is MigrationLifecycleStatus.PAUSED
    assert result.freeze_started_at is not None
    assert saved_state is not None
    assert saved_state.status is MigrationLifecycleStatus.PAUSED
    assert saved_state.freeze_started_at is not None
    assert [event.event_type for event in audit_events] == ["migration_cutover_frozen"]


@pytest.mark.asyncio
async def test_state_control_rejects_changes_after_completion():
    manifest = build_manifest()
    migration_state_repo, audit_repo, pause_use_case, _, _ = build_use_cases()
    completed_at = datetime.now(tz=UTC)
    await migration_state_repo.save(
        MigrationStateRecord(
            migration_id=manifest.migration_id,
            status=MigrationLifecycleStatus.COMPLETED,
            created_at=completed_at,
            updated_at=completed_at,
            finalized_at=completed_at,
        ),
    )

    with pytest.raises(FatalItemError):
        await pause_use_case.execute(PauseDeltaCommand(manifest=manifest))

    audit_events = await audit_repo.list_all()
    assert audit_events == []
