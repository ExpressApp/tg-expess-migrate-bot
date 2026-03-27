from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

from extg_migration_runtime.application.use_cases.manage_migration_state import (
    ManageMigrationStateCommand,
    ManageMigrationStateResult,
    ManageMigrationStateUseCase,
)
from extg_shared.contracts.manifest import MigrationManifest
from extg_shared.contracts.models import MigrationLifecycleStatus


@dataclass(frozen=True, slots=True)
class PauseDeltaCommand:
    manifest: MigrationManifest


@dataclass(frozen=True, slots=True)
class ResumeDeltaCommand:
    manifest: MigrationManifest


@dataclass(frozen=True, slots=True)
class FreezeCutoverCommand:
    manifest: MigrationManifest


@dataclass(frozen=True, slots=True)
class MigrationStateControlResult:
    migration_id: str
    status: str
    migration_state: MigrationLifecycleStatus
    previous_migration_state: MigrationLifecycleStatus | None
    updated_at: datetime | None
    freeze_started_at: datetime | None
    finalized_at: datetime | None
    last_reconcile_at: datetime | None
    changed: bool
    blockers: list[str]


class PauseDeltaUseCase:
    def __init__(self, *, manage_migration_state_use_case: ManageMigrationStateUseCase) -> None:
        self._manage_migration_state_use_case = manage_migration_state_use_case

    async def execute(self, command: PauseDeltaCommand) -> MigrationStateControlResult:
        result = await self._manage_migration_state_use_case.execute(
            ManageMigrationStateCommand(
                manifest=command.manifest,
                action="pause_delta",
            ),
        )
        return _to_control_result(result)


class ResumeDeltaUseCase:
    def __init__(self, *, manage_migration_state_use_case: ManageMigrationStateUseCase) -> None:
        self._manage_migration_state_use_case = manage_migration_state_use_case

    async def execute(self, command: ResumeDeltaCommand) -> MigrationStateControlResult:
        result = await self._manage_migration_state_use_case.execute(
            ManageMigrationStateCommand(
                manifest=command.manifest,
                action="resume_delta",
            ),
        )
        return _to_control_result(result)


class FreezeCutoverUseCase:
    def __init__(self, *, manage_migration_state_use_case: ManageMigrationStateUseCase) -> None:
        self._manage_migration_state_use_case = manage_migration_state_use_case

    async def execute(self, command: FreezeCutoverCommand) -> MigrationStateControlResult:
        result = await self._manage_migration_state_use_case.execute(
            ManageMigrationStateCommand(
                manifest=command.manifest,
                action="freeze_cutover",
            ),
        )
        return _to_control_result(result)


def _to_control_result(result: ManageMigrationStateResult) -> MigrationStateControlResult:
    return MigrationStateControlResult(
        migration_id=result.migration_id,
        status="changed" if result.changed else "no_change",
        migration_state=result.current_state,
        previous_migration_state=result.previous_state,
        updated_at=result.updated_at,
        freeze_started_at=result.freeze_started_at,
        finalized_at=result.finalized_at,
        last_reconcile_at=result.last_reconcile_at,
        changed=result.changed,
        blockers=result.last_blockers,
    )
