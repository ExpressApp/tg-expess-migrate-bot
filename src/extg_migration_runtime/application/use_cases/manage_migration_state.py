from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime

from extg_shared.contracts.ports import AuditRepository, MigrationStateRepository
from extg_shared.contracts.errors import FatalItemError
from extg_shared.contracts.manifest import MigrationManifest
from extg_shared.contracts.models import (
    AuditEvent,
    AuditSeverity,
    MigrationLifecycleStatus,
    MigrationStateRecord,
)


@dataclass(frozen=True, slots=True)
class ManageMigrationStateCommand:
    manifest: MigrationManifest
    action: str


@dataclass(frozen=True, slots=True)
class ManageMigrationStateResult:
    migration_id: str
    action: str
    previous_state: MigrationLifecycleStatus | None
    current_state: MigrationLifecycleStatus
    changed: bool
    updated_at: datetime
    freeze_started_at: datetime | None
    finalized_at: datetime | None
    last_reconcile_at: datetime | None
    last_blockers: list[str]


class ManageMigrationStateUseCase:
    def __init__(
        self,
        *,
        migration_state_repository: MigrationStateRepository,
        audit_repository: AuditRepository,
    ) -> None:
        self._migration_state_repository = migration_state_repository
        self._audit_repository = audit_repository

    async def execute(
        self,
        command: ManageMigrationStateCommand,
    ) -> ManageMigrationStateResult:
        if command.action not in {"pause_delta", "resume_delta", "freeze_cutover"}:
            raise FatalItemError(f"unsupported migration state action={command.action!r}")

        now = self._now()
        existing = await self._migration_state_repository.get(command.manifest.migration_id)
        if existing is None:
            existing = MigrationStateRecord(
                migration_id=command.manifest.migration_id,
                status=MigrationLifecycleStatus.ACTIVE,
                created_at=now,
                updated_at=now,
            )

        if existing.status is MigrationLifecycleStatus.COMPLETED:
            raise FatalItemError(
                f"migration_id={command.manifest.migration_id} is already completed and cannot be changed",
            )

        updated = self._transition(
            existing=existing,
            action=command.action,
            now=now,
        )
        saved = await self._migration_state_repository.save(updated)
        await self._audit_repository.add(
            AuditEvent(
                migration_id=command.manifest.migration_id,
                event_type=self._event_type(command.action),
                severity=AuditSeverity.INFO,
                payload_json={
                    "action": command.action,
                    "previous_state": existing.status.value,
                    "current_state": saved.status.value,
                    "changed": self._has_changed(existing, saved),
                    "freeze_started_at": saved.freeze_started_at.isoformat()
                    if saved.freeze_started_at is not None
                    else None,
                    "finalized_at": saved.finalized_at.isoformat()
                    if saved.finalized_at is not None
                    else None,
                    "last_blockers": saved.last_blockers,
                },
                created_at=now,
            ),
        )
        return ManageMigrationStateResult(
            migration_id=command.manifest.migration_id,
            action=command.action,
            previous_state=existing.status,
            current_state=saved.status,
            changed=self._has_changed(existing, saved),
            updated_at=saved.updated_at,
            freeze_started_at=saved.freeze_started_at,
            finalized_at=saved.finalized_at,
            last_reconcile_at=saved.last_reconcile_at,
            last_blockers=saved.last_blockers,
        )

    def _transition(
        self,
        *,
        existing: MigrationStateRecord,
        action: str,
        now: datetime,
    ) -> MigrationStateRecord:
        if action == "pause_delta":
            return MigrationStateRecord(
                migration_id=existing.migration_id,
                status=MigrationLifecycleStatus.PAUSED,
                created_at=existing.created_at,
                updated_at=now,
                freeze_started_at=existing.freeze_started_at,
                finalized_at=existing.finalized_at,
                last_reconcile_at=existing.last_reconcile_at,
                last_blockers=list(existing.last_blockers),
            )

        if action == "resume_delta":
            return MigrationStateRecord(
                migration_id=existing.migration_id,
                status=MigrationLifecycleStatus.ACTIVE,
                created_at=existing.created_at,
                updated_at=now,
                freeze_started_at=None,
                finalized_at=None,
                last_reconcile_at=existing.last_reconcile_at,
                last_blockers=[],
            )

        return MigrationStateRecord(
            migration_id=existing.migration_id,
            status=MigrationLifecycleStatus.PAUSED,
            created_at=existing.created_at,
            updated_at=now,
            freeze_started_at=now,
            finalized_at=None,
            last_reconcile_at=existing.last_reconcile_at,
            last_blockers=list(existing.last_blockers),
        )

    def _event_type(self, action: str) -> str:
        mapping = {
            "pause_delta": "migration_delta_paused",
            "resume_delta": "migration_delta_resumed",
            "freeze_cutover": "migration_cutover_frozen",
        }
        return mapping[action]

    def _has_changed(
        self,
        previous: MigrationStateRecord,
        current: MigrationStateRecord,
    ) -> bool:
        return any(
            [
                previous.status is not current.status,
                previous.freeze_started_at != current.freeze_started_at,
                previous.finalized_at != current.finalized_at,
                previous.last_blockers != current.last_blockers,
            ],
        )

    def _now(self) -> datetime:
        return datetime.now(tz=UTC)
