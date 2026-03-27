from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime

from extg_shared.contracts.ports import AuditRepository, MigrationStateRepository
from extg_migration_runtime.application.use_cases.reconcile_migration import (
    ReconcileMigrationCommand,
    ReconcileMigrationResult,
    ReconcileMigrationUseCase,
)
from extg_shared.contracts.manifest import MigrationManifest
from extg_shared.contracts.models import (
    AuditEvent,
    AuditSeverity,
    MigrationLifecycleStatus,
    MigrationStateRecord,
)


@dataclass(frozen=True, slots=True)
class FinalizeCutoverCommand:
    manifest: MigrationManifest
    at_least_once_policy_acknowledged: bool = False


@dataclass(frozen=True, slots=True)
class FinalizeCutoverResult:
    migration_id: str
    status: str
    migration_state: MigrationLifecycleStatus
    finalized_at: datetime | None
    freeze_started_at: datetime | None
    blockers: list[str] = field(default_factory=list)
    checklist: dict[str, bool] = field(default_factory=dict)
    reconcile_result: ReconcileMigrationResult | None = None


class FinalizeCutoverUseCase:
    def __init__(
        self,
        *,
        reconcile_migration_use_case: ReconcileMigrationUseCase,
        migration_state_repository: MigrationStateRepository,
        audit_repository: AuditRepository,
    ) -> None:
        self._reconcile_migration_use_case = reconcile_migration_use_case
        self._migration_state_repository = migration_state_repository
        self._audit_repository = audit_repository

    async def execute(
        self,
        command: FinalizeCutoverCommand,
    ) -> FinalizeCutoverResult:
        now = self._now()
        existing_state = await self._migration_state_repository.get(
            command.manifest.migration_id,
        )
        reconcile_result = await self._reconcile_migration_use_case.execute(
            ReconcileMigrationCommand(manifest=command.manifest),
        )
        checklist = self._build_checklist(
            reconcile_result=reconcile_result,
            existing_state=existing_state,
            at_least_once_policy_acknowledged=command.at_least_once_policy_acknowledged,
        )
        blockers = [
            self._checklist_blocker_message(item)
            for item, passed in checklist.items()
            if not passed
        ]
        freeze_started_at = existing_state.freeze_started_at if existing_state else None

        if blockers:
            saved_state = await self._save_state(
                existing_state=existing_state,
                migration_id=command.manifest.migration_id,
                status=MigrationLifecycleStatus.NEEDS_MANUAL_RECONCILE,
                updated_at=now,
                freeze_started_at=freeze_started_at,
                finalized_at=None,
                last_reconcile_at=now,
                last_blockers=blockers,
            )
            await self._audit_repository.add(
                AuditEvent(
                    migration_id=command.manifest.migration_id,
                    event_type="cutover_finalize_blocked",
                    severity=AuditSeverity.WARNING,
                    payload_json={
                        "blockers": blockers,
                        "checklist": checklist,
                        "reconcile_summary": self._reconcile_summary_payload(
                            reconcile_result,
                        ),
                    },
                    created_at=now,
                ),
            )
            return FinalizeCutoverResult(
                migration_id=command.manifest.migration_id,
                status="blocked",
                migration_state=saved_state.status,
                finalized_at=saved_state.finalized_at,
                freeze_started_at=saved_state.freeze_started_at,
                blockers=blockers,
                checklist=checklist,
                reconcile_result=reconcile_result,
            )

        saved_state = await self._save_state(
            existing_state=existing_state,
            migration_id=command.manifest.migration_id,
            status=MigrationLifecycleStatus.COMPLETED,
            updated_at=now,
            freeze_started_at=freeze_started_at,
            finalized_at=now,
            last_reconcile_at=now,
            last_blockers=[],
        )
        await self._audit_repository.add(
            AuditEvent(
                migration_id=command.manifest.migration_id,
                event_type="cutover_finalized",
                severity=AuditSeverity.INFO,
                payload_json={
                    "checklist": checklist,
                    "reconcile_summary": self._reconcile_summary_payload(
                        reconcile_result,
                    ),
                    "finalized_at": now.isoformat(),
                },
                created_at=now,
            ),
        )
        return FinalizeCutoverResult(
            migration_id=command.manifest.migration_id,
            status="completed",
            migration_state=saved_state.status,
            finalized_at=saved_state.finalized_at,
            freeze_started_at=saved_state.freeze_started_at,
            blockers=[],
            checklist=checklist,
            reconcile_result=reconcile_result,
        )

    async def _save_state(
        self,
        *,
        existing_state: MigrationStateRecord | None,
        migration_id: str,
        status: MigrationLifecycleStatus,
        updated_at: datetime,
        freeze_started_at: datetime | None,
        finalized_at: datetime | None,
        last_reconcile_at: datetime,
        last_blockers: list[str],
    ) -> MigrationStateRecord:
        created_at = existing_state.created_at if existing_state is not None else updated_at
        return await self._migration_state_repository.save(
            MigrationStateRecord(
                migration_id=migration_id,
                status=status,
                created_at=created_at,
                updated_at=updated_at,
                freeze_started_at=freeze_started_at,
                finalized_at=finalized_at,
                last_reconcile_at=last_reconcile_at,
                last_blockers=list(last_blockers),
            ),
        )

    def _build_checklist(
        self,
        *,
        reconcile_result: ReconcileMigrationResult,
        existing_state: MigrationStateRecord | None,
        at_least_once_policy_acknowledged: bool,
    ) -> dict[str, bool]:
        return {
            "inventory_present_for_all_chats": reconcile_result.inventory_missing_chats == 0,
            "message_counts_match": reconcile_result.gap_total_known == 0,
            "no_message_failures": reconcile_result.failed_count == 0,
            "no_message_ambiguity": reconcile_result.ambiguous_count == 0,
            "no_message_processing_backlog": reconcile_result.processing_count == 0,
            "no_attachment_failures": reconcile_result.attachment_failed_count == 0,
            "no_attachment_ambiguity": reconcile_result.attachment_ambiguous_count == 0,
            "no_attachment_processing_backlog": reconcile_result.attachment_processing_count == 0,
            "freeze_window_started": existing_state.freeze_started_at is not None
            if existing_state is not None
            else False,
            "migration_paused_for_cutover": existing_state.status is MigrationLifecycleStatus.PAUSED
            if existing_state is not None
            else False,
            "at_least_once_policy_acknowledged": at_least_once_policy_acknowledged,
        }

    def _checklist_blocker_message(self, item: str) -> str:
        messages = {
            "inventory_present_for_all_chats": "inventory snapshot is missing for one or more chats",
            "message_counts_match": "message counts do not match inventory",
            "no_message_failures": "there are failed message imports",
            "no_message_ambiguity": "there are ambiguous message imports",
            "no_message_processing_backlog": "there are message items still in processing state",
            "no_attachment_failures": "there are failed attachment imports",
            "no_attachment_ambiguity": "there are ambiguous attachment imports",
            "no_attachment_processing_backlog": "there are attachment items still in processing state",
            "freeze_window_started": "freeze window was not started before finalize-cutover",
            "migration_paused_for_cutover": "migration is not paused for cutover",
            "at_least_once_policy_acknowledged": (
                "at-least-once delivery policy was not explicitly acknowledged "
                "for cutover on a BotX target without exactly-once visible effect"
            ),
        }
        return messages[item]

    def _reconcile_summary_payload(
        self,
        reconcile_result: ReconcileMigrationResult,
    ) -> dict[str, object]:
        return {
            "chats_total": reconcile_result.chats_total,
            "attention_chats": reconcile_result.attention_chats,
            "inventory_missing_chats": reconcile_result.inventory_missing_chats,
            "imported_count": reconcile_result.imported_count,
            "failed_count": reconcile_result.failed_count,
            "ambiguous_count": reconcile_result.ambiguous_count,
            "processing_count": reconcile_result.processing_count,
            "gap_total_known": reconcile_result.gap_total_known,
            "attachment_imported_count": reconcile_result.attachment_imported_count,
            "attachment_failed_count": reconcile_result.attachment_failed_count,
            "attachment_ambiguous_count": reconcile_result.attachment_ambiguous_count,
            "attachment_processing_count": reconcile_result.attachment_processing_count,
            "attachment_skipped_count": reconcile_result.attachment_skipped_count,
        }

    def _now(self) -> datetime:
        return datetime.now(tz=UTC)
