from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime

from extg_shared.contracts.ports import (
    AuditRepository,
    InventorySnapshotRepository,
    TelegramGateway,
)
from extg_shared.contracts.manifest import MigrationManifest
from extg_shared.contracts.models import (
    AuditEvent,
    AuditSeverity,
    InventorySnapshotRecord,
    SourceDialog,
)
from extg_shared.utils.retry import AsyncRetryPolicy


@dataclass(frozen=True, slots=True)
class InventoryCommand:
    manifest: MigrationManifest


@dataclass(frozen=True, slots=True)
class InventoryDialogResult:
    source_chat_id: str
    source_chat_type: str
    title: str
    message_count: int
    media_count: int
    approximate_bytes: int

    @classmethod
    def from_source_dialog(cls, dialog: SourceDialog) -> "InventoryDialogResult":
        return cls(
            source_chat_id=dialog.dialog_id,
            source_chat_type=dialog.chat_type,
            title=dialog.title,
            message_count=dialog.message_count,
            media_count=dialog.media_count,
            approximate_bytes=dialog.approximate_bytes,
        )


@dataclass(frozen=True, slots=True)
class InventoryResult:
    migration_id: str
    dialogs_total: int
    messages_total: int
    media_total: int
    approximate_bytes_total: int
    dialogs: list[InventoryDialogResult]


class InventoryUseCase:
    def __init__(
        self,
        *,
        telegram_gateway: TelegramGateway,
        inventory_snapshot_repository: InventorySnapshotRepository,
        audit_repository: AuditRepository,
        retry_policy: AsyncRetryPolicy,
    ) -> None:
        self._telegram_gateway = telegram_gateway
        self._inventory_snapshot_repository = inventory_snapshot_repository
        self._audit_repository = audit_repository
        self._retry_policy = retry_policy

    async def execute(self, command: InventoryCommand) -> InventoryResult:
        dialogs = await self._retry_policy.run(
            lambda: self._telegram_gateway.list_dialogs(command.manifest),
        )
        results = [
            InventoryDialogResult.from_source_dialog(dialog)
            for dialog in dialogs
        ]
        inventory_result = InventoryResult(
            migration_id=command.manifest.migration_id,
            dialogs_total=len(results),
            messages_total=sum(dialog.message_count for dialog in results),
            media_total=sum(dialog.media_count for dialog in results),
            approximate_bytes_total=sum(
                dialog.approximate_bytes for dialog in results
            ),
            dialogs=results,
        )
        captured_at = self._now()
        await self._inventory_snapshot_repository.replace_for_migration(
            command.manifest.migration_id,
            [
                InventorySnapshotRecord(
                    migration_id=command.manifest.migration_id,
                    source_chat_id=dialog.source_chat_id,
                    source_chat_type=dialog.source_chat_type,
                    source_chat_title=dialog.title,
                    message_count=dialog.message_count,
                    media_count=dialog.media_count,
                    approximate_bytes=dialog.approximate_bytes,
                    captured_at=captured_at,
                )
                for dialog in results
            ],
        )
        await self._audit_repository.add(
            AuditEvent(
                migration_id=command.manifest.migration_id,
                event_type="inventory_snapshot_persisted",
                severity=AuditSeverity.INFO,
                payload_json={
                    "dialogs_total": inventory_result.dialogs_total,
                    "messages_total": inventory_result.messages_total,
                    "media_total": inventory_result.media_total,
                    "approximate_bytes_total": inventory_result.approximate_bytes_total,
                },
                created_at=captured_at,
            ),
        )
        return inventory_result

    def _now(self) -> datetime:
        return datetime.now(tz=UTC)
