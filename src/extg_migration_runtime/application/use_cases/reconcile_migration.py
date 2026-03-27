from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime

from extg_shared.contracts.ports import (
    AttachmentMappingRepository,
    AttachmentStatusSummary,
    AuditRepository,
    InventorySnapshotRepository,
    MessageMappingRepository,
    MessageStatusSummary,
)
from extg_shared.contracts.manifest import ManifestDialog, MigrationManifest
from extg_shared.contracts.models import (
    AttachmentImportStatus,
    AuditEvent,
    AuditSeverity,
    InventorySnapshotRecord,
    MessageImportStatus,
)


@dataclass(frozen=True, slots=True)
class ReconcileMigrationCommand:
    manifest: MigrationManifest
    source_chat_id: str | None = None


@dataclass(frozen=True, slots=True)
class ReconcileChatResult:
    source_chat_id: str
    source_chat_type: str
    source_chat_title: str
    inventory_present: bool
    source_message_count: int | None
    imported_count: int
    failed_count: int
    ambiguous_count: int
    processing_count: int
    mapped_total: int
    source_media_count: int | None
    attachment_imported_count: int
    attachment_failed_count: int
    attachment_ambiguous_count: int
    attachment_processing_count: int
    attachment_skipped_count: int
    attachment_mapped_total: int
    gap_count: int | None
    requires_attention: bool


@dataclass(frozen=True, slots=True)
class ReconcileMigrationResult:
    migration_id: str
    chats_total: int
    attention_chats: int
    inventory_missing_chats: int
    source_messages_total_known: int
    imported_count: int
    failed_count: int
    ambiguous_count: int
    processing_count: int
    mapped_total: int
    gap_total_known: int
    source_media_total_known: int
    attachment_imported_count: int
    attachment_failed_count: int
    attachment_ambiguous_count: int
    attachment_processing_count: int
    attachment_skipped_count: int
    attachment_mapped_total: int
    chats: list[ReconcileChatResult]


class ReconcileMigrationUseCase:
    def __init__(
        self,
        *,
        inventory_snapshot_repository: InventorySnapshotRepository,
        message_mapping_repository: MessageMappingRepository,
        attachment_mapping_repository: AttachmentMappingRepository,
        audit_repository: AuditRepository,
    ) -> None:
        self._inventory_snapshot_repository = inventory_snapshot_repository
        self._message_mapping_repository = message_mapping_repository
        self._attachment_mapping_repository = attachment_mapping_repository
        self._audit_repository = audit_repository

    async def execute(
        self,
        command: ReconcileMigrationCommand,
    ) -> ReconcileMigrationResult:
        inventory_snapshots = await self._inventory_snapshot_repository.list_by_migration(
            command.manifest.migration_id,
        )
        status_summaries = await self._message_mapping_repository.summarize_by_chat(
            command.manifest.migration_id,
        )
        attachment_status_summaries = await self._attachment_mapping_repository.summarize_by_chat(
            command.manifest.migration_id,
        )

        if command.source_chat_id is not None:
            inventory_snapshots = [
                snapshot
                for snapshot in inventory_snapshots
                if snapshot.source_chat_id == command.source_chat_id
            ]
            status_summaries = [
                summary
                for summary in status_summaries
                if summary.source_chat_id == command.source_chat_id
            ]
            attachment_status_summaries = [
                summary
                for summary in attachment_status_summaries
                if summary.source_chat_id == command.source_chat_id
            ]

        manifest_dialogs = {
            dialog.source_chat_id: dialog
            for dialog in command.manifest.dialogs
            if command.source_chat_id is None or dialog.source_chat_id == command.source_chat_id
        }
        snapshots_by_chat = {
            snapshot.source_chat_id: snapshot
            for snapshot in inventory_snapshots
        }
        summaries_by_chat = self._summaries_by_chat(status_summaries)
        attachment_summaries_by_chat = self._attachment_summaries_by_chat(
            attachment_status_summaries,
        )

        chat_ids = sorted(
            set(manifest_dialogs)
            | set(snapshots_by_chat)
            | set(summaries_by_chat)
            | set(attachment_summaries_by_chat)
        )
        chat_results = [
            self._build_chat_result(
                source_chat_id=source_chat_id,
                manifest_dialog=manifest_dialogs.get(source_chat_id),
                inventory_snapshot=snapshots_by_chat.get(source_chat_id),
                status_summaries=summaries_by_chat.get(source_chat_id, []),
                attachment_status_summaries=attachment_summaries_by_chat.get(source_chat_id, []),
            )
            for source_chat_id in chat_ids
        ]

        result = ReconcileMigrationResult(
            migration_id=command.manifest.migration_id,
            chats_total=len(chat_results),
            attention_chats=sum(1 for chat in chat_results if chat.requires_attention),
            inventory_missing_chats=sum(1 for chat in chat_results if not chat.inventory_present),
            source_messages_total_known=sum(chat.source_message_count or 0 for chat in chat_results),
            imported_count=sum(chat.imported_count for chat in chat_results),
            failed_count=sum(chat.failed_count for chat in chat_results),
            ambiguous_count=sum(chat.ambiguous_count for chat in chat_results),
            processing_count=sum(chat.processing_count for chat in chat_results),
            mapped_total=sum(chat.mapped_total for chat in chat_results),
            gap_total_known=sum(chat.gap_count or 0 for chat in chat_results),
            source_media_total_known=sum(chat.source_media_count or 0 for chat in chat_results),
            attachment_imported_count=sum(chat.attachment_imported_count for chat in chat_results),
            attachment_failed_count=sum(chat.attachment_failed_count for chat in chat_results),
            attachment_ambiguous_count=sum(chat.attachment_ambiguous_count for chat in chat_results),
            attachment_processing_count=sum(chat.attachment_processing_count for chat in chat_results),
            attachment_skipped_count=sum(chat.attachment_skipped_count for chat in chat_results),
            attachment_mapped_total=sum(chat.attachment_mapped_total for chat in chat_results),
            chats=chat_results,
        )
        await self._audit_repository.add(
            AuditEvent(
                migration_id=command.manifest.migration_id,
                event_type="reconcile_report_generated",
                severity=AuditSeverity.INFO,
                payload_json={
                    "chats_total": result.chats_total,
                    "attention_chats": result.attention_chats,
                    "inventory_missing_chats": result.inventory_missing_chats,
                    "source_messages_total_known": result.source_messages_total_known,
                    "imported_count": result.imported_count,
                    "failed_count": result.failed_count,
                    "ambiguous_count": result.ambiguous_count,
                    "processing_count": result.processing_count,
                    "mapped_total": result.mapped_total,
                    "gap_total_known": result.gap_total_known,
                    "source_media_total_known": result.source_media_total_known,
                    "attachment_imported_count": result.attachment_imported_count,
                    "attachment_failed_count": result.attachment_failed_count,
                    "attachment_ambiguous_count": result.attachment_ambiguous_count,
                    "attachment_processing_count": result.attachment_processing_count,
                    "attachment_skipped_count": result.attachment_skipped_count,
                    "attachment_mapped_total": result.attachment_mapped_total,
                },
                created_at=self._now(),
            ),
        )
        return result

    def _build_chat_result(
        self,
        *,
        source_chat_id: str,
        manifest_dialog: ManifestDialog | None,
        inventory_snapshot: InventorySnapshotRecord | None,
        status_summaries: list[MessageStatusSummary],
        attachment_status_summaries: list[AttachmentStatusSummary],
    ) -> ReconcileChatResult:
        imported_count = self._count_for_status(status_summaries, MessageImportStatus.IMPORTED)
        failed_count = self._count_for_status(status_summaries, MessageImportStatus.FAILED)
        ambiguous_count = self._count_for_status(status_summaries, MessageImportStatus.AMBIGUOUS)
        processing_count = self._count_for_status(status_summaries, MessageImportStatus.PROCESSING)
        mapped_total = sum(summary.count for summary in status_summaries)
        attachment_imported_count = self._count_attachment_for_status(
            attachment_status_summaries,
            AttachmentImportStatus.IMPORTED,
        )
        attachment_failed_count = self._count_attachment_for_status(
            attachment_status_summaries,
            AttachmentImportStatus.FAILED,
        )
        attachment_ambiguous_count = self._count_attachment_for_status(
            attachment_status_summaries,
            AttachmentImportStatus.AMBIGUOUS,
        )
        attachment_processing_count = self._count_attachment_for_status(
            attachment_status_summaries,
            AttachmentImportStatus.PROCESSING,
        )
        attachment_skipped_count = self._count_attachment_for_status(
            attachment_status_summaries,
            AttachmentImportStatus.SKIPPED,
        )
        attachment_mapped_total = sum(summary.count for summary in attachment_status_summaries)

        source_message_count = inventory_snapshot.message_count if inventory_snapshot else None
        source_media_count = inventory_snapshot.media_count if inventory_snapshot else None
        gap_count = None
        if source_message_count is not None:
            gap_count = abs(source_message_count - imported_count)

        source_chat_type = (
            inventory_snapshot.source_chat_type
            if inventory_snapshot is not None
            else (manifest_dialog.source_chat_type if manifest_dialog is not None else "unknown")
        )
        source_chat_title = (
            inventory_snapshot.source_chat_title
            if inventory_snapshot is not None
            else source_chat_id
        )
        requires_attention = (
            inventory_snapshot is None
            or failed_count > 0
            or ambiguous_count > 0
            or processing_count > 0
            or attachment_failed_count > 0
            or attachment_ambiguous_count > 0
            or attachment_processing_count > 0
            or (gap_count or 0) > 0
        )
        return ReconcileChatResult(
            source_chat_id=source_chat_id,
            source_chat_type=source_chat_type,
            source_chat_title=source_chat_title,
            inventory_present=inventory_snapshot is not None,
            source_message_count=source_message_count,
            imported_count=imported_count,
            failed_count=failed_count,
            ambiguous_count=ambiguous_count,
            processing_count=processing_count,
            mapped_total=mapped_total,
            source_media_count=source_media_count,
            attachment_imported_count=attachment_imported_count,
            attachment_failed_count=attachment_failed_count,
            attachment_ambiguous_count=attachment_ambiguous_count,
            attachment_processing_count=attachment_processing_count,
            attachment_skipped_count=attachment_skipped_count,
            attachment_mapped_total=attachment_mapped_total,
            gap_count=gap_count,
            requires_attention=requires_attention,
        )

    def _summaries_by_chat(
        self,
        summaries: list[MessageStatusSummary],
    ) -> dict[str, list[MessageStatusSummary]]:
        grouped: dict[str, list[MessageStatusSummary]] = {}
        for summary in summaries:
            grouped.setdefault(summary.source_chat_id, []).append(summary)
        return grouped

    def _attachment_summaries_by_chat(
        self,
        summaries: list[AttachmentStatusSummary],
    ) -> dict[str, list[AttachmentStatusSummary]]:
        grouped: dict[str, list[AttachmentStatusSummary]] = {}
        for summary in summaries:
            grouped.setdefault(summary.source_chat_id, []).append(summary)
        return grouped

    def _count_for_status(
        self,
        summaries: list[MessageStatusSummary],
        status: MessageImportStatus,
    ) -> int:
        return sum(summary.count for summary in summaries if summary.import_status is status)

    def _count_attachment_for_status(
        self,
        summaries: list[AttachmentStatusSummary],
        status: AttachmentImportStatus,
    ) -> int:
        return sum(summary.count for summary in summaries if summary.import_status is status)

    def _now(self) -> datetime:
        return datetime.now(tz=UTC)
