from datetime import UTC, datetime, timedelta

import pytest

from extg_migration_runtime.application.use_cases.reconcile_migration import (
    ReconcileMigrationCommand,
    ReconcileMigrationUseCase,
)
from extg_shared.contracts.manifest import MigrationManifest
from extg_shared.contracts.models import (
    AttachmentImportStatus,
    AttachmentMappingRecord,
    CanonicalAttachment,
    InventorySnapshotRecord,
    MessageImportStatus,
    MessageMappingRecord,
)
from extg_migration_runtime.infrastructure.persistence.in_memory import (
    InMemoryAttachmentMappingRepository,
    InMemoryInventorySnapshotRepository,
    InMemoryMessageMappingRepository,
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
                {
                    "source_chat_id": "chat-2",
                    "source_chat_type": "channel",
                    "target_strategy": "bind",
                    "target_chat_id": "express-chat-2",
                },
            ],
        },
    )


def build_mapping(
    *,
    source_chat_id: str,
    source_message_id: str,
    minute_offset: int,
    target_chat_id: str,
) -> MessageMappingRecord:
    return MessageMappingRecord(
        migration_id="migration-1",
        source_chat_id=source_chat_id,
        source_message_id=source_message_id,
        source_sent_at=datetime(2026, 3, 18, 10, 0, tzinfo=UTC) + timedelta(minutes=minute_offset),
        target_chat_id=target_chat_id,
        checksum=f"checksum-{source_chat_id}-{source_message_id}",
        import_status=MessageImportStatus.PROCESSING,
    )


async def claim_imported(
    repository: InMemoryMessageMappingRepository,
    *,
    source_chat_id: str,
    source_message_id: str,
    minute_offset: int,
    target_chat_id: str,
) -> None:
    await repository.claim(
        build_mapping(
            source_chat_id=source_chat_id,
            source_message_id=source_message_id,
            minute_offset=minute_offset,
            target_chat_id=target_chat_id,
        ),
    )
    await repository.mark_imported(
        "migration-1",
        source_chat_id,
        source_message_id,
        target_sync_id=f"sync-{source_chat_id}-{source_message_id}",
        rendered_body=f"Rendered {source_message_id}",
    )


async def claim_attachment(
    repository: InMemoryAttachmentMappingRepository,
    *,
    source_chat_id: str,
    source_message_id: str,
    attachment_index: int,
    target_chat_id: str,
    import_status: AttachmentImportStatus,
) -> None:
    attachment = CanonicalAttachment(
        source_file_id=f"file-{source_chat_id}-{source_message_id}-{attachment_index}",
        filename=f"{source_message_id}-{attachment_index}.jpg",
        mime_type="image/jpeg",
        size_bytes=128,
        media_kind="photo",
        sha256=f"sha-{source_chat_id}-{source_message_id}-{attachment_index}",
    )
    await repository.claim(
        AttachmentMappingRecord(
            migration_id="migration-1",
            source_chat_id=source_chat_id,
            source_message_id=source_message_id,
            attachment_index=attachment_index,
            source_file_id=attachment.source_file_id,
            source_filename=attachment.filename,
            media_kind=attachment.media_kind,
            checksum=attachment.sha256,
            size_bytes=attachment.size_bytes,
            target_chat_id=target_chat_id,
            import_status=AttachmentImportStatus.PROCESSING,
        ),
    )
    if import_status is AttachmentImportStatus.IMPORTED:
        await repository.mark_imported(
            "migration-1",
            source_chat_id,
            source_message_id,
            attachment_index,
            attachment=attachment,
            target_sync_id=f"att-sync-{source_chat_id}-{source_message_id}-{attachment_index}",
        )
    elif import_status is AttachmentImportStatus.FAILED:
        await repository.mark_failed(
            "migration-1",
            source_chat_id,
            source_message_id,
            attachment_index,
            attachment=attachment,
            error_code="RecoverableItemError",
            error_payload={"message": "planned attachment failure"},
        )
    elif import_status is AttachmentImportStatus.AMBIGUOUS:
        await repository.mark_ambiguous(
            "migration-1",
            source_chat_id,
            source_message_id,
            attachment_index,
            attachment=attachment,
            error_code="AmbiguousDeliveryError",
            error_payload={"message": "planned ambiguous attachment"},
        )
    elif import_status is AttachmentImportStatus.SKIPPED:
        await repository.mark_skipped(
            "migration-1",
            source_chat_id,
            source_message_id,
            attachment_index,
            attachment=attachment,
            reason="disabled by policy",
        )


@pytest.mark.asyncio
async def test_reconcile_reports_attention_and_inventory_gaps():
    manifest = build_manifest()
    inventory_repo = InMemoryInventorySnapshotRepository()
    message_repo = InMemoryMessageMappingRepository()
    attachment_repo = InMemoryAttachmentMappingRepository()
    audit_repo = InMemoryAuditRepository()

    await inventory_repo.replace_for_migration(
        manifest.migration_id,
        [
            InventorySnapshotRecord(
                migration_id=manifest.migration_id,
                source_chat_id="chat-1",
                source_chat_type="supergroup",
                source_chat_title="Telegram Project Chat",
                message_count=3,
                media_count=0,
                approximate_bytes=512,
                captured_at=datetime(2026, 3, 18, 9, 0, tzinfo=UTC),
            ),
        ],
    )
    await claim_imported(
        message_repo,
        source_chat_id="chat-1",
        source_message_id="1",
        minute_offset=0,
        target_chat_id="express-chat-1",
    )
    await claim_imported(
        message_repo,
        source_chat_id="chat-1",
        source_message_id="2",
        minute_offset=1,
        target_chat_id="express-chat-1",
    )
    await message_repo.claim(
        build_mapping(
            source_chat_id="chat-1",
            source_message_id="3",
            minute_offset=2,
            target_chat_id="express-chat-1",
        ),
    )
    await message_repo.mark_failed(
        "migration-1",
        "chat-1",
        "3",
        error_code="RecoverableItemError",
        error_payload={"message": "planned failure"},
    )
    await message_repo.claim(
        build_mapping(
            source_chat_id="chat-2",
            source_message_id="10",
            minute_offset=10,
            target_chat_id="express-chat-2",
        ),
    )
    await message_repo.mark_ambiguous(
        "migration-1",
        "chat-2",
        "10",
        error_code="AmbiguousDeliveryError",
        error_payload={"message": "delivery outcome unknown"},
    )
    await message_repo.claim(
        build_mapping(
            source_chat_id="chat-2",
            source_message_id="11",
            minute_offset=11,
            target_chat_id="express-chat-2",
        ),
    )
    await claim_attachment(
        attachment_repo,
        source_chat_id="chat-1",
        source_message_id="1",
        attachment_index=0,
        target_chat_id="express-chat-1",
        import_status=AttachmentImportStatus.IMPORTED,
    )
    await claim_attachment(
        attachment_repo,
        source_chat_id="chat-1",
        source_message_id="3",
        attachment_index=0,
        target_chat_id="express-chat-1",
        import_status=AttachmentImportStatus.FAILED,
    )
    await claim_attachment(
        attachment_repo,
        source_chat_id="chat-2",
        source_message_id="10",
        attachment_index=0,
        target_chat_id="express-chat-2",
        import_status=AttachmentImportStatus.AMBIGUOUS,
    )
    await attachment_repo.claim(
        AttachmentMappingRecord(
            migration_id="migration-1",
            source_chat_id="chat-2",
            source_message_id="11",
            attachment_index=0,
            source_file_id="file-chat-2-11-0",
            source_filename="11-0.jpg",
            media_kind="photo",
            checksum="sha-chat-2-11-0",
            size_bytes=128,
            target_chat_id="express-chat-2",
            import_status=AttachmentImportStatus.PROCESSING,
        ),
    )

    result = await ReconcileMigrationUseCase(
        inventory_snapshot_repository=inventory_repo,
        message_mapping_repository=message_repo,
        attachment_mapping_repository=attachment_repo,
        audit_repository=audit_repo,
    ).execute(
        ReconcileMigrationCommand(manifest=manifest),
    )

    assert result.migration_id == manifest.migration_id
    assert result.chats_total == 2
    assert result.attention_chats == 2
    assert result.inventory_missing_chats == 1
    assert result.source_messages_total_known == 3
    assert result.imported_count == 2
    assert result.failed_count == 1
    assert result.ambiguous_count == 1
    assert result.processing_count == 1
    assert result.mapped_total == 5
    assert result.gap_total_known == 1
    assert result.source_media_total_known == 0
    assert result.attachment_imported_count == 1
    assert result.attachment_failed_count == 1
    assert result.attachment_ambiguous_count == 1
    assert result.attachment_processing_count == 1
    assert result.attachment_skipped_count == 0
    assert result.attachment_mapped_total == 4

    chat_1 = next(chat for chat in result.chats if chat.source_chat_id == "chat-1")
    chat_2 = next(chat for chat in result.chats if chat.source_chat_id == "chat-2")

    assert chat_1.inventory_present is True
    assert chat_1.source_chat_title == "Telegram Project Chat"
    assert chat_1.source_message_count == 3
    assert chat_1.imported_count == 2
    assert chat_1.failed_count == 1
    assert chat_1.gap_count == 1
    assert chat_1.attachment_imported_count == 1
    assert chat_1.attachment_failed_count == 1
    assert chat_1.requires_attention is True

    assert chat_2.inventory_present is False
    assert chat_2.source_chat_type == "channel"
    assert chat_2.source_message_count is None
    assert chat_2.ambiguous_count == 1
    assert chat_2.processing_count == 1
    assert chat_2.attachment_ambiguous_count == 1
    assert chat_2.attachment_processing_count == 1
    assert chat_2.requires_attention is True

    audit_events = await audit_repo.list_all()
    assert len(audit_events) == 1
    assert audit_events[0].event_type == "reconcile_report_generated"


@pytest.mark.asyncio
async def test_reconcile_filters_to_one_chat_and_reports_clean_state():
    manifest = build_manifest()
    inventory_repo = InMemoryInventorySnapshotRepository()
    message_repo = InMemoryMessageMappingRepository()
    attachment_repo = InMemoryAttachmentMappingRepository()
    audit_repo = InMemoryAuditRepository()

    await inventory_repo.replace_for_migration(
        manifest.migration_id,
        [
            InventorySnapshotRecord(
                migration_id=manifest.migration_id,
                source_chat_id="chat-1",
                source_chat_type="supergroup",
                source_chat_title="Telegram Project Chat",
                message_count=2,
                media_count=0,
                approximate_bytes=256,
                captured_at=datetime(2026, 3, 18, 9, 0, tzinfo=UTC),
            ),
        ],
    )
    await claim_imported(
        message_repo,
        source_chat_id="chat-1",
        source_message_id="1",
        minute_offset=0,
        target_chat_id="express-chat-1",
    )
    await claim_imported(
        message_repo,
        source_chat_id="chat-1",
        source_message_id="2",
        minute_offset=1,
        target_chat_id="express-chat-1",
    )

    result = await ReconcileMigrationUseCase(
        inventory_snapshot_repository=inventory_repo,
        message_mapping_repository=message_repo,
        attachment_mapping_repository=attachment_repo,
        audit_repository=audit_repo,
    ).execute(
        ReconcileMigrationCommand(
            manifest=manifest,
            source_chat_id="chat-1",
        ),
    )

    assert result.chats_total == 1
    assert result.attention_chats == 0
    assert result.inventory_missing_chats == 0
    assert result.imported_count == 2
    assert result.failed_count == 0
    assert result.ambiguous_count == 0
    assert result.processing_count == 0
    assert result.gap_total_known == 0
    assert result.attachment_failed_count == 0
    assert result.attachment_ambiguous_count == 0
    assert result.attachment_processing_count == 0
    assert result.chats[0].source_chat_id == "chat-1"
    assert result.chats[0].requires_attention is False
