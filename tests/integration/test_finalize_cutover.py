from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest

from extg_migration_runtime.application.use_cases.finalize_cutover import (
    FinalizeCutoverCommand,
    FinalizeCutoverUseCase,
)
from extg_migration_runtime.application.use_cases.reconcile_migration import (
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
    MigrationLifecycleStatus,
    MigrationStateRecord,
)
from extg_migration_runtime.infrastructure.persistence.in_memory import (
    InMemoryAttachmentMappingRepository,
    InMemoryInventorySnapshotRepository,
    InMemoryMessageMappingRepository,
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


def build_message_mapping(
    *,
    source_message_id: str,
    minute_offset: int,
    import_status: MessageImportStatus = MessageImportStatus.PROCESSING,
) -> MessageMappingRecord:
    return MessageMappingRecord(
        migration_id="migration-1",
        source_chat_id="chat-1",
        source_message_id=source_message_id,
        source_sent_at=datetime(2026, 3, 18, 10, 0, tzinfo=UTC) + timedelta(minutes=minute_offset),
        target_chat_id="express-chat-1",
        checksum=f"checksum-{source_message_id}",
        import_status=import_status,
    )


async def claim_message_status(
    repository: InMemoryMessageMappingRepository,
    *,
    source_message_id: str,
    minute_offset: int,
    import_status: MessageImportStatus,
) -> None:
    await repository.claim(
        build_message_mapping(
            source_message_id=source_message_id,
            minute_offset=minute_offset,
        ),
    )
    if import_status is MessageImportStatus.IMPORTED:
        await repository.mark_imported(
            "migration-1",
            "chat-1",
            source_message_id,
            target_sync_id=f"sync-{source_message_id}",
            rendered_body=f"Message {source_message_id}",
        )
    elif import_status is MessageImportStatus.FAILED:
        await repository.mark_failed(
            "migration-1",
            "chat-1",
            source_message_id,
            error_code="RecoverableItemError",
            error_payload={"message": "planned message failure"},
        )
    elif import_status is MessageImportStatus.AMBIGUOUS:
        await repository.mark_ambiguous(
            "migration-1",
            "chat-1",
            source_message_id,
            error_code="AmbiguousDeliveryError",
            error_payload={"message": "planned ambiguous message"},
        )


async def claim_attachment_status(
    repository: InMemoryAttachmentMappingRepository,
    *,
    source_message_id: str,
    attachment_index: int,
    import_status: AttachmentImportStatus,
) -> None:
    attachment = CanonicalAttachment(
        source_file_id=f"file-{source_message_id}-{attachment_index}",
        filename=f"{source_message_id}-{attachment_index}.jpg",
        mime_type="image/jpeg",
        size_bytes=256,
        media_kind="photo",
        sha256=f"sha-{source_message_id}-{attachment_index}",
    )
    await repository.claim(
        AttachmentMappingRecord(
            migration_id="migration-1",
            source_chat_id="chat-1",
            source_message_id=source_message_id,
            attachment_index=attachment_index,
            source_file_id=attachment.source_file_id,
            source_filename=attachment.filename,
            media_kind=attachment.media_kind,
            checksum=attachment.sha256,
            size_bytes=attachment.size_bytes,
            target_chat_id="express-chat-1",
            import_status=AttachmentImportStatus.PROCESSING,
        ),
    )
    if import_status is AttachmentImportStatus.IMPORTED:
        await repository.mark_imported(
            "migration-1",
            "chat-1",
            source_message_id,
            attachment_index,
            attachment=attachment,
            target_sync_id=f"att-sync-{source_message_id}-{attachment_index}",
        )
    elif import_status is AttachmentImportStatus.FAILED:
        await repository.mark_failed(
            "migration-1",
            "chat-1",
            source_message_id,
            attachment_index,
            attachment=attachment,
            error_code="RecoverableItemError",
            error_payload={"message": "planned attachment failure"},
        )
    elif import_status is AttachmentImportStatus.AMBIGUOUS:
        await repository.mark_ambiguous(
            "migration-1",
            "chat-1",
            source_message_id,
            attachment_index,
            attachment=attachment,
            error_code="AmbiguousDeliveryError",
            error_payload={"message": "planned ambiguous attachment"},
        )


def build_use_case(
    *,
    inventory_repo: InMemoryInventorySnapshotRepository,
    message_repo: InMemoryMessageMappingRepository,
    attachment_repo: InMemoryAttachmentMappingRepository,
    migration_state_repo: InMemoryMigrationStateRepository,
    audit_repo: InMemoryAuditRepository,
) -> FinalizeCutoverUseCase:
    reconcile_use_case = ReconcileMigrationUseCase(
        inventory_snapshot_repository=inventory_repo,
        message_mapping_repository=message_repo,
        attachment_mapping_repository=attachment_repo,
        audit_repository=audit_repo,
    )
    return FinalizeCutoverUseCase(
        reconcile_migration_use_case=reconcile_use_case,
        migration_state_repository=migration_state_repo,
        audit_repository=audit_repo,
    )


async def seed_cutover_freeze_state(
    migration_state_repo: InMemoryMigrationStateRepository,
) -> None:
    frozen_at = datetime.now(tz=UTC)
    await migration_state_repo.save(
        MigrationStateRecord(
            migration_id="migration-1",
            status=MigrationLifecycleStatus.PAUSED,
            created_at=frozen_at,
            updated_at=frozen_at,
            freeze_started_at=frozen_at,
        ),
    )


@pytest.mark.asyncio
async def test_finalize_cutover_marks_migration_completed_when_reconcile_is_clean():
    manifest = build_manifest()
    inventory_repo = InMemoryInventorySnapshotRepository()
    message_repo = InMemoryMessageMappingRepository()
    attachment_repo = InMemoryAttachmentMappingRepository()
    migration_state_repo = InMemoryMigrationStateRepository()
    audit_repo = InMemoryAuditRepository()

    await inventory_repo.save(
        InventorySnapshotRecord(
            migration_id=manifest.migration_id,
            source_chat_id="chat-1",
            source_chat_type="supergroup",
            source_chat_title="Telegram Project Chat",
            message_count=2,
            media_count=1,
            approximate_bytes=512,
            captured_at=datetime.now(tz=UTC),
        ),
    )
    await claim_message_status(
        message_repo,
        source_message_id="1",
        minute_offset=0,
        import_status=MessageImportStatus.IMPORTED,
    )
    await claim_message_status(
        message_repo,
        source_message_id="2",
        minute_offset=1,
        import_status=MessageImportStatus.IMPORTED,
    )
    await claim_attachment_status(
        attachment_repo,
        source_message_id="2",
        attachment_index=0,
        import_status=AttachmentImportStatus.IMPORTED,
    )
    await seed_cutover_freeze_state(migration_state_repo)

    use_case = build_use_case(
        inventory_repo=inventory_repo,
        message_repo=message_repo,
        attachment_repo=attachment_repo,
        migration_state_repo=migration_state_repo,
        audit_repo=audit_repo,
    )

    result = await use_case.execute(
        FinalizeCutoverCommand(
            manifest=manifest,
            at_least_once_policy_acknowledged=True,
        ),
    )
    saved_state = await migration_state_repo.get(manifest.migration_id)
    audit_events = await audit_repo.list_all()

    assert result.status == "completed"
    assert result.migration_state is MigrationLifecycleStatus.COMPLETED
    assert result.finalized_at is not None
    assert result.freeze_started_at is not None
    assert result.blockers == []
    assert result.checklist["at_least_once_policy_acknowledged"] is True
    assert result.reconcile_result is not None
    assert result.reconcile_result.attention_chats == 0
    assert result.reconcile_result.attachment_failed_count == 0

    assert saved_state is not None
    assert saved_state.status is MigrationLifecycleStatus.COMPLETED
    assert saved_state.finalized_at is not None
    assert saved_state.last_blockers == []

    assert [event.event_type for event in audit_events] == [
        "reconcile_report_generated",
        "cutover_finalized",
    ]


@pytest.mark.asyncio
async def test_finalize_cutover_marks_needs_manual_reconcile_when_blockers_exist():
    manifest = build_manifest()
    inventory_repo = InMemoryInventorySnapshotRepository()
    message_repo = InMemoryMessageMappingRepository()
    attachment_repo = InMemoryAttachmentMappingRepository()
    migration_state_repo = InMemoryMigrationStateRepository()
    audit_repo = InMemoryAuditRepository()

    await inventory_repo.save(
        InventorySnapshotRecord(
            migration_id=manifest.migration_id,
            source_chat_id="chat-1",
            source_chat_type="supergroup",
            source_chat_title="Telegram Project Chat",
            message_count=3,
            media_count=1,
            approximate_bytes=768,
            captured_at=datetime.now(tz=UTC),
        ),
    )
    await claim_message_status(
        message_repo,
        source_message_id="1",
        minute_offset=0,
        import_status=MessageImportStatus.IMPORTED,
    )
    await claim_message_status(
        message_repo,
        source_message_id="2",
        minute_offset=1,
        import_status=MessageImportStatus.FAILED,
    )
    await claim_message_status(
        message_repo,
        source_message_id="3",
        minute_offset=2,
        import_status=MessageImportStatus.AMBIGUOUS,
    )
    await claim_attachment_status(
        attachment_repo,
        source_message_id="2",
        attachment_index=0,
        import_status=AttachmentImportStatus.FAILED,
    )
    await seed_cutover_freeze_state(migration_state_repo)

    use_case = build_use_case(
        inventory_repo=inventory_repo,
        message_repo=message_repo,
        attachment_repo=attachment_repo,
        migration_state_repo=migration_state_repo,
        audit_repo=audit_repo,
    )

    result = await use_case.execute(
        FinalizeCutoverCommand(
            manifest=manifest,
            at_least_once_policy_acknowledged=False,
        ),
    )
    saved_state = await migration_state_repo.get(manifest.migration_id)
    audit_events = await audit_repo.list_all()

    assert result.status == "blocked"
    assert result.migration_state is MigrationLifecycleStatus.NEEDS_MANUAL_RECONCILE
    assert result.finalized_at is None
    assert result.blockers
    assert any("failed message imports" in blocker for blocker in result.blockers)
    assert any("ambiguous message imports" in blocker for blocker in result.blockers)
    assert any("failed attachment imports" in blocker for blocker in result.blockers)
    assert any("at-least-once delivery policy" in blocker for blocker in result.blockers)
    assert result.reconcile_result is not None
    assert result.reconcile_result.attention_chats == 1
    assert result.reconcile_result.gap_total_known == 2

    assert saved_state is not None
    assert saved_state.status is MigrationLifecycleStatus.NEEDS_MANUAL_RECONCILE
    assert saved_state.finalized_at is None
    assert saved_state.last_blockers == result.blockers

    assert [event.event_type for event in audit_events] == [
        "reconcile_report_generated",
        "cutover_finalize_blocked",
    ]


@pytest.mark.asyncio
async def test_finalize_cutover_blocks_without_freeze_window():
    manifest = build_manifest()
    inventory_repo = InMemoryInventorySnapshotRepository()
    message_repo = InMemoryMessageMappingRepository()
    attachment_repo = InMemoryAttachmentMappingRepository()
    migration_state_repo = InMemoryMigrationStateRepository()
    audit_repo = InMemoryAuditRepository()

    await inventory_repo.save(
        InventorySnapshotRecord(
            migration_id=manifest.migration_id,
            source_chat_id="chat-1",
            source_chat_type="supergroup",
            source_chat_title="Telegram Project Chat",
            message_count=1,
            media_count=0,
            approximate_bytes=128,
            captured_at=datetime.now(tz=UTC),
        ),
    )
    await claim_message_status(
        message_repo,
        source_message_id="1",
        minute_offset=0,
        import_status=MessageImportStatus.IMPORTED,
    )

    use_case = build_use_case(
        inventory_repo=inventory_repo,
        message_repo=message_repo,
        attachment_repo=attachment_repo,
        migration_state_repo=migration_state_repo,
        audit_repo=audit_repo,
    )

    result = await use_case.execute(
        FinalizeCutoverCommand(
            manifest=manifest,
            at_least_once_policy_acknowledged=True,
        ),
    )

    assert result.status == "blocked"
    assert any("freeze window" in blocker for blocker in result.blockers)
    assert any("not paused for cutover" in blocker for blocker in result.blockers)
