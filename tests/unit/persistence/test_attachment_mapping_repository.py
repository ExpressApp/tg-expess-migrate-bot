from __future__ import annotations

from datetime import UTC, datetime, timedelta
from types import SimpleNamespace

import pytest

from extg_shared.contracts.models import (
    AttachmentImportStatus,
    AttachmentMappingRecord,
    AttachmentStageRecord,
    AttachmentStageStatus,
    CanonicalAttachment,
    ClaimState,
    MIGRATION_JOB_CANCELLED_ERROR_CODE,
)
from extg_shared.contracts.ports import AttachmentStatusSummary
from extg_migration_runtime.infrastructure.persistence.models import (
    AttachmentStageModel,
    MigrationAttachmentMapModel,
)
from extg_migration_runtime.infrastructure.persistence.repositories import (
    PostgresAttachmentMappingRepository,
    PostgresAttachmentStageRepository,
)
from extg_migration_runtime.infrastructure.persistence.in_memory import (
    InMemoryAttachmentMappingRepository,
    InMemoryAttachmentStageRepository,
)


def _build_attachment(
    *,
    source_file_id: str = "file-1",
    filename: str = "document.txt",
    media_kind: str = "document",
) -> CanonicalAttachment:
    return CanonicalAttachment(
        source_file_id=source_file_id,
        filename=filename,
        mime_type="text/plain",
        size_bytes=12,
        media_kind=media_kind,
        sha256="deadbeef",
    )


def _build_record(
    *,
    attachment_index: int = 0,
    status: AttachmentImportStatus = AttachmentImportStatus.PROCESSING,
) -> AttachmentMappingRecord:
    return AttachmentMappingRecord(
        migration_id="migration-1",
        source_chat_id="chat-1",
        source_message_id="100",
        attachment_index=attachment_index,
        source_file_id="file-1",
        source_filename="document.txt",
        media_kind="document",
        checksum="deadbeef",
        size_bytes=12,
        target_chat_id="target-chat",
        import_status=status,
    )


def _build_model(
    *,
    attachment_index: int = 0,
    status: AttachmentImportStatus = AttachmentImportStatus.PROCESSING,
) -> MigrationAttachmentMapModel:
    return MigrationAttachmentMapModel(
        migration_id="migration-1",
        source_chat_id="chat-1",
        source_message_id="100",
        attachment_index=attachment_index,
        source_file_id="file-1",
        source_filename="document.txt",
        media_kind="document",
        checksum="deadbeef",
        size_bytes=12,
        target_chat_id="target-chat",
        target_sync_id="sync-1",
        import_status=status.value,
        imported_at=datetime(2026, 3, 18, 10, 0, tzinfo=UTC)
        if status is AttachmentImportStatus.IMPORTED
        else None,
        last_error_code="broken" if status is AttachmentImportStatus.FAILED else None,
        last_error_payload={"reason": "broken"} if status is AttachmentImportStatus.FAILED else None,
        created_at=datetime(2026, 3, 18, 10, 0, tzinfo=UTC),
        updated_at=datetime(2026, 3, 18, 10, 1, tzinfo=UTC),
    )


def _build_stage_record(
    *,
    attachment_index: int = 0,
    status: AttachmentStageStatus = AttachmentStageStatus.PROCESSING,
    updated_at: datetime | None = None,
) -> AttachmentStageRecord:
    return AttachmentStageRecord(
        migration_id="migration-1",
        source_chat_id="chat-1",
        source_message_id="100",
        attachment_index=attachment_index,
        target_chat_id="target-chat",
        source_locator="/tmp/file-1",
        media_kind="document",
        checksum="deadbeef",
        size_bytes=12,
        status=status,
        created_at=datetime(2026, 3, 18, 10, 0, tzinfo=UTC),
        updated_at=updated_at,
    )


def _build_stage_model(
    *,
    attachment_index: int = 0,
    status: AttachmentStageStatus = AttachmentStageStatus.PROCESSING,
    updated_at: datetime | None = None,
) -> AttachmentStageModel:
    return AttachmentStageModel(
        migration_id="migration-1",
        source_chat_id="chat-1",
        source_message_id="100",
        attachment_index=attachment_index,
        target_chat_id="target-chat",
        source_locator="/tmp/file-1",
        media_kind="document",
        checksum="deadbeef",
        size_bytes=12,
        status=status.value,
        express_file_id=None,
        express_file_payload=None,
        target_sync_id=None,
        last_error_code=None,
        last_error_payload=None,
        created_at=datetime(2026, 3, 18, 10, 0, tzinfo=UTC),
        updated_at=updated_at or datetime(2026, 3, 18, 10, 1, tzinfo=UTC),
    )


class _Result:
    def __init__(self, value):
        self._value = value

    def scalar_one_or_none(self):
        return self._value

    def scalar_one(self):
        if self._value is None:
            raise AssertionError("expected scalar_one value, got None")
        return self._value

    def scalars(self):
        values = self._value if isinstance(self._value, list) else [self._value]
        return _Scalars(values)

    def all(self):
        return self._value if isinstance(self._value, list) else [self._value]


class _Scalars:
    def __init__(self, values):
        self._values = values

    def all(self):
        return self._values


class _FakeSession:
    def __init__(self, execute_results):
        self._execute_results = list(execute_results)
        self.commit_calls = 0
        self.rollback_calls = 0

    async def execute(self, _statement):
        return _Result(self._execute_results.pop(0))

    async def commit(self):
        self.commit_calls += 1

    async def rollback(self):
        self.rollback_calls += 1


class _SessionFactory:
    def __init__(self, session: _FakeSession):
        self._session = session

    def __call__(self):
        return self

    async def __aenter__(self):
        return self._session

    async def __aexit__(self, exc_type, exc, tb):
        return False


@pytest.mark.asyncio
async def test_inmemory_attachment_repo_supports_claim_mutations_and_summary():
    repository = InMemoryAttachmentMappingRepository()
    attachment = _build_attachment()

    first = await repository.claim(_build_record(attachment_index=0))
    second = await repository.claim(_build_record(attachment_index=1))
    third = await repository.claim(_build_record(attachment_index=2))
    fourth = await repository.claim(_build_record(attachment_index=3))

    assert first.state is ClaimState.CLAIMED
    assert second.state is ClaimState.CLAIMED
    assert third.state is ClaimState.CLAIMED
    assert fourth.state is ClaimState.CLAIMED

    await repository.mark_imported(
        "migration-1",
        "chat-1",
        "100",
        0,
        attachment=attachment,
        target_sync_id="sync-1",
    )
    await repository.mark_failed(
        "migration-1",
        "chat-1",
        "100",
        1,
        attachment=attachment,
        error_code="send_failed",
        error_payload={"reason": "timeout"},
    )
    await repository.mark_ambiguous(
        "migration-1",
        "chat-1",
        "100",
        2,
        attachment=attachment,
        error_code="ambiguous_delivery",
        error_payload={"reason": "timeout"},
    )
    await repository.mark_skipped(
        "migration-1",
        "chat-1",
        "100",
        3,
        attachment=attachment,
        reason="policy disabled",
    )

    listed = await repository.list_by_message("migration-1", "chat-1", "100")
    summary = await repository.summarize_by_chat("migration-1")

    assert [item.attachment_index for item in listed] == [0, 1, 2, 3]
    assert [item.import_status for item in listed] == [
        AttachmentImportStatus.IMPORTED,
        AttachmentImportStatus.FAILED,
        AttachmentImportStatus.AMBIGUOUS,
        AttachmentImportStatus.SKIPPED,
    ]
    assert summary == [
        AttachmentStatusSummary(
            migration_id="migration-1",
            source_chat_id="chat-1",
            import_status=AttachmentImportStatus.AMBIGUOUS,
            count=1,
        ),
        AttachmentStatusSummary(
            migration_id="migration-1",
            source_chat_id="chat-1",
            import_status=AttachmentImportStatus.FAILED,
            count=1,
        ),
        AttachmentStatusSummary(
            migration_id="migration-1",
            source_chat_id="chat-1",
            import_status=AttachmentImportStatus.IMPORTED,
            count=1,
        ),
        AttachmentStatusSummary(
            migration_id="migration-1",
            source_chat_id="chat-1",
            import_status=AttachmentImportStatus.SKIPPED,
            count=1,
        ),
    ]


@pytest.mark.asyncio
async def test_inmemory_attachment_repo_requeues_failed_records_when_requested():
    repository = InMemoryAttachmentMappingRepository()
    record = _build_record()

    await repository.claim(record)
    await repository.mark_failed(
        "migration-1",
        "chat-1",
        "100",
        0,
        attachment=_build_attachment(),
        error_code="send_failed",
        error_payload={"reason": "timeout"},
    )

    claim = await repository.claim(record, retry_failed=True)
    stored = await repository.get("migration-1", "chat-1", "100", 0)

    assert claim.state is ClaimState.CLAIMED
    assert stored is not None
    assert stored.import_status is AttachmentImportStatus.PROCESSING


@pytest.mark.asyncio
async def test_inmemory_attachment_repo_reclaims_cancel_failed_records_without_retry_flag():
    repository = InMemoryAttachmentMappingRepository()
    record = _build_record()

    await repository.claim(record)
    await repository.mark_failed(
        "migration-1",
        "chat-1",
        "100",
        0,
        attachment=_build_attachment(),
        error_code=MIGRATION_JOB_CANCELLED_ERROR_CODE,
        error_payload={"reason": "cancelled"},
    )

    claim = await repository.claim(record)

    assert claim.state is ClaimState.CLAIMED
    assert claim.record.import_status is AttachmentImportStatus.PROCESSING


@pytest.mark.asyncio
async def test_inmemory_attachment_stage_repo_reclaims_stale_processing_records():
    repository = InMemoryAttachmentStageRepository(processing_stale_after_seconds=1.0)
    stale_record = _build_stage_record()
    key = (
        stale_record.migration_id,
        stale_record.source_chat_id,
        stale_record.source_message_id,
        stale_record.attachment_index,
        stale_record.target_chat_id,
    )

    await repository.claim(stale_record)
    repository._processing_started_at[key] = datetime.now(tz=UTC) - timedelta(minutes=5)

    reclaimed = await repository.claim(_build_stage_record())

    assert reclaimed.state is ClaimState.CLAIMED
    assert reclaimed.record.status is AttachmentStageStatus.PROCESSING


@pytest.mark.asyncio
async def test_inmemory_attachment_repo_treats_skipped_records_as_terminal():
    repository = InMemoryAttachmentMappingRepository()
    record = _build_record()

    await repository.claim(record)
    await repository.mark_skipped(
        "migration-1",
        "chat-1",
        "100",
        0,
        attachment=_build_attachment(),
        reason="policy disabled",
    )

    claim = await repository.claim(record)

    assert claim.state is ClaimState.AMBIGUOUS


@pytest.mark.asyncio
async def test_postgres_attachment_repo_claim_and_requeue_failed():
    session = _FakeSession(
        execute_results=[
            _build_model(),
            None,
            _build_model(status=AttachmentImportStatus.FAILED),
            _build_model(status=AttachmentImportStatus.PROCESSING),
        ],
    )
    repository = PostgresAttachmentMappingRepository(_SessionFactory(session))

    inserted = await repository.claim(_build_record())
    requeued = await repository.claim(_build_record(), retry_failed=True)

    assert inserted.state is ClaimState.CLAIMED
    assert requeued.state is ClaimState.CLAIMED
    assert requeued.record.import_status is AttachmentImportStatus.PROCESSING
    assert session.commit_calls == 2
    assert session.rollback_calls == 1


@pytest.mark.asyncio
async def test_postgres_attachment_repo_reclaims_cancel_failed_without_retry_flag():
    failed = _build_model(status=AttachmentImportStatus.FAILED)
    failed.last_error_code = MIGRATION_JOB_CANCELLED_ERROR_CODE
    failed.last_error_payload = {"reason": "cancelled"}
    requeued = _build_model(status=AttachmentImportStatus.PROCESSING)
    session = _FakeSession(
        execute_results=[
            None,
            failed,
            requeued,
        ],
    )
    repository = PostgresAttachmentMappingRepository(_SessionFactory(session))

    claim = await repository.claim(_build_record())

    assert claim.state is ClaimState.CLAIMED
    assert claim.record.import_status is AttachmentImportStatus.PROCESSING
    assert session.commit_calls == 1
    assert session.rollback_calls == 1


@pytest.mark.asyncio
async def test_postgres_attachment_stage_repo_reclaims_stale_processing_records():
    stale_model = _build_stage_model(
        updated_at=datetime.now(tz=UTC) - timedelta(minutes=5),
    )
    reclaimed_model = _build_stage_model(
        updated_at=datetime.now(tz=UTC),
    )
    session = _FakeSession(
        execute_results=[
            None,
            stale_model,
            reclaimed_model,
        ],
    )
    repository = PostgresAttachmentStageRepository(
        _SessionFactory(session),
        processing_stale_after_seconds=1.0,
    )

    claim = await repository.claim(_build_stage_record())

    assert claim.state is ClaimState.CLAIMED
    assert claim.record.status is AttachmentStageStatus.PROCESSING
    assert session.commit_calls == 1
    assert session.rollback_calls == 0


@pytest.mark.asyncio
async def test_postgres_attachment_repo_terminal_states_and_summary():
    session = _FakeSession(
        execute_results=[
            _build_model(status=AttachmentImportStatus.IMPORTED),
            _build_model(status=AttachmentImportStatus.FAILED),
            _build_model(status=AttachmentImportStatus.AMBIGUOUS),
            _build_model(status=AttachmentImportStatus.SKIPPED),
            [
                _build_model(attachment_index=0, status=AttachmentImportStatus.IMPORTED),
                _build_model(attachment_index=1, status=AttachmentImportStatus.FAILED),
            ],
            [
                SimpleNamespace(
                    source_chat_id="chat-1",
                    import_status=AttachmentImportStatus.FAILED.value,
                    count=1,
                ),
                SimpleNamespace(
                    source_chat_id="chat-1",
                    import_status=AttachmentImportStatus.IMPORTED.value,
                    count=1,
                ),
                SimpleNamespace(
                    source_chat_id="chat-1",
                    import_status=AttachmentImportStatus.SKIPPED.value,
                    count=1,
                ),
            ],
        ],
    )
    repository = PostgresAttachmentMappingRepository(_SessionFactory(session))
    attachment = _build_attachment()

    imported = await repository.mark_imported(
        "migration-1",
        "chat-1",
        "100",
        0,
        attachment=attachment,
        target_sync_id="sync-1",
    )
    failed = await repository.mark_failed(
        "migration-1",
        "chat-1",
        "100",
        0,
        attachment=attachment,
        error_code="send_failed",
        error_payload={"reason": "timeout"},
    )
    ambiguous = await repository.mark_ambiguous(
        "migration-1",
        "chat-1",
        "100",
        0,
        attachment=attachment,
        error_code="ambiguous_delivery",
        error_payload={"reason": "timeout"},
    )
    skipped = await repository.mark_skipped(
        "migration-1",
        "chat-1",
        "100",
        0,
        attachment=attachment,
        reason="policy disabled",
    )
    listed = await repository.list_by_message("migration-1", "chat-1", "100")
    summary = await repository.summarize_by_chat("migration-1")

    assert imported.import_status is AttachmentImportStatus.IMPORTED
    assert failed.import_status is AttachmentImportStatus.FAILED
    assert ambiguous.import_status is AttachmentImportStatus.AMBIGUOUS
    assert skipped.import_status is AttachmentImportStatus.SKIPPED
    assert [item.attachment_index for item in listed] == [0, 1]
    assert summary == [
        AttachmentStatusSummary(
            migration_id="migration-1",
            source_chat_id="chat-1",
            import_status=AttachmentImportStatus.FAILED,
            count=1,
        ),
        AttachmentStatusSummary(
            migration_id="migration-1",
            source_chat_id="chat-1",
            import_status=AttachmentImportStatus.IMPORTED,
            count=1,
        ),
        AttachmentStatusSummary(
            migration_id="migration-1",
            source_chat_id="chat-1",
            import_status=AttachmentImportStatus.SKIPPED,
            count=1,
        ),
    ]
