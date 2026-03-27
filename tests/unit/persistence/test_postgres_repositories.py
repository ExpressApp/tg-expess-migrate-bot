from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest
from sqlalchemy import BigInteger

from extg_shared.contracts.models import (
    AuditEvent,
    AuditSeverity,
    ClaimState,
    ConsumerInboxRecord,
    IntegrationOutboxEventRecord,
    IntegrationOutboxStatus,
    MigrationJobRecord,
    MigrationJobStatus,
    MIGRATION_JOB_CANCELLED_ERROR_CODE,
    MIGRATION_JOB_CANCEL_REQUESTED_ERROR_CODE,
    MessageImportStatus,
    MessageMappingRecord,
    OperatorTelegramSessionRecord,
    PublishedIntegrationEvent,
)
from extg_migration_runtime.infrastructure.persistence.models import (
    ConsumerInboxModel,
    IntegrationOutboxModel,
    MigrationJobModel,
    MigrationMessageMapModel,
)
from extg_telethon_service.infrastructure.persistence.models import (
    OperatorTelegramSessionModel,
)
from extg_migration_runtime.infrastructure.persistence.repositories import (
    PostgresInboxRepository,
    PostgresInventorySnapshotRepository,
    PostgresMessageMappingRepository,
    PostgresMigrationJobRepository,
    PostgresOutboxRepository,
)
from extg_migration_runtime.infrastructure.persistence.models import (
    MigrationInventorySnapshotModel,
)
from extg_telethon_service.infrastructure.persistence.repositories import (
    PostgresAuditRepository,
    PostgresOperatorTelegramSessionRepository,
)
from extg_shared.contracts.models import InventorySnapshotRecord
from extg_migration_runtime.infrastructure.persistence.in_memory import (
    InMemoryInventorySnapshotRepository,
    InMemoryInboxRepository,
    InMemoryMessageMappingRepository,
    InMemoryOutboxRepository,
)
from extg_telethon_service.infrastructure.persistence.in_memory import (
    InMemoryOperatorTelegramSessionRepository,
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
        self.executed_statements = []

    async def execute(self, _statement):
        self.executed_statements.append(_statement)
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


def _build_record(status: MessageImportStatus = MessageImportStatus.PROCESSING):
    return MessageMappingRecord(
        migration_id="migration-1",
        source_chat_id="chat-1",
        source_message_id="100",
        source_sent_at=datetime(2026, 3, 17, 10, 0, tzinfo=UTC),
        target_chat_id="express-chat-1",
        checksum="abc",
        import_status=status,
    )


def _build_model(status: MessageImportStatus = MessageImportStatus.PROCESSING):
    return MigrationMessageMapModel(
        migration_id="migration-1",
        source_chat_id="chat-1",
        source_message_id="100",
        source_sent_at=datetime(2026, 3, 17, 10, 0, tzinfo=UTC),
        target_chat_id="express-chat-1",
        target_sync_id="sync-1",
        checksum="abc",
        import_status=status.value,
        imported_at=datetime(2026, 3, 17, 10, 1, tzinfo=UTC),
        rendered_body="body",
        last_error_code=None,
        last_error_payload=None,
        created_at=datetime(2026, 3, 17, 10, 0, tzinfo=UTC),
        updated_at=datetime(2026, 3, 17, 10, 1, tzinfo=UTC),
    )


def _build_model_with_raw_status(status: str):
    return MigrationMessageMapModel(
        migration_id="migration-1",
        source_chat_id="chat-1",
        source_message_id="100",
        source_sent_at=datetime(2026, 3, 17, 10, 0, tzinfo=UTC),
        target_chat_id="express-chat-1",
        target_sync_id=None,
        checksum="abc",
        import_status=status,
        imported_at=None,
        rendered_body=None,
        last_error_code="ambiguous_delivery",
        last_error_payload={"reason": "timeout"},
        created_at=datetime(2026, 3, 17, 10, 0, tzinfo=UTC),
        updated_at=datetime(2026, 3, 17, 10, 1, tzinfo=UTC),
    )


def _build_operator_session_record(
    *,
    operator_huid: str = "operator-1",
    phone_number: str = "+79990000000",
    session_path: str = "/var/lib/extg/operator-1.session",
    telegram_user_id: str = "123456",
    telegram_username: str | None = "operator_one",
    telegram_display_name: str = "Operator One",
    created_at: datetime | None = None,
    updated_at: datetime | None = None,
    last_used_at: datetime | None = None,
) -> OperatorTelegramSessionRecord:
    created_at = created_at or datetime(2026, 3, 18, 12, 0, tzinfo=UTC)
    updated_at = updated_at or created_at
    return OperatorTelegramSessionRecord(
        operator_huid=operator_huid,
        phone_number=phone_number,
        session_path=session_path,
        telegram_user_id=telegram_user_id,
        telegram_username=telegram_username,
        telegram_display_name=telegram_display_name,
        created_at=created_at,
        updated_at=updated_at,
        last_used_at=last_used_at,
    )


def _build_operator_session_model(
    *,
    operator_huid: str = "operator-1",
    phone_number: str = "+79990000000",
    session_path: str = "/var/lib/extg/operator-1.session",
    telegram_user_id: str = "123456",
    telegram_username: str | None = "operator_one",
    telegram_display_name: str = "Operator One",
    created_at: datetime | None = None,
    updated_at: datetime | None = None,
    last_used_at: datetime | None = None,
) -> OperatorTelegramSessionModel:
    created_at = created_at or datetime(2026, 3, 18, 12, 0, tzinfo=UTC)
    updated_at = updated_at or created_at
    return OperatorTelegramSessionModel(
        operator_huid=operator_huid,
        phone_number=phone_number,
        session_path=session_path,
        telegram_user_id=telegram_user_id,
        telegram_username=telegram_username,
        telegram_display_name=telegram_display_name,
        created_at=created_at,
        updated_at=updated_at,
        last_used_at=last_used_at,
    )


def _build_outbox_record(
    *,
    event_id: str = "event-1",
    status: IntegrationOutboxStatus = IntegrationOutboxStatus.PENDING,
) -> IntegrationOutboxEventRecord:
    return IntegrationOutboxEventRecord(
        event_id=event_id,
        aggregate_type="migration_job",
        aggregate_id="job-1",
        event_type="migration.job.requested",
        payload_json={"job_key": "job-1", "error": "secret"},
        headers_json={"source": "bot_control"},
        status=status,
        created_at=datetime(2026, 3, 24, 12, 0, tzinfo=UTC),
    )


def _build_outbox_model(
    *,
    event_id: str = "event-1",
    status: IntegrationOutboxStatus = IntegrationOutboxStatus.PENDING,
) -> IntegrationOutboxModel:
    return IntegrationOutboxModel(
        event_id=event_id,
        aggregate_type="migration_job",
        aggregate_id="job-1",
        event_type="migration.job.requested",
        payload_json={"job_key": "job-1"},
        headers_json={"source": "bot_control"},
        status=status.value,
        leased_by="publisher-1" if status is IntegrationOutboxStatus.LEASED else None,
        leased_at=datetime(2026, 3, 24, 12, 1, tzinfo=UTC)
        if status is IntegrationOutboxStatus.LEASED
        else None,
        lease_expires_at=datetime(2026, 3, 24, 12, 2, tzinfo=UTC)
        if status is IntegrationOutboxStatus.LEASED
        else None,
        published_at=datetime(2026, 3, 24, 12, 3, tzinfo=UTC)
        if status is IntegrationOutboxStatus.PUBLISHED
        else None,
        broker_topic="shadow.migration.job.requested"
        if status is IntegrationOutboxStatus.PUBLISHED
        else None,
        broker_partition=0 if status is IntegrationOutboxStatus.PUBLISHED else None,
        broker_offset=1 if status is IntegrationOutboxStatus.PUBLISHED else None,
        attempts=1,
        last_error_code="RuntimeError" if status is IntegrationOutboxStatus.FAILED else None,
        last_error_payload={"error": "boom"} if status is IntegrationOutboxStatus.FAILED else None,
        created_at=datetime(2026, 3, 24, 12, 0, tzinfo=UTC),
        updated_at=datetime(2026, 3, 24, 12, 0, tzinfo=UTC),
    )


def _build_job_model(
    *,
    status: MigrationJobStatus,
    last_error_code: str | None = None,
    last_error_payload: dict | None = None,
) -> MigrationJobModel:
    return MigrationJobModel(
        job_key="job-1",
        migration_id="migration-1",
        operation="migrate_chat",
        operator_huid="operator-1",
        source_chat_ids=["chat-1"],
        anchor_cts_host=None,
        anchor_bot_id=None,
        batch_size=100,
        status=status.value,
        worker_id="worker-1" if status is MigrationJobStatus.RUNNING else None,
        attempts=0,
        last_error_code=last_error_code,
        last_error_payload=last_error_payload,
        requested_at=datetime(2026, 3, 24, 12, 0, tzinfo=UTC),
        started_at=datetime(2026, 3, 24, 12, 1, tzinfo=UTC) if status is MigrationJobStatus.RUNNING else None,
        finished_at=None,
        heartbeat_at=datetime(2026, 3, 24, 12, 2, tzinfo=UTC) if status is MigrationJobStatus.RUNNING else None,
        lease_expires_at=datetime(2026, 3, 24, 12, 31, tzinfo=UTC) if status is MigrationJobStatus.RUNNING else None,
        updated_at=datetime(2026, 3, 24, 12, 2, tzinfo=UTC),
    )


@pytest.mark.asyncio
async def test_claim_returns_claimed_when_insert_succeeds():
    session = _FakeSession(execute_results=[_build_model()])
    repository = PostgresMessageMappingRepository(_SessionFactory(session))

    result = await repository.claim(_build_record())

    assert result.state is ClaimState.CLAIMED
    assert result.record.import_status is MessageImportStatus.PROCESSING
    assert session.commit_calls == 1
    assert session.rollback_calls == 0


@pytest.mark.asyncio
async def test_claim_returns_imported_when_existing_row_is_imported():
    session = _FakeSession(execute_results=[None, _build_model(MessageImportStatus.IMPORTED)])
    repository = PostgresMessageMappingRepository(_SessionFactory(session))

    result = await repository.claim(_build_record())

    assert result.state is ClaimState.IMPORTED
    assert result.record.import_status is MessageImportStatus.IMPORTED
    assert session.commit_calls == 0
    assert session.rollback_calls == 1


@pytest.mark.asyncio
async def test_claim_returns_ambiguous_for_raw_ambiguous_status():
    session = _FakeSession(execute_results=[None, _build_model_with_raw_status("ambiguous")])
    repository = PostgresMessageMappingRepository(_SessionFactory(session))

    result = await repository.claim(_build_record())

    assert result.state is ClaimState.AMBIGUOUS
    assert result.record.import_status is MessageImportStatus.AMBIGUOUS
    assert result.record.last_error_code == "ambiguous_delivery"


@pytest.mark.asyncio
async def test_claim_reclaims_stale_processing_row():
    existing = _build_model(MessageImportStatus.PROCESSING)
    existing.imported_at = None
    existing.updated_at = datetime(2026, 3, 17, 9, 0, tzinfo=UTC)
    reclaimed = _build_model(MessageImportStatus.PROCESSING)
    reclaimed.imported_at = None
    reclaimed.updated_at = datetime(2026, 3, 17, 10, 5, tzinfo=UTC)
    session = _FakeSession(execute_results=[None, existing, reclaimed])
    repository = PostgresMessageMappingRepository(
        _SessionFactory(session),
        processing_stale_after_seconds=60.0,
    )

    result = await repository.claim(_build_record())

    assert result.state is ClaimState.CLAIMED
    assert result.record.import_status is MessageImportStatus.PROCESSING
    assert session.commit_calls == 1
    assert session.rollback_calls == 0


@pytest.mark.asyncio
async def test_claim_reclaims_cancel_failed_row_immediately():
    failed = _build_model(MessageImportStatus.FAILED)
    failed.imported_at = None
    failed.target_sync_id = None
    failed.rendered_body = None
    failed.last_error_code = MIGRATION_JOB_CANCELLED_ERROR_CODE
    failed.last_error_payload = {"message": "cancelled"}
    requeued = _build_model(MessageImportStatus.PROCESSING)
    requeued.imported_at = None
    requeued.target_sync_id = None
    requeued.rendered_body = None
    session = _FakeSession(execute_results=[None, failed, requeued])
    repository = PostgresMessageMappingRepository(_SessionFactory(session))

    result = await repository.claim(_build_record())

    assert result.state is ClaimState.CLAIMED
    assert result.record.import_status is MessageImportStatus.PROCESSING
    assert session.commit_calls == 1
    assert session.rollback_calls == 0


@pytest.mark.asyncio
async def test_claim_reclaims_stale_ambiguous_row_with_retryable_error():
    ambiguous = _build_model(MessageImportStatus.AMBIGUOUS)
    ambiguous.imported_at = None
    ambiguous.target_sync_id = None
    ambiguous.rendered_body = None
    ambiguous.last_error_code = "AmbiguousDeliveryError"
    ambiguous.last_error_payload = {"message": "stale attachment stage"}
    ambiguous.updated_at = datetime.now(tz=UTC) - timedelta(minutes=5)
    reclaimed = _build_model(MessageImportStatus.PROCESSING)
    reclaimed.imported_at = None
    reclaimed.target_sync_id = None
    reclaimed.rendered_body = None
    reclaimed.updated_at = datetime.now(tz=UTC)
    session = _FakeSession(execute_results=[None, ambiguous, reclaimed])
    repository = PostgresMessageMappingRepository(
        _SessionFactory(session),
        processing_stale_after_seconds=1.0,
    )

    result = await repository.claim(_build_record())

    assert result.state is ClaimState.CLAIMED
    assert result.record.import_status is MessageImportStatus.PROCESSING
    assert session.commit_calls == 1
    assert session.rollback_calls == 0


@pytest.mark.asyncio
async def test_requeue_failed_returns_processing_records():
    failed = _build_model(MessageImportStatus.FAILED)
    requeued = _build_model(MessageImportStatus.PROCESSING)
    session = _FakeSession(execute_results=[[failed], [requeued]])
    repository = PostgresMessageMappingRepository(_SessionFactory(session))

    result = await repository.requeue_failed("migration-1", limit=10)

    assert len(result) == 1
    assert result[0].import_status is MessageImportStatus.PROCESSING
    assert session.commit_calls == 1


@pytest.mark.asyncio
async def test_inventory_snapshot_save_returns_record():
    snapshot_model = type(
        "SnapshotModel",
        (),
        {
            "migration_id": "migration-1",
            "source_chat_id": "chat-1",
            "source_chat_type": "supergroup",
            "source_chat_title": "Chat",
            "messages_count": 100,
            "media_count": 3,
            "approximate_bytes": 2048,
            "snapshot_payload": {"sample": True},
            "created_at": datetime(2026, 3, 17, 10, 0, tzinfo=UTC),
            "updated_at": datetime(2026, 3, 17, 10, 0, tzinfo=UTC),
        },
    )()
    session = _FakeSession(execute_results=[snapshot_model])
    repository = PostgresInventorySnapshotRepository(_SessionFactory(session))

    saved = await repository.save(
        InventorySnapshotRecord(
            migration_id="migration-1",
            source_chat_id="chat-1",
            source_chat_type="supergroup",
            source_chat_title="Chat",
            message_count=100,
            media_count=3,
            approximate_bytes=2048,
            captured_at=datetime(2026, 3, 17, 10, 0, tzinfo=UTC),
        ),
    )

    assert saved.source_chat_id == "chat-1"
    assert saved.message_count == 100
    assert session.commit_calls == 1


def test_inventory_snapshot_model_uses_bigint_for_approximate_bytes():
    assert isinstance(
        MigrationInventorySnapshotModel.__table__.c.approximate_bytes.type,
        BigInteger,
    )


def test_postgres_message_repository_drops_rendered_body_when_persistence_is_disabled():
    repository = PostgresMessageMappingRepository(
        _SessionFactory(_FakeSession(execute_results=[])),
        persist_message_bodies=False,
    )

    assert repository._sanitize_rendered_body("secret message body") is None


def test_postgres_message_repository_keeps_rendered_body_when_persistence_is_enabled():
    repository = PostgresMessageMappingRepository(
        _SessionFactory(_FakeSession(execute_results=[])),
        persist_message_bodies=True,
    )

    assert repository._sanitize_rendered_body("secret message body") == "secret message body"


def test_postgres_message_repository_redacts_sensitive_error_payloads_by_default():
    repository = PostgresMessageMappingRepository(
        _SessionFactory(_FakeSession(execute_results=[])),
        payload_storage_mode="redacted",
    )

    sanitized = repository._sanitize_json_payload(
        {
            "phase": "send",
            "message": "secret message body",
            "source_chat_id": "chat-1",
            "participant_username": "alice",
            "resolved_participants_count": 2,
        },
    )

    assert sanitized == {
        "phase": "send",
        "message": "[redacted]",
        "source_chat_id": "chat-1",
        "participant_username": "[redacted]",
        "resolved_participants_count": 2,
    }


@pytest.mark.asyncio
async def test_postgres_audit_repository_redacts_payloads_by_default():
    session = _FakeSession(execute_results=[None])
    repository = PostgresAuditRepository(
        _SessionFactory(session),
        payload_storage_mode="redacted",
    )

    await repository.add(
        AuditEvent(
            migration_id="migration-1",
            event_type="attachment_send_failed",
            source_chat_id="chat-1",
            source_message_id="100",
            severity=AuditSeverity.ERROR,
            created_at=datetime(2026, 3, 19, 10, 0, tzinfo=UTC),
            payload_json={
                "source_chat_id": "chat-1",
                "error": "leaked body",
                "filename": "secret.pdf",
                "resolved_participants_count": 2,
            },
        ),
    )

    payload = session.executed_statements[0].compile().params["payload_json"]

    assert payload == {
        "source_chat_id": "chat-1",
        "error": "[redacted]",
        "filename": "[redacted]",
        "resolved_participants_count": 2,
    }


@pytest.mark.asyncio
async def test_inmemory_message_repo_supports_ambiguous_and_requeue():
    repository = InMemoryMessageMappingRepository()
    record = _build_record()

    claim = await repository.claim(record)
    assert claim.state is ClaimState.CLAIMED

    await repository.mark_ambiguous(
        "migration-1",
        "chat-1",
        "100",
        error_code="ambiguous_delivery",
        error_payload={"reason": "network timeout"},
    )
    claim_after_ambiguous = await repository.claim(record)
    assert claim_after_ambiguous.state is ClaimState.AMBIGUOUS

    await repository.mark_failed(
        "migration-1",
        "chat-1",
        "100",
        error_code="send_failed",
        error_payload={"reason": "timeout"},
    )
    failed = await repository.list_failed("migration-1", limit=10)
    assert len(failed) == 1
    assert failed[0].import_status is MessageImportStatus.FAILED

    requeued = await repository.requeue_failed("migration-1", limit=10)
    assert len(requeued) == 1
    assert requeued[0].import_status is MessageImportStatus.PROCESSING


@pytest.mark.asyncio
async def test_inmemory_message_repo_reclaims_cancel_failed_records():
    repository = InMemoryMessageMappingRepository()
    record = _build_record()

    await repository.claim(record)
    await repository.mark_failed(
        "migration-1",
        "chat-1",
        "100",
        error_code=MIGRATION_JOB_CANCELLED_ERROR_CODE,
        error_payload={"reason": "cancelled"},
    )

    reclaimed = await repository.claim(record)

    assert reclaimed.state is ClaimState.CLAIMED
    assert reclaimed.record.import_status is MessageImportStatus.PROCESSING


@pytest.mark.asyncio
async def test_inmemory_message_repo_reclaims_stale_ambiguous_records():
    repository = InMemoryMessageMappingRepository(processing_stale_after_seconds=1.0)
    record = _build_record()
    key = (record.migration_id, record.source_chat_id, record.source_message_id)

    await repository.claim(record)
    await repository.mark_ambiguous(
        "migration-1",
        "chat-1",
        "100",
        error_code="AmbiguousDeliveryError",
        error_payload={"reason": "stale attachment stage"},
    )
    repository._processing_started_at[key] = datetime.now(tz=UTC) - timedelta(minutes=5)

    reclaimed = await repository.claim(record)

    assert reclaimed.state is ClaimState.CLAIMED
    assert reclaimed.record.import_status is MessageImportStatus.PROCESSING


@pytest.mark.asyncio
async def test_postgres_operator_session_repo_upserts_gets_and_deletes():
    saved_model = _build_operator_session_model(last_used_at=datetime(2026, 3, 18, 12, 5, tzinfo=UTC))
    session = _FakeSession(execute_results=[saved_model])
    repository = PostgresOperatorTelegramSessionRepository(_SessionFactory(session))

    saved = await repository.save(_build_operator_session_record(last_used_at=saved_model.last_used_at))

    assert saved.operator_huid == "operator-1"
    assert saved.session_path == "/var/lib/extg/operator-1.session"
    assert saved.last_used_at == saved_model.last_used_at
    assert session.commit_calls == 1

    load_session = _FakeSession(execute_results=[saved_model])
    load_repository = PostgresOperatorTelegramSessionRepository(_SessionFactory(load_session))
    loaded = await load_repository.get_by_operator("operator-1")

    assert loaded is not None
    assert loaded.telegram_display_name == "Operator One"
    assert load_session.commit_calls == 0

    delete_session = _FakeSession(execute_results=[1])
    delete_repository = PostgresOperatorTelegramSessionRepository(_SessionFactory(delete_session))
    deleted = await delete_repository.delete("operator-1")

    assert deleted is True
    assert delete_session.commit_calls == 1


@pytest.mark.asyncio
async def test_postgres_migration_job_repo_enqueues_shadow_outbox_atomically():
    session = _FakeSession(execute_results=["job-1", None])
    repository = PostgresMigrationJobRepository(_SessionFactory(session))

    enqueued = await repository.enqueue(
        record=MigrationJobRecord(
            job_key="job-1",
            migration_id="migration-1",
            operation="migrate_chat",
            operator_huid="operator-1",
            source_chat_ids=("chat-1",),
            batch_size=100,
            status=MigrationJobStatus.QUEUED,
            requested_at=datetime(2026, 3, 24, 12, 0, tzinfo=UTC),
        ),
        outbox_events=(_build_outbox_record(),),
    )

    assert enqueued is True
    assert session.commit_calls == 1
    assert len(session.executed_statements) == 2


@pytest.mark.asyncio
async def test_postgres_migration_job_repo_request_cancel_marks_queued_job_failed():
    session = _FakeSession(
        execute_results=[
            _build_job_model(
                status=MigrationJobStatus.QUEUED,
                last_error_code=MIGRATION_JOB_CANCELLED_ERROR_CODE,
                last_error_payload={"requested_by": "operator-1"},
            ),
        ],
    )
    repository = PostgresMigrationJobRepository(_SessionFactory(session))

    updated = await repository.request_cancel(
        job_key="job-1",
        operator_huid="operator-1",
    )

    assert updated is not None
    assert updated.status is MigrationJobStatus.QUEUED
    assert updated.last_error_code == MIGRATION_JOB_CANCELLED_ERROR_CODE
    assert session.commit_calls == 1
    assert len(session.executed_statements) == 1


@pytest.mark.asyncio
async def test_postgres_migration_job_repo_request_cancel_marks_running_job_requested():
    session = _FakeSession(
        execute_results=[
            None,
            _build_job_model(
                status=MigrationJobStatus.RUNNING,
                last_error_code=MIGRATION_JOB_CANCEL_REQUESTED_ERROR_CODE,
                last_error_payload={"requested_by": "operator-1"},
            ),
        ],
    )
    repository = PostgresMigrationJobRepository(_SessionFactory(session))

    updated = await repository.request_cancel(
        job_key="job-1",
        operator_huid="operator-1",
    )

    assert updated is not None
    assert updated.status is MigrationJobStatus.RUNNING
    assert updated.last_error_code == MIGRATION_JOB_CANCEL_REQUESTED_ERROR_CODE
    assert session.commit_calls == 1
    assert len(session.executed_statements) == 2


@pytest.mark.asyncio
async def test_postgres_outbox_repository_leases_and_marks_published():
    leased_model = _build_outbox_model(status=IntegrationOutboxStatus.PENDING)
    published_model = _build_outbox_model(status=IntegrationOutboxStatus.PUBLISHED)
    session = _FakeSession(execute_results=[[leased_model], published_model])
    repository = PostgresOutboxRepository(_SessionFactory(session))

    leased = await repository.lease_batch(
        publisher_id="publisher-1",
        limit=10,
        lease_duration_seconds=30.0,
    )
    updated = await repository.mark_published(
        event_id="event-1",
        publisher_id="publisher-1",
        result=PublishedIntegrationEvent(
            broker_topic="shadow.migration.job.requested",
            broker_partition=0,
            broker_offset=1,
            published_at=datetime(2026, 3, 24, 12, 3, tzinfo=UTC),
        ),
    )

    assert len(leased) == 1
    assert leased[0].status is IntegrationOutboxStatus.LEASED
    assert updated is not None
    assert updated.status is IntegrationOutboxStatus.PUBLISHED
    assert session.commit_calls == 2


@pytest.mark.asyncio
async def test_postgres_inbox_repository_marks_processed_once():
    session = _FakeSession(
        execute_results=[
            "message-1",
            ConsumerInboxModel(
                consumer_name="worker-1",
                message_id="message-1",
                processed_at=datetime(2026, 3, 24, 12, 0, tzinfo=UTC),
                result_code="ok",
                result_payload={"status": "done"},
            ),
        ],
    )
    repository = PostgresInboxRepository(_SessionFactory(session))

    inserted = await repository.mark_processed(
        ConsumerInboxRecord(
            consumer_name="worker-1",
            message_id="message-1",
            processed_at=datetime(2026, 3, 24, 12, 0, tzinfo=UTC),
            result_code="ok",
            result_payload={"status": "done"},
        ),
    )
    duplicate_seen = await repository.is_processed(
        consumer_name="worker-1",
        message_id="message-1",
    )

    assert inserted is True
    assert duplicate_seen is True
    assert session.commit_calls == 1


@pytest.mark.asyncio
async def test_inmemory_operator_session_repo_upserts_and_deletes():
    repository = InMemoryOperatorTelegramSessionRepository()
    first = await repository.save(_build_operator_session_record())
    second = await repository.save(
        _build_operator_session_record(
            telegram_display_name="Operator One Updated",
            last_used_at=datetime(2026, 3, 18, 12, 5, tzinfo=UTC),
            updated_at=datetime(2026, 3, 18, 12, 10, tzinfo=UTC),
        ),
    )

    assert first.created_at == second.created_at
    assert second.telegram_display_name == "Operator One Updated"

    loaded = await repository.get_by_operator("operator-1")
    assert loaded is not None
    assert loaded.telegram_display_name == "Operator One Updated"

    deleted = await repository.delete("operator-1")
    missing = await repository.delete("operator-1")

    assert deleted is True
    assert missing is False


@pytest.mark.asyncio
async def test_inmemory_inventory_snapshot_repo_upserts():
    repository = InMemoryInventorySnapshotRepository()
    first = await repository.save(
        InventorySnapshotRecord(
            migration_id="migration-1",
            source_chat_id="chat-1",
            source_chat_type="supergroup",
            source_chat_title="Chat",
            message_count=10,
            media_count=2,
            approximate_bytes=512,
            captured_at=datetime(2026, 3, 17, 10, 0, tzinfo=UTC),
        ),
    )
    second = await repository.save(
        InventorySnapshotRecord(
            migration_id="migration-1",
            source_chat_id="chat-1",
            source_chat_type="supergroup",
            source_chat_title="Chat updated",
            message_count=20,
            media_count=5,
            approximate_bytes=1024,
            captured_at=datetime(2026, 3, 17, 10, 5, tzinfo=UTC),
        ),
    )

    assert first.captured_at != second.captured_at
    assert second.message_count == 20
    listed = await repository.list_by_migration("migration-1")
    assert len(listed) == 1


@pytest.mark.asyncio
async def test_inmemory_outbox_and_inbox_repositories_support_foundation_flow():
    outbox = InMemoryOutboxRepository()
    inbox = InMemoryInboxRepository()
    await outbox.append_many((_build_outbox_record(event_id="event-2"),))

    leased = await outbox.lease_batch(
        publisher_id="publisher-1",
        limit=10,
        lease_duration_seconds=30.0,
    )
    published = await outbox.mark_published(
        event_id="event-2",
        publisher_id="publisher-1",
        result=PublishedIntegrationEvent(
            broker_topic="shadow.migration.job.requested",
            broker_partition=0,
            broker_offset=2,
            published_at=datetime(2026, 3, 24, 12, 4, tzinfo=UTC),
        ),
    )
    first_mark = await inbox.mark_processed(
        ConsumerInboxRecord(
            consumer_name="consumer-1",
            message_id="message-1",
            processed_at=datetime(2026, 3, 24, 12, 5, tzinfo=UTC),
            result_code="ok",
            result_payload={},
        ),
    )
    second_mark = await inbox.mark_processed(
        ConsumerInboxRecord(
            consumer_name="consumer-1",
            message_id="message-1",
            processed_at=datetime(2026, 3, 24, 12, 6, tzinfo=UTC),
            result_code="ok",
            result_payload={},
        ),
    )

    assert len(leased) == 1
    assert published is not None
    assert published.status is IntegrationOutboxStatus.PUBLISHED
    assert first_mark is True
    assert second_mark is False
