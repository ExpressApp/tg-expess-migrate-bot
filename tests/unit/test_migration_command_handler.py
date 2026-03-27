from datetime import UTC, datetime

import pytest

from extg_migration_runtime.application.migration_command_handler import MigrationCommandHandler
from extg_shared.contracts.events import IntegrationEnvelope
from extg_shared.contracts.models import MigrationJobRecord, MigrationJobStatus
from extg_migration_runtime.infrastructure.persistence.in_memory import (
    InMemoryInboxRepository,
    InMemoryMigrationJobRepository,
)


class StubWorker:
    def __init__(self) -> None:
        self.executed_job_keys: list[str] = []

    async def execute_claimed_job(self, job: MigrationJobRecord) -> None:
        self.executed_job_keys.append(job.job_key)


class StubLogger:
    def info(self, *_args, **_kwargs) -> None:
        return None


def build_job(*, job_key: str = "job-1") -> MigrationJobRecord:
    return MigrationJobRecord(
        job_key=job_key,
        migration_id="migration-1",
        operation="migrate_chat",
        operator_huid="operator-1",
        source_chat_ids=("chat-1",),
        batch_size=100,
        status=MigrationJobStatus.QUEUED,
        requested_at=datetime(2026, 3, 24, 12, 0, tzinfo=UTC),
    )


def build_envelope(*, message_id: str = "event-1", job_key: str = "job-1") -> IntegrationEnvelope:
    return IntegrationEnvelope(
        message_id=message_id,
        message_type="migration.job.requested",
        schema_version=1,
        occurred_at=datetime(2026, 3, 24, 12, 0, tzinfo=UTC),
        aggregate_type="migration_job",
        aggregate_id=job_key,
        migration_id="migration-1",
        logical_chat_key="chat-1",
        payload_json={
            "job_key": job_key,
            "migration_id": "migration-1",
            "source_chat_ids": ["chat-1"],
        },
        headers_json={"schema_version": 1},
    )


@pytest.mark.asyncio
async def test_migration_command_handler_claims_and_executes_job() -> None:
    inbox = InMemoryInboxRepository()
    jobs = InMemoryMigrationJobRepository()
    worker = StubWorker()
    await jobs.enqueue(build_job())
    handler = MigrationCommandHandler(
        inbox_repository=inbox,
        migration_job_repository=jobs,
        migration_job_worker=worker,
        logger=StubLogger(),
        lease_duration_seconds=30.0,
    )

    result = await handler.handle(build_envelope())

    assert result == "processed"
    assert worker.executed_job_keys == ["job-1"]
    assert await inbox.is_processed(
        consumer_name="migration-command-consumer",
        message_id="event-1",
    )


@pytest.mark.asyncio
async def test_migration_command_handler_is_idempotent_via_inbox() -> None:
    inbox = InMemoryInboxRepository()
    jobs = InMemoryMigrationJobRepository()
    worker = StubWorker()
    await jobs.enqueue(build_job())
    handler = MigrationCommandHandler(
        inbox_repository=inbox,
        migration_job_repository=jobs,
        migration_job_worker=worker,
        logger=StubLogger(),
        lease_duration_seconds=30.0,
    )

    first = await handler.handle(build_envelope())
    second = await handler.handle(build_envelope())

    assert first == "processed"
    assert second == "duplicate"
    assert worker.executed_job_keys == ["job-1"]
