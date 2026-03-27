from __future__ import annotations

import asyncio
from datetime import UTC, datetime

import pytest

from extg_migration_runtime.application.migration_job_worker import MigrationJobWorker
from extg_shared.contracts.models import (
    AuditSeverity,
    MigrationJobRecord,
    MigrationJobStatus,
)
from extg_migration_runtime.infrastructure.persistence.in_memory import (
    InMemoryMigrationJobRepository,
)


class StubBotControlService:
    def __init__(self, *, should_fail: bool = False) -> None:
        self.should_fail = should_fail
        self.executed_jobs: list[str] = []

    async def execute_migration_job(self, job: MigrationJobRecord) -> None:
        self.executed_jobs.append(job.job_key)
        if self.should_fail:
            raise RuntimeError("boom")


class BlockingBotControlService:
    def __init__(self) -> None:
        self.executed_jobs: list[str] = []
        self.started = asyncio.Event()
        self.cancelled = asyncio.Event()

    async def execute_migration_job(self, job: MigrationJobRecord) -> None:
        self.executed_jobs.append(job.job_key)
        self.started.set()
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            self.cancelled.set()
            raise


class StubAuditRepository:
    def __init__(self) -> None:
        self.events = []

    async def add(self, event) -> None:
        self.events.append(event)


class StubLogger:
    def exception(self, *_args, **_kwargs) -> None:
        return None

    def warning(self, *_args, **_kwargs) -> None:
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
        requested_at=datetime(2026, 3, 23, 18, 0, tzinfo=UTC),
    )


@pytest.mark.asyncio
async def test_migration_job_worker_completes_queued_job():
    repository = InMemoryMigrationJobRepository()
    await repository.enqueue(build_job())
    audit_repository = StubAuditRepository()
    service = StubBotControlService()
    worker = MigrationJobWorker(
        migration_job_repository=repository,
        bot_control_service=service,
        audit_repository=audit_repository,
        logger=StubLogger(),
        concurrency=1,
        poll_interval_seconds=0.01,
        lease_duration_seconds=30.0,
        heartbeat_interval_seconds=0.01,
    )

    await worker.start()
    await asyncio.sleep(0.05)
    await worker.stop()

    assert service.executed_jobs == ["job-1"]
    active_jobs = await repository.list_active("migration-1")
    assert active_jobs == []
    assert audit_repository.events[-1].event_type == "bot_migration_job_completed"
    assert audit_repository.events[-1].severity is AuditSeverity.INFO


@pytest.mark.asyncio
async def test_migration_job_worker_marks_failed_job():
    repository = InMemoryMigrationJobRepository()
    await repository.enqueue(build_job(job_key="job-2"))
    audit_repository = StubAuditRepository()
    service = StubBotControlService(should_fail=True)
    worker = MigrationJobWorker(
        migration_job_repository=repository,
        bot_control_service=service,
        audit_repository=audit_repository,
        logger=StubLogger(),
        concurrency=1,
        poll_interval_seconds=0.01,
        lease_duration_seconds=30.0,
        heartbeat_interval_seconds=0.01,
    )

    await worker.start()
    await asyncio.sleep(0.05)
    await worker.stop()

    assert service.executed_jobs == ["job-2"]
    active_jobs = await repository.list_active("migration-1")
    assert active_jobs == []
    assert audit_repository.events[-1].event_type == "bot_migration_job_failed"
    assert audit_repository.events[-1].severity is AuditSeverity.ERROR


@pytest.mark.asyncio
async def test_migration_job_worker_cancels_running_job_on_cancel_request():
    repository = InMemoryMigrationJobRepository()
    await repository.enqueue(build_job(job_key="job-3"))
    audit_repository = StubAuditRepository()
    service = BlockingBotControlService()
    worker = MigrationJobWorker(
        migration_job_repository=repository,
        bot_control_service=service,
        audit_repository=audit_repository,
        logger=StubLogger(),
        concurrency=1,
        poll_interval_seconds=0.01,
        lease_duration_seconds=30.0,
        heartbeat_interval_seconds=0.01,
    )

    await worker.start()
    await asyncio.wait_for(service.started.wait(), timeout=1)
    await repository.request_cancel(job_key="job-3", operator_huid="operator-1")
    await asyncio.wait_for(service.cancelled.wait(), timeout=2)
    await asyncio.sleep(0.05)
    await worker.stop()

    active_jobs = await repository.list_active("migration-1")
    job_record = await repository.get("job-3")
    assert service.executed_jobs == ["job-3"]
    assert active_jobs == []
    assert job_record is not None
    assert job_record.status is MigrationJobStatus.FAILED
    assert job_record.last_error_code == "cancelled_by_operator"
    assert audit_repository.events[-1].event_type == "bot_migration_job_cancelled"
    assert audit_repository.events[-1].severity is AuditSeverity.WARNING
