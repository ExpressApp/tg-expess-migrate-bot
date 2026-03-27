from __future__ import annotations

from datetime import UTC, datetime

import pytest

from extg_migration_runtime.application.worker_runtime_metrics import WorkerRuntimeMetricsReporter
from extg_shared.contracts.models import (
    IntegrationOutboxEventRecord,
    MigrationJobRecord,
    MigrationJobStatus,
    ServiceWatermarkRecord,
)
from extg_migration_runtime.infrastructure.persistence.in_memory import (
    InMemoryMigrationJobRepository,
    InMemoryOutboxRepository,
    InMemoryServiceWatermarkRepository,
)


class StubLogger:
    def __init__(self) -> None:
        self.records: list[tuple[str, dict[str, object]]] = []

    def info(self, message: str, **kwargs) -> None:
        self.records.append((message, kwargs))


@pytest.mark.asyncio
async def test_worker_runtime_metrics_reporter_snapshot_counts_runtime_state() -> None:
    jobs = InMemoryMigrationJobRepository()
    outbox = InMemoryOutboxRepository()
    watermarks = InMemoryServiceWatermarkRepository()
    await jobs.enqueue(
        MigrationJobRecord(
            job_key="job-1",
            migration_id="migration-1",
            operation="migrate_chat",
            operator_huid="operator-1",
            source_chat_ids=("chat-1",),
            batch_size=10,
            status=MigrationJobStatus.QUEUED,
            requested_at=datetime(2026, 3, 24, 12, 0, tzinfo=UTC),
        ),
    )
    await outbox.append_many(
        (
            IntegrationOutboxEventRecord(
                event_id="event-1",
                aggregate_type="migration_job",
                aggregate_id="job-1",
                event_type="migration.job.requested",
                payload_json={"job_key": "job-1"},
                headers_json={},
                created_at=datetime(2026, 3, 24, 12, 0, tzinfo=UTC),
            ),
        ),
    )
    await watermarks.upsert(
        ServiceWatermarkRecord(
            consumer_name="migration-command-consumer",
            logical_stream_key="extg.migration.command.v1:0",
            watermark_value="42",
            updated_at=datetime(2026, 3, 24, 12, 1, tzinfo=UTC),
        ),
    )
    reporter = WorkerRuntimeMetricsReporter(
        migration_job_repository=jobs,
        outbox_repository=outbox,
        service_watermark_repository=watermarks,
        logger=StubLogger(),
        enabled=True,
        poll_interval_seconds=30.0,
    )

    snapshot = await reporter.snapshot()

    assert snapshot.job_counts["queued"] == 1
    assert snapshot.outbox_counts["pending"] == 1
    assert snapshot.watermark_consumer_count == 1
    assert snapshot.watermark_stream_count == 1
    assert snapshot.watermark_max_updated_at == "2026-03-24T12:01:00Z"
