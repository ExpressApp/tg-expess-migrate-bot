from __future__ import annotations

import asyncio
from datetime import UTC, datetime

import pytest

from extg_migration_runtime.application.integration_outbox_publisher import IntegrationOutboxPublisher
from extg_shared.contracts.models import (
    IntegrationOutboxEventRecord,
    IntegrationOutboxStatus,
    PublishedIntegrationEvent,
)
from extg_migration_runtime.infrastructure.persistence.in_memory import (
    InMemoryOutboxRepository,
    InMemoryServiceWatermarkRepository,
)


class StubLogger:
    def exception(self, *_args, **_kwargs) -> None:
        return None


class SuccessfulPublisher:
    def __init__(self) -> None:
        self.published_event_ids: list[str] = []

    async def publish(
        self,
        event: IntegrationOutboxEventRecord,
    ) -> PublishedIntegrationEvent:
        self.published_event_ids.append(event.event_id)
        return PublishedIntegrationEvent(
            broker_topic="shadow.migration.job.requested",
            broker_partition=0,
            broker_offset=1,
            published_at=datetime(2026, 3, 24, 12, 0, tzinfo=UTC),
        )


class FailingPublisher:
    async def publish(
        self,
        event: IntegrationOutboxEventRecord,
    ) -> PublishedIntegrationEvent:
        raise RuntimeError(f"boom:{event.event_id}")


def build_event(*, event_id: str = "event-1") -> IntegrationOutboxEventRecord:
    return IntegrationOutboxEventRecord(
        event_id=event_id,
        aggregate_type="migration_job",
        aggregate_id="job-1",
        event_type="migration.job.requested",
        payload_json={"job_key": "job-1"},
        headers_json={},
        created_at=datetime(2026, 3, 24, 11, 59, tzinfo=UTC),
    )


@pytest.mark.asyncio
async def test_integration_outbox_publisher_marks_events_published() -> None:
    repository = InMemoryOutboxRepository()
    watermark_repository = InMemoryServiceWatermarkRepository()
    await repository.append_many((build_event(),))
    transport = SuccessfulPublisher()
    publisher = IntegrationOutboxPublisher(
        outbox_repository=repository,
        event_publisher=transport,
        service_watermark_repository=watermark_repository,
        logger=StubLogger(),
        batch_size=10,
        poll_interval_seconds=0.01,
        lease_duration_seconds=30.0,
    )

    await publisher.start()
    await asyncio.sleep(0.05)
    await publisher.stop()

    record = await repository.get("event-1")
    assert transport.published_event_ids == ["event-1"]
    assert record is not None
    assert record.status is IntegrationOutboxStatus.PUBLISHED
    assert record.broker_topic == "shadow.migration.job.requested"
    watermark = await watermark_repository.get(
        consumer_name="integration-outbox-publisher",
        logical_stream_key="shadow.migration.job.requested:0",
    )
    assert watermark is not None
    assert watermark.watermark_value == "1"


@pytest.mark.asyncio
async def test_integration_outbox_publisher_updates_watermark_when_broker_offset_is_available() -> None:
    repository = InMemoryOutboxRepository()
    watermark_repository = InMemoryServiceWatermarkRepository()
    await repository.append_many((build_event(event_id="event-2"),))

    class KafkaLikePublisher(SuccessfulPublisher):
        async def publish(
            self,
            event: IntegrationOutboxEventRecord,
        ) -> PublishedIntegrationEvent:
            published = await super().publish(event)
            return PublishedIntegrationEvent(
                broker_topic="extg.migration.command.v1",
                broker_partition=2,
                broker_offset=42,
                published_at=published.published_at,
            )

    publisher = IntegrationOutboxPublisher(
        outbox_repository=repository,
        event_publisher=KafkaLikePublisher(),
        service_watermark_repository=watermark_repository,
        logger=StubLogger(),
        batch_size=10,
        poll_interval_seconds=0.01,
        lease_duration_seconds=30.0,
    )

    await publisher.start()
    await asyncio.sleep(0.05)
    await publisher.stop()

    watermark = await watermark_repository.get(
        consumer_name="integration-outbox-publisher",
        logical_stream_key="extg.migration.command.v1:2",
    )
    assert watermark is not None
    assert watermark.watermark_value == "42"


@pytest.mark.asyncio
async def test_integration_outbox_publisher_marks_failed_events() -> None:
    repository = InMemoryOutboxRepository()
    watermark_repository = InMemoryServiceWatermarkRepository()
    await repository.append_many((build_event(event_id="event-2"),))
    publisher = IntegrationOutboxPublisher(
        outbox_repository=repository,
        event_publisher=FailingPublisher(),
        service_watermark_repository=watermark_repository,
        logger=StubLogger(),
        batch_size=10,
        poll_interval_seconds=0.01,
        lease_duration_seconds=30.0,
    )

    await publisher.start()
    await asyncio.sleep(0.05)
    await publisher.stop()

    record = await repository.get("event-2")
    assert record is not None
    assert record.status is IntegrationOutboxStatus.FAILED
    assert record.last_error_code == "RuntimeError"
