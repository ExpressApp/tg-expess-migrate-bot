from datetime import UTC, datetime
import json

import pytest

from extg_shared.contracts.models import IntegrationOutboxEventRecord
from extg_migration_runtime.infrastructure.events.confluent_kafka_event_publisher import (
    ConfluentKafkaIntegrationEventPublisher,
)


class StubMessage:
    def topic(self) -> str:
        return "extg.migration.command.v1"

    def partition(self) -> int:
        return 0

    def offset(self) -> int:
        return 42


class StubProducer:
    def __init__(self) -> None:
        self.calls: list[tuple[str, bytes | None, bytes]] = []

    def produce(self, *, topic, key, value, on_delivery) -> None:
        self.calls.append((topic, key, value))
        on_delivery(None, StubMessage())

    def poll(self, _timeout: float) -> None:
        return None

    def flush(self, _timeout: float) -> int:
        return 0


class StubLogger:
    def info(self, *_args, **_kwargs) -> None:
        return None


@pytest.mark.asyncio
async def test_confluent_kafka_event_publisher_serializes_outbox_event() -> None:
    producer = StubProducer()
    publisher = ConfluentKafkaIntegrationEventPublisher(
        bootstrap_servers=["kafka:9092"],
        command_topic="extg.migration.command.v1",
        producer_client_id="producer-1",
        logger=StubLogger(),
        producer=producer,
    )
    event = IntegrationOutboxEventRecord(
        event_id="event-1",
        aggregate_type="migration_job",
        aggregate_id="job-1",
        event_type="migration.job.requested",
        payload_json={
            "job_key": "job-1",
            "migration_id": "migration-1",
            "source_chat_ids": ["chat-1"],
        },
        headers_json={"schema_version": 1},
        created_at=datetime(2026, 3, 24, 12, 0, tzinfo=UTC),
    )

    published = await publisher.publish(event)

    assert published.broker_topic == "extg.migration.command.v1"
    assert producer.calls[0][0] == "extg.migration.command.v1"
    assert producer.calls[0][1] == b"migration-1:chat-1"
    payload = json.loads(producer.calls[0][2].decode("utf-8"))
    assert payload["message_id"] == "event-1"
    assert payload["message_type"] == "migration.job.requested"
    assert payload["payload"]["job_key"] == "job-1"
