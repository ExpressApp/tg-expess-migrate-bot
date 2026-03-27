from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from typing import Any

from confluent_kafka import Producer
from extg_shared.contracts.events import (
    integration_envelope_from_outbox_event,
    integration_envelope_to_json_bytes,
    integration_partition_key_for_outbox_event,
)

from extg_shared.contracts.models import IntegrationOutboxEventRecord, PublishedIntegrationEvent


class ConfluentKafkaIntegrationEventPublisher:
    def __init__(
        self,
        *,
        bootstrap_servers: list[str],
        command_topic: str,
        producer_client_id: str,
        producer_flush_timeout_seconds: float = 10.0,
        logger: Any,
        producer: Producer | None = None,
    ) -> None:
        self._command_topic = command_topic
        self._producer_flush_timeout_seconds = max(1.0, producer_flush_timeout_seconds)
        self._logger = logger
        self._producer = producer or Producer(
            {
                "bootstrap.servers": ",".join(bootstrap_servers),
                "client.id": producer_client_id,
                "enable.idempotence": True,
                "acks": "all",
            },
        )

    async def publish(
        self,
        event: IntegrationOutboxEventRecord,
    ) -> PublishedIntegrationEvent:
        envelope = integration_envelope_from_outbox_event(event)
        topic = self._resolve_topic(event)
        key = integration_partition_key_for_outbox_event(event)
        payload = integration_envelope_to_json_bytes(envelope)
        loop = asyncio.get_running_loop()
        future: asyncio.Future[PublishedIntegrationEvent] = loop.create_future()

        def on_delivery(error, message) -> None:
            if error is not None:
                loop.call_soon_threadsafe(
                    future.set_exception,
                    RuntimeError(str(error)),
                )
                return
            published = PublishedIntegrationEvent(
                broker_topic=message.topic(),
                broker_partition=message.partition(),
                broker_offset=message.offset(),
                published_at=datetime.now(tz=UTC),
            )
            loop.call_soon_threadsafe(future.set_result, published)

        def publish_sync() -> None:
            self._producer.produce(
                topic=topic,
                key=key.encode("utf-8") if key else None,
                value=payload,
                on_delivery=on_delivery,
            )
            self._producer.poll(0)
            remaining = self._producer.flush(self._producer_flush_timeout_seconds)
            if remaining > 0 and not future.done():
                loop.call_soon_threadsafe(
                    future.set_exception,
                    TimeoutError(
                        f"kafka producer flush timed out with {remaining} undelivered message(s)",
                    ),
                )

        await asyncio.to_thread(publish_sync)
        return await future

    async def close(self) -> None:
        await asyncio.to_thread(self._producer.flush, self._producer_flush_timeout_seconds)

    def _resolve_topic(self, event: IntegrationOutboxEventRecord) -> str:
        topic = event.headers_json.get("kafka_topic")
        if isinstance(topic, str) and topic.strip():
            return topic.strip()
        if event.event_type == "migration.job.requested":
            return self._command_topic
        return self._command_topic
