from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from extg_shared.contracts.models import IntegrationOutboxEventRecord, PublishedIntegrationEvent


class LoggingIntegrationEventPublisher:
    def __init__(self, *, logger: Any, topic_prefix: str = "shadow") -> None:
        self._logger = logger
        self._topic_prefix = topic_prefix.strip() or "shadow"

    async def publish(
        self,
        event: IntegrationOutboxEventRecord,
    ) -> PublishedIntegrationEvent:
        topic = event.headers_json.get("shadow_topic")
        if not isinstance(topic, str) or not topic.strip():
            topic = f"{self._topic_prefix}.{event.event_type}"
        published_at = datetime.now(tz=UTC)
        self._logger.info(
            "integration outbox event published",
            event_id=event.event_id,
            aggregate_type=event.aggregate_type,
            aggregate_id=event.aggregate_id,
            event_type=event.event_type,
            broker_topic=topic,
            published_at=published_at.isoformat(),
        )
        return PublishedIntegrationEvent(
            broker_topic=topic,
            broker_partition=0,
            broker_offset=None,
            published_at=published_at,
        )
