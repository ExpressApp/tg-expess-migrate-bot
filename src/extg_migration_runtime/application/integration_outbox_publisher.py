from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from datetime import UTC
from typing import Any
from uuid import uuid4

from extg_shared.contracts.ports import (
    IntegrationEventPublisher,
    OutboxRepository,
    ServiceWatermarkRepository,
)
from extg_shared.contracts.models import PublishedIntegrationEvent, ServiceWatermarkRecord


class IntegrationOutboxPublisher:
    def __init__(
        self,
        *,
        outbox_repository: OutboxRepository,
        event_publisher: IntegrationEventPublisher,
        service_watermark_repository: ServiceWatermarkRepository | None,
        logger: Any,
        enabled: bool = True,
        batch_size: int = 50,
        poll_interval_seconds: float = 1.0,
        lease_duration_seconds: float = 30.0,
        publisher_name: str = "integration-outbox-publisher",
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
    ) -> None:
        self._outbox_repository = outbox_repository
        self._event_publisher = event_publisher
        self._service_watermark_repository = service_watermark_repository
        self._logger = logger
        self._enabled = enabled
        self._batch_size = max(1, batch_size)
        self._poll_interval_seconds = max(0.1, poll_interval_seconds)
        self._lease_duration_seconds = max(5.0, lease_duration_seconds)
        self._publisher_name = publisher_name
        self._sleep = sleep
        self._publisher_id = uuid4().hex
        self._stop_event = asyncio.Event()
        self._supervisor_task: asyncio.Task[None] | None = None

    async def start(self) -> None:
        if not self._enabled or self._supervisor_task is not None:
            return
        self._stop_event.clear()
        self._supervisor_task = asyncio.create_task(
            self._run(),
            name=f"integration-outbox-publisher:{self._publisher_id}",
        )

    async def stop(self) -> None:
        self._stop_event.set()
        supervisor_task = self._supervisor_task
        self._supervisor_task = None
        if supervisor_task is not None:
            await asyncio.gather(supervisor_task, return_exceptions=True)

    async def _run(self) -> None:
        while not self._stop_event.is_set():
            events = await self._outbox_repository.lease_batch(
                publisher_id=self._publisher_id,
                limit=self._batch_size,
                lease_duration_seconds=self._lease_duration_seconds,
            )
            if not events:
                await self._sleep(self._poll_interval_seconds)
                continue
            for event in events:
                if self._stop_event.is_set():
                    return
                await self._publish_one(event)

    async def _publish_one(self, event) -> None:
        try:
            result = await self._event_publisher.publish(event)
        except Exception as error:
            self._logger.exception(
                "integration outbox publish failed",
                event_id=event.event_id,
                aggregate_type=event.aggregate_type,
                aggregate_id=event.aggregate_id,
                event_type=event.event_type,
            )
            await self._outbox_repository.mark_failed(
                event_id=event.event_id,
                publisher_id=self._publisher_id,
                error_code=type(error).__name__,
                error_payload={"error": str(error)},
            )
            return
        await self._outbox_repository.mark_published(
            event_id=event.event_id,
            publisher_id=self._publisher_id,
            result=result,
        )
        await self._update_watermark(result)

    async def _update_watermark(self, result: PublishedIntegrationEvent) -> None:
        if self._service_watermark_repository is None:
            return
        if result.broker_partition is None or result.broker_offset is None:
            return
        await self._service_watermark_repository.upsert(
            ServiceWatermarkRecord(
                consumer_name=self._publisher_name,
                logical_stream_key=f"{result.broker_topic}:{result.broker_partition}",
                watermark_value=str(result.broker_offset),
                updated_at=result.published_at.astimezone(UTC),
            ),
        )
