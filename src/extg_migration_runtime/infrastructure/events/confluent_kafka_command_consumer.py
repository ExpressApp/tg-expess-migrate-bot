from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime
from typing import Any

from confluent_kafka import Consumer, TopicPartition
from extg_shared.contracts.events import integration_envelope_from_json_bytes

from extg_migration_runtime.application.migration_command_handler import (
    MigrationCommandHandler,
)
from extg_shared.contracts.ports import ServiceWatermarkRepository
from extg_shared.contracts.models import ServiceWatermarkRecord


class ConfluentKafkaMigrationCommandConsumer:
    def __init__(
        self,
        *,
        bootstrap_servers: list[str],
        topic: str,
        consumer_group: str,
        consumer_client_id: str,
        handler: MigrationCommandHandler,
        service_watermark_repository: ServiceWatermarkRepository | None,
        logger: Any,
        enabled: bool = True,
        poll_timeout_seconds: float = 1.0,
        consumer_name: str = "migration-command-consumer",
        consumer: Consumer | None = None,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
    ) -> None:
        self._topic = topic
        self._handler = handler
        self._service_watermark_repository = service_watermark_repository
        self._logger = logger
        self._enabled = enabled
        self._poll_timeout_seconds = max(0.1, poll_timeout_seconds)
        self._sleep = sleep
        self._bootstrap_servers = bootstrap_servers
        self._consumer_group = consumer_group
        self._consumer_client_id = consumer_client_id
        self._consumer_name = consumer_name
        self._consumer = consumer
        self._stop_event = asyncio.Event()
        self._supervisor_task: asyncio.Task[None] | None = None
        self._subscribed = False

    async def start(self) -> None:
        if not self._enabled or self._supervisor_task is not None:
            return
        self._stop_event.clear()
        if self._consumer is None:
            self._consumer = Consumer(
                {
                    "bootstrap.servers": ",".join(self._bootstrap_servers),
                    "group.id": self._consumer_group,
                    "client.id": self._consumer_client_id,
                    "enable.auto.commit": False,
                    "auto.offset.reset": "earliest",
                },
            )
        await self._ensure_subscribed()
        self._supervisor_task = asyncio.create_task(
            self._run(),
            name="migration-command-consumer",
        )

    async def stop(self) -> None:
        self._stop_event.set()
        supervisor_task = self._supervisor_task
        self._supervisor_task = None
        if supervisor_task is not None:
            await asyncio.gather(supervisor_task, return_exceptions=True)
        await self.close()

    async def close(self) -> None:
        if self._consumer is None:
            return
        await asyncio.to_thread(self._consumer.close)

    async def _ensure_subscribed(self) -> None:
        if self._subscribed:
            return
        assert self._consumer is not None
        await asyncio.to_thread(self._consumer.subscribe, [self._topic])
        self._subscribed = True

    async def _run(self) -> None:
        assert self._consumer is not None
        while not self._stop_event.is_set():
            message = await asyncio.to_thread(self._consumer.poll, self._poll_timeout_seconds)
            if message is None:
                await self._sleep(0)
                continue
            error = message.error()
            if error is not None:
                self._logger.warning(
                    "kafka consumer returned message error",
                    topic=self._topic,
                    error=str(error),
                )
                continue

            try:
                envelope = integration_envelope_from_json_bytes(message.value())
            except Exception:
                self._logger.exception(
                    "kafka migration command envelope parsing failed",
                    topic=message.topic(),
                    partition=message.partition(),
                    offset=message.offset(),
                )
                result_code = "invalid_envelope"
                await asyncio.to_thread(
                    self._consumer.commit,
                    message=message,
                    asynchronous=False,
                )
                self._logger.info(
                    "kafka migration command committed",
                    topic=message.topic(),
                    partition=message.partition(),
                    offset=message.offset(),
                    result_code=result_code,
                )
                continue

            try:
                result_code = await self._handler.handle(envelope)
            except Exception:
                self._logger.exception(
                    "kafka migration command handling failed",
                    topic=message.topic(),
                    partition=message.partition(),
                    offset=message.offset(),
                )
                await asyncio.to_thread(
                    self._consumer.seek,
                    TopicPartition(message.topic(), message.partition(), message.offset()),
                )
                await self._sleep(1.0)
                continue
            await asyncio.to_thread(
                self._consumer.commit,
                message=message,
                asynchronous=False,
            )
            await self._update_watermark(message.topic(), message.partition(), message.offset())
            self._logger.info(
                "kafka migration command committed",
                topic=message.topic(),
                partition=message.partition(),
                offset=message.offset(),
                result_code=result_code,
            )

    async def _update_watermark(self, topic: str, partition: int, offset: int) -> None:
        if self._service_watermark_repository is None:
            return
        await self._service_watermark_repository.upsert(
            ServiceWatermarkRecord(
                consumer_name=self._consumer_name,
                logical_stream_key=f"{topic}:{partition}",
                watermark_value=str(offset),
                updated_at=datetime.now(tz=UTC),
            ),
        )
