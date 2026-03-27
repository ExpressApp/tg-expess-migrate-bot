from __future__ import annotations

import asyncio
from collections import defaultdict
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import UTC
from typing import Any

from extg_shared.contracts.ports import (
    MigrationJobRepository,
    OutboxRepository,
    ServiceWatermarkRepository,
)
from extg_shared.contracts.models import IntegrationOutboxStatus, MigrationJobStatus


@dataclass(frozen=True, slots=True)
class WorkerRuntimeMetricsSnapshot:
    job_counts: dict[str, int]
    outbox_counts: dict[str, int]
    watermark_consumer_count: int
    watermark_stream_count: int
    watermark_max_updated_at: str | None


class WorkerRuntimeMetricsReporter:
    def __init__(
        self,
        *,
        migration_job_repository: MigrationJobRepository,
        outbox_repository: OutboxRepository,
        service_watermark_repository: ServiceWatermarkRepository,
        logger: Any,
        enabled: bool = True,
        poll_interval_seconds: float = 30.0,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
    ) -> None:
        self._migration_job_repository = migration_job_repository
        self._outbox_repository = outbox_repository
        self._service_watermark_repository = service_watermark_repository
        self._logger = logger
        self._enabled = enabled
        self._poll_interval_seconds = max(1.0, poll_interval_seconds)
        self._sleep = sleep
        self._stop_event = asyncio.Event()
        self._supervisor_task: asyncio.Task[None] | None = None

    async def start(self) -> None:
        if not self._enabled or self._supervisor_task is not None:
            return
        self._stop_event.clear()
        self._supervisor_task = asyncio.create_task(
            self._run(),
            name="worker-runtime-metrics",
        )

    async def stop(self) -> None:
        self._stop_event.set()
        supervisor_task = self._supervisor_task
        self._supervisor_task = None
        if supervisor_task is not None:
            await asyncio.gather(supervisor_task, return_exceptions=True)

    async def snapshot(self) -> WorkerRuntimeMetricsSnapshot:
        job_counts = await self._migration_job_repository.count_by_status()
        outbox_counts = await self._outbox_repository.count_by_status()
        watermarks = await self._service_watermark_repository.list_all()
        grouped = defaultdict(list)
        for watermark in watermarks:
            grouped[watermark.consumer_name].append(watermark)
        max_updated_at = None
        if watermarks:
            max_updated_at = max(
                watermark.updated_at for watermark in watermarks
            ).astimezone(UTC).isoformat().replace("+00:00", "Z")
        return WorkerRuntimeMetricsSnapshot(
            job_counts={
                status.value: job_counts.get(status, 0)
                for status in MigrationJobStatus
            },
            outbox_counts={
                status.value: outbox_counts.get(status, 0)
                for status in IntegrationOutboxStatus
            },
            watermark_consumer_count=len(grouped),
            watermark_stream_count=len(watermarks),
            watermark_max_updated_at=max_updated_at,
        )

    async def _run(self) -> None:
        while not self._stop_event.is_set():
            snapshot = await self.snapshot()
            self._logger.info(
                "worker runtime metrics snapshot",
                job_counts=snapshot.job_counts,
                outbox_counts=snapshot.outbox_counts,
                watermark_consumer_count=snapshot.watermark_consumer_count,
                watermark_stream_count=snapshot.watermark_stream_count,
                watermark_max_updated_at=snapshot.watermark_max_updated_at,
            )
            await self._sleep(self._poll_interval_seconds)
