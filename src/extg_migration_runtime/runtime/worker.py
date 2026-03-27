from __future__ import annotations

import asyncio
import signal
from contextlib import suppress

from extg_migration_runtime.bootstrap.container import (
    MigrationRuntimeContainer,
    create_container as create_worker_container,
)
from extg_shared.contracts.errors import ConfigurationError
from extg_shared.utils.runtime import close_runtime_resources


async def run_worker(
    container: MigrationRuntimeContainer | None = None,
    *,
    stop_event: asyncio.Event | None = None,
) -> None:
    container = container or create_worker_container()
    settings = container.settings()
    if not settings.worker.enabled:
        raise ConfigurationError(
            "worker runtime is disabled; set EXTG_WORKER__ENABLED=true before starting extg-worker",
        )

    worker = container.migration_job_worker()
    command_consumer = container.migration_command_consumer()
    outbox_publisher = container.integration_outbox_publisher()
    metrics_reporter = container.worker_runtime_metrics_reporter()
    janitor_provider = getattr(container, "attachment_temp_file_janitor", None)
    attachment_temp_file_janitor = (
        janitor_provider()
        if janitor_provider is not None
        else None
    )
    archive_janitor_provider = getattr(container, "archive_import_temp_file_janitor", None)
    archive_import_temp_file_janitor = (
        archive_janitor_provider()
        if archive_janitor_provider is not None
        else None
    )
    logger = container.logger()
    runtime_stop_event = stop_event or asyncio.Event()
    owns_signal_handlers = stop_event is None
    loop = asyncio.get_running_loop()

    if owns_signal_handlers:
        for sig in (signal.SIGINT, signal.SIGTERM):
            with suppress(NotImplementedError):
                loop.add_signal_handler(sig, runtime_stop_event.set)

    logger.info(
        "migration worker runtime starting",
        kafka_enabled=settings.kafka.enabled,
        concurrency=settings.worker.concurrency,
        poll_interval_seconds=settings.worker.poll_interval_seconds,
        lease_duration_seconds=settings.worker.lease_duration_seconds,
        heartbeat_interval_seconds=settings.worker.heartbeat_interval_seconds,
        backpressure_max_queued_jobs=settings.worker.backpressure_max_queued_jobs,
        backpressure_max_running_jobs=settings.worker.backpressure_max_running_jobs,
        outbox_publisher_enabled=settings.worker.outbox_publisher_enabled,
        outbox_publisher_batch_size=settings.worker.outbox_publisher_batch_size,
        outbox_publisher_poll_interval_seconds=settings.worker.outbox_publisher_poll_interval_seconds,
        outbox_publisher_lease_duration_seconds=settings.worker.outbox_publisher_lease_duration_seconds,
        runtime_metrics_enabled=settings.worker.runtime_metrics_enabled,
        runtime_metrics_poll_interval_seconds=settings.worker.runtime_metrics_poll_interval_seconds,
    )
    if attachment_temp_file_janitor is not None:
        await attachment_temp_file_janitor.start()
    if archive_import_temp_file_janitor is not None:
        await archive_import_temp_file_janitor.start()
    await outbox_publisher.start()
    await metrics_reporter.start()
    if settings.kafka.enabled:
        await command_consumer.start()
    else:
        await worker.start()
    try:
        await runtime_stop_event.wait()
    finally:
        logger.info("migration worker runtime stopping")
        if settings.kafka.enabled:
            await command_consumer.stop()
        else:
            await worker.stop()
        if attachment_temp_file_janitor is not None:
            await attachment_temp_file_janitor.stop()
        if archive_import_temp_file_janitor is not None:
            await archive_import_temp_file_janitor.stop()
        await outbox_publisher.stop()
        await metrics_reporter.stop()
        await close_runtime_resources(container)


def main() -> None:
    try:
        asyncio.run(run_worker())
    except KeyboardInterrupt:
        return
