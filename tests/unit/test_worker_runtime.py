from __future__ import annotations

import asyncio
from types import SimpleNamespace

import pytest

from extg_migration_runtime.runtime.worker import run_worker
from extg_shared.contracts.errors import ConfigurationError


class StubWorker:
    def __init__(self) -> None:
        self.started = False
        self.stopped = False

    async def start(self) -> None:
        self.started = True

    async def stop(self) -> None:
        self.stopped = True


class StubPublisher:
    def __init__(self) -> None:
        self.started = False
        self.stopped = False

    async def start(self) -> None:
        self.started = True

    async def stop(self) -> None:
        self.stopped = True


class StubConsumer:
    def __init__(self) -> None:
        self.started = False
        self.stopped = False

    async def start(self) -> None:
        self.started = True

    async def stop(self) -> None:
        self.stopped = True


class StubMetricsReporter:
    def __init__(self) -> None:
        self.started = False
        self.stopped = False

    async def start(self) -> None:
        self.started = True

    async def stop(self) -> None:
        self.stopped = True


class StubLogger:
    def __init__(self) -> None:
        self.records: list[tuple[str, dict[str, object]]] = []

    def info(self, message: str, **kwargs) -> None:
        self.records.append((message, kwargs))


class StubClosable:
    def __init__(self) -> None:
        self.closed = False

    async def close(self) -> None:
        self.closed = True


class StubProvider:
    def __init__(self, instance) -> None:
        self._instance = instance

    def __call__(self):
        return self._instance


class StubContainer:
    def __init__(self, *, worker_enabled: bool = True, kafka_enabled: bool = False) -> None:
        self._worker = StubWorker()
        self._publisher = StubPublisher()
        self._consumer = StubConsumer()
        self._metrics_reporter = StubMetricsReporter()
        self._logger = StubLogger()
        self._telegram_gateway = StubClosable()
        self._express_gateway = StubClosable()
        self._database = StubClosable()
        self.migration_job_worker = StubProvider(self._worker)
        self.integration_outbox_publisher = StubProvider(self._publisher)
        self.migration_command_consumer = StubProvider(self._consumer)
        self.worker_runtime_metrics_reporter = StubProvider(self._metrics_reporter)
        self.telegram_gateway = StubProvider(self._telegram_gateway)
        self.express_gateway = StubProvider(self._express_gateway)
        self.database = StubProvider(self._database)
        self._settings = SimpleNamespace(
            worker=SimpleNamespace(
                enabled=worker_enabled,
                concurrency=2,
                poll_interval_seconds=1.0,
                lease_duration_seconds=30.0,
                heartbeat_interval_seconds=10.0,
                backpressure_max_queued_jobs=1000,
                backpressure_max_running_jobs=100,
                runtime_metrics_enabled=True,
                runtime_metrics_poll_interval_seconds=30.0,
                outbox_publisher_enabled=True,
                outbox_publisher_batch_size=100,
                outbox_publisher_poll_interval_seconds=1.0,
                outbox_publisher_lease_duration_seconds=30.0,
            ),
            kafka=SimpleNamespace(enabled=kafka_enabled),
        )

    def settings(self):
        return self._settings

    def logger(self):
        return self._logger

    def persistence_backend(self) -> str:
        return "postgres"


@pytest.mark.asyncio
async def test_run_worker_starts_and_stops_runtime() -> None:
    container = StubContainer()
    stop_event = asyncio.Event()
    task = asyncio.create_task(run_worker(container, stop_event=stop_event))

    await asyncio.sleep(0.01)
    stop_event.set()
    await task

    assert container._worker.started is True
    assert container._worker.stopped is True
    assert container._publisher.started is True
    assert container._publisher.stopped is True
    assert container._metrics_reporter.started is True
    assert container._metrics_reporter.stopped is True
    assert container._consumer.started is False
    assert container._telegram_gateway.closed is True
    assert container._express_gateway.closed is True
    assert container._database.closed is True
    assert container._logger.records[0][0] == "migration worker runtime starting"
    assert container._logger.records[-1][0] == "migration worker runtime stopping"


@pytest.mark.asyncio
async def test_run_worker_fails_fast_when_disabled() -> None:
    container = StubContainer(worker_enabled=False)

    with pytest.raises(ConfigurationError):
        await run_worker(container, stop_event=asyncio.Event())

    assert container._worker.started is False


@pytest.mark.asyncio
async def test_run_worker_uses_kafka_consumer_when_enabled() -> None:
    container = StubContainer(kafka_enabled=True)
    stop_event = asyncio.Event()
    task = asyncio.create_task(run_worker(container, stop_event=stop_event))

    await asyncio.sleep(0.01)
    stop_event.set()
    await task

    assert container._worker.started is False
    assert container._consumer.started is True
    assert container._consumer.stopped is True
    assert container._publisher.started is True
    assert container._metrics_reporter.started is True
    assert container._metrics_reporter.stopped is True
