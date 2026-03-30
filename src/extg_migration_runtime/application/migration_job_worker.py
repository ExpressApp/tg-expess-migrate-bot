from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime
from typing import Any
from uuid import uuid4

from extg_bot_ui.application.bot_control import MigrationBotControlService
from extg_shared.contracts.ports import AuditRepository, MigrationJobRepository
from extg_shared.contracts.models import (
    AuditEvent,
    AuditSeverity,
    MIGRATION_JOB_CANCELLED_ERROR_CODE,
    MIGRATION_JOB_CANCEL_REQUESTED_ERROR_CODE,
    MigrationJobRecord,
)


class MigrationJobWorker:
    def __init__(
        self,
        *,
        migration_job_repository: MigrationJobRepository,
        bot_control_service: MigrationBotControlService,
        audit_repository: AuditRepository,
        logger: Any,
        enabled: bool = True,
        concurrency: int = 2,
        poll_interval_seconds: float = 1.0,
        lease_duration_seconds: float = 30.0,
        heartbeat_interval_seconds: float = 10.0,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
    ) -> None:
        self._migration_job_repository = migration_job_repository
        self._bot_control_service = bot_control_service
        self._audit_repository = audit_repository
        self._logger = logger
        self._enabled = enabled
        self._concurrency = max(1, concurrency)
        self._poll_interval_seconds = max(0.1, poll_interval_seconds)
        self._lease_duration_seconds = max(5.0, lease_duration_seconds)
        self._heartbeat_interval_seconds = max(1.0, heartbeat_interval_seconds)
        self._sleep = sleep
        self._worker_id = uuid4().hex
        self._stop_event = asyncio.Event()
        self._supervisor_task: asyncio.Task[None] | None = None
        self._job_tasks: set[asyncio.Task[None]] = set()

    async def start(self) -> None:
        if not self._enabled or self._supervisor_task is not None:
            return
        self._stop_event.clear()
        self._supervisor_task = asyncio.create_task(
            self._run(),
            name=f"migration-job-worker:{self._worker_id}",
        )

    async def stop(self, *, graceful_timeout_seconds: float = 1.0) -> None:
        self._stop_event.set()
        supervisor_task = self._supervisor_task
        self._supervisor_task = None
        if supervisor_task is not None:
            await asyncio.gather(supervisor_task, return_exceptions=True)

        if not self._job_tasks:
            return
        done, pending = await asyncio.wait(
            self._job_tasks,
            timeout=graceful_timeout_seconds,
        )
        for task in pending:
            task.cancel()
        if pending:
            await asyncio.gather(*pending, return_exceptions=True)
        self._job_tasks.difference_update(done)
        self._job_tasks.difference_update(pending)

    async def _run(self) -> None:
        while not self._stop_event.is_set():
            acquired_any = False
            while len(self._job_tasks) < self._concurrency and not self._stop_event.is_set():
                job = await self._migration_job_repository.acquire_next(
                    worker_id=self._worker_id,
                    lease_duration_seconds=self._lease_duration_seconds,
                )
                if job is None:
                    break
                acquired_any = True
                task = asyncio.create_task(
                    self.execute_claimed_job(job),
                    name=f"migration-job:{job.job_key}",
                )
                self._job_tasks.add(task)
                task.add_done_callback(self._job_tasks.discard)

            if self._stop_event.is_set():
                break
            if self._job_tasks:
                await asyncio.wait(
                    self._job_tasks,
                    timeout=0 if acquired_any else self._poll_interval_seconds,
                    return_when=asyncio.FIRST_COMPLETED,
                )
                continue
            await self._sleep(self._poll_interval_seconds)

    async def execute_claimed_job(self, job: MigrationJobRecord) -> None:
        # `worker_id` in the job record is the current lease owner token.
        # Reuse it for heartbeat and terminal updates even when the executor
        # differs from the component that originally claimed the job.
        lease_owner_id = job.worker_id or self._worker_id
        cancel_requested = asyncio.Event()
        current_task = asyncio.current_task()
        assert current_task is not None
        heartbeat_task = asyncio.create_task(
            self._heartbeat_loop(
                job.job_key,
                lease_owner_id=lease_owner_id,
                cancel_requested=cancel_requested,
                task_to_cancel=current_task,
            ),
            name=f"migration-job-heartbeat:{job.job_key}",
        )
        try:
            await self._bot_control_service.execute_migration_job(job)
        except asyncio.CancelledError:
            if cancel_requested.is_set():
                await self._migration_job_repository.mark_failed(
                    job_key=job.job_key,
                    worker_id=lease_owner_id,
                    error_code=MIGRATION_JOB_CANCELLED_ERROR_CODE,
                    error_payload={"requested_by": job.operator_huid},
                )
                await self._audit_repository.add(
                    AuditEvent(
                        migration_id=job.migration_id,
                        event_type="bot_migration_job_cancelled",
                        severity=AuditSeverity.WARNING,
                        payload_json={
                            "job_key": job.job_key,
                            "operation": job.operation,
                            "source_chat_ids": list(job.source_chat_ids),
                            "requested_by": job.operator_huid,
                        },
                        created_at=self._now(),
                    ),
                )
                return
            self._logger.warning(
                "migration job execution cancelled",
                worker_id=self._worker_id,
                job_key=job.job_key,
                migration_id=job.migration_id,
                operation=job.operation,
            )
            raise
        except Exception as error:
            self._logger.exception(
                "migration job execution failed",
                worker_id=self._worker_id,
                job_key=job.job_key,
                migration_id=job.migration_id,
                operation=job.operation,
            )
            await self._migration_job_repository.mark_failed(
                job_key=job.job_key,
                worker_id=lease_owner_id,
                error_code=type(error).__name__,
                error_payload={"error": str(error)},
            )
            await self._audit_repository.add(
                AuditEvent(
                    migration_id=job.migration_id,
                    event_type="bot_migration_job_failed",
                    severity=AuditSeverity.ERROR,
                    payload_json={
                        "job_key": job.job_key,
                        "operation": job.operation,
                        "source_chat_ids": list(job.source_chat_ids),
                        "error": str(error),
                    },
                    created_at=self._now(),
                ),
            )
            return
        finally:
            heartbeat_task.cancel()
            await asyncio.gather(heartbeat_task, return_exceptions=True)

        await self._migration_job_repository.mark_completed(
            job_key=job.job_key,
            worker_id=lease_owner_id,
        )
        await self._audit_repository.add(
            AuditEvent(
                migration_id=job.migration_id,
                event_type="bot_migration_job_completed",
                severity=AuditSeverity.INFO,
                payload_json={
                    "job_key": job.job_key,
                    "operation": job.operation,
                    "source_chat_ids": list(job.source_chat_ids),
                },
                created_at=self._now(),
            ),
        )

    async def _heartbeat_loop(
        self,
        job_key: str,
        *,
        lease_owner_id: str,
        cancel_requested: asyncio.Event,
        task_to_cancel: asyncio.Task[None],
    ) -> None:
        while not self._stop_event.is_set():
            await self._sleep(self._heartbeat_interval_seconds)
            current = await self._migration_job_repository.get(job_key)
            if current is None:
                return
            if current.last_error_code == MIGRATION_JOB_CANCEL_REQUESTED_ERROR_CODE:
                self._logger.warning(
                    "migration job cancellation requested",
                    worker_id=self._worker_id,
                    job_key=job_key,
                )
                cancel_requested.set()
                task_to_cancel.cancel()
                return
            updated = await self._migration_job_repository.heartbeat(
                job_key=job_key,
                worker_id=lease_owner_id,
                lease_duration_seconds=self._lease_duration_seconds,
            )
            if not updated:
                self._logger.warning(
                    "migration job heartbeat lost lease",
                    worker_id=self._worker_id,
                    lease_owner_id=lease_owner_id,
                    job_key=job_key,
                )
                return

    def _now(self) -> datetime:
        return datetime.now(tz=UTC)
