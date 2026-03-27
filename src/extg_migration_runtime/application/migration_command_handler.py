from __future__ import annotations

from datetime import UTC, datetime
from typing import Any
from uuid import uuid4
from extg_shared.contracts.events import IntegrationEnvelope

from extg_migration_runtime.application.migration_job_worker import MigrationJobWorker
from extg_shared.contracts.ports import InboxRepository, MigrationJobRepository
from extg_shared.contracts.models import ConsumerInboxRecord, MigrationJobStatus


class MigrationCommandHandler:
    def __init__(
        self,
        *,
        inbox_repository: InboxRepository,
        migration_job_repository: MigrationJobRepository,
        migration_job_worker: MigrationJobWorker,
        logger: Any,
        consumer_name: str = "migration-command-consumer",
        lease_duration_seconds: float = 30.0,
    ) -> None:
        self._inbox_repository = inbox_repository
        self._migration_job_repository = migration_job_repository
        self._migration_job_worker = migration_job_worker
        self._logger = logger
        self._consumer_name = consumer_name
        self._lease_duration_seconds = max(5.0, lease_duration_seconds)
        self._worker_id = f"{self._consumer_name}:{uuid4().hex}"

    async def handle(self, envelope: IntegrationEnvelope) -> str:
        if await self._inbox_repository.is_processed(
            consumer_name=self._consumer_name,
            message_id=envelope.message_id,
        ):
            return "duplicate"

        if envelope.message_type != "migration.job.requested":
            await self._mark_processed(
                message_id=envelope.message_id,
                result_code="ignored_message_type",
                result_payload={"message_type": envelope.message_type},
            )
            return "ignored_message_type"

        job_key = envelope.payload_json.get("job_key")
        if not isinstance(job_key, str) or not job_key:
            await self._mark_processed(
                message_id=envelope.message_id,
                result_code="invalid_payload",
                result_payload={"reason": "job_key is required"},
            )
            return "invalid_payload"

        job = await self._migration_job_repository.claim(
            job_key=job_key,
            worker_id=self._worker_id,
            lease_duration_seconds=self._lease_duration_seconds,
        )
        if job is None:
            await self._mark_processed(
                message_id=envelope.message_id,
                result_code="not_claimed",
                result_payload={"job_key": job_key},
            )
            return "not_claimed"

        self._logger.info(
            "migration command claimed from broker",
            consumer_name=self._consumer_name,
            worker_id=self._worker_id,
            message_id=envelope.message_id,
            message_type=envelope.message_type,
            migration_id=job.migration_id,
            job_key=job.job_key,
        )
        await self._migration_job_worker.execute_claimed_job(job)
        current = await self._migration_job_repository.get(job.job_key)
        result_code = "processed"
        if current is not None and current.status is MigrationJobStatus.FAILED:
            result_code = "job_failed"
        elif current is not None and current.status is MigrationJobStatus.COMPLETED:
            result_code = "job_completed"
        await self._mark_processed(
            message_id=envelope.message_id,
            result_code=result_code,
            result_payload={
                "job_key": job.job_key,
                "status": current.status.value if current is not None else None,
            },
        )
        return result_code

    async def _mark_processed(
        self,
        *,
        message_id: str,
        result_code: str,
        result_payload: dict[str, object],
    ) -> None:
        await self._inbox_repository.mark_processed(
            ConsumerInboxRecord(
                consumer_name=self._consumer_name,
                message_id=message_id,
                processed_at=datetime.now(tz=UTC),
                result_code=result_code,
                result_payload=result_payload,
            ),
        )
