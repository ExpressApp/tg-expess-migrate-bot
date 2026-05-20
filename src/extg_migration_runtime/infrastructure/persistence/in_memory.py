from __future__ import annotations

import asyncio
from dataclasses import replace
from datetime import UTC, datetime, timedelta

from extg_shared.contracts.ports import AttachmentStatusSummary, MessageStatusSummary
from extg_shared.contracts.models import (
    AttachmentClaimResult,
    AttachmentImportStatus,
    AttachmentMappingRecord,
    AttachmentStageClaimResult,
    AttachmentStageRecord,
    AttachmentStageStatus,
    AuditEvent,
    ChatMigrationConfigRecord,
    ChatMappingRecord,
    ClaimState,
    CanonicalAttachment,
    ConsumerInboxRecord,
    ExpressBotHuidBindingRecord,
    ExpressStagedFile,
    ExpressUserCtsBindingRecord,
    IdentityMappingRecord,
    IntegrationOutboxEventRecord,
    IntegrationOutboxStatus,
    InventorySnapshotRecord,
    MigrationJobRecord,
    MigrationJobStatus,
    MIGRATION_JOB_CANCELLED_ERROR_CODE,
    MIGRATION_JOB_CANCEL_REQUESTED_ERROR_CODE,
    MigrationStateRecord,
    MessageClaimResult,
    MessageImportStatus,
    MessageMappingRecord,
    MigrationCheckpoint,
    OperatorMigrationDefaultsRecord,
    OperatorTelegramSessionRecord,
    PublishedIntegrationEvent,
    ServiceWatermarkRecord,
    TelegramExportSnapshotRecord,
)


def _express_staged_file_to_payload(staged_file: ExpressStagedFile) -> dict[str, object]:
    return {
        "attachment_type": staged_file.attachment_type,
        "file_id": staged_file.file_id,
        "file_url": staged_file.file_url,
        "filename": staged_file.filename,
        "size_bytes": staged_file.size_bytes,
        "mime_type": staged_file.mime_type,
        "file_hash": staged_file.file_hash,
        "duration_seconds": staged_file.duration_seconds,
        "preview_url": staged_file.preview_url,
        "preview_height": staged_file.preview_height,
        "preview_width": staged_file.preview_width,
        "encryption_algo": staged_file.encryption_algo,
        "chunk_size": staged_file.chunk_size,
        "caption": staged_file.caption,
    }


class InMemoryChatMappingRepository:
    def __init__(self) -> None:
        self._items: dict[tuple[str, str], ChatMappingRecord] = {}
        self._lock = asyncio.Lock()

    async def get(
        self,
        migration_id: str,
        source_chat_id: str,
    ) -> ChatMappingRecord | None:
        async with self._lock:
            return self._items.get((migration_id, source_chat_id))

    async def save(self, record: ChatMappingRecord) -> None:
        async with self._lock:
            self._items[(record.migration_id, record.source_chat_id)] = record

    async def list_by_migration(
        self,
        migration_id: str,
    ) -> list[ChatMappingRecord]:
        async with self._lock:
            records = [
                record
                for (current_migration_id, _), record in self._items.items()
                if current_migration_id == migration_id
            ]
            records.sort(key=lambda item: item.source_chat_id)
            return records


class InMemoryChatMigrationConfigRepository:
    def __init__(self) -> None:
        self._items: dict[tuple[str, str], ChatMigrationConfigRecord] = {}
        self._lock = asyncio.Lock()

    async def get(
        self,
        migration_id: str,
        source_chat_id: str,
    ) -> ChatMigrationConfigRecord | None:
        async with self._lock:
            return self._items.get((migration_id, source_chat_id))

    async def list_by_migration(
        self,
        migration_id: str,
    ) -> list[ChatMigrationConfigRecord]:
        async with self._lock:
            records = [
                record
                for (current_migration_id, _), record in self._items.items()
                if current_migration_id == migration_id
            ]
            records.sort(key=lambda item: item.source_chat_id)
            return records

    async def save(
        self,
        record: ChatMigrationConfigRecord,
    ) -> ChatMigrationConfigRecord:
        key = (record.migration_id, record.source_chat_id)
        async with self._lock:
            existing = self._items.get(key)
            if existing is not None and existing.updated_by_huid != record.updated_by_huid:
                return existing
            updated = ChatMigrationConfigRecord(
                migration_id=record.migration_id,
                source_chat_id=record.source_chat_id,
                source_chat_type=record.source_chat_type,
                source_chat_title=record.source_chat_title,
                target_strategy=record.target_strategy,
                target_title=record.target_title,
                target_chat_id=record.target_chat_id,
                include_from=record.include_from,
                include_to=record.include_to,
                migrate_media=record.migrate_media,
                media_kinds=record.media_kinds,
                service_messages=record.service_messages,
                reply_mode=record.reply_mode,
                output_template=record.output_template,
                identity_policy=record.identity_policy,
                updated_by_huid=record.updated_by_huid,
                created_at=existing.created_at if existing is not None else record.created_at,
                updated_at=record.updated_at,
                anchor_cts_host=record.anchor_cts_host,
                anchor_bot_id=record.anchor_bot_id,
                topic_strategy=record.topic_strategy,
                source_backend=record.source_backend,
                access_strategy=record.access_strategy,
                skip_in_all=record.skip_in_all,
                telegram_chat_id=record.telegram_chat_id,
                source_topic_id=record.source_topic_id,
                source_thread_id=record.source_thread_id,
                source_thread_title=record.source_thread_title,
            )
            self._items[key] = updated
            return updated

    async def delete(
        self,
        migration_id: str,
        source_chat_id: str,
    ) -> bool:
        key = (migration_id, source_chat_id)
        async with self._lock:
            return self._items.pop(key, None) is not None


class InMemoryOperatorMigrationDefaultsRepository:
    def __init__(self) -> None:
        self._items: dict[tuple[str, str], OperatorMigrationDefaultsRecord] = {}
        self._lock = asyncio.Lock()

    async def get(
        self,
        migration_id: str,
        operator_huid: str,
    ) -> OperatorMigrationDefaultsRecord | None:
        async with self._lock:
            return self._items.get((migration_id, operator_huid))

    async def save(
        self,
        record: OperatorMigrationDefaultsRecord,
    ) -> OperatorMigrationDefaultsRecord:
        key = (record.migration_id, record.operator_huid)
        async with self._lock:
            existing = self._items.get(key)
            updated = replace(
                record,
                created_at=existing.created_at if existing is not None else record.created_at,
            )
            self._items[key] = updated
            return updated

class InMemoryMigrationJobRepository:
    def __init__(
        self,
        *,
        outbox_repository: InMemoryOutboxRepository | None = None,
    ) -> None:
        self._items: dict[str, MigrationJobRecord] = {}
        self._outbox_repository = outbox_repository
        self._lock = asyncio.Lock()

    async def enqueue(
        self,
        record: MigrationJobRecord,
        *,
        outbox_events: tuple[IntegrationOutboxEventRecord, ...] = (),
    ) -> bool:
        async with self._lock:
            if record.job_key in self._items:
                return False
            self._items[record.job_key] = record
        if self._outbox_repository is not None and outbox_events:
            await self._outbox_repository.append_many(outbox_events)
        return True

    async def get(self, job_key: str) -> MigrationJobRecord | None:
        async with self._lock:
            return self._items.get(job_key)

    async def list_active(self, migration_id: str) -> list[MigrationJobRecord]:
        async with self._lock:
            records = [
                record
                for record in self._items.values()
                if record.migration_id == migration_id
                and record.status in {MigrationJobStatus.QUEUED, MigrationJobStatus.RUNNING}
            ]
            records.sort(key=lambda item: item.requested_at)
            return records

    async def claim(
        self,
        *,
        job_key: str,
        worker_id: str,
        lease_duration_seconds: float,
    ) -> MigrationJobRecord | None:
        now = datetime.now(tz=UTC)
        async with self._lock:
            record = self._items.get(job_key)
            if record is None:
                return None
            claimable = (
                record.status is MigrationJobStatus.QUEUED
                or (
                    record.status is MigrationJobStatus.RUNNING
                    and record.last_error_code != MIGRATION_JOB_CANCEL_REQUESTED_ERROR_CODE
                    and (
                        record.lease_expires_at is None
                        or record.lease_expires_at < now
                    )
                )
            )
            if not claimable:
                return None
            updated = replace(
                record,
                status=MigrationJobStatus.RUNNING,
                worker_id=worker_id,
                started_at=record.started_at or now,
                heartbeat_at=now,
                lease_expires_at=now.replace(microsecond=0)
                + timedelta(seconds=lease_duration_seconds),
                attempts=record.attempts + 1,
                last_error_code=None,
                last_error_payload=None,
            )
            self._items[job_key] = updated
            return updated

    async def acquire_next(
        self,
        *,
        worker_id: str,
        lease_duration_seconds: float,
    ) -> MigrationJobRecord | None:
        now = datetime.now(tz=UTC)
        async with self._lock:
            candidates = [
                record
                for record in self._items.values()
                if record.status is MigrationJobStatus.QUEUED
                or (
                    record.status is MigrationJobStatus.RUNNING
                    and record.last_error_code != MIGRATION_JOB_CANCEL_REQUESTED_ERROR_CODE
                    and (
                        record.lease_expires_at is None
                        or record.lease_expires_at < now
                    )
                )
            ]
            candidates.sort(key=lambda item: item.requested_at)
            if not candidates:
                return None
            record = candidates[0]
            updated = replace(
                record,
                status=MigrationJobStatus.RUNNING,
                worker_id=worker_id,
                started_at=record.started_at or now,
                heartbeat_at=now,
                lease_expires_at=now.replace(microsecond=0)
                + timedelta(seconds=lease_duration_seconds),
                attempts=record.attempts + 1,
                last_error_code=None,
                last_error_payload=None,
            )
            self._items[record.job_key] = updated
            return updated

    async def heartbeat(
        self,
        *,
        job_key: str,
        worker_id: str,
        lease_duration_seconds: float,
    ) -> bool:
        now = datetime.now(tz=UTC)
        async with self._lock:
            record = self._items.get(job_key)
            if record is None or record.worker_id != worker_id or record.status is not MigrationJobStatus.RUNNING:
                return False
            self._items[job_key] = replace(
                record,
                heartbeat_at=now,
                lease_expires_at=now.replace(microsecond=0) + timedelta(seconds=lease_duration_seconds),
            )
            return True

    async def mark_completed(
        self,
        *,
        job_key: str,
        worker_id: str,
    ) -> MigrationJobRecord | None:
        now = datetime.now(tz=UTC)
        async with self._lock:
            record = self._items.get(job_key)
            if record is None or record.worker_id != worker_id or record.status is not MigrationJobStatus.RUNNING:
                return None
            updated = replace(
                record,
                status=MigrationJobStatus.COMPLETED,
                finished_at=now,
                heartbeat_at=now,
                lease_expires_at=None,
            )
            self._items[job_key] = updated
            return updated

    async def mark_failed(
        self,
        *,
        job_key: str,
        worker_id: str,
        error_code: str,
        error_payload: dict[str, object],
    ) -> MigrationJobRecord | None:
        now = datetime.now(tz=UTC)
        async with self._lock:
            record = self._items.get(job_key)
            if record is None or record.worker_id != worker_id or record.status is not MigrationJobStatus.RUNNING:
                return None
            updated = replace(
                record,
                status=MigrationJobStatus.FAILED,
                finished_at=now,
                heartbeat_at=now,
                lease_expires_at=None,
                last_error_code=error_code,
                last_error_payload=error_payload,
            )
            self._items[job_key] = updated
            return updated

    async def request_cancel(
        self,
        *,
        job_key: str,
        operator_huid: str,
    ) -> MigrationJobRecord | None:
        now = datetime.now(tz=UTC)
        async with self._lock:
            record = self._items.get(job_key)
            if record is None or record.operator_huid != operator_huid:
                return None
            if record.status is MigrationJobStatus.QUEUED:
                updated = replace(
                    record,
                    status=MigrationJobStatus.FAILED,
                    finished_at=now,
                    heartbeat_at=now,
                    lease_expires_at=None,
                    last_error_code=MIGRATION_JOB_CANCELLED_ERROR_CODE,
                    last_error_payload={"requested_by": operator_huid},
                )
                self._items[job_key] = updated
                return updated
            if record.status is MigrationJobStatus.RUNNING:
                updated = replace(
                    record,
                    last_error_code=MIGRATION_JOB_CANCEL_REQUESTED_ERROR_CODE,
                    last_error_payload={"requested_by": operator_huid},
                )
                self._items[job_key] = updated
                return updated
            return None

    async def count_by_status(self) -> dict[MigrationJobStatus, int]:
        async with self._lock:
            counts: dict[MigrationJobStatus, int] = {}
            for record in self._items.values():
                counts[record.status] = counts.get(record.status, 0) + 1
            return counts


class InMemoryTelegramExportSnapshotRepository:
    def __init__(self) -> None:
        self._items: dict[str, TelegramExportSnapshotRecord] = {}
        self._lock = asyncio.Lock()

    async def get(
        self,
        source_chat_id: str,
    ) -> TelegramExportSnapshotRecord | None:
        async with self._lock:
            return self._items.get(source_chat_id)

    async def save(
        self,
        record: TelegramExportSnapshotRecord,
    ) -> TelegramExportSnapshotRecord:
        async with self._lock:
            self._items[record.source_chat_id] = record
            return record

    async def delete(
        self,
        source_chat_id: str,
    ) -> bool:
        async with self._lock:
            return self._items.pop(source_chat_id, None) is not None


class InMemoryOutboxRepository:
    def __init__(self) -> None:
        self._items: dict[str, IntegrationOutboxEventRecord] = {}
        self._lock = asyncio.Lock()

    async def append_many(
        self,
        events: tuple[IntegrationOutboxEventRecord, ...],
    ) -> None:
        async with self._lock:
            for event in events:
                self._items[event.event_id] = event

    async def get(self, event_id: str) -> IntegrationOutboxEventRecord | None:
        async with self._lock:
            return self._items.get(event_id)

    async def lease_batch(
        self,
        *,
        publisher_id: str,
        limit: int,
        lease_duration_seconds: float,
    ) -> list[IntegrationOutboxEventRecord]:
        now = datetime.now(tz=UTC)
        async with self._lock:
            candidates = [
                event
                for event in self._items.values()
                if event.status is IntegrationOutboxStatus.PENDING
                or (
                    event.status is IntegrationOutboxStatus.LEASED
                    and (
                        event.lease_expires_at is None
                        or event.lease_expires_at < now
                    )
                )
            ]
            candidates.sort(key=lambda item: item.created_at)
            leased: list[IntegrationOutboxEventRecord] = []
            for event in candidates[:limit]:
                updated = replace(
                    event,
                    status=IntegrationOutboxStatus.LEASED,
                    leased_by=publisher_id,
                    leased_at=now,
                    lease_expires_at=now.replace(microsecond=0)
                    + timedelta(seconds=lease_duration_seconds),
                    attempts=event.attempts + 1,
                )
                self._items[event.event_id] = updated
                leased.append(updated)
            return leased

    async def mark_published(
        self,
        *,
        event_id: str,
        publisher_id: str,
        result: PublishedIntegrationEvent,
    ) -> IntegrationOutboxEventRecord | None:
        async with self._lock:
            event = self._items.get(event_id)
            if (
                event is None
                or event.leased_by != publisher_id
                or event.status is not IntegrationOutboxStatus.LEASED
            ):
                return None
            updated = replace(
                event,
                status=IntegrationOutboxStatus.PUBLISHED,
                published_at=result.published_at,
                broker_topic=result.broker_topic,
                broker_partition=result.broker_partition,
                broker_offset=result.broker_offset,
                leased_by=None,
                leased_at=None,
                lease_expires_at=None,
            )
            self._items[event_id] = updated
            return updated

    async def mark_failed(
        self,
        *,
        event_id: str,
        publisher_id: str,
        error_code: str,
        error_payload: dict[str, object],
    ) -> IntegrationOutboxEventRecord | None:
        async with self._lock:
            event = self._items.get(event_id)
            if (
                event is None
                or event.leased_by != publisher_id
                or event.status is not IntegrationOutboxStatus.LEASED
            ):
                return None
            updated = replace(
                event,
                status=IntegrationOutboxStatus.FAILED,
                leased_by=None,
                leased_at=None,
                lease_expires_at=None,
                last_error_code=error_code,
                last_error_payload=dict(error_payload),
            )
            self._items[event_id] = updated
            return updated

    async def count_by_status(self) -> dict[IntegrationOutboxStatus, int]:
        async with self._lock:
            counts: dict[IntegrationOutboxStatus, int] = {}
            for record in self._items.values():
                counts[record.status] = counts.get(record.status, 0) + 1
            return counts


class InMemoryInboxRepository:
    def __init__(self) -> None:
        self._items: dict[tuple[str, str], ConsumerInboxRecord] = {}
        self._lock = asyncio.Lock()

    async def is_processed(self, *, consumer_name: str, message_id: str) -> bool:
        async with self._lock:
            return (consumer_name, message_id) in self._items

    async def mark_processed(self, record: ConsumerInboxRecord) -> bool:
        key = (record.consumer_name, record.message_id)
        async with self._lock:
            if key in self._items:
                return False
            self._items[key] = record
            return True


class InMemoryServiceWatermarkRepository:
    def __init__(self) -> None:
        self._items: dict[tuple[str, str], ServiceWatermarkRecord] = {}
        self._lock = asyncio.Lock()

    async def get(
        self,
        *,
        consumer_name: str,
        logical_stream_key: str,
    ) -> ServiceWatermarkRecord | None:
        async with self._lock:
            return self._items.get((consumer_name, logical_stream_key))

    async def upsert(
        self,
        record: ServiceWatermarkRecord,
    ) -> ServiceWatermarkRecord:
        async with self._lock:
            self._items[(record.consumer_name, record.logical_stream_key)] = record
            return record

    async def list_all(self) -> list[ServiceWatermarkRecord]:
        async with self._lock:
            return sorted(
                self._items.values(),
                key=lambda item: (item.consumer_name, item.logical_stream_key),
            )


class InMemoryMessageMappingRepository:
    def __init__(self, *, processing_stale_after_seconds: float = 30.0) -> None:
        self._items: dict[tuple[str, str, str], MessageMappingRecord] = {}
        self._ambiguous_keys: set[tuple[str, str, str]] = set()
        self._processing_started_at: dict[tuple[str, str, str], datetime] = {}
        self._processing_stale_after = timedelta(
            seconds=max(processing_stale_after_seconds, 1.0),
        )
        self._lock = asyncio.Lock()

    async def claim(self, record: MessageMappingRecord) -> MessageClaimResult:
        key = (record.migration_id, record.source_chat_id, record.source_message_id)
        async with self._lock:
            existing = self._items.get(key)
            if existing:
                if key in self._ambiguous_keys:
                    claimed_at = self._processing_started_at.get(key)
                    if (
                        existing.last_error_code == "AmbiguousDeliveryError"
                        and existing.target_sync_id is None
                        and claimed_at is not None
                        and claimed_at <= datetime.now(tz=UTC) - self._processing_stale_after
                    ):
                        updated = replace(
                            existing,
                            source_sent_at=record.source_sent_at,
                            target_chat_id=record.target_chat_id,
                            checksum=record.checksum,
                            import_status=MessageImportStatus.PROCESSING,
                            imported_at=None,
                            target_sync_id=None,
                            rendered_body=record.rendered_body,
                            last_error_code=None,
                            last_error_payload=None,
                        )
                        self._items[key] = updated
                        self._ambiguous_keys.discard(key)
                        self._processing_started_at[key] = datetime.now(tz=UTC)
                        return MessageClaimResult(ClaimState.CLAIMED, updated)
                    return MessageClaimResult(ClaimState.AMBIGUOUS, existing)
                if existing.import_status is MessageImportStatus.IMPORTED:
                    return MessageClaimResult(ClaimState.IMPORTED, existing)
                if (
                    existing.import_status is MessageImportStatus.FAILED
                    and existing.last_error_code in {
                        MIGRATION_JOB_CANCELLED_ERROR_CODE,
                        MIGRATION_JOB_CANCEL_REQUESTED_ERROR_CODE,
                    }
                ):
                    updated = replace(
                        existing,
                        source_sent_at=record.source_sent_at,
                        target_chat_id=record.target_chat_id,
                        checksum=record.checksum,
                        import_status=MessageImportStatus.PROCESSING,
                        imported_at=None,
                        target_sync_id=None,
                        rendered_body=record.rendered_body,
                        last_error_code=None,
                        last_error_payload=None,
                    )
                    self._items[key] = updated
                    self._processing_started_at[key] = datetime.now(tz=UTC)
                    return MessageClaimResult(ClaimState.CLAIMED, updated)
                if existing.import_status is MessageImportStatus.FAILED:
                    return MessageClaimResult(ClaimState.FAILED, existing)
                claimed_at = self._processing_started_at.get(key)
                if (
                    existing.import_status is MessageImportStatus.PROCESSING
                    and (
                        existing.last_error_code in {
                            MIGRATION_JOB_CANCELLED_ERROR_CODE,
                            MIGRATION_JOB_CANCEL_REQUESTED_ERROR_CODE,
                        }
                        or (
                            claimed_at is not None
                            and claimed_at <= datetime.now(tz=UTC) - self._processing_stale_after
                        )
                    )
                ):
                    self._items[key] = record
                    self._processing_started_at[key] = datetime.now(tz=UTC)
                    return MessageClaimResult(ClaimState.CLAIMED, record)
                return MessageClaimResult(ClaimState.AMBIGUOUS, existing)

            self._items[key] = record
            self._processing_started_at[key] = datetime.now(tz=UTC)
            return MessageClaimResult(ClaimState.CLAIMED, record)

    async def get(
        self,
        migration_id: str,
        source_chat_id: str,
        source_message_id: str,
    ) -> MessageMappingRecord | None:
        async with self._lock:
            return self._items.get((migration_id, source_chat_id, source_message_id))

    async def mark_imported(
        self,
        migration_id: str,
        source_chat_id: str,
        source_message_id: str,
        *,
        target_sync_id: str,
        rendered_body: str,
    ) -> MessageMappingRecord:
        key = (migration_id, source_chat_id, source_message_id)
        async with self._lock:
            record = self._items[key]
            updated = replace(
                record,
                import_status=MessageImportStatus.IMPORTED,
                imported_at=datetime.now(tz=UTC),
                target_sync_id=target_sync_id,
                rendered_body=rendered_body,
                last_error_code=None,
                last_error_payload=None,
            )
            self._items[key] = updated
            self._ambiguous_keys.discard(key)
            self._processing_started_at.pop(key, None)
            return updated

    async def mark_failed(
        self,
        migration_id: str,
        source_chat_id: str,
        source_message_id: str,
        *,
        error_code: str,
        error_payload: dict[str, object],
    ) -> MessageMappingRecord:
        key = (migration_id, source_chat_id, source_message_id)
        async with self._lock:
            record = self._items[key]
            updated = replace(
                record,
                import_status=MessageImportStatus.FAILED,
                last_error_code=error_code,
                last_error_payload=error_payload,
            )
            self._items[key] = updated
            self._ambiguous_keys.discard(key)
            self._processing_started_at.pop(key, None)
            return updated

    async def list_all(self) -> list[MessageMappingRecord]:
        async with self._lock:
            return list(self._items.values())

    async def summarize_by_chat(
        self,
        migration_id: str,
    ) -> list[MessageStatusSummary]:
        counts: dict[tuple[str, MessageImportStatus], int] = {}
        async with self._lock:
            for (current_migration_id, source_chat_id, _), record in self._items.items():
                if current_migration_id != migration_id:
                    continue
                key = (source_chat_id, record.import_status)
                counts[key] = counts.get(key, 0) + 1

        summaries = [
            MessageStatusSummary(
                migration_id=migration_id,
                source_chat_id=chat_id,
                import_status=status,
                count=count,
            )
            for (chat_id, status), count in counts.items()
        ]
        summaries.sort(
            key=lambda item: (item.source_chat_id, item.import_status.value),
        )
        return summaries

    async def mark_ambiguous(
        self,
        migration_id: str,
        source_chat_id: str,
        source_message_id: str,
        *,
        error_code: str,
        error_payload: dict[str, object],
    ) -> MessageMappingRecord:
        key = (migration_id, source_chat_id, source_message_id)
        async with self._lock:
            record = self._items[key]
            updated = replace(
                record,
                import_status=MessageImportStatus.AMBIGUOUS,
                last_error_code=error_code,
                last_error_payload=error_payload,
            )
            self._items[key] = updated
            self._ambiguous_keys.add(key)
            self._processing_started_at[key] = datetime.now(tz=UTC)
            return updated

    async def list_failed(
        self,
        migration_id: str,
        *,
        source_chat_id: str | None = None,
        limit: int = 100,
    ) -> list[MessageMappingRecord]:
        async with self._lock:
            failed = [
                record
                for key, record in self._items.items()
                if key[0] == migration_id
                and record.import_status is MessageImportStatus.FAILED
                and (source_chat_id is None or key[1] == source_chat_id)
            ]
            failed.sort(key=lambda item: (item.source_sent_at, item.source_message_id))
            return failed[:limit]

    async def requeue_failed(
        self,
        migration_id: str,
        *,
        source_chat_id: str | None = None,
        limit: int = 100,
    ) -> list[MessageMappingRecord]:
        async with self._lock:
            candidates = [
                (key, record)
                for key, record in self._items.items()
                if key[0] == migration_id
                and record.import_status is MessageImportStatus.FAILED
                and (source_chat_id is None or key[1] == source_chat_id)
            ]
            candidates.sort(key=lambda item: (item[1].source_sent_at, item[1].source_message_id))
            selected = candidates[:limit]

            updated_records: list[MessageMappingRecord] = []
            for key, record in selected:
                updated = replace(
                    record,
                    import_status=MessageImportStatus.PROCESSING,
                    imported_at=None,
                    last_error_code=None,
                    last_error_payload=None,
                )
                self._items[key] = updated
                self._ambiguous_keys.discard(key)
                self._processing_started_at[key] = datetime.now(tz=UTC)
                updated_records.append(updated)
            return updated_records


class InMemoryAttachmentMappingRepository:
    def __init__(self) -> None:
        self._items: dict[tuple[str, str, str, int], AttachmentMappingRecord] = {}
        self._lock = asyncio.Lock()

    async def claim(
        self,
        record: AttachmentMappingRecord,
        *,
        retry_failed: bool = False,
    ) -> AttachmentClaimResult:
        key = (
            record.migration_id,
            record.source_chat_id,
            record.source_message_id,
            record.attachment_index,
        )
        async with self._lock:
            existing = self._items.get(key)
            if existing:
                if existing.import_status is AttachmentImportStatus.FAILED and (
                    retry_failed
                    or existing.last_error_code
                    in {
                        MIGRATION_JOB_CANCELLED_ERROR_CODE,
                        MIGRATION_JOB_CANCEL_REQUESTED_ERROR_CODE,
                    }
                ):
                    updated = replace(
                        existing,
                        import_status=AttachmentImportStatus.PROCESSING,
                        imported_at=None,
                        last_error_code=None,
                        last_error_payload=None,
                    )
                    self._items[key] = updated
                    return AttachmentClaimResult(ClaimState.CLAIMED, updated)
                if existing.import_status is AttachmentImportStatus.IMPORTED:
                    return AttachmentClaimResult(ClaimState.IMPORTED, existing)
                if existing.import_status is AttachmentImportStatus.FAILED:
                    return AttachmentClaimResult(ClaimState.FAILED, existing)
                return AttachmentClaimResult(ClaimState.AMBIGUOUS, existing)

            self._items[key] = record
            return AttachmentClaimResult(ClaimState.CLAIMED, record)

    async def get(
        self,
        migration_id: str,
        source_chat_id: str,
        source_message_id: str,
        attachment_index: int,
    ) -> AttachmentMappingRecord | None:
        async with self._lock:
            return self._items.get(
                (migration_id, source_chat_id, source_message_id, attachment_index),
            )

    async def list_by_message(
        self,
        migration_id: str,
        source_chat_id: str,
        source_message_id: str,
    ) -> list[AttachmentMappingRecord]:
        async with self._lock:
            records = [
                record
                for (current_migration_id, current_chat_id, current_message_id, _), record
                in self._items.items()
                if current_migration_id == migration_id
                and current_chat_id == source_chat_id
                and current_message_id == source_message_id
            ]
            records.sort(key=lambda item: item.attachment_index)
            return records

    async def mark_imported(
        self,
        migration_id: str,
        source_chat_id: str,
        source_message_id: str,
        attachment_index: int,
        *,
        attachment: CanonicalAttachment,
        target_sync_id: str,
    ) -> AttachmentMappingRecord:
        key = (migration_id, source_chat_id, source_message_id, attachment_index)
        async with self._lock:
            record = self._items[key]
            updated = replace(
                record,
                source_file_id=attachment.source_file_id,
                source_filename=attachment.filename,
                media_kind=attachment.media_kind,
                checksum=attachment.sha256,
                size_bytes=attachment.size_bytes,
                import_status=AttachmentImportStatus.IMPORTED,
                imported_at=datetime.now(tz=UTC),
                target_sync_id=target_sync_id,
                last_error_code=None,
                last_error_payload=None,
            )
            self._items[key] = updated
            return updated

    async def mark_failed(
        self,
        migration_id: str,
        source_chat_id: str,
        source_message_id: str,
        attachment_index: int,
        *,
        attachment: CanonicalAttachment | None,
        error_code: str,
        error_payload: dict[str, object],
    ) -> AttachmentMappingRecord:
        key = (migration_id, source_chat_id, source_message_id, attachment_index)
        async with self._lock:
            record = self._items[key]
            updated = replace(
                record,
                source_file_id=attachment.source_file_id if attachment else record.source_file_id,
                source_filename=attachment.filename if attachment else record.source_filename,
                media_kind=attachment.media_kind if attachment else record.media_kind,
                checksum=attachment.sha256 if attachment else record.checksum,
                size_bytes=attachment.size_bytes if attachment else record.size_bytes,
                import_status=AttachmentImportStatus.FAILED,
                imported_at=None,
                last_error_code=error_code,
                last_error_payload=error_payload,
            )
            self._items[key] = updated
            return updated

    async def mark_ambiguous(
        self,
        migration_id: str,
        source_chat_id: str,
        source_message_id: str,
        attachment_index: int,
        *,
        attachment: CanonicalAttachment | None,
        error_code: str,
        error_payload: dict[str, object],
    ) -> AttachmentMappingRecord:
        key = (migration_id, source_chat_id, source_message_id, attachment_index)
        async with self._lock:
            record = self._items[key]
            updated = replace(
                record,
                source_file_id=attachment.source_file_id if attachment else record.source_file_id,
                source_filename=attachment.filename if attachment else record.source_filename,
                media_kind=attachment.media_kind if attachment else record.media_kind,
                checksum=attachment.sha256 if attachment else record.checksum,
                size_bytes=attachment.size_bytes if attachment else record.size_bytes,
                import_status=AttachmentImportStatus.AMBIGUOUS,
                imported_at=None,
                last_error_code=error_code,
                last_error_payload=error_payload,
            )
            self._items[key] = updated
            return updated

    async def mark_skipped(
        self,
        migration_id: str,
        source_chat_id: str,
        source_message_id: str,
        attachment_index: int,
        *,
        attachment: CanonicalAttachment,
        reason: str,
    ) -> AttachmentMappingRecord:
        key = (migration_id, source_chat_id, source_message_id, attachment_index)
        async with self._lock:
            record = self._items[key]
            updated = replace(
                record,
                source_file_id=attachment.source_file_id,
                source_filename=attachment.filename,
                media_kind=attachment.media_kind,
                checksum=attachment.sha256,
                size_bytes=attachment.size_bytes,
                import_status=AttachmentImportStatus.SKIPPED,
                imported_at=None,
                last_error_code="skipped",
                last_error_payload={"reason": reason},
            )
            self._items[key] = updated
            return updated

    async def summarize_by_chat(
        self,
        migration_id: str,
    ) -> list[AttachmentStatusSummary]:
        counts: dict[tuple[str, AttachmentImportStatus], int] = {}
        async with self._lock:
            for (current_migration_id, source_chat_id, _, _), record in self._items.items():
                if current_migration_id != migration_id:
                    continue
                key = (source_chat_id, record.import_status)
                counts[key] = counts.get(key, 0) + 1

        summaries = [
            AttachmentStatusSummary(
                migration_id=migration_id,
                source_chat_id=chat_id,
                import_status=status,
                count=count,
            )
            for (chat_id, status), count in counts.items()
        ]
        summaries.sort(
            key=lambda item: (item.source_chat_id, item.import_status.value),
        )
        return summaries


class InMemoryAttachmentStageRepository:
    def __init__(self, *, processing_stale_after_seconds: float = 30.0) -> None:
        self._items: dict[tuple[str, str, str, int, str], AttachmentStageRecord] = {}
        self._processing_started_at: dict[tuple[str, str, str, int, str], datetime] = {}
        self._processing_stale_after = timedelta(
            seconds=max(processing_stale_after_seconds, 1.0),
        )
        self._lock = asyncio.Lock()

    async def claim(
        self,
        record: AttachmentStageRecord,
        *,
        retry_failed: bool = False,
    ) -> AttachmentStageClaimResult:
        key = (
            record.migration_id,
            record.source_chat_id,
            record.source_message_id,
            record.attachment_index,
            record.target_chat_id,
        )
        async with self._lock:
            existing = self._items.get(key)
            if existing is None:
                inserted = replace(
                    record,
                    created_at=record.created_at or datetime.now(tz=UTC),
                    updated_at=datetime.now(tz=UTC),
                )
                self._items[key] = inserted
                self._processing_started_at[key] = datetime.now(tz=UTC)
                return AttachmentStageClaimResult(state=ClaimState.CLAIMED, record=inserted)
            claimed_at = self._processing_started_at.get(key)
            if (
                existing.status is AttachmentStageStatus.PROCESSING
                and (
                    existing.last_error_code
                    in {
                        MIGRATION_JOB_CANCELLED_ERROR_CODE,
                        MIGRATION_JOB_CANCEL_REQUESTED_ERROR_CODE,
                    }
                    or (
                        claimed_at is not None
                        and claimed_at <= datetime.now(tz=UTC) - self._processing_stale_after
                    )
                )
            ):
                updated = replace(
                    existing,
                    source_locator=record.source_locator,
                    media_kind=record.media_kind,
                    checksum=record.checksum,
                    size_bytes=record.size_bytes,
                    status=AttachmentStageStatus.PROCESSING,
                    express_file_id=None,
                    express_file_payload=None,
                    target_sync_id=None,
                    last_error_code=None,
                    last_error_payload=None,
                    updated_at=datetime.now(tz=UTC),
                )
                self._items[key] = updated
                self._processing_started_at[key] = datetime.now(tz=UTC)
                return AttachmentStageClaimResult(state=ClaimState.CLAIMED, record=updated)
            if existing.status is AttachmentStageStatus.FAILED and (
                retry_failed
                or existing.last_error_code
                in {
                    MIGRATION_JOB_CANCELLED_ERROR_CODE,
                    MIGRATION_JOB_CANCEL_REQUESTED_ERROR_CODE,
                }
            ):
                updated = replace(
                    existing,
                    status=AttachmentStageStatus.PROCESSING,
                    last_error_code=None,
                    last_error_payload=None,
                    updated_at=datetime.now(tz=UTC),
                )
                self._items[key] = updated
                self._processing_started_at[key] = datetime.now(tz=UTC)
                return AttachmentStageClaimResult(state=ClaimState.CLAIMED, record=updated)
            if existing.status in (
                AttachmentStageStatus.UPLOADED,
                AttachmentStageStatus.ATTACHED,
            ):
                return AttachmentStageClaimResult(state=ClaimState.IMPORTED, record=existing)
            if existing.status is AttachmentStageStatus.FAILED:
                return AttachmentStageClaimResult(state=ClaimState.FAILED, record=existing)
            return AttachmentStageClaimResult(state=ClaimState.AMBIGUOUS, record=existing)

    async def get(
        self,
        migration_id: str,
        source_chat_id: str,
        source_message_id: str,
        attachment_index: int,
        target_chat_id: str,
    ) -> AttachmentStageRecord | None:
        key = (
            migration_id,
            source_chat_id,
            source_message_id,
            attachment_index,
            target_chat_id,
        )
        async with self._lock:
            return self._items.get(key)

    async def mark_uploaded(
        self,
        migration_id: str,
        source_chat_id: str,
        source_message_id: str,
        attachment_index: int,
        target_chat_id: str,
        *,
        staged_file: ExpressStagedFile,
    ) -> AttachmentStageRecord:
        key = (
            migration_id,
            source_chat_id,
            source_message_id,
            attachment_index,
            target_chat_id,
        )
        async with self._lock:
            record = self._items[key]
            updated = replace(
                record,
                status=AttachmentStageStatus.UPLOADED,
                express_file_id=staged_file.file_id,
                express_file_payload=_express_staged_file_to_payload(staged_file),
                last_error_code=None,
                last_error_payload=None,
                updated_at=datetime.now(tz=UTC),
            )
            self._items[key] = updated
            self._processing_started_at.pop(key, None)
            return updated

    async def mark_attached(
        self,
        migration_id: str,
        source_chat_id: str,
        source_message_id: str,
        attachment_index: int,
        target_chat_id: str,
        *,
        target_sync_id: str,
    ) -> AttachmentStageRecord:
        key = (
            migration_id,
            source_chat_id,
            source_message_id,
            attachment_index,
            target_chat_id,
        )
        async with self._lock:
            record = self._items[key]
            updated = replace(
                record,
                status=AttachmentStageStatus.ATTACHED,
                target_sync_id=target_sync_id,
                last_error_code=None,
                last_error_payload=None,
                updated_at=datetime.now(tz=UTC),
            )
            self._items[key] = updated
            self._processing_started_at.pop(key, None)
            return updated

    async def mark_failed(
        self,
        migration_id: str,
        source_chat_id: str,
        source_message_id: str,
        attachment_index: int,
        target_chat_id: str,
        *,
        error_code: str,
        error_payload: dict[str, object],
    ) -> AttachmentStageRecord:
        key = (
            migration_id,
            source_chat_id,
            source_message_id,
            attachment_index,
            target_chat_id,
        )
        async with self._lock:
            record = self._items[key]
            updated = replace(
                record,
                status=AttachmentStageStatus.FAILED,
                last_error_code=error_code,
                last_error_payload=error_payload,
                updated_at=datetime.now(tz=UTC),
            )
            self._items[key] = updated
            self._processing_started_at.pop(key, None)
            return updated


class InMemoryCheckpointRepository:
    def __init__(self) -> None:
        self._items: dict[tuple[str, str, str], MigrationCheckpoint] = {}
        self._lock = asyncio.Lock()

    async def get(
        self,
        migration_id: str,
        source_chat_id: str,
        mode: str,
    ) -> MigrationCheckpoint | None:
        async with self._lock:
            return self._items.get((migration_id, source_chat_id, mode))

    async def save(self, checkpoint: MigrationCheckpoint) -> None:
        async with self._lock:
            self._items[
                (checkpoint.migration_id, checkpoint.source_chat_id, checkpoint.mode)
            ] = checkpoint

class InMemoryInventorySnapshotRepository:
    def __init__(self) -> None:
        self._items: dict[tuple[str, str], InventorySnapshotRecord] = {}
        self._lock = asyncio.Lock()

    async def save(self, record: InventorySnapshotRecord) -> InventorySnapshotRecord:
        key = (record.migration_id, record.source_chat_id)
        now = datetime.now(tz=UTC)
        async with self._lock:
            updated = InventorySnapshotRecord(
                migration_id=record.migration_id,
                source_chat_id=record.source_chat_id,
                source_chat_type=record.source_chat_type,
                source_chat_title=record.source_chat_title,
                message_count=record.message_count,
                media_count=record.media_count,
                approximate_bytes=record.approximate_bytes,
                captured_at=record.captured_at or now,
            )
            self._items[key] = updated
            return updated

    async def replace_for_migration(
        self,
        migration_id: str,
        snapshots: list[InventorySnapshotRecord],
    ) -> None:
        async with self._lock:
            retained = {
                key: record
                for key, record in self._items.items()
                if key[0] != migration_id
            }
            for snapshot in snapshots:
                retained[(snapshot.migration_id, snapshot.source_chat_id)] = snapshot
            self._items = retained

    async def get(
        self,
        migration_id: str,
        source_chat_id: str,
    ) -> InventorySnapshotRecord | None:
        async with self._lock:
            return self._items.get((migration_id, source_chat_id))

    async def list_by_migration(self, migration_id: str) -> list[InventorySnapshotRecord]:
        async with self._lock:
            records = [
                record
                for (current_migration_id, _), record in self._items.items()
                if current_migration_id == migration_id
            ]
            records.sort(key=lambda item: item.source_chat_id)
            return records


class InMemoryMigrationStateRepository:
    def __init__(self) -> None:
        self._items: dict[str, MigrationStateRecord] = {}
        self._lock = asyncio.Lock()

    async def get(
        self,
        migration_id: str,
    ) -> MigrationStateRecord | None:
        async with self._lock:
            return self._items.get(migration_id)

    async def save(
        self,
        record: MigrationStateRecord,
    ) -> MigrationStateRecord:
        async with self._lock:
            self._items[record.migration_id] = record
            return record


class InMemoryIdentityMappingRepository:
    def __init__(self) -> None:
        self._items_by_user_id: dict[tuple[str | None, str], IdentityMappingRecord] = {}
        self._items_by_username: dict[tuple[str | None, str], IdentityMappingRecord] = {}
        self._lock = asyncio.Lock()

    async def get_by_user_id(
        self,
        telegram_user_id: str,
        *,
        express_host: str | None = None,
    ) -> IdentityMappingRecord | None:
        key = telegram_user_id.strip()
        async with self._lock:
            return self._items_by_user_id.get((express_host, key))

    async def get_by_username(
        self,
        telegram_username: str,
        *,
        express_host: str | None = None,
    ) -> IdentityMappingRecord | None:
        key = telegram_username.strip().lstrip("@").lower()
        async with self._lock:
            return self._items_by_username.get((express_host, key))

    async def save(
        self,
        record: IdentityMappingRecord,
    ) -> IdentityMappingRecord:
        normalized_user_id = (
            record.telegram_user_id.strip()
            if record.telegram_user_id is not None
            else None
        )
        normalized_host = record.express_host.strip().lower() if record.express_host else None
        normalized_username = (
            record.telegram_username.strip().lstrip("@").lower()
            if record.telegram_username is not None
            else None
        )
        now = datetime.now(tz=UTC)
        async with self._lock:
            existing_by_user_id = (
                self._items_by_user_id.get((normalized_host, normalized_user_id))
                if normalized_user_id is not None
                else None
            )
            existing_by_username = (
                self._items_by_username.get((normalized_host, normalized_username))
                if normalized_username is not None
                else None
            )
            existing = existing_by_user_id or existing_by_username
            updated = IdentityMappingRecord(
                express_host=normalized_host or (
                    existing.express_host if existing is not None else None
                ),
                telegram_user_id=normalized_user_id or (
                    existing.telegram_user_id if existing is not None else None
                ),
                telegram_username=normalized_username or (
                    existing.telegram_username if existing is not None else None
                ),
                telegram_display_name=record.telegram_display_name or (
                    existing.telegram_display_name if existing is not None else None
                ),
                corporate_email=record.corporate_email,
                target_huid=record.target_huid,
                created_at=existing.created_at if existing is not None else (record.created_at or now),
                updated_at=record.updated_at or now,
                last_resolved_at=record.last_resolved_at,
            )
            if existing_by_user_id is not None and existing_by_user_id.telegram_username:
                self._items_by_username.pop(
                    (existing_by_user_id.express_host, existing_by_user_id.telegram_username),
                    None,
                )
            if existing_by_username is not None and existing_by_username.telegram_user_id:
                self._items_by_user_id.pop(
                    (existing_by_username.express_host, existing_by_username.telegram_user_id),
                    None,
                )
            if updated.telegram_user_id is not None:
                self._items_by_user_id[(updated.express_host, updated.telegram_user_id)] = updated
            if updated.telegram_username is not None:
                self._items_by_username[(updated.express_host, updated.telegram_username)] = updated
            return updated


class InMemoryExpressUserCtsBindingRepository:
    def __init__(self) -> None:
        self._items_by_huid: dict[str, ExpressUserCtsBindingRecord] = {}
        self._items_by_email: dict[str, ExpressUserCtsBindingRecord] = {}
        self._lock = asyncio.Lock()

    async def get_by_target_huid(
        self,
        target_huid: str,
    ) -> ExpressUserCtsBindingRecord | None:
        normalized = target_huid.strip()
        async with self._lock:
            return self._items_by_huid.get(normalized)

    async def get_by_email(
        self,
        corporate_email: str,
    ) -> ExpressUserCtsBindingRecord | None:
        normalized = corporate_email.strip().lower()
        async with self._lock:
            return self._items_by_email.get(normalized)

    async def save(
        self,
        record: ExpressUserCtsBindingRecord,
    ) -> ExpressUserCtsBindingRecord:
        normalized_huid = record.target_huid.strip() if record.target_huid else None
        normalized_email = record.corporate_email.strip().lower() if record.corporate_email else None
        normalized_cts_host = record.cts_host.strip().lower()
        if not normalized_cts_host:
            raise ValueError("cts_host is required")
        if normalized_huid is None and normalized_email is None:
            raise ValueError("target_huid or corporate_email is required")
        now = datetime.now(tz=UTC)
        async with self._lock:
            existing_by_huid = (
                self._items_by_huid.get(normalized_huid)
                if normalized_huid is not None
                else None
            )
            existing_by_email = (
                self._items_by_email.get(normalized_email)
                if normalized_email is not None
                else None
            )
            existing = existing_by_huid or existing_by_email
            updated = ExpressUserCtsBindingRecord(
                target_huid=normalized_huid or (existing.target_huid if existing else None),
                corporate_email=normalized_email or (existing.corporate_email if existing else None),
                cts_host=normalized_cts_host,
                created_at=existing.created_at if existing is not None else (record.created_at or now),
                updated_at=record.updated_at or now,
                last_verified_at=record.last_verified_at,
            )
            if existing_by_huid is not None and existing_by_huid.corporate_email:
                self._items_by_email.pop(existing_by_huid.corporate_email, None)
            if existing_by_email is not None and existing_by_email.target_huid:
                self._items_by_huid.pop(existing_by_email.target_huid, None)
            if updated.target_huid is not None:
                self._items_by_huid[updated.target_huid] = updated
            if updated.corporate_email is not None:
                self._items_by_email[updated.corporate_email] = updated
            return updated


class InMemoryExpressBotHuidBindingRepository:
    def __init__(self) -> None:
        self._items_by_bot_id: dict[str, ExpressBotHuidBindingRecord] = {}
        self._lock = asyncio.Lock()

    async def get_by_bot_id(
        self,
        bot_id: str,
    ) -> ExpressBotHuidBindingRecord | None:
        normalized = bot_id.strip()
        async with self._lock:
            return self._items_by_bot_id.get(normalized)

    async def save(
        self,
        record: ExpressBotHuidBindingRecord,
    ) -> ExpressBotHuidBindingRecord:
        normalized_bot_id = record.bot_id.strip()
        normalized_cts_host = record.cts_host.strip().lower()
        normalized_bot_huid = record.bot_huid.strip()
        if not normalized_bot_id:
            raise ValueError("bot_id is required")
        if not normalized_cts_host:
            raise ValueError("cts_host is required")
        if not normalized_bot_huid:
            raise ValueError("bot_huid is required")
        now = datetime.now(tz=UTC)
        async with self._lock:
            existing = self._items_by_bot_id.get(normalized_bot_id)
            updated = ExpressBotHuidBindingRecord(
                bot_id=normalized_bot_id,
                cts_host=normalized_cts_host,
                bot_huid=normalized_bot_huid,
                created_at=existing.created_at if existing is not None else (record.created_at or now),
                updated_at=record.updated_at or now,
                last_learned_at=record.last_learned_at or now,
            )
            self._items_by_bot_id[normalized_bot_id] = updated
            return updated
