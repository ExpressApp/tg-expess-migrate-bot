from __future__ import annotations

import asyncio
from collections import defaultdict
from dataclasses import dataclass, replace
from datetime import UTC, datetime

from extg_migration_runtime.application.message_delivery import (
    MessageDeliveryResult,
    MessageDeliveryService,
    PrefetchedAttachment,
)
from extg_migration_runtime.application.normalizer import TelegramMessageNormalizer
from extg_shared.contracts.ports import (
    AuditRepository,
    ChatMappingRepository,
    MessageMappingRepository,
    TelegramGateway,
)
from extg_shared.utils.retry import AsyncRetryPolicy
from extg_shared.contracts.errors import (
    AmbiguousDeliveryError,
    FatalItemError,
    RecoverableItemError,
)
from extg_shared.contracts.manifest import ManifestDialog, MigrationManifest
from extg_shared.contracts.models import (
    AuditEvent,
    AuditSeverity,
    CanonicalMessage,
    HistoryCursor,
    MIGRATION_JOB_CANCELLED_ERROR_CODE,
    MessageImportStatus,
    MessageMappingRecord,
    ReplyPreview,
    SentMessageRef,
)


@dataclass(frozen=True, slots=True)
class ReplayFailedCommand:
    manifest: MigrationManifest
    source_chat_id: str | None = None
    limit: int = 100
    batch_size: int = 100


@dataclass(frozen=True, slots=True)
class ReplayFailedResult:
    migration_id: str
    requested_count: int
    recovered_count: int
    failed_count: int
    ambiguous_count: int
    missing_count: int
    skipped_count: int
    source_chat_ids: list[str]


class ReplayFailedUseCase:
    def __init__(
        self,
        *,
        telegram_gateway: TelegramGateway,
        chat_mapping_repository: ChatMappingRepository,
        message_mapping_repository: MessageMappingRepository,
        audit_repository: AuditRepository,
        normalizer: TelegramMessageNormalizer,
        message_delivery_service: MessageDeliveryService,
        retry_policy: AsyncRetryPolicy,
    ) -> None:
        self._telegram_gateway = telegram_gateway
        self._chat_mapping_repository = chat_mapping_repository
        self._message_mapping_repository = message_mapping_repository
        self._audit_repository = audit_repository
        self._normalizer = normalizer
        self._message_delivery_service = message_delivery_service
        self._retry_policy = retry_policy

    async def execute(self, command: ReplayFailedCommand) -> ReplayFailedResult:
        failed_records = await self._message_mapping_repository.list_failed(
            command.manifest.migration_id,
            source_chat_id=command.source_chat_id,
            limit=command.limit,
        )
        if not failed_records:
            return ReplayFailedResult(
                migration_id=command.manifest.migration_id,
                requested_count=0,
                recovered_count=0,
                failed_count=0,
                ambiguous_count=0,
                missing_count=0,
                skipped_count=0,
                source_chat_ids=[],
            )

        for record in failed_records:
            command.manifest.dialog_for(record.source_chat_id)

        grouped_records: dict[str, list[MessageMappingRecord]] = defaultdict(list)
        for record in failed_records:
            grouped_records[record.source_chat_id].append(record)

        recovered = 0
        failed = 0
        ambiguous = 0
        missing = 0
        skipped = 0

        for source_chat_id, records in grouped_records.items():
            records.sort(key=lambda item: (item.source_sent_at, item.source_message_id))
            counts = await self._replay_chat(
                migration_id=command.manifest.migration_id,
                source_chat_id=source_chat_id,
                dialog=command.manifest.dialog_for(source_chat_id),
                records=records,
                manifest=command.manifest,
                batch_size=command.batch_size,
            )
            await self._mark_chat_completed_if_clean(
                migration_id=command.manifest.migration_id,
                source_chat_id=source_chat_id,
            )
            recovered += counts["recovered"]
            failed += counts["failed"]
            ambiguous += counts["ambiguous"]
            missing += counts["missing"]
            skipped += counts["skipped"]

        await self._audit_repository.add(
            AuditEvent(
                migration_id=command.manifest.migration_id,
                event_type="failed_message_replay_completed",
                severity=AuditSeverity.INFO,
                payload_json={
                    "requested_count": len(failed_records),
                    "recovered_count": recovered,
                    "failed_count": failed,
                    "ambiguous_count": ambiguous,
                    "missing_count": missing,
                    "skipped_count": skipped,
                    "source_chat_ids": sorted(grouped_records),
                },
                created_at=self._now(),
            ),
        )
        return ReplayFailedResult(
            migration_id=command.manifest.migration_id,
            requested_count=len(failed_records),
            recovered_count=recovered,
            failed_count=failed,
            ambiguous_count=ambiguous,
            missing_count=missing,
            skipped_count=skipped,
            source_chat_ids=sorted(grouped_records),
        )

    async def _replay_chat(
        self,
        *,
        migration_id: str,
        source_chat_id: str,
        dialog: ManifestDialog,
        records: list[MessageMappingRecord],
        manifest: MigrationManifest,
        batch_size: int,
    ) -> dict[str, int]:
        records_by_message_id = {
            record.source_message_id: record
            for record in records
        }
        remaining_message_ids = set(records_by_message_id)
        cache: dict[str, CanonicalMessage] = {}
        cursor: HistoryCursor | None = None
        counts = {
            "recovered": 0,
            "failed": 0,
            "ambiguous": 0,
            "missing": 0,
            "skipped": 0,
        }
        migrate_media = self._resolve_migrate_media(manifest=manifest, dialog=dialog)
        reply_mode = self._resolve_reply_mode(manifest=manifest, dialog=dialog)

        while remaining_message_ids:
            batch = await self._retry_policy.run(
                lambda: self._telegram_gateway.fetch_history(
                    dialog.physical_source_chat_id,
                    cursor,
                    batch_size,
                    source_backend=dialog.source_backend,
                ),
            )
            if not batch.messages:
                break

            next_prefetch_task: asyncio.Task[tuple[PrefetchedAttachment, ...]] | None = None
            try:
                for index, raw_message in enumerate(batch.messages):
                    current_prefetch_task = next_prefetch_task
                    next_prefetch_task = None
                    canonical = await self._normalizer.normalize(raw_message)
                    if canonical.source_chat_id != dialog.source_chat_id:
                        canonical = replace(canonical, source_chat_id=dialog.source_chat_id)
                    if not dialog.matches_source_message(
                        source_message_id=canonical.source_message_id,
                        source_thread_id=canonical.source_thread_id,
                    ):
                        await self._cleanup_prefetch_task(current_prefetch_task)
                        continue
                    cache[canonical.source_message_id] = canonical
                    if canonical.source_message_id not in remaining_message_ids:
                        await self._cleanup_prefetch_task(current_prefetch_task)
                        continue
                    for next_raw_message in batch.messages[index + 1 :]:
                        next_canonical = await self._normalizer.normalize(next_raw_message)
                        if next_canonical.source_chat_id != dialog.source_chat_id:
                            next_canonical = replace(
                                next_canonical,
                                source_chat_id=dialog.source_chat_id,
                            )
                        if not dialog.matches_source_message(
                            source_message_id=next_canonical.source_message_id,
                            source_thread_id=next_canonical.source_thread_id,
                        ):
                            continue
                        if next_canonical.source_message_id not in remaining_message_ids:
                            continue
                        next_prefetch_task = asyncio.create_task(
                            self._message_delivery_service.prefetch_attachments(
                                canonical=next_canonical,
                                source_backend=dialog.source_backend,
                                migrate_media=migrate_media,
                            ),
                        )
                        break

                    current_record = await self._message_mapping_repository.get(
                        migration_id,
                        canonical.source_chat_id,
                        canonical.source_message_id,
                    )
                    if current_record is None:
                        await self._cleanup_prefetch_task(current_prefetch_task)
                        counts["missing"] += 1
                        remaining_message_ids.discard(canonical.source_message_id)
                        continue
                    if current_record.import_status is not MessageImportStatus.FAILED:
                        await self._cleanup_prefetch_task(current_prefetch_task)
                        counts["skipped"] += 1
                        remaining_message_ids.discard(canonical.source_message_id)
                        continue

                    reply_preview = self._build_reply_preview(canonical, cache)
                    prefetched_attachments = await self._resolve_prefetched_attachments(
                        current_prefetch_task,
                    )
                    outcome = await self._replay_message(
                        migration_id=migration_id,
                        canonical=canonical,
                        current_record=current_record,
                        source_backend=dialog.source_backend,
                        target_chat_id=current_record.target_chat_id,
                        source_chat_title=(
                            dialog.source_thread_title
                            or batch.dialog_title
                            or raw_message.chat_title
                        ),
                        reply_preview=reply_preview,
                        migrate_media=migrate_media,
                        reply_mode=reply_mode,
                        prefetched_attachments=prefetched_attachments,
                    )
                    counts[outcome] += 1
                    remaining_message_ids.discard(canonical.source_message_id)
            finally:
                await self._cleanup_prefetch_task(next_prefetch_task)

            if not batch.has_more:
                break
            cursor = batch.next_cursor

        for source_message_id in sorted(remaining_message_ids):
            await self._audit_repository.add(
                AuditEvent(
                    migration_id=migration_id,
                    source_chat_id=source_chat_id,
                    source_message_id=source_message_id,
                    event_type="failed_message_replay_missing",
                    severity=AuditSeverity.ERROR,
                    payload_json={
                        "reason": "source message not found during replay scan",
                    },
                    created_at=self._now(),
                ),
            )
            counts["missing"] += 1

        return counts

    async def _mark_chat_completed_if_clean(
        self,
        *,
        migration_id: str,
        source_chat_id: str,
    ) -> None:
        existing = await self._chat_mapping_repository.get(migration_id, source_chat_id)
        if existing is None or existing.status == "completed":
            return
        summaries = await self._message_mapping_repository.summarize_by_chat(migration_id)
        has_rows = False
        for summary in summaries:
            if summary.source_chat_id != source_chat_id:
                continue
            has_rows = True
            if summary.import_status in {
                MessageImportStatus.FAILED,
                MessageImportStatus.AMBIGUOUS,
                MessageImportStatus.PROCESSING,
            }:
                return
        if not has_rows:
            return
        await self._chat_mapping_repository.save(
            replace(
                existing,
                status="completed",
                updated_at=self._now(),
            ),
        )

    async def _replay_message(
        self,
        *,
        migration_id: str,
        canonical: CanonicalMessage,
        current_record: MessageMappingRecord,
        source_backend: str,
        target_chat_id: str,
        source_chat_title: str | None,
        reply_preview: ReplyPreview | None,
        migrate_media: bool,
        reply_mode: str,
        prefetched_attachments: tuple[PrefetchedAttachment, ...] | None = None,
    ) -> str:
        existing_primary_message: SentMessageRef | None = None
        if current_record.target_sync_id:
            existing_primary_message = SentMessageRef(
                target_chat_id=current_record.target_chat_id,
                target_sync_id=current_record.target_sync_id,
                deduplicated=True,
            )
        try:
            delivery = await self._message_delivery_service.deliver(
                migration_id=migration_id,
                canonical=canonical,
                source_backend=source_backend,
                target_chat_id=target_chat_id,
                source_chat_title=source_chat_title,
                reply_preview=reply_preview,
                migrate_media=migrate_media,
                reply_mode=reply_mode,
                existing_primary_message=existing_primary_message,
                existing_rendered_body=current_record.rendered_body,
                retry_failed_attachments=True,
                prefetched_attachments=prefetched_attachments,
            )
        except asyncio.CancelledError:
            await self._message_mapping_repository.mark_failed(
                migration_id,
                canonical.source_chat_id,
                canonical.source_message_id,
                error_code=MIGRATION_JOB_CANCELLED_ERROR_CODE,
                error_payload={
                    "message": "message delivery cancelled",
                    "phase": "delivery",
                },
            )
            raise
        except AmbiguousDeliveryError as error:
            await self._message_mapping_repository.mark_ambiguous(
                migration_id,
                canonical.source_chat_id,
                canonical.source_message_id,
                error_code=type(error).__name__,
                error_payload={"message": str(error)},
            )
            await self._audit_repository.add(
                AuditEvent(
                    migration_id=migration_id,
                    source_chat_id=canonical.source_chat_id,
                    source_message_id=canonical.source_message_id,
                    event_type="failed_message_replay_ambiguous",
                    severity=AuditSeverity.ERROR,
                    payload_json={"error": str(error)},
                    created_at=self._now(),
                ),
            )
            return "ambiguous"
        except (RecoverableItemError, FatalItemError) as error:
            await self._message_mapping_repository.mark_failed(
                migration_id,
                canonical.source_chat_id,
                canonical.source_message_id,
                error_code=type(error).__name__,
                error_payload={"message": str(error)},
            )
            await self._audit_repository.add(
                AuditEvent(
                    migration_id=migration_id,
                    source_chat_id=canonical.source_chat_id,
                    source_message_id=canonical.source_message_id,
                    event_type="failed_message_replay_failed",
                    severity=AuditSeverity.ERROR,
                    payload_json={"error": str(error)},
                    created_at=self._now(),
                ),
            )
            return "failed"

        await self._message_mapping_repository.mark_imported(
            migration_id,
            canonical.source_chat_id,
            canonical.source_message_id,
            target_sync_id=delivery.sent_message.target_sync_id,
            rendered_body=delivery.rendered_body,
        )
        if delivery.has_blocking_attachment_failures:
            error_payload = self._attachment_failure_payload(delivery)
            if delivery.has_ambiguous_attachment_failures:
                await self._message_mapping_repository.mark_ambiguous(
                    migration_id,
                    canonical.source_chat_id,
                    canonical.source_message_id,
                    error_code="AttachmentDeliveryAmbiguous",
                    error_payload=error_payload,
                )
                await self._audit_repository.add(
                    AuditEvent(
                        migration_id=migration_id,
                        source_chat_id=canonical.source_chat_id,
                        source_message_id=canonical.source_message_id,
                        event_type="failed_message_replay_attachment_ambiguous",
                        severity=AuditSeverity.ERROR,
                        payload_json=error_payload,
                        created_at=self._now(),
                    ),
                )
                return "ambiguous"
            await self._message_mapping_repository.mark_failed(
                migration_id,
                canonical.source_chat_id,
                canonical.source_message_id,
                error_code="AttachmentDeliveryIncomplete",
                error_payload=error_payload,
            )
            await self._audit_repository.add(
                AuditEvent(
                    migration_id=migration_id,
                    source_chat_id=canonical.source_chat_id,
                    source_message_id=canonical.source_message_id,
                    event_type="failed_message_replay_attachment_failed",
                    severity=AuditSeverity.ERROR,
                    payload_json=error_payload,
                    created_at=self._now(),
                ),
            )
            return "failed"

        await self._audit_repository.add(
            AuditEvent(
                migration_id=migration_id,
                source_chat_id=canonical.source_chat_id,
                source_message_id=canonical.source_message_id,
                event_type="failed_message_replay_imported",
                severity=AuditSeverity.INFO,
                payload_json={
                    "target_chat_id": target_chat_id,
                    "target_sync_id": delivery.sent_message.target_sync_id,
                },
                created_at=self._now(),
            ),
        )
        return "recovered"

    def _build_reply_preview(
        self,
        message: CanonicalMessage,
        cache: dict[str, CanonicalMessage],
    ) -> ReplyPreview | None:
        if not message.source_reply_to_message_id:
            return None
        reply_target = cache.get(message.source_reply_to_message_id)
        if not reply_target:
            return ReplyPreview(
                sent_at_utc=None,
                author_display_name=f"source message #{message.source_reply_to_message_id}",
                excerpt=None,
            )
        excerpt = reply_target.body_plain or reply_target.body_rendered or ""
        excerpt = excerpt.strip().replace("\n", " ")
        if len(excerpt) > 120:
            excerpt = f"{excerpt[:117]}..."
        return ReplyPreview(
            sent_at_utc=reply_target.sent_at_utc,
            author_display_name=reply_target.sender_display_name,
            excerpt=excerpt or None,
        )

    def _now(self) -> datetime:
        return datetime.now(tz=UTC)

    def _resolve_migrate_media(
        self,
        *,
        manifest: MigrationManifest,
        dialog: ManifestDialog,
    ) -> bool:
        if dialog.migrate_media is not None:
            return dialog.migrate_media
        return manifest.defaults.migrate_media

    def _resolve_reply_mode(
        self,
        *,
        manifest: MigrationManifest,
        dialog: ManifestDialog,
    ) -> str:
        if dialog.reply_mode is not None:
            return dialog.reply_mode
        return manifest.defaults.reply_mode

    def _attachment_failure_payload(
        self,
        delivery: MessageDeliveryResult,
    ) -> dict[str, object]:
        blocking = delivery.blocking_attachment_failures
        return {
            "reason": "one or more attachments require replay",
            "attachments": [
                {
                    "attachment_index": outcome.attachment_index,
                    "status": outcome.import_status.value,
                    "phase": outcome.phase,
                    "reason": outcome.reason,
                }
                for outcome in blocking
            ],
        }

    async def _resolve_prefetched_attachments(
        self,
        task: asyncio.Task[tuple[PrefetchedAttachment, ...]] | None,
    ) -> tuple[PrefetchedAttachment, ...] | None:
        if task is None:
            return None
        try:
            return await task
        except Exception:
            return None

    async def _cleanup_prefetch_task(
        self,
        task: asyncio.Task[tuple[PrefetchedAttachment, ...]] | None,
    ) -> None:
        prefetched = await self._resolve_prefetched_attachments(task)
        self._message_delivery_service.cleanup_prefetched_attachments(prefetched)
