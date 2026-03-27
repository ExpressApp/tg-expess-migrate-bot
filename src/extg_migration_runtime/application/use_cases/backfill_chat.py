from __future__ import annotations

import asyncio
from dataclasses import dataclass, replace
from datetime import UTC, datetime
from typing import Any

from extg_migration_runtime.application.message_checksum import MessageChecksumService
from extg_migration_runtime.application.message_delivery import (
    MessageDeliveryResult,
    MessageDeliveryService,
    PrefetchedAttachment,
)
from extg_migration_runtime.application.normalizer import TelegramMessageNormalizer
from extg_migration_runtime.application.target_chat_provisioning import (
    TargetChatProvisioningService,
)
from extg_shared.contracts.ports import (
    AuditRepository,
    ChatMappingRepository,
    CheckpointRepository,
    ExpressGateway,
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
    ChatMappingRecord,
    ClaimState,
    HistoryCursor,
    MessageImportStatus,
    MessageMappingRecord,
    MIGRATION_JOB_CANCELLED_ERROR_CODE,
    MigrationCheckpoint,
    ReplyPreview,
)


@dataclass(frozen=True, slots=True)
class BackfillChatCommand:
    manifest: MigrationManifest
    source_chat_id: str
    batch_size: int = 100


@dataclass(frozen=True, slots=True)
class BackfillChatResult:
    migration_id: str
    source_chat_id: str
    target_chat_id: str
    imported_count: int
    skipped_count: int
    failed_count: int
    last_source_message_id: str | None


@dataclass(frozen=True, slots=True)
class BackfillBatchEntry:
    raw_message: Any
    canonical: CanonicalMessage
    source_chat_title: str | None
    should_deliver: bool
    stop_after: bool = False


class BackfillChatUseCase:
    def __init__(
        self,
        *,
        telegram_gateway: TelegramGateway,
        express_gateway: ExpressGateway,
        chat_mapping_repository: ChatMappingRepository,
        message_mapping_repository: MessageMappingRepository,
        checkpoint_repository: CheckpointRepository,
        audit_repository: AuditRepository,
        normalizer: TelegramMessageNormalizer,
        message_delivery_service: MessageDeliveryService,
        target_chat_provisioning_service: TargetChatProvisioningService,
        retry_policy: AsyncRetryPolicy,
        checksum_service: MessageChecksumService,
    ) -> None:
        self._telegram_gateway = telegram_gateway
        self._express_gateway = express_gateway
        self._chat_mapping_repository = chat_mapping_repository
        self._message_mapping_repository = message_mapping_repository
        self._checkpoint_repository = checkpoint_repository
        self._audit_repository = audit_repository
        self._normalizer = normalizer
        self._message_delivery_service = message_delivery_service
        self._target_chat_provisioning_service = target_chat_provisioning_service
        self._retry_policy = retry_policy
        self._checksum_service = checksum_service

    async def execute(self, command: BackfillChatCommand) -> BackfillChatResult:
        dialog = command.manifest.dialog_for(command.source_chat_id)
        physical_source_chat_id = dialog.physical_source_chat_id
        target_chat = await self._ensure_target_chat(
            migration_id=command.manifest.migration_id,
            dialog=dialog,
        )
        include_from = self._parse_optional_datetime(dialog.include_from)
        include_to = self._parse_optional_datetime(dialog.include_to)
        checkpoint = await self._checkpoint_repository.get(
            command.manifest.migration_id,
            command.source_chat_id,
            "backfill",
        )
        cursor = checkpoint.cursor if checkpoint else None
        imported = 0
        skipped = 0
        failed = 0
        cache: dict[str, CanonicalMessage] = {}
        last_message_id: str | None = cursor.last_source_message_id if cursor else None
        stop_requested = False

        while not stop_requested:
            batch = await self._retry_policy.run(
                lambda: self._telegram_gateway.fetch_history(
                    physical_source_chat_id,
                    cursor,
                    command.batch_size,
                    source_backend=dialog.source_backend,
                ),
            )
            if not batch.messages:
                break

            entries = await self._build_batch_entries(
                command=command,
                dialog=dialog,
                batch=batch,
                include_from=include_from,
                include_to=include_to,
                cache=cache,
            )
            next_prefetch_task: asyncio.Task[tuple[PrefetchedAttachment, ...]] | None = None
            try:
                for index, entry in enumerate(entries):
                    current_prefetch_task = next_prefetch_task
                    next_prefetch_task = None
                    if index + 1 < len(entries):
                        next_entry = entries[index + 1]
                        if next_entry.should_deliver:
                            next_prefetch_task = asyncio.create_task(
                                self._message_delivery_service.prefetch_attachments(
                                    canonical=next_entry.canonical,
                                    source_backend=dialog.source_backend,
                                    migrate_media=self._resolve_migrate_media(
                                        manifest=command.manifest,
                                        dialog=dialog,
                                    ),
                                ),
                            )
                    canonical = entry.canonical
                    if not entry.should_deliver:
                        await self._cleanup_prefetch_task(current_prefetch_task)
                        cursor = await self._advance_checkpoint(command, canonical)
                        last_message_id = canonical.source_message_id
                        if entry.stop_after:
                            stop_requested = True
                            break
                        continue
                    mapping_record = MessageMappingRecord(
                        migration_id=command.manifest.migration_id,
                        source_chat_id=canonical.source_chat_id,
                        source_message_id=canonical.source_message_id,
                        source_sent_at=canonical.sent_at_utc,
                        target_chat_id=target_chat.target_chat_id,
                        checksum=self._checksum_service.checksum(canonical),
                        import_status=MessageImportStatus.PROCESSING,
                    )
                    claim = await self._message_mapping_repository.claim(mapping_record)
                    if claim.state is ClaimState.IMPORTED:
                        await self._cleanup_prefetch_task(current_prefetch_task)
                        skipped += 1
                        cursor = await self._advance_checkpoint(command, canonical)
                        last_message_id = canonical.source_message_id
                        continue
                    if claim.state is ClaimState.FAILED:
                        await self._cleanup_prefetch_task(current_prefetch_task)
                        skipped += 1
                        cursor = await self._advance_checkpoint(command, canonical)
                        last_message_id = canonical.source_message_id
                        continue
                    if claim.state is ClaimState.AMBIGUOUS:
                        await self._cleanup_prefetch_task(current_prefetch_task)
                        await self._audit_repository.add(
                            AuditEvent(
                                migration_id=command.manifest.migration_id,
                                source_chat_id=canonical.source_chat_id,
                                source_message_id=canonical.source_message_id,
                                event_type="ambiguous_delivery_state",
                                severity=AuditSeverity.ERROR,
                                payload_json={
                                    "reason": "message is still marked as processing",
                                },
                                created_at=self._now(),
                            ),
                        )
                        raise AmbiguousDeliveryError(
                            "message is stuck in processing state; manual reconciliation required",
                        )

                    reply_preview = self._build_reply_preview(canonical, cache)
                    migrate_media = self._resolve_migrate_media(
                        manifest=command.manifest,
                        dialog=dialog,
                    )
                    reply_mode = self._resolve_reply_mode(
                        manifest=command.manifest,
                        dialog=dialog,
                    )

                    prefetched_attachments = await self._resolve_prefetched_attachments(
                        current_prefetch_task,
                    )
                    try:
                        delivery = await self._message_delivery_service.deliver(
                            migration_id=command.manifest.migration_id,
                            canonical=canonical,
                            source_backend=dialog.source_backend,
                            target_chat_id=target_chat.target_chat_id,
                            source_chat_title=entry.source_chat_title,
                            reply_preview=reply_preview,
                            migrate_media=migrate_media,
                            reply_mode=reply_mode,
                            prefetched_attachments=prefetched_attachments,
                        )
                    except asyncio.CancelledError:
                        await self._message_mapping_repository.mark_failed(
                            command.manifest.migration_id,
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
                            command.manifest.migration_id,
                            canonical.source_chat_id,
                            canonical.source_message_id,
                            error_code=type(error).__name__,
                            error_payload={"message": str(error)},
                        )
                        await self._audit_repository.add(
                            AuditEvent(
                                migration_id=command.manifest.migration_id,
                                source_chat_id=canonical.source_chat_id,
                                source_message_id=canonical.source_message_id,
                                event_type="message_delivery_ambiguous",
                                severity=AuditSeverity.ERROR,
                                payload_json={"error": str(error)},
                                created_at=self._now(),
                            ),
                        )
                        raise
                    except (RecoverableItemError, FatalItemError) as error:
                        await self._message_mapping_repository.mark_failed(
                            command.manifest.migration_id,
                            canonical.source_chat_id,
                            canonical.source_message_id,
                            error_code=type(error).__name__,
                            error_payload={"message": str(error)},
                        )
                        await self._audit_repository.add(
                            AuditEvent(
                                migration_id=command.manifest.migration_id,
                                source_chat_id=canonical.source_chat_id,
                                source_message_id=canonical.source_message_id,
                                event_type="message_import_failed",
                                severity=AuditSeverity.ERROR,
                                payload_json={"error": str(error)},
                                created_at=self._now(),
                            ),
                        )
                        failed += 1
                        cursor = await self._advance_checkpoint(command, canonical)
                        last_message_id = canonical.source_message_id
                        continue

                    await self._message_mapping_repository.mark_imported(
                        command.manifest.migration_id,
                        canonical.source_chat_id,
                        canonical.source_message_id,
                        target_sync_id=delivery.sent_message.target_sync_id,
                        rendered_body=delivery.rendered_body,
                    )
                    if delivery.has_blocking_attachment_failures:
                        error_payload = self._attachment_failure_payload(delivery)
                        if delivery.has_ambiguous_attachment_failures:
                            await self._message_mapping_repository.mark_ambiguous(
                                command.manifest.migration_id,
                                canonical.source_chat_id,
                                canonical.source_message_id,
                                error_code="AttachmentDeliveryAmbiguous",
                                error_payload=error_payload,
                            )
                            await self._audit_repository.add(
                                AuditEvent(
                                    migration_id=command.manifest.migration_id,
                                    source_chat_id=canonical.source_chat_id,
                                    source_message_id=canonical.source_message_id,
                                    event_type="message_attachment_delivery_ambiguous",
                                    severity=AuditSeverity.ERROR,
                                    payload_json=error_payload,
                                    created_at=self._now(),
                                ),
                            )
                            raise AmbiguousDeliveryError(
                                "attachment delivery is ambiguous after primary message import",
                            )
                        await self._message_mapping_repository.mark_failed(
                            command.manifest.migration_id,
                            canonical.source_chat_id,
                            canonical.source_message_id,
                            error_code="AttachmentDeliveryIncomplete",
                            error_payload=error_payload,
                        )
                        await self._audit_repository.add(
                            AuditEvent(
                                migration_id=command.manifest.migration_id,
                                source_chat_id=canonical.source_chat_id,
                                source_message_id=canonical.source_message_id,
                                event_type="message_attachment_delivery_failed",
                                severity=AuditSeverity.ERROR,
                                payload_json=error_payload,
                                created_at=self._now(),
                            ),
                        )
                        failed += 1
                        cursor = await self._advance_checkpoint(command, canonical)
                        last_message_id = canonical.source_message_id
                        continue
                    cursor = await self._advance_checkpoint(command, canonical)
                    last_message_id = canonical.source_message_id
                    imported += 1
            finally:
                await self._cleanup_prefetch_task(next_prefetch_task)

            if not batch.has_more:
                break

        if failed == 0 and target_chat.status != "completed":
            target_chat = replace(
                target_chat,
                status="completed",
                updated_at=self._now(),
            )
            await self._chat_mapping_repository.save(target_chat)

        return BackfillChatResult(
            migration_id=command.manifest.migration_id,
            source_chat_id=command.source_chat_id,
            target_chat_id=target_chat.target_chat_id,
            imported_count=imported,
            skipped_count=skipped,
            failed_count=failed,
            last_source_message_id=last_message_id,
        )

    async def _ensure_target_chat(
        self,
        *,
        migration_id: str,
        dialog: ManifestDialog,
    ) -> ChatMappingRecord:
        return await self._target_chat_provisioning_service.ensure_target_chat(
            migration_id=migration_id,
            dialog=dialog,
        )

    async def _advance_checkpoint(
        self,
        command: BackfillChatCommand,
        message: CanonicalMessage,
    ) -> HistoryCursor:
        cursor = HistoryCursor(
            last_source_message_id=message.source_message_id,
            last_source_sent_at=message.sent_at_utc,
        )
        await self._checkpoint_repository.save(
            MigrationCheckpoint(
                migration_id=command.manifest.migration_id,
                source_chat_id=command.source_chat_id,
                mode="backfill",
                cursor=cursor,
                updated_at=self._now(),
            ),
        )
        return cursor

    async def _build_batch_entries(
        self,
        *,
        command: BackfillChatCommand,
        dialog: ManifestDialog,
        batch: Any,
        include_from: datetime | None,
        include_to: datetime | None,
        cache: dict[str, CanonicalMessage],
    ) -> list[BackfillBatchEntry]:
        entries: list[BackfillBatchEntry] = []
        for raw_message in batch.messages:
            canonical = await self._normalizer.normalize(raw_message)
            if canonical.source_chat_id != command.source_chat_id:
                canonical = replace(canonical, source_chat_id=command.source_chat_id)

            should_deliver = dialog.matches_source_message(
                source_message_id=canonical.source_message_id,
                source_thread_id=canonical.source_thread_id,
            )
            if should_deliver:
                cache[canonical.source_message_id] = canonical
            stop_after = False
            if should_deliver and include_from and canonical.sent_at_utc < include_from:
                should_deliver = False
            if should_deliver and include_to and canonical.sent_at_utc > include_to:
                should_deliver = False
                stop_after = True

            entries.append(
                BackfillBatchEntry(
                    raw_message=raw_message,
                    canonical=canonical,
                    source_chat_title=(
                        dialog.source_thread_title
                        or batch.dialog_title
                        or raw_message.chat_title
                    ),
                    should_deliver=should_deliver,
                    stop_after=stop_after,
                ),
            )
            if stop_after:
                break
        return entries

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

    def _parse_optional_datetime(self, value: str | None) -> datetime | None:
        if value is None:
            return None
        normalized = value.replace("Z", "+00:00")
        parsed = datetime.fromisoformat(normalized)
        if parsed.tzinfo is None:
            return parsed.replace(tzinfo=UTC)
        return parsed.astimezone(UTC)

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
