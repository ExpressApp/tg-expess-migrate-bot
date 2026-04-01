from __future__ import annotations

import asyncio
from collections import defaultdict
from dataclasses import dataclass, field, replace
from datetime import UTC, datetime

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
    MigrationStateRepository,
    TelegramGateway,
)
from extg_shared.utils.retry import AsyncRetryPolicy
from extg_shared.contracts.errors import (
    AmbiguousDeliveryError,
    ConfigurationError,
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
    MigrationLifecycleStatus,
    MigrationStateRecord,
    ReplyPreview,
    TelegramSourceMessage,
)


@dataclass(frozen=True, slots=True)
class DeltaSyncCommand:
    manifest: MigrationManifest
    source_chat_ids: list[str] | None = None
    batch_size: int = 100
    max_events: int | None = None


@dataclass(frozen=True, slots=True)
class DeltaSyncResult:
    migration_id: str
    source_chat_ids: list[str]
    status: str
    processed_count: int
    imported_count: int
    skipped_count: int
    failed_count: int
    ambiguous_count: int
    last_source_message_ids: dict[str, str | None] = field(default_factory=dict)
    last_source_message_id: str | None = None
    migration_state: MigrationLifecycleStatus | None = None
    reason: str | None = None


class DeltaSyncUseCase:
    def __init__(
        self,
        *,
        telegram_gateway: TelegramGateway,
        express_gateway: ExpressGateway,
        chat_mapping_repository: ChatMappingRepository,
        message_mapping_repository: MessageMappingRepository,
        checkpoint_repository: CheckpointRepository,
        migration_state_repository: MigrationStateRepository,
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
        self._migration_state_repository = migration_state_repository
        self._audit_repository = audit_repository
        self._normalizer = normalizer
        self._message_delivery_service = message_delivery_service
        self._target_chat_provisioning_service = target_chat_provisioning_service
        self._retry_policy = retry_policy
        self._checksum_service = checksum_service

    async def execute(self, command: DeltaSyncCommand) -> DeltaSyncResult:
        initial_state = await self._load_or_initialize_state(command.manifest.migration_id)
        source_chat_ids = self._resolve_source_chat_ids(command)
        target_chats: dict[str, ChatMappingRecord] = {}
        caches_by_chat: dict[str, dict[str, CanonicalMessage]] = {}
        last_source_message_ids: dict[str, str | None] = {}
        counts = {
            "processed": 0,
            "imported": 0,
            "skipped": 0,
            "failed": 0,
            "ambiguous": 0,
        }
        for source_chat_id in source_chat_ids:
            last_source_message_ids[source_chat_id] = None

        blocked = await self._blocked_result_if_needed(
            command=command,
            state=initial_state,
            source_chat_ids=source_chat_ids,
            counts=counts,
            last_source_message_ids=last_source_message_ids,
        )
        if blocked is not None:
            return blocked

        for source_chat_id in source_chat_ids:
            dialog = command.manifest.dialog_for(source_chat_id)
            target_chats[source_chat_id] = await self._ensure_target_chat(
                migration_id=command.manifest.migration_id,
                dialog=dialog,
            )
            caches_by_chat[source_chat_id] = {}

        await self._audit_repository.add(
            AuditEvent(
                migration_id=command.manifest.migration_id,
                event_type="delta_sync_started",
                severity=AuditSeverity.INFO,
                payload_json={
                    "source_chat_ids": source_chat_ids,
                    "batch_size": command.batch_size,
                    "max_events": command.max_events,
                },
                created_at=self._now(),
            ),
        )

        if command.max_events is not None and command.max_events <= 0:
            return await self._complete(
                command=command,
                source_chat_ids=source_chat_ids,
                counts=counts,
                last_source_message_ids=last_source_message_ids,
                status="max_events_reached",
        )

        for source_chat_id in source_chat_ids:
            runtime_state = await self._load_or_initialize_state(command.manifest.migration_id)
            blocked = await self._blocked_result_if_needed(
                command=command,
                state=runtime_state,
                source_chat_ids=source_chat_ids,
                counts=counts,
                last_source_message_ids=last_source_message_ids,
            )
            if blocked is not None:
                return blocked
            reached_limit = await self._catch_up_chat(
                command=command,
                source_chat_id=source_chat_id,
                dialog=command.manifest.dialog_for(source_chat_id),
                target_chat=target_chats[source_chat_id],
                cache=caches_by_chat[source_chat_id],
                counts=counts,
                last_source_message_ids=last_source_message_ids,
            )
            if reached_limit:
                return await self._complete(
                    command=command,
                    source_chat_ids=source_chat_ids,
                    counts=counts,
                    last_source_message_ids=last_source_message_ids,
                    status="max_events_reached",
                )

        dialogs_by_physical_chat_id = self._dialogs_by_physical_chat_id(
            command.manifest,
            source_chat_ids,
        )
        source_backends = {
            command.manifest.dialog_for(source_chat_id).source_backend
            for source_chat_id in source_chat_ids
        }
        if len(source_backends) != 1:
            raise ConfigurationError(
                "delta-sync currently requires all selected chats to use the same source_backend",
            )
        source_backend = next(iter(source_backends))

        delta_stream = self._telegram_gateway.subscribe_delta(
            sorted(dialogs_by_physical_chat_id),
            source_backend=source_backend,
        )
        try:
            async for event in delta_stream:
                runtime_state = await self._load_or_initialize_state(command.manifest.migration_id)
                blocked = await self._blocked_result_if_needed(
                    command=command,
                    state=runtime_state,
                    source_chat_ids=source_chat_ids,
                    counts=counts,
                    last_source_message_ids=last_source_message_ids,
                )
                if blocked is not None:
                    return blocked
                candidate_dialogs = dialogs_by_physical_chat_id.get(event.source_chat_id, ())
                for dialog in candidate_dialogs:
                    source_chat_id = dialog.source_chat_id
                    outcome = await self._process_message(
                        manifest=command.manifest,
                        dialog=dialog,
                        raw_message=event.source_message,
                        source_chat_title=(
                            dialog.source_thread_title or event.source_message.chat_title
                        ),
                        target_chat=target_chats[source_chat_id],
                        cache=caches_by_chat[source_chat_id],
                        advance_mode="delta",
                    )
                    if outcome == "ignored":
                        continue
                    counts[outcome] += 1
                    counts["processed"] += 1
                    last_source_message_ids[source_chat_id] = event.source_message.message_id
                    if command.max_events is not None and counts["processed"] >= command.max_events:
                        return await self._complete(
                            command=command,
                            source_chat_ids=source_chat_ids,
                            counts=counts,
                            last_source_message_ids=last_source_message_ids,
                            status="max_events_reached",
                        )
        finally:
            await delta_stream.aclose()

        return await self._complete(
            command=command,
            source_chat_ids=source_chat_ids,
            counts=counts,
            last_source_message_ids=last_source_message_ids,
            status="completed",
        )

    async def _catch_up_chat(
        self,
        *,
        command: DeltaSyncCommand,
        source_chat_id: str,
        dialog: ManifestDialog,
        target_chat: ChatMappingRecord,
        cache: dict[str, CanonicalMessage],
        counts: dict[str, int],
        last_source_message_ids: dict[str, str | None],
    ) -> bool:
        physical_source_chat_id = dialog.physical_source_chat_id
        cursor = await self._seed_delta_cursor(
            migration_id=command.manifest.migration_id,
            source_chat_id=source_chat_id,
        )
        while True:
            batch = await self._retry_policy.run(
                lambda: self._telegram_gateway.fetch_history(
                    physical_source_chat_id,
                    cursor,
                    command.batch_size,
                    source_backend=dialog.source_backend,
                    thread_id=dialog.source_thread_id,
                ),
            )
            if not batch.messages:
                return False

            migrate_media = self._resolve_migrate_media(
                manifest=command.manifest,
                dialog=dialog,
            )
            next_prefetch_task: asyncio.Task[tuple[PrefetchedAttachment, ...]] | None = None
            try:
                for index, raw_message in enumerate(batch.messages):
                    current_prefetch_task = next_prefetch_task
                    next_prefetch_task = None
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
                        next_prefetch_task = asyncio.create_task(
                            self._message_delivery_service.prefetch_attachments(
                                canonical=next_canonical,
                                source_backend=dialog.source_backend,
                                migrate_media=migrate_media,
                            ),
                        )
                        break
                    outcome = await self._process_message(
                        manifest=command.manifest,
                        dialog=dialog,
                        raw_message=raw_message,
                        source_chat_title=(
                            dialog.source_thread_title
                            or batch.dialog_title
                            or raw_message.chat_title
                        ),
                        target_chat=target_chat,
                        cache=cache,
                        advance_mode="delta",
                        prefetched_attachments=await self._resolve_prefetched_attachments(
                            current_prefetch_task,
                        ),
                    )
                    if outcome == "ignored":
                        last_source_message_ids[source_chat_id] = raw_message.message_id
                        continue
                    counts[outcome] += 1
                    counts["processed"] += 1
                    last_source_message_ids[source_chat_id] = raw_message.message_id
                    if command.max_events is not None and counts["processed"] >= command.max_events:
                        return True
            finally:
                await self._cleanup_prefetch_task(next_prefetch_task)

            if not batch.has_more:
                return False
            cursor = batch.next_cursor

    async def _process_message(
        self,
        *,
        manifest: MigrationManifest,
        dialog: ManifestDialog,
        raw_message: TelegramSourceMessage,
        source_chat_title: str | None,
        target_chat: ChatMappingRecord,
        cache: dict[str, CanonicalMessage],
        advance_mode: str,
        prefetched_attachments: tuple[PrefetchedAttachment, ...] | None = None,
    ) -> str:
        delivery_started = False
        try:
            canonical = await self._normalizer.normalize(raw_message)
            if canonical.source_chat_id != dialog.source_chat_id:
                canonical = replace(canonical, source_chat_id=dialog.source_chat_id)
            if not dialog.matches_source_message(
                source_message_id=canonical.source_message_id,
                source_thread_id=canonical.source_thread_id,
            ):
                await self._advance_checkpoint(
                    migration_id=manifest.migration_id,
                    source_chat_id=canonical.source_chat_id,
                    mode=advance_mode,
                    message=canonical,
                )
                return "ignored"
            cache[canonical.source_message_id] = canonical

            include_from = self._parse_optional_datetime(dialog.include_from)
            include_to = self._parse_optional_datetime(dialog.include_to)
            if include_from and canonical.sent_at_utc < include_from:
                await self._advance_checkpoint(
                    migration_id=manifest.migration_id,
                    source_chat_id=canonical.source_chat_id,
                    mode=advance_mode,
                    message=canonical,
                )
                return "skipped"
            if include_to and canonical.sent_at_utc > include_to:
                await self._advance_checkpoint(
                    migration_id=manifest.migration_id,
                    source_chat_id=canonical.source_chat_id,
                    mode=advance_mode,
                    message=canonical,
                )
                return "skipped"

            mapping_record = MessageMappingRecord(
                migration_id=manifest.migration_id,
                source_chat_id=canonical.source_chat_id,
                source_message_id=canonical.source_message_id,
                source_sent_at=canonical.sent_at_utc,
                target_chat_id=target_chat.target_chat_id,
                checksum=self._checksum_service.checksum(canonical),
                import_status=MessageImportStatus.PROCESSING,
            )
            claim = await self._message_mapping_repository.claim(mapping_record)
            if claim.state is ClaimState.IMPORTED:
                await self._advance_checkpoint(
                    migration_id=manifest.migration_id,
                    source_chat_id=canonical.source_chat_id,
                    mode=advance_mode,
                    message=canonical,
                )
                return "skipped"
            if claim.state is ClaimState.FAILED:
                await self._audit_repository.add(
                    AuditEvent(
                        migration_id=manifest.migration_id,
                        source_chat_id=canonical.source_chat_id,
                        source_message_id=canonical.source_message_id,
                        event_type="delta_existing_failed_message_skipped",
                        severity=AuditSeverity.WARNING,
                        payload_json={
                            "reason": "message is failed and requires replay-failed",
                        },
                        created_at=self._now(),
                    ),
                )
                await self._advance_checkpoint(
                    migration_id=manifest.migration_id,
                    source_chat_id=canonical.source_chat_id,
                    mode=advance_mode,
                    message=canonical,
                )
                return "skipped"
            if claim.state is ClaimState.AMBIGUOUS:
                await self._audit_repository.add(
                    AuditEvent(
                        migration_id=manifest.migration_id,
                        source_chat_id=canonical.source_chat_id,
                        source_message_id=canonical.source_message_id,
                        event_type="delta_existing_ambiguous_message_skipped",
                        severity=AuditSeverity.ERROR,
                        payload_json={
                            "reason": "message is ambiguous and requires manual reconciliation",
                        },
                        created_at=self._now(),
                    ),
                )
                await self._advance_checkpoint(
                    migration_id=manifest.migration_id,
                    source_chat_id=canonical.source_chat_id,
                    mode=advance_mode,
                    message=canonical,
                )
                return "ambiguous"

            reply_preview = self._build_reply_preview(canonical, cache)
            migrate_media = self._resolve_migrate_media(
                manifest=manifest,
                dialog=dialog,
            )
            reply_mode = self._resolve_reply_mode(
                manifest=manifest,
                dialog=dialog,
            )
            try:
                delivery_started = True
                delivery = await self._message_delivery_service.deliver(
                    migration_id=manifest.migration_id,
                    canonical=canonical,
                    source_backend=dialog.source_backend,
                    target_chat_id=target_chat.target_chat_id,
                    source_chat_title=source_chat_title,
                    reply_preview=reply_preview,
                    migrate_media=migrate_media,
                    reply_mode=reply_mode,
                    prefetched_attachments=prefetched_attachments,
                )
            except asyncio.CancelledError:
                await self._message_mapping_repository.mark_failed(
                    manifest.migration_id,
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
                    manifest.migration_id,
                    canonical.source_chat_id,
                    canonical.source_message_id,
                    error_code=type(error).__name__,
                    error_payload={"message": str(error)},
                )
                await self._audit_repository.add(
                    AuditEvent(
                        migration_id=manifest.migration_id,
                        source_chat_id=canonical.source_chat_id,
                        source_message_id=canonical.source_message_id,
                        event_type="delta_message_delivery_ambiguous",
                        severity=AuditSeverity.ERROR,
                        payload_json={"error": str(error)},
                        created_at=self._now(),
                    ),
                )
                await self._advance_checkpoint(
                    migration_id=manifest.migration_id,
                    source_chat_id=canonical.source_chat_id,
                    mode=advance_mode,
                    message=canonical,
                )
                return "ambiguous"
            except (RecoverableItemError, FatalItemError) as error:
                await self._message_mapping_repository.mark_failed(
                    manifest.migration_id,
                    canonical.source_chat_id,
                    canonical.source_message_id,
                    error_code=type(error).__name__,
                    error_payload={"message": str(error)},
                )
                await self._audit_repository.add(
                    AuditEvent(
                        migration_id=manifest.migration_id,
                        source_chat_id=canonical.source_chat_id,
                        source_message_id=canonical.source_message_id,
                        event_type="delta_message_import_failed",
                        severity=AuditSeverity.ERROR,
                        payload_json={"error": str(error)},
                        created_at=self._now(),
                    ),
                )
                await self._advance_checkpoint(
                    migration_id=manifest.migration_id,
                    source_chat_id=canonical.source_chat_id,
                    mode=advance_mode,
                    message=canonical,
                )
                return "failed"

            await self._message_mapping_repository.mark_imported(
                manifest.migration_id,
                canonical.source_chat_id,
                canonical.source_message_id,
                target_sync_id=delivery.sent_message.target_sync_id,
                rendered_body=delivery.rendered_body,
            )
            if delivery.has_blocking_attachment_failures:
                error_payload = self._attachment_failure_payload(delivery)
                if delivery.has_ambiguous_attachment_failures:
                    await self._message_mapping_repository.mark_ambiguous(
                        manifest.migration_id,
                        canonical.source_chat_id,
                        canonical.source_message_id,
                        error_code="AttachmentDeliveryAmbiguous",
                        error_payload=error_payload,
                    )
                    await self._audit_repository.add(
                        AuditEvent(
                            migration_id=manifest.migration_id,
                            source_chat_id=canonical.source_chat_id,
                            source_message_id=canonical.source_message_id,
                            event_type="delta_message_attachment_delivery_ambiguous",
                            severity=AuditSeverity.ERROR,
                            payload_json=error_payload,
                            created_at=self._now(),
                        ),
                    )
                    await self._advance_checkpoint(
                        migration_id=manifest.migration_id,
                        source_chat_id=canonical.source_chat_id,
                        mode=advance_mode,
                        message=canonical,
                    )
                    return "ambiguous"

                await self._message_mapping_repository.mark_failed(
                    manifest.migration_id,
                    canonical.source_chat_id,
                    canonical.source_message_id,
                    error_code="AttachmentDeliveryIncomplete",
                    error_payload=error_payload,
                )
                await self._audit_repository.add(
                    AuditEvent(
                        migration_id=manifest.migration_id,
                        source_chat_id=canonical.source_chat_id,
                        source_message_id=canonical.source_message_id,
                        event_type="delta_message_attachment_delivery_failed",
                        severity=AuditSeverity.ERROR,
                        payload_json=error_payload,
                        created_at=self._now(),
                    ),
                )
                await self._advance_checkpoint(
                    migration_id=manifest.migration_id,
                    source_chat_id=canonical.source_chat_id,
                    mode=advance_mode,
                    message=canonical,
                )
                return "failed"

            await self._advance_checkpoint(
                migration_id=manifest.migration_id,
                source_chat_id=canonical.source_chat_id,
                mode=advance_mode,
                message=canonical,
            )
            return "imported"
        finally:
            if not delivery_started:
                self._message_delivery_service.cleanup_prefetched_attachments(prefetched_attachments)

    async def _seed_delta_cursor(
        self,
        *,
        migration_id: str,
        source_chat_id: str,
    ) -> HistoryCursor:
        delta_checkpoint = await self._checkpoint_repository.get(
            migration_id,
            source_chat_id,
            "delta",
        )
        if delta_checkpoint is not None:
            return delta_checkpoint.cursor

        backfill_checkpoint = await self._checkpoint_repository.get(
            migration_id,
            source_chat_id,
            "backfill",
        )
        if backfill_checkpoint is not None:
            return backfill_checkpoint.cursor

        raise FatalItemError(
            "delta sync requires an existing backfill or delta checkpoint for "
            f"source_chat_id={source_chat_id}",
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
        *,
        migration_id: str,
        source_chat_id: str,
        mode: str,
        message: CanonicalMessage,
    ) -> HistoryCursor:
        cursor = HistoryCursor(
            last_source_message_id=message.source_message_id,
            last_source_sent_at=message.sent_at_utc,
        )
        await self._checkpoint_repository.save(
            MigrationCheckpoint(
                migration_id=migration_id,
                source_chat_id=source_chat_id,
                mode=mode,
                cursor=cursor,
                updated_at=self._now(),
            ),
        )
        return cursor

    async def _complete(
        self,
        *,
        command: DeltaSyncCommand,
        source_chat_ids: list[str],
        counts: dict[str, int],
        last_source_message_ids: dict[str, str | None],
        status: str,
        migration_state: MigrationLifecycleStatus | None = None,
        reason: str | None = None,
    ) -> DeltaSyncResult:
        await self._audit_repository.add(
            AuditEvent(
                migration_id=command.manifest.migration_id,
                event_type="delta_sync_completed",
                severity=AuditSeverity.INFO,
                payload_json={
                    "source_chat_ids": source_chat_ids,
                    "status": status,
                    "processed_count": counts["processed"],
                    "imported_count": counts["imported"],
                    "skipped_count": counts["skipped"],
                    "failed_count": counts["failed"],
                    "ambiguous_count": counts["ambiguous"],
                    "last_source_message_ids": last_source_message_ids,
                    "migration_state": migration_state.value
                    if migration_state is not None
                    else None,
                    "reason": reason,
                },
                created_at=self._now(),
            ),
        )
        return DeltaSyncResult(
            migration_id=command.manifest.migration_id,
            source_chat_ids=source_chat_ids,
            status=status,
            processed_count=counts["processed"],
            imported_count=counts["imported"],
            skipped_count=counts["skipped"],
            failed_count=counts["failed"],
            ambiguous_count=counts["ambiguous"],
            last_source_message_ids=last_source_message_ids,
            last_source_message_id=last_source_message_ids[source_chat_ids[0]]
            if len(source_chat_ids) == 1
            else None,
            migration_state=migration_state,
            reason=reason,
        )

    async def _load_or_initialize_state(
        self,
        migration_id: str,
    ) -> MigrationStateRecord:
        existing = await self._migration_state_repository.get(migration_id)
        if existing is not None:
            return existing
        now = self._now()
        return await self._migration_state_repository.save(
            MigrationStateRecord(
                migration_id=migration_id,
                status=MigrationLifecycleStatus.ACTIVE,
                created_at=now,
                updated_at=now,
            ),
        )

    async def _blocked_result_if_needed(
        self,
        *,
        command: DeltaSyncCommand,
        state: MigrationStateRecord,
        source_chat_ids: list[str],
        counts: dict[str, int],
        last_source_message_ids: dict[str, str | None],
    ) -> DeltaSyncResult | None:
        if state.status is MigrationLifecycleStatus.ACTIVE:
            return None

        status_map = {
            MigrationLifecycleStatus.PAUSED: ("paused", "migration is paused"),
            MigrationLifecycleStatus.NEEDS_MANUAL_RECONCILE: (
                "blocked_needs_manual_reconcile",
                "migration requires manual reconciliation before delta sync can continue",
            ),
            MigrationLifecycleStatus.COMPLETED: (
                "blocked_completed",
                "migration is already finalized",
            ),
        }
        status, reason = status_map[state.status]
        await self._audit_repository.add(
            AuditEvent(
                migration_id=command.manifest.migration_id,
                event_type="delta_sync_blocked_by_state",
                severity=AuditSeverity.WARNING,
                payload_json={
                    "migration_state": state.status.value,
                    "reason": reason,
                    "source_chat_ids": source_chat_ids,
                },
                created_at=self._now(),
            ),
        )
        return await self._complete(
            command=command,
            source_chat_ids=source_chat_ids,
            counts=counts,
            last_source_message_ids=last_source_message_ids,
            status=status,
            migration_state=state.status,
            reason=reason,
        )

    def _resolve_source_chat_ids(self, command: DeltaSyncCommand) -> list[str]:
        source_chat_ids = command.source_chat_ids or [
            dialog.source_chat_id
            for dialog in command.manifest.dialogs
        ]
        return list(dict.fromkeys(source_chat_ids))

    def _dialogs_by_physical_chat_id(
        self,
        manifest: MigrationManifest,
        source_chat_ids: list[str],
    ) -> dict[str, tuple[ManifestDialog, ...]]:
        dialogs_by_physical_chat_id: dict[str, list[ManifestDialog]] = defaultdict(list)
        for source_chat_id in source_chat_ids:
            dialog = manifest.dialog_for(source_chat_id)
            dialogs_by_physical_chat_id[dialog.physical_source_chat_id].append(dialog)
        return {
            dialog_id: tuple(dialogs)
            for dialog_id, dialogs in dialogs_by_physical_chat_id.items()
        }

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

    def _now(self) -> datetime:
        return datetime.now(tz=UTC)
