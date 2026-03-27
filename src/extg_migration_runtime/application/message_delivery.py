from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from dataclasses import dataclass, replace
from datetime import UTC, datetime
import mimetypes
from pathlib import Path
from typing import Any

from extg_shared.contracts.ports import (
    AttachmentMappingRepository,
    AttachmentStageRepository,
    AuditRepository,
    ExpressFileStore,
    ExpressGateway,
    TelegramGateway,
)
from extg_migration_runtime.application.renderer import MessageRenderer
from extg_shared.utils.retry import AsyncRetryPolicy
from extg_shared.contracts.errors import (
    AmbiguousDeliveryError,
    FatalItemError,
    RecoverableItemError,
)
from extg_shared.contracts.models import (
    AttachmentImportStatus,
    AttachmentMappingRecord,
    AttachmentStageRecord,
    AttachmentStageStatus,
    AuditEvent,
    AuditSeverity,
    CanonicalAttachment,
    CanonicalMessage,
    ClaimState,
    DownloadedAttachment,
    ExpressStagedFile,
    FilePayload,
    MIGRATION_JOB_CANCELLED_ERROR_CODE,
    MIGRATION_JOB_CANCEL_REQUESTED_ERROR_CODE,
    ReplyPreview,
    SentMessageRef,
)


@dataclass(frozen=True, slots=True)
class PreparedAttachment:
    attachment_index: int
    attachment: CanonicalAttachment
    file_payload: FilePayload


@dataclass(frozen=True, slots=True)
class PrefetchedAttachment:
    attachment_index: int
    attachment: CanonicalAttachment
    downloaded_attachment: DownloadedAttachment | None = None
    error: RecoverableItemError | FatalItemError | None = None
    skipped_reason: str | None = None


@dataclass(frozen=True, slots=True)
class AttachmentDeliveryOutcome:
    attachment_index: int
    attachment: CanonicalAttachment
    import_status: AttachmentImportStatus
    phase: str
    blocks_message_completion: bool
    target_sync_id: str | None = None
    reason: str | None = None


@dataclass(frozen=True, slots=True)
class AttachmentUploadResolution:
    staged_file: ExpressStagedFile | None = None
    outcome: AttachmentDeliveryOutcome | None = None
    use_direct_file_fallback: bool = False


@dataclass(frozen=True, slots=True)
class MessageDeliveryResult:
    canonical: CanonicalMessage
    rendered_body: str
    sent_message: SentMessageRef
    attachment_outcomes: list[AttachmentDeliveryOutcome]
    primary_reused: bool = False

    @property
    def blocking_attachment_failures(self) -> list[AttachmentDeliveryOutcome]:
        return [
            outcome
            for outcome in self.attachment_outcomes
            if outcome.blocks_message_completion
        ]

    @property
    def has_blocking_attachment_failures(self) -> bool:
        return bool(self.blocking_attachment_failures)

    @property
    def has_ambiguous_attachment_failures(self) -> bool:
        return any(
            outcome.import_status is AttachmentImportStatus.AMBIGUOUS
            for outcome in self.blocking_attachment_failures
        )


class MessageDeliveryService:
    def __init__(
        self,
        *,
        telegram_gateway: TelegramGateway,
        express_gateway: ExpressGateway,
        express_file_store: ExpressFileStore,
        attachment_mapping_repository: AttachmentMappingRepository,
        attachment_stage_repository: AttachmentStageRepository,
        audit_repository: AuditRepository,
        renderer: MessageRenderer,
        retry_policy: AsyncRetryPolicy,
        logger: Any | None = None,
        max_attachment_upload_size_bytes: int = 100 * 1024 * 1024,
        attachment_transfer_semaphore: asyncio.Semaphore | None = None,
    ) -> None:
        self._telegram_gateway = telegram_gateway
        self._express_gateway = express_gateway
        self._express_file_store = express_file_store
        self._attachment_mapping_repository = attachment_mapping_repository
        self._attachment_stage_repository = attachment_stage_repository
        self._audit_repository = audit_repository
        self._renderer = renderer
        self._retry_policy = retry_policy
        self._logger = logger
        self._max_attachment_upload_size_bytes = max_attachment_upload_size_bytes
        self._attachment_transfer_semaphore = attachment_transfer_semaphore

    async def deliver(
        self,
        *,
        migration_id: str,
        canonical: CanonicalMessage,
        source_backend: str,
        target_chat_id: str,
        source_chat_title: str | None,
        reply_preview: ReplyPreview | None,
        migrate_media: bool,
        reply_mode: str = "inline_quote",
        existing_primary_message: SentMessageRef | None = None,
        existing_rendered_body: str | None = None,
        retry_failed_attachments: bool = False,
        prefetched_attachments: tuple[PrefetchedAttachment, ...] | None = None,
    ) -> MessageDeliveryResult:
        prepared_message, prepared_attachments, outcomes_by_index = await self._prepare_message(
            migration_id=migration_id,
            canonical=canonical,
            source_backend=source_backend,
            migrate_media=migrate_media,
            target_chat_id=target_chat_id,
            reuse_existing_primary=existing_primary_message is not None,
            prefetched_attachments=prefetched_attachments,
        )
        inline_attachment = (
            prepared_attachments[0]
            if existing_primary_message is None and len(prepared_attachments) == 1
            else None
        )
        followup_attachments = [] if inline_attachment is not None else prepared_attachments
        inline_upload_resolution = AttachmentUploadResolution()
        try:
            if inline_attachment is not None:
                inline_upload_resolution = await self._ensure_uploaded_attachment(
                    migration_id=migration_id,
                    canonical=canonical,
                    target_chat_id=target_chat_id,
                    prepared_attachment=inline_attachment,
                    retry_failed=retry_failed_attachments,
                )
                if inline_upload_resolution.outcome is not None:
                    outcomes_by_index[inline_attachment.attachment_index] = (
                        inline_upload_resolution.outcome
                    )

            if existing_primary_message is None:
                rendered = self._renderer.render(
                    prepared_message,
                    source_chat_title=source_chat_title,
                    reply_preview=reply_preview,
                    reply_mode=reply_mode,
                )
                rendered_body = rendered.render_text()
                try:
                    sent_message = await self._retry_policy.run(
                        lambda: self._express_gateway.send_message(
                            target_chat_id,
                            rendered_body,
                            idempotency_key=canonical.idempotency_key,
                            file=(
                                inline_attachment.file_payload
                                if inline_attachment is not None
                                and inline_upload_resolution.use_direct_file_fallback
                                else None
                            ),
                            staged_file=inline_upload_resolution.staged_file,
                            fallback_file=(
                                inline_attachment.file_payload
                                if inline_upload_resolution.staged_file is not None
                                and inline_attachment is not None
                                else None
                            ),
                        ),
                    )
                except Exception as error:
                    self._log(
                        "warning",
                        "primary message delivery failed",
                        migration_id=migration_id,
                        source_chat_id=canonical.source_chat_id,
                        source_message_id=canonical.source_message_id,
                        target_chat_id=target_chat_id,
                        has_inline_attachment=inline_attachment is not None,
                        inline_attachment_index=(
                            inline_attachment.attachment_index
                            if inline_attachment is not None
                            else None
                        ),
                        inline_media_kind=(
                            inline_attachment.attachment.media_kind
                            if inline_attachment is not None
                            else None
                        ),
                        error_type=type(error).__name__,
                        error=str(error),
                    )
                    raise
                primary_reused = False
            else:
                rendered_body = existing_rendered_body or self._renderer.render(
                    prepared_message,
                    source_chat_title=source_chat_title,
                    reply_preview=reply_preview,
                    reply_mode=reply_mode,
                ).render_text()
                sent_message = existing_primary_message
                primary_reused = True

            if inline_attachment is not None:
                if (
                    inline_upload_resolution.staged_file is not None
                    or inline_upload_resolution.use_direct_file_fallback
                ):
                    outcome = await self._mark_inline_attachment_imported(
                        migration_id=migration_id,
                        canonical=canonical,
                        target_chat_id=target_chat_id,
                        prepared_attachment=inline_attachment,
                        target_sync_id=sent_message.target_sync_id,
                        mark_stage_attached=inline_upload_resolution.staged_file is not None,
                        delivery_mode=(
                            "inline_primary_staged"
                            if inline_upload_resolution.staged_file is not None
                            else "inline_primary_direct_file"
                        ),
                    )
                    outcomes_by_index[inline_attachment.attachment_index] = outcome
        finally:
            if inline_attachment is not None:
                self._cleanup_file_payload(inline_attachment.file_payload)

        for prepared_attachment in followup_attachments:
            outcome = await self._deliver_attachment(
                migration_id=migration_id,
                canonical=canonical,
                target_chat_id=target_chat_id,
                prepared_attachment=prepared_attachment,
                retry_failed=retry_failed_attachments,
            )
            outcomes_by_index[prepared_attachment.attachment_index] = outcome

        return MessageDeliveryResult(
            canonical=prepared_message,
            rendered_body=rendered_body,
            sent_message=sent_message,
            attachment_outcomes=[
                outcome
                for _, outcome in sorted(
                    outcomes_by_index.items(),
                    key=lambda item: item[0],
                )
            ],
            primary_reused=primary_reused,
        )

    async def prefetch_attachments(
        self,
        *,
        canonical: CanonicalMessage,
        source_backend: str,
        migrate_media: bool,
    ) -> tuple[PrefetchedAttachment, ...]:
        if not migrate_media or not canonical.attachments:
            return ()

        prefetched: list[PrefetchedAttachment] = []
        for attachment_index, attachment in enumerate(canonical.attachments):
            limit_reason = self._attachment_size_limit_reason(
                attachment=attachment,
                size_bytes=attachment.size_bytes,
            )
            if limit_reason is not None:
                prefetched.append(
                    PrefetchedAttachment(
                        attachment_index=attachment_index,
                        attachment=attachment,
                        skipped_reason=limit_reason,
                    ),
                )
                continue
            try:
                async with self._attachment_transfer_slot():
                    downloaded_attachment = await self._retry_policy.run(
                        lambda attachment=attachment: self._telegram_gateway.download_attachment(
                            attachment,
                            source_backend=source_backend,
                        ),
                    )
            except (RecoverableItemError, FatalItemError) as error:
                prefetched.append(
                    PrefetchedAttachment(
                        attachment_index=attachment_index,
                        attachment=attachment,
                        error=error,
                    ),
                )
                continue

            downloaded_size = self._downloaded_attachment_size_bytes(downloaded_attachment)
            limit_reason = self._attachment_size_limit_reason(
                attachment=downloaded_attachment.attachment,
                size_bytes=downloaded_size,
            )
            if limit_reason is not None:
                self._cleanup_downloaded_attachment(downloaded_attachment)
                prefetched.append(
                    PrefetchedAttachment(
                        attachment_index=attachment_index,
                        attachment=replace(
                            downloaded_attachment.attachment,
                            size_bytes=downloaded_size,
                        ),
                        skipped_reason=limit_reason,
                    ),
                )
                continue

            prefetched.append(
                PrefetchedAttachment(
                    attachment_index=attachment_index,
                    attachment=downloaded_attachment.attachment,
                    downloaded_attachment=downloaded_attachment,
                ),
            )

        return tuple(prefetched)

    def cleanup_prefetched_attachments(
        self,
        prefetched_attachments: tuple[PrefetchedAttachment, ...] | None,
    ) -> None:
        if not prefetched_attachments:
            return
        for prefetched in prefetched_attachments:
            if prefetched.downloaded_attachment is None:
                continue
            self._cleanup_downloaded_attachment(prefetched.downloaded_attachment)

    async def _prepare_message(
        self,
        *,
        migration_id: str,
        canonical: CanonicalMessage,
        source_backend: str,
        migrate_media: bool,
        target_chat_id: str,
        reuse_existing_primary: bool,
        prefetched_attachments: tuple[PrefetchedAttachment, ...] | None = None,
    ) -> tuple[CanonicalMessage, list[PreparedAttachment], dict[int, AttachmentDeliveryOutcome]]:
        if not canonical.attachments:
            return canonical, [], {}

        prepared_attachments: list[PreparedAttachment] = []
        outcomes_by_index: dict[int, AttachmentDeliveryOutcome] = {}
        failures = list(canonical.attachment_failures)
        prefetched_by_index = {
            item.attachment_index: item
            for item in (prefetched_attachments or ())
        }

        if not migrate_media:
            for attachment_index, attachment in enumerate(canonical.attachments):
                record = await self._claim_terminal_attachment(
                    migration_id=migration_id,
                    canonical=canonical,
                    attachment_index=attachment_index,
                    attachment=attachment,
                    target_chat_id=target_chat_id,
                )
                if record.import_status is AttachmentImportStatus.SKIPPED:
                    outcomes_by_index[attachment_index] = self._outcome_from_record(
                        attachment=attachment,
                        record=record,
                    )
                    continue
                record = await self._attachment_mapping_repository.mark_skipped(
                    migration_id,
                    canonical.source_chat_id,
                    canonical.source_message_id,
                    attachment_index,
                    attachment=attachment,
                    reason="media migration disabled by manifest",
                )
                await self._audit_repository.add(
                    AuditEvent(
                        migration_id=migration_id,
                        source_chat_id=canonical.source_chat_id,
                        source_message_id=canonical.source_message_id,
                        event_type="attachment_skipped_by_policy",
                        severity=AuditSeverity.INFO,
                        payload_json=self._attachment_payload(
                            attachment,
                            reason="media migration disabled by manifest",
                        ),
                        created_at=self._now(),
                    ),
                )
                outcomes_by_index[attachment_index] = self._outcome_from_record(
                    attachment=attachment,
                    record=record,
                )
            return replace(canonical, attachments=[]), [], outcomes_by_index

        for attachment_index, attachment in enumerate(canonical.attachments):
            prefetched = prefetched_by_index.get(attachment_index)
            existing_record = await self._attachment_mapping_repository.get(
                migration_id,
                canonical.source_chat_id,
                canonical.source_message_id,
                attachment_index,
            )
            if reuse_existing_primary and existing_record is not None:
                existing_outcome = self._outcome_from_record(
                    attachment=attachment,
                    record=existing_record,
                )
                if not existing_outcome.blocks_message_completion:
                    outcomes_by_index[attachment_index] = existing_outcome
                    continue
            if prefetched is not None and prefetched.skipped_reason is not None:
                limit_reason = prefetched.skipped_reason
            else:
                limit_reason = self._attachment_size_limit_reason(
                    attachment=attachment,
                    size_bytes=attachment.size_bytes,
                )
            if limit_reason is not None:
                failures.append(limit_reason)
                record = await self._mark_attachment_skipped(
                    migration_id=migration_id,
                    canonical=canonical,
                    attachment_index=attachment_index,
                    attachment=attachment,
                    target_chat_id=target_chat_id,
                    reason=limit_reason,
                )
                outcomes_by_index[attachment_index] = self._outcome_from_record(
                    attachment=attachment,
                    record=record,
                )
                continue
            downloaded_attachment: DownloadedAttachment | None = None
            error: RecoverableItemError | FatalItemError | None = None
            if prefetched is not None:
                downloaded_attachment = prefetched.downloaded_attachment
                error = prefetched.error
            else:
                try:
                    async with self._attachment_transfer_slot():
                        downloaded_attachment = await self._retry_policy.run(
                            lambda attachment=attachment: self._telegram_gateway.download_attachment(
                                attachment,
                                source_backend=source_backend,
                            ),
                        )
                except (RecoverableItemError, FatalItemError) as raised_error:
                    error = raised_error
            if error is not None:
                reason = str(error)
                failures.append(reason)
                record = await self._claim_terminal_attachment(
                    migration_id=migration_id,
                    canonical=canonical,
                    attachment_index=attachment_index,
                    attachment=attachment,
                    target_chat_id=target_chat_id,
                    retry_failed=not reuse_existing_primary,
                )
                if record.import_status is not AttachmentImportStatus.FAILED:
                    record = await self._attachment_mapping_repository.mark_failed(
                        migration_id,
                        canonical.source_chat_id,
                        canonical.source_message_id,
                        attachment_index,
                        attachment=attachment,
                        error_code=type(error).__name__,
                        error_payload={
                            "message": reason,
                            "phase": "download",
                        },
                    )
                await self._audit_repository.add(
                    AuditEvent(
                        migration_id=migration_id,
                        source_chat_id=canonical.source_chat_id,
                        source_message_id=canonical.source_message_id,
                        event_type="attachment_download_failed",
                        severity=AuditSeverity.WARNING,
                        payload_json=self._attachment_payload(
                            attachment,
                            reason=reason,
                        ),
                        created_at=self._now(),
                    ),
                )
                self._log(
                    "warning",
                    "attachment download failed",
                    migration_id=migration_id,
                    source_chat_id=canonical.source_chat_id,
                    source_message_id=canonical.source_message_id,
                    target_chat_id=target_chat_id,
                    attachment_index=attachment_index,
                    media_kind=attachment.media_kind,
                    filename=attachment.filename,
                    error_type=type(error).__name__,
                    error=reason,
                )
                outcomes_by_index[attachment_index] = self._outcome_from_record(
                    attachment=attachment,
                    record=record,
                )
                continue
            if downloaded_attachment is None:
                raise FatalItemError("attachment preparation has no downloaded payload")
            downloaded_size = self._downloaded_attachment_size_bytes(downloaded_attachment)
            limit_reason = self._attachment_size_limit_reason(
                attachment=downloaded_attachment.attachment,
                size_bytes=downloaded_size,
            )
            if limit_reason is not None:
                failures.append(limit_reason)
                self._cleanup_downloaded_attachment(downloaded_attachment)
                record = await self._mark_attachment_skipped(
                    migration_id=migration_id,
                    canonical=canonical,
                    attachment_index=attachment_index,
                    attachment=replace(
                        downloaded_attachment.attachment,
                        size_bytes=downloaded_size,
                    ),
                    target_chat_id=target_chat_id,
                    reason=limit_reason,
                )
                outcomes_by_index[attachment_index] = self._outcome_from_record(
                    attachment=attachment,
                    record=record,
                )
                continue
            try:
                file_payload, prepared_attachment = self._to_file_payload(
                    downloaded_attachment,
                )
            except (RecoverableItemError, FatalItemError) as error:
                reason = str(error)
                failures.append(reason)
                record = await self._claim_terminal_attachment(
                    migration_id=migration_id,
                    canonical=canonical,
                    attachment_index=attachment_index,
                    attachment=attachment,
                    target_chat_id=target_chat_id,
                    retry_failed=not reuse_existing_primary,
                )
                if record.import_status is not AttachmentImportStatus.FAILED:
                    record = await self._attachment_mapping_repository.mark_failed(
                        migration_id,
                        canonical.source_chat_id,
                        canonical.source_message_id,
                        attachment_index,
                        attachment=downloaded_attachment.attachment,
                        error_code=type(error).__name__,
                        error_payload={
                            "message": reason,
                            "phase": "prepare",
                        },
                    )
                await self._audit_repository.add(
                    AuditEvent(
                        migration_id=migration_id,
                        source_chat_id=canonical.source_chat_id,
                        source_message_id=canonical.source_message_id,
                        event_type="attachment_prepare_failed",
                        severity=AuditSeverity.WARNING,
                        payload_json=self._attachment_payload(
                            downloaded_attachment.attachment,
                            reason=reason,
                        ),
                        created_at=self._now(),
                    ),
                )
                self._log(
                    "warning",
                    "attachment prepare failed",
                    migration_id=migration_id,
                    source_chat_id=canonical.source_chat_id,
                    source_message_id=canonical.source_message_id,
                    target_chat_id=target_chat_id,
                    attachment_index=attachment_index,
                    media_kind=downloaded_attachment.attachment.media_kind,
                    filename=downloaded_attachment.attachment.filename,
                    error_type=type(error).__name__,
                    error=reason,
                )
                outcomes_by_index[attachment_index] = self._outcome_from_record(
                    attachment=attachment,
                    record=record,
                )
                continue
            limit_reason = self._attachment_size_limit_reason(
                attachment=prepared_attachment,
                size_bytes=self._file_payload_size_bytes(file_payload),
            )
            if limit_reason is not None:
                failures.append(limit_reason)
                record = await self._mark_attachment_skipped(
                    migration_id=migration_id,
                    canonical=canonical,
                    attachment_index=attachment_index,
                    attachment=replace(
                        prepared_attachment,
                        size_bytes=self._file_payload_size_bytes(file_payload),
                    ),
                    target_chat_id=target_chat_id,
                    reason=limit_reason,
                )
                outcomes_by_index[attachment_index] = self._outcome_from_record(
                    attachment=attachment,
                    record=record,
                )
                continue

            prepared_attachments.append(
                PreparedAttachment(
                    attachment_index=attachment_index,
                    attachment=prepared_attachment,
                    file_payload=file_payload,
                ),
            )

        return (
            replace(
                canonical,
                attachments=[],
                attachment_failures=failures,
            ),
            prepared_attachments,
            outcomes_by_index,
        )

    async def _deliver_attachment(
        self,
        *,
        migration_id: str,
        canonical: CanonicalMessage,
        target_chat_id: str,
        prepared_attachment: PreparedAttachment,
        retry_failed: bool,
    ) -> AttachmentDeliveryOutcome:
        existing_record = await self._attachment_mapping_repository.get(
            migration_id,
            canonical.source_chat_id,
            canonical.source_message_id,
            prepared_attachment.attachment_index,
        )
        try:
            if existing_record is not None:
                existing_outcome = self._outcome_from_record(
                    attachment=prepared_attachment.attachment,
                    record=existing_record,
                )
                if (
                    existing_record.import_status in (
                        AttachmentImportStatus.IMPORTED,
                        AttachmentImportStatus.SKIPPED,
                    )
                    or (
                        existing_record.import_status is AttachmentImportStatus.FAILED
                        and not retry_failed
                        and existing_record.last_error_code
                        not in {
                            MIGRATION_JOB_CANCELLED_ERROR_CODE,
                            MIGRATION_JOB_CANCEL_REQUESTED_ERROR_CODE,
                        }
                    )
                    or (
                        existing_record.import_status is AttachmentImportStatus.FAILED
                        and retry_failed
                        and existing_outcome.phase not in {"send", "upload"}
                    )
                ):
                    return existing_outcome
                if existing_record.import_status is AttachmentImportStatus.AMBIGUOUS:
                    raise AmbiguousDeliveryError(
                        "attachment is in ambiguous state; manual reconciliation required",
                    )

            claim = await self._attachment_mapping_repository.claim(
                self._build_attachment_record(
                    migration_id=migration_id,
                    canonical=canonical,
                    target_chat_id=target_chat_id,
                    attachment_index=prepared_attachment.attachment_index,
                    attachment=prepared_attachment.attachment,
                ),
                retry_failed=retry_failed,
            )
            if claim.state is ClaimState.IMPORTED:
                return self._outcome_from_record(
                    attachment=prepared_attachment.attachment,
                    record=claim.record,
                )
            if claim.state is ClaimState.FAILED:
                return self._outcome_from_record(
                    attachment=prepared_attachment.attachment,
                    record=claim.record,
                )
            if claim.state is ClaimState.AMBIGUOUS:
                if claim.record.import_status is AttachmentImportStatus.SKIPPED:
                    return self._outcome_from_record(
                        attachment=prepared_attachment.attachment,
                        record=claim.record,
                    )
                raise AmbiguousDeliveryError(
                    "attachment is already being processed or is ambiguous",
                )

            upload_resolution = await self._ensure_uploaded_attachment(
                migration_id=migration_id,
                canonical=canonical,
                target_chat_id=target_chat_id,
                prepared_attachment=prepared_attachment,
                retry_failed=retry_failed,
            )
            if upload_resolution.outcome is not None:
                return upload_resolution.outcome
            if upload_resolution.staged_file is None and not upload_resolution.use_direct_file_fallback:
                return self._outcome_from_record(
                    attachment=prepared_attachment.attachment,
                    record=claim.record,
                )

            try:
                sent_message = await self._retry_policy.run(
                    lambda: self._express_gateway.send_message(
                        target_chat_id,
                        self._render_attachment_body(
                            prepared_attachment.attachment,
                            attachment_index=prepared_attachment.attachment_index,
                        ),
                        idempotency_key=self._attachment_idempotency_key(
                            canonical=canonical,
                            attachment_index=prepared_attachment.attachment_index,
                        ),
                        file=(
                            prepared_attachment.file_payload
                            if upload_resolution.use_direct_file_fallback
                            else None
                        ),
                        staged_file=upload_resolution.staged_file,
                        fallback_file=(
                            prepared_attachment.file_payload
                            if upload_resolution.staged_file is not None
                            else None
                        ),
                    ),
                )
            except AmbiguousDeliveryError as error:
                record = await self._attachment_mapping_repository.mark_ambiguous(
                    migration_id,
                    canonical.source_chat_id,
                    canonical.source_message_id,
                    prepared_attachment.attachment_index,
                    attachment=prepared_attachment.attachment,
                    error_code=type(error).__name__,
                    error_payload={
                        "message": str(error),
                        "phase": "send",
                    },
                )
                await self._audit_repository.add(
                    AuditEvent(
                        migration_id=migration_id,
                        source_chat_id=canonical.source_chat_id,
                        source_message_id=canonical.source_message_id,
                        event_type="attachment_send_ambiguous",
                        severity=AuditSeverity.ERROR,
                        payload_json=self._attachment_payload(
                            prepared_attachment.attachment,
                            reason=str(error),
                        ),
                        created_at=self._now(),
                    ),
                )
                self._log(
                    "error",
                    "attachment send result is ambiguous",
                    migration_id=migration_id,
                    source_chat_id=canonical.source_chat_id,
                    source_message_id=canonical.source_message_id,
                    target_chat_id=target_chat_id,
                    attachment_index=prepared_attachment.attachment_index,
                    media_kind=prepared_attachment.attachment.media_kind,
                    filename=prepared_attachment.attachment.filename,
                    error_type=type(error).__name__,
                    error=str(error),
                )
                return self._outcome_from_record(
                    attachment=prepared_attachment.attachment,
                    record=record,
                )
            except (RecoverableItemError, FatalItemError) as error:
                record = await self._attachment_mapping_repository.mark_failed(
                    migration_id,
                    canonical.source_chat_id,
                    canonical.source_message_id,
                    prepared_attachment.attachment_index,
                    attachment=prepared_attachment.attachment,
                    error_code=type(error).__name__,
                    error_payload={
                        "message": str(error),
                        "phase": "send",
                    },
                )
                await self._audit_repository.add(
                    AuditEvent(
                        migration_id=migration_id,
                        source_chat_id=canonical.source_chat_id,
                        source_message_id=canonical.source_message_id,
                        event_type="attachment_send_failed",
                        severity=AuditSeverity.ERROR,
                        payload_json=self._attachment_payload(
                            prepared_attachment.attachment,
                            reason=str(error),
                        ),
                        created_at=self._now(),
                    ),
                )
                self._log(
                    "error",
                    "attachment send failed",
                    migration_id=migration_id,
                    source_chat_id=canonical.source_chat_id,
                    source_message_id=canonical.source_message_id,
                    target_chat_id=target_chat_id,
                    attachment_index=prepared_attachment.attachment_index,
                    media_kind=prepared_attachment.attachment.media_kind,
                    filename=prepared_attachment.attachment.filename,
                    error_type=type(error).__name__,
                    error=str(error),
                )
                return self._outcome_from_record(
                    attachment=prepared_attachment.attachment,
                    record=record,
                )

            record = await self._attachment_mapping_repository.mark_imported(
                migration_id,
                canonical.source_chat_id,
                canonical.source_message_id,
                prepared_attachment.attachment_index,
                attachment=prepared_attachment.attachment,
                target_sync_id=sent_message.target_sync_id,
            )
            if upload_resolution.staged_file is not None:
                await self._attachment_stage_repository.mark_attached(
                    migration_id,
                    canonical.source_chat_id,
                    canonical.source_message_id,
                    prepared_attachment.attachment_index,
                    target_chat_id,
                    target_sync_id=sent_message.target_sync_id,
                )
            await self._audit_repository.add(
                AuditEvent(
                    migration_id=migration_id,
                    source_chat_id=canonical.source_chat_id,
                    source_message_id=canonical.source_message_id,
                    event_type="attachment_imported",
                    severity=AuditSeverity.INFO,
                    payload_json={
                        "target_chat_id": target_chat_id,
                        "target_sync_id": sent_message.target_sync_id,
                        "attachment_index": prepared_attachment.attachment_index,
                        "filename": prepared_attachment.attachment.filename,
                        "media_kind": prepared_attachment.attachment.media_kind,
                        "size_bytes": prepared_attachment.attachment.size_bytes,
                        "sha256": prepared_attachment.attachment.sha256,
                        "delivery_mode": (
                            "staged_file"
                            if upload_resolution.staged_file is not None
                            else "direct_file_fallback"
                        ),
                    },
                    created_at=self._now(),
                ),
            )
            self._log(
                "info",
                "attachment imported",
                migration_id=migration_id,
                source_chat_id=canonical.source_chat_id,
                source_message_id=canonical.source_message_id,
                target_chat_id=target_chat_id,
                target_sync_id=sent_message.target_sync_id,
                attachment_index=prepared_attachment.attachment_index,
                media_kind=prepared_attachment.attachment.media_kind,
                filename=prepared_attachment.attachment.filename,
            )
            return self._outcome_from_record(
                attachment=prepared_attachment.attachment,
                record=record,
            )
        except asyncio.CancelledError:
            await self._attachment_mapping_repository.mark_failed(
                migration_id,
                canonical.source_chat_id,
                canonical.source_message_id,
                prepared_attachment.attachment_index,
                attachment=prepared_attachment.attachment,
                error_code=MIGRATION_JOB_CANCELLED_ERROR_CODE,
                error_payload={
                    "message": "attachment delivery cancelled",
                    "phase": "delivery",
                },
            )
            stage_record = await self._attachment_stage_repository.get(
                migration_id,
                canonical.source_chat_id,
                canonical.source_message_id,
                prepared_attachment.attachment_index,
                target_chat_id,
            )
            if stage_record is not None and stage_record.status is AttachmentStageStatus.PROCESSING:
                await self._attachment_stage_repository.mark_failed(
                    migration_id,
                    canonical.source_chat_id,
                    canonical.source_message_id,
                    prepared_attachment.attachment_index,
                    target_chat_id,
                    error_code=MIGRATION_JOB_CANCELLED_ERROR_CODE,
                    error_payload={
                        "message": "attachment upload cancelled",
                        "phase": "upload",
                    },
                )
            raise
        finally:
            self._cleanup_file_payload(prepared_attachment.file_payload)

    async def _mark_inline_attachment_imported(
        self,
        *,
        migration_id: str,
        canonical: CanonicalMessage,
        target_chat_id: str,
        prepared_attachment: PreparedAttachment,
        target_sync_id: str,
        mark_stage_attached: bool,
        delivery_mode: str,
    ) -> AttachmentDeliveryOutcome:
        claim = await self._attachment_mapping_repository.claim(
            self._build_attachment_record(
                migration_id=migration_id,
                canonical=canonical,
                target_chat_id=target_chat_id,
                attachment_index=prepared_attachment.attachment_index,
                attachment=prepared_attachment.attachment,
            ),
            retry_failed=True,
        )
        if claim.state is ClaimState.AMBIGUOUS:
            if claim.record.import_status is AttachmentImportStatus.SKIPPED:
                return self._outcome_from_record(
                    attachment=prepared_attachment.attachment,
                    record=claim.record,
                )
            raise AmbiguousDeliveryError(
                "attachment is already being processed or is ambiguous",
            )
        if claim.state is ClaimState.IMPORTED:
            return self._outcome_from_record(
                attachment=prepared_attachment.attachment,
                record=claim.record,
            )

        try:
            record = await self._attachment_mapping_repository.mark_imported(
                migration_id,
                canonical.source_chat_id,
                canonical.source_message_id,
                prepared_attachment.attachment_index,
                attachment=prepared_attachment.attachment,
                target_sync_id=target_sync_id,
            )
            if mark_stage_attached:
                await self._attachment_stage_repository.mark_attached(
                    migration_id,
                    canonical.source_chat_id,
                    canonical.source_message_id,
                    prepared_attachment.attachment_index,
                    target_chat_id,
                    target_sync_id=target_sync_id,
                )
            await self._audit_repository.add(
                AuditEvent(
                    migration_id=migration_id,
                    source_chat_id=canonical.source_chat_id,
                    source_message_id=canonical.source_message_id,
                    event_type="attachment_imported",
                    severity=AuditSeverity.INFO,
                    payload_json={
                        "target_chat_id": target_chat_id,
                        "target_sync_id": target_sync_id,
                        "attachment_index": prepared_attachment.attachment_index,
                        "filename": prepared_attachment.attachment.filename,
                        "media_kind": prepared_attachment.attachment.media_kind,
                        "size_bytes": prepared_attachment.attachment.size_bytes,
                        "sha256": prepared_attachment.attachment.sha256,
                        "delivery_mode": delivery_mode,
                    },
                    created_at=self._now(),
                ),
            )
            self._log(
                "info",
                "inline attachment imported",
                migration_id=migration_id,
                source_chat_id=canonical.source_chat_id,
                source_message_id=canonical.source_message_id,
                target_chat_id=target_chat_id,
                target_sync_id=target_sync_id,
                attachment_index=prepared_attachment.attachment_index,
                media_kind=prepared_attachment.attachment.media_kind,
                filename=prepared_attachment.attachment.filename,
            )
            return self._outcome_from_record(
                attachment=prepared_attachment.attachment,
                record=record,
            )
        except asyncio.CancelledError:
            await self._attachment_mapping_repository.mark_failed(
                migration_id,
                canonical.source_chat_id,
                canonical.source_message_id,
                prepared_attachment.attachment_index,
                attachment=prepared_attachment.attachment,
                error_code=MIGRATION_JOB_CANCELLED_ERROR_CODE,
                error_payload={
                    "message": "inline attachment delivery cancelled",
                    "phase": "inline_attach",
                },
            )
            raise

    async def _ensure_uploaded_attachment(
        self,
        *,
        migration_id: str,
        canonical: CanonicalMessage,
        target_chat_id: str,
        prepared_attachment: PreparedAttachment,
        retry_failed: bool,
    ) -> AttachmentUploadResolution:
        claim = await self._attachment_stage_repository.claim(
            self._build_attachment_stage_record(
                migration_id=migration_id,
                canonical=canonical,
                target_chat_id=target_chat_id,
                attachment_index=prepared_attachment.attachment_index,
                attachment=prepared_attachment.attachment,
            ),
            retry_failed=retry_failed,
        )
        if claim.state is ClaimState.IMPORTED:
            staged_file = self._staged_file_from_payload(claim.record.express_file_payload)
            if staged_file is not None:
                return AttachmentUploadResolution(staged_file=staged_file)
        elif claim.state is ClaimState.FAILED and not retry_failed:
            record = await self._attachment_mapping_repository.get(
                migration_id,
                canonical.source_chat_id,
                canonical.source_message_id,
                prepared_attachment.attachment_index,
            )
            if record is not None:
                return AttachmentUploadResolution(
                    outcome=self._outcome_from_record(
                        attachment=prepared_attachment.attachment,
                        record=record,
                    ),
                )
        elif claim.state is ClaimState.AMBIGUOUS:
            raise AmbiguousDeliveryError(
                "attachment upload stage is already being processed or is ambiguous",
            )

        try:
            async with self._attachment_transfer_slot():
                staged_file = await self._retry_policy.run(
                    lambda: self._express_file_store.upload_file(
                        target_chat_id,
                        file=prepared_attachment.file_payload,
                    ),
                )
        except asyncio.CancelledError:
            await self._attachment_stage_repository.mark_failed(
                migration_id,
                canonical.source_chat_id,
                canonical.source_message_id,
                prepared_attachment.attachment_index,
                target_chat_id,
                error_code=MIGRATION_JOB_CANCELLED_ERROR_CODE,
                error_payload={
                    "message": "attachment upload cancelled",
                    "phase": "upload",
                },
            )
            raise
        except (RecoverableItemError, FatalItemError, AmbiguousDeliveryError) as error:
            reason = str(error)
            await self._attachment_stage_repository.mark_failed(
                migration_id,
                canonical.source_chat_id,
                canonical.source_message_id,
                prepared_attachment.attachment_index,
                target_chat_id,
                error_code=type(error).__name__,
                error_payload={
                    "message": reason,
                    "phase": "upload",
                },
            )
            if self._should_fallback_to_direct_file_send(error):
                await self._audit_repository.add(
                    AuditEvent(
                        migration_id=migration_id,
                        source_chat_id=canonical.source_chat_id,
                        source_message_id=canonical.source_message_id,
                        event_type="attachment_upload_fallback_to_direct_send",
                        severity=AuditSeverity.WARNING,
                        payload_json=self._attachment_payload(
                            prepared_attachment.attachment,
                            reason=reason,
                        ),
                        created_at=self._now(),
                    ),
                )
                self._log(
                    "warning",
                    "attachment upload degraded to direct file send",
                    migration_id=migration_id,
                    source_chat_id=canonical.source_chat_id,
                    source_message_id=canonical.source_message_id,
                    target_chat_id=target_chat_id,
                    attachment_index=prepared_attachment.attachment_index,
                    media_kind=prepared_attachment.attachment.media_kind,
                    filename=prepared_attachment.attachment.filename,
                    error_type=type(error).__name__,
                    error=reason,
                )
                return AttachmentUploadResolution(use_direct_file_fallback=True)
            record = await self._claim_terminal_attachment(
                migration_id=migration_id,
                canonical=canonical,
                attachment_index=prepared_attachment.attachment_index,
                attachment=prepared_attachment.attachment,
                target_chat_id=target_chat_id,
                retry_failed=retry_failed,
            )
            if record.import_status is not AttachmentImportStatus.FAILED:
                record = await self._attachment_mapping_repository.mark_failed(
                    migration_id,
                    canonical.source_chat_id,
                    canonical.source_message_id,
                    prepared_attachment.attachment_index,
                    attachment=prepared_attachment.attachment,
                    error_code=type(error).__name__,
                    error_payload={
                        "message": reason,
                        "phase": "upload",
                    },
                )
            await self._audit_repository.add(
                AuditEvent(
                    migration_id=migration_id,
                    source_chat_id=canonical.source_chat_id,
                    source_message_id=canonical.source_message_id,
                    event_type="attachment_upload_failed",
                    severity=AuditSeverity.WARNING,
                    payload_json=self._attachment_payload(
                        prepared_attachment.attachment,
                        reason=reason,
                    ),
                    created_at=self._now(),
                ),
            )
            self._log(
                "warning",
                "attachment upload failed",
                migration_id=migration_id,
                source_chat_id=canonical.source_chat_id,
                source_message_id=canonical.source_message_id,
                target_chat_id=target_chat_id,
                attachment_index=prepared_attachment.attachment_index,
                media_kind=prepared_attachment.attachment.media_kind,
                filename=prepared_attachment.attachment.filename,
                error_type=type(error).__name__,
                error=reason,
            )
            return AttachmentUploadResolution(
                outcome=self._outcome_from_record(
                    attachment=prepared_attachment.attachment,
                    record=record,
                ),
            )

        await self._attachment_stage_repository.mark_uploaded(
            migration_id,
            canonical.source_chat_id,
            canonical.source_message_id,
            prepared_attachment.attachment_index,
            target_chat_id,
            staged_file=staged_file,
        )
        await self._audit_repository.add(
            AuditEvent(
                migration_id=migration_id,
                source_chat_id=canonical.source_chat_id,
                source_message_id=canonical.source_message_id,
                event_type="attachment_uploaded_to_files_api",
                severity=AuditSeverity.INFO,
                payload_json={
                    "target_chat_id": target_chat_id,
                    "attachment_index": prepared_attachment.attachment_index,
                    "express_file_id": staged_file.file_id,
                    "filename": staged_file.filename,
                    "media_kind": prepared_attachment.attachment.media_kind,
                    "size_bytes": staged_file.size_bytes,
                },
                created_at=self._now(),
            ),
        )
        self._log(
            "info",
            "attachment uploaded to files api",
            migration_id=migration_id,
            source_chat_id=canonical.source_chat_id,
            source_message_id=canonical.source_message_id,
            target_chat_id=target_chat_id,
            attachment_index=prepared_attachment.attachment_index,
            express_file_id=staged_file.file_id,
            media_kind=prepared_attachment.attachment.media_kind,
            filename=staged_file.filename,
        )
        return AttachmentUploadResolution(staged_file=staged_file)

    def _to_file_payload(
        self,
        downloaded: DownloadedAttachment,
    ) -> tuple[FilePayload, CanonicalAttachment]:
        effective_attachment = replace(
            downloaded.attachment,
            filename=self._effective_attachment_filename(downloaded.attachment),
        )
        filename = effective_attachment.filename
        if downloaded.content is not None:
            content = downloaded.content
            self._cleanup_downloaded_attachment(downloaded)
            return (
                FilePayload(
                    filename=filename or "attachment.bin",
                    content=content,
                    mime_type=effective_attachment.mime_type,
                    media_kind=effective_attachment.media_kind,
                    duration_seconds=effective_attachment.duration_seconds,
                ),
                effective_attachment,
            )

        if not downloaded.local_path:
            raise FatalItemError(
                "attachment download did not provide content or local_path",
            )
        source_path = Path(downloaded.local_path)
        if not source_path.exists():
            raise FatalItemError("attachment temp file is missing")
        effective_filename = filename or source_path.name or "attachment.bin"
        return (
            FilePayload(
                filename=effective_filename,
                local_path=str(source_path),
                mime_type=effective_attachment.mime_type,
                media_kind=effective_attachment.media_kind,
                duration_seconds=effective_attachment.duration_seconds,
            ),
            effective_attachment,
        )

    def _cleanup_downloaded_attachment(self, downloaded: DownloadedAttachment) -> None:
        if not downloaded.local_path:
            return
        try:
            Path(downloaded.local_path).unlink(missing_ok=True)
        except OSError:
            return

    def _cleanup_file_payload(self, file_payload: FilePayload) -> None:
        if not file_payload.local_path:
            return
        try:
            Path(file_payload.local_path).unlink(missing_ok=True)
        except OSError:
            return

    @asynccontextmanager
    async def _attachment_transfer_slot(self):
        if self._attachment_transfer_semaphore is None:
            yield
            return
        async with self._attachment_transfer_semaphore:
            yield

    def _attachment_payload(
        self,
        attachment: CanonicalAttachment,
        *,
        reason: str,
    ) -> dict[str, object]:
        return {
            "reason": reason,
            "source_file_id": attachment.source_file_id,
            "filename": attachment.filename,
            "media_kind": attachment.media_kind,
            "size_bytes": attachment.size_bytes,
            "sha256": attachment.sha256,
            "download_url": attachment.download_url,
        }

    def _downloaded_attachment_size_bytes(
        self,
        downloaded: DownloadedAttachment,
    ) -> int | None:
        if downloaded.size_bytes is not None:
            return downloaded.size_bytes
        if downloaded.content is not None:
            return len(downloaded.content)
        if downloaded.local_path:
            try:
                return Path(downloaded.local_path).stat().st_size
            except OSError:
                return downloaded.attachment.size_bytes
        return downloaded.attachment.size_bytes

    def _file_payload_size_bytes(
        self,
        file_payload: FilePayload,
    ) -> int | None:
        if file_payload.content is not None:
            return len(file_payload.content)
        if file_payload.local_path:
            try:
                return Path(file_payload.local_path).stat().st_size
            except OSError:
                return None
        return None

    def _attachment_size_limit_reason(
        self,
        *,
        attachment: CanonicalAttachment,
        size_bytes: int | None,
    ) -> str | None:
        if size_bytes is None or size_bytes <= self._max_attachment_upload_size_bytes:
            return None
        return (
            "skipped: file exceeds eXpress BotX upload limit "
            f"({self._format_size_bytes(size_bytes)} > "
            f"{self._format_size_bytes(self._max_attachment_upload_size_bytes)})"
        )

    async def _mark_attachment_skipped(
        self,
        *,
        migration_id: str,
        canonical: CanonicalMessage,
        attachment_index: int,
        attachment: CanonicalAttachment,
        target_chat_id: str,
        reason: str,
    ) -> AttachmentMappingRecord:
        record = await self._claim_terminal_attachment(
            migration_id=migration_id,
            canonical=canonical,
            attachment_index=attachment_index,
            attachment=attachment,
            target_chat_id=target_chat_id,
        )
        if record.import_status is not AttachmentImportStatus.SKIPPED:
            record = await self._attachment_mapping_repository.mark_skipped(
                migration_id,
                canonical.source_chat_id,
                canonical.source_message_id,
                attachment_index,
                attachment=attachment,
                reason=reason,
            )
        await self._audit_repository.add(
            AuditEvent(
                migration_id=migration_id,
                source_chat_id=canonical.source_chat_id,
                source_message_id=canonical.source_message_id,
                event_type="attachment_skipped_by_policy",
                severity=AuditSeverity.INFO,
                payload_json={
                    **self._attachment_payload(attachment, reason=reason),
                    "policy": "express_botx_max_upload_size",
                    "size_limit_bytes": self._max_attachment_upload_size_bytes,
                },
                created_at=self._now(),
            ),
        )
        self._log(
            "info",
            "attachment skipped by size limit",
            migration_id=migration_id,
            source_chat_id=canonical.source_chat_id,
            source_message_id=canonical.source_message_id,
            target_chat_id=target_chat_id,
            attachment_index=attachment_index,
            media_kind=attachment.media_kind,
            filename=attachment.filename,
            size_bytes=attachment.size_bytes,
            size_limit_bytes=self._max_attachment_upload_size_bytes,
        )
        return record

    async def _claim_terminal_attachment(
        self,
        *,
        migration_id: str,
        canonical: CanonicalMessage,
        attachment_index: int,
        attachment: CanonicalAttachment,
        target_chat_id: str,
        retry_failed: bool = False,
    ) -> AttachmentMappingRecord:
        claim = await self._attachment_mapping_repository.claim(
            self._build_attachment_record(
                migration_id=migration_id,
                canonical=canonical,
                target_chat_id=target_chat_id,
                attachment_index=attachment_index,
                attachment=attachment,
            ),
            retry_failed=retry_failed,
        )
        if claim.state is ClaimState.AMBIGUOUS:
            if claim.record.import_status is AttachmentImportStatus.SKIPPED:
                return claim.record
            raise AmbiguousDeliveryError(
                "attachment is already being processed or is ambiguous",
            )
        return claim.record

    def _build_attachment_record(
        self,
        *,
        migration_id: str,
        canonical: CanonicalMessage,
        target_chat_id: str,
        attachment_index: int,
        attachment: CanonicalAttachment,
    ) -> AttachmentMappingRecord:
        return AttachmentMappingRecord(
            migration_id=migration_id,
            source_chat_id=canonical.source_chat_id,
            source_message_id=canonical.source_message_id,
            attachment_index=attachment_index,
            source_file_id=attachment.source_file_id,
            source_filename=attachment.filename,
            media_kind=attachment.media_kind,
            checksum=attachment.sha256,
            size_bytes=attachment.size_bytes,
            target_chat_id=target_chat_id,
            import_status=AttachmentImportStatus.PROCESSING,
        )

    def _build_attachment_stage_record(
        self,
        *,
        migration_id: str,
        canonical: CanonicalMessage,
        target_chat_id: str,
        attachment_index: int,
        attachment: CanonicalAttachment,
    ) -> AttachmentStageRecord:
        return AttachmentStageRecord(
            migration_id=migration_id,
            source_chat_id=canonical.source_chat_id,
            source_message_id=canonical.source_message_id,
            attachment_index=attachment_index,
            target_chat_id=target_chat_id,
            source_locator=attachment.download_url or attachment.source_file_id,
            media_kind=attachment.media_kind,
            checksum=attachment.sha256,
            size_bytes=attachment.size_bytes,
            status=AttachmentStageStatus.PROCESSING,
        )

    def _outcome_from_record(
        self,
        *,
        attachment: CanonicalAttachment,
        record: AttachmentMappingRecord,
    ) -> AttachmentDeliveryOutcome:
        phase = self._attachment_phase(record.last_error_payload)
        return AttachmentDeliveryOutcome(
            attachment_index=record.attachment_index,
            attachment=attachment,
            import_status=record.import_status,
            phase=phase,
            blocks_message_completion=record.import_status in (
                AttachmentImportStatus.FAILED,
                AttachmentImportStatus.AMBIGUOUS,
            )
            and phase == "send",
            target_sync_id=record.target_sync_id,
            reason=self._attachment_reason(record.last_error_payload),
        )

    def _attachment_phase(self, payload: dict[str, object] | None) -> str:
        value = (payload or {}).get("phase")
        return value if isinstance(value, str) else "unknown"

    def _attachment_reason(self, payload: dict[str, object] | None) -> str | None:
        value = (payload or {}).get("message")
        if isinstance(value, str):
            return value
        value = (payload or {}).get("reason")
        return value if isinstance(value, str) else None

    def _should_fallback_to_direct_file_send(
        self,
        error: RecoverableItemError | FatalItemError | AmbiguousDeliveryError,
    ) -> bool:
        if isinstance(error, AmbiguousDeliveryError):
            return False
        normalized = str(error).lower()
        preview_failure_markers = (
            "get preview failed",
            "flow_processing_error",
            "file_preview",
            "preview generation",
        )
        return any(marker in normalized for marker in preview_failure_markers)

    def _staged_file_from_payload(
        self,
        payload: dict[str, object] | None,
    ) -> ExpressStagedFile | None:
        if not isinstance(payload, dict):
            return None

        attachment_type = payload.get("attachment_type")
        file_id = payload.get("file_id")
        file_url = payload.get("file_url")
        filename = payload.get("filename")
        size_bytes = payload.get("size_bytes")
        mime_type = payload.get("mime_type")
        file_hash = payload.get("file_hash")
        if not all(
            isinstance(value, str) and value
            for value in (
                attachment_type,
                file_id,
                file_url,
                filename,
                mime_type,
                file_hash,
            )
        ):
            return None
        if not isinstance(size_bytes, int):
            return None

        return ExpressStagedFile(
            attachment_type=attachment_type,
            file_id=file_id,
            file_url=file_url,
            filename=filename,
            size_bytes=size_bytes,
            mime_type=mime_type,
            file_hash=file_hash,
            duration_seconds=(
                payload.get("duration_seconds")
                if isinstance(payload.get("duration_seconds"), int)
                else None
            ),
            preview_url=(
                payload.get("preview_url")
                if isinstance(payload.get("preview_url"), str)
                else None
            ),
            preview_height=(
                payload.get("preview_height")
                if isinstance(payload.get("preview_height"), int)
                else None
            ),
            preview_width=(
                payload.get("preview_width")
                if isinstance(payload.get("preview_width"), int)
                else None
            ),
            encryption_algo=(
                payload.get("encryption_algo")
                if isinstance(payload.get("encryption_algo"), str)
                else None
            ),
            chunk_size=(
                payload.get("chunk_size")
                if isinstance(payload.get("chunk_size"), int)
                else None
            ),
            caption=(
                payload.get("caption")
                if isinstance(payload.get("caption"), str)
                else None
            ),
        )

    def _attachment_idempotency_key(
        self,
        *,
        canonical: CanonicalMessage,
        attachment_index: int,
    ) -> str:
        return f"{canonical.idempotency_key}:attachment:{attachment_index}"

    def _format_size_bytes(self, value: int) -> str:
        if value >= 1024 * 1024:
            return f"{value / (1024 * 1024):.1f} MiB"
        if value >= 1024:
            return f"{value / 1024:.1f} KiB"
        return f"{value} B"

    def _render_attachment_body(
        self,
        attachment: CanonicalAttachment,
        *,
        attachment_index: int,
    ) -> str:
        if attachment.filename:
            return f"[{attachment.media_kind}] {attachment.filename}"
        if attachment.source_file_id:
            return (
                f"[{attachment.media_kind}] "
                f"{self._effective_attachment_filename(attachment)}"
            )
        return (
            f"[{attachment.media_kind}] "
            f"{self._effective_attachment_filename(attachment)}"
        )

    def _effective_attachment_filename(
        self,
        attachment: CanonicalAttachment,
    ) -> str:
        if attachment.filename:
            return attachment.filename
        extension = self._attachment_extension(attachment)
        stem = attachment.media_kind or "attachment"
        stem = stem.replace("/", "_").replace(" ", "_")
        return f"{stem}{extension}"

    def _attachment_extension(
        self,
        attachment: CanonicalAttachment,
    ) -> str:
        if attachment.mime_type:
            normalized = attachment.mime_type.lower()
            explicit = {
                "image/jpeg": ".jpg",
                "image/jpg": ".jpg",
                "image/png": ".png",
                "image/gif": ".gif",
                "image/webp": ".webp",
                "video/mp4": ".mp4",
                "video/quicktime": ".mov",
                "audio/ogg": ".ogg",
                "audio/mpeg": ".mp3",
                "application/pdf": ".pdf",
            }.get(normalized)
            if explicit:
                return explicit
            guessed = mimetypes.guess_extension(normalized)
            if guessed == ".jpe":
                return ".jpg"
            if guessed:
                return guessed
        return {
            "photo": ".jpg",
            "video": ".mp4",
            "voice": ".ogg",
            "audio": ".mp3",
            "sticker": ".webp",
        }.get(attachment.media_kind, ".bin")

    def _now(self) -> datetime:
        return datetime.now(tz=UTC)

    def _log(
        self,
        level: str,
        event: str,
        **payload: object,
    ) -> None:
        if self._logger is None:
            return
        log_method = getattr(self._logger, level, None)
        if callable(log_method):
            log_method(event, **payload)
