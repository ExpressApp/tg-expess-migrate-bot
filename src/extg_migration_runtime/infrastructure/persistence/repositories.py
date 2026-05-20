from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any

from sqlalchemy import delete, func, select, tuple_, update
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

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
    HistoryCursor,
    IdentityMappingRecord,
    IntegrationOutboxEventRecord,
    IntegrationOutboxStatus,
    InventorySnapshotRecord,
    MigrationJobRecord,
    MigrationJobStatus,
    MIGRATION_JOB_CANCELLED_ERROR_CODE,
    MIGRATION_JOB_CANCEL_REQUESTED_ERROR_CODE,
    MigrationLifecycleStatus,
    MessageClaimResult,
    MessageImportStatus,
    MessageMappingRecord,
    MigrationCheckpoint,
    MigrationStateRecord,
    OperatorMigrationDefaultsRecord,
    OperatorTelegramSessionRecord,
    PublishedIntegrationEvent,
    ServiceWatermarkRecord,
    TelegramExportSnapshotRecord,
)
from extg_migration_runtime.infrastructure.persistence.models import (
    ConsumerInboxModel,
    ExpressBotHuidBindingModel,
    ExpressUserCtsBindingModel,
    MigrationChatConfigModel,
    MigrationChatMapModel,
    MigrationCheckpointModel,
    IdentityMappingModel,
    IntegrationOutboxModel,
    AttachmentStageModel,
    MigrationAttachmentMapModel,
    MigrationInventorySnapshotModel,
    MigrationJobModel,
    MigrationMessageMapModel,
    MigrationOperatorDefaultsModel,
    MigrationStateModel,
    ServiceWatermarkModel,
    TelegramExportSnapshotModel,
)


_FULL_PAYLOAD_STORAGE_MODE = "full"
_REDACTED_PAYLOAD_STORAGE_MODE = "redacted"
_NONE_PAYLOAD_STORAGE_MODE = "none"
_REDACTED_VALUE = "[redacted]"
_TRUNCATED_VALUE = "[truncated]"
_RETRYABLE_CANCEL_ERROR_CODES = frozenset(
    {
        MIGRATION_JOB_CANCELLED_ERROR_CODE,
        MIGRATION_JOB_CANCEL_REQUESTED_ERROR_CODE,
    },
)
_RETRYABLE_AMBIGUOUS_ERROR_CODES = frozenset({"AmbiguousDeliveryError"})
_SAFE_STRING_KEYS = frozenset(
    {
        "migration_id",
        "source_chat_id",
        "source_message_id",
        "target_chat_id",
        "operation",
        "job_key",
        "phase",
        "status",
        "mode",
        "source",
        "event_type",
        "severity",
        "media_kind",
        "resolution_source",
        "reply_mode",
        "identity_policy",
        "cursor_type",
        "cursor_value",
        "source_chat_type",
        "target_strategy",
    },
)
_SAFE_STRING_SUFFIXES = ("_id", "_ids", "_type", "_mode", "_status", "_policy")
_SENSITIVE_STRING_KEYS = frozenset(
    {
        "body",
        "rendered_body",
        "message",
        "error",
        "reason",
        "excerpt",
        "content",
        "phone_number",
        "corporate_email",
        "source_chat_title",
        "target_chat_title",
        "filename",
        "download_url",
        "session_path",
        "phone_code_hash",
        "password",
    },
)
_SENSITIVE_KEY_SUFFIXES = (
    "_username",
    "_display_name",
    "_email",
    "_phone_number",
    "_title",
    "_filename",
    "_download_url",
    "_session_path",
    "_huid",
    "_huids",
)


def _is_retryable_cancel_error_code(error_code: str | None) -> bool:
    return error_code in _RETRYABLE_CANCEL_ERROR_CODES


def _is_retryable_ambiguous_error_code(
    error_code: str | None,
    *,
    target_sync_id: str | None = None,
) -> bool:
    return target_sync_id is None and error_code in _RETRYABLE_AMBIGUOUS_ERROR_CODES


def _sanitize_payload(payload: dict[str, object] | None, *, mode: str) -> dict[str, object]:
    if mode == _FULL_PAYLOAD_STORAGE_MODE:
        return dict(payload or {})
    if mode == _NONE_PAYLOAD_STORAGE_MODE:
        return {}
    sanitized = _sanitize_payload_value(payload or {}, key=None, depth=0)
    return sanitized if isinstance(sanitized, dict) else {}


def _sanitize_payload_value(
    value: object,
    *,
    key: str | None,
    depth: int,
) -> object:
    if depth >= 6:
        return _TRUNCATED_VALUE
    if key is not None and _is_sensitive_key(key):
        return _REDACTED_VALUE
    if value is None or isinstance(value, bool | int | float):
        return value
    if isinstance(value, str):
        return value if _is_safe_string_key(key) else _REDACTED_VALUE
    if isinstance(value, dict):
        return {
            str(child_key): _sanitize_payload_value(
                child_value,
                key=str(child_key),
                depth=depth + 1,
            )
            for child_key, child_value in value.items()
        }
    if isinstance(value, (list, tuple, set)):
        items = list(value)
        return [
            _sanitize_payload_value(item, key=key, depth=depth + 1)
            for item in items[:50]
        ]
    return _REDACTED_VALUE


def _is_safe_string_key(key: str | None) -> bool:
    if key is None:
        return False
    normalized = key.strip().lower()
    if _is_sensitive_key(normalized):
        return False
    return normalized in _SAFE_STRING_KEYS or normalized.endswith(_SAFE_STRING_SUFFIXES)


def _is_sensitive_key(key: str) -> bool:
    normalized = key.strip().lower()
    return normalized in _SENSITIVE_STRING_KEYS or normalized.endswith(_SENSITIVE_KEY_SUFFIXES)


class PostgresChatMappingRepository:
    def __init__(self, session_factory: async_sessionmaker[AsyncSession]) -> None:
        self._session_factory = session_factory

    async def get(
        self,
        migration_id: str,
        source_chat_id: str,
    ) -> ChatMappingRecord | None:
        async with self._session_factory() as session:
            result = await session.execute(
                select(MigrationChatMapModel).where(
                    MigrationChatMapModel.migration_id == migration_id,
                    MigrationChatMapModel.source_chat_id == source_chat_id,
                ),
            )
            model = result.scalar_one_or_none()
            return _chat_to_domain(model) if model else None

    async def save(self, record: ChatMappingRecord) -> None:
        values = {
            "migration_id": record.migration_id,
            "source_chat_id": record.source_chat_id,
            "source_chat_type": record.source_chat_type,
            "source_chat_title": record.source_chat_title,
            "anchor_cts_host": record.anchor_cts_host,
            "anchor_bot_id": record.anchor_bot_id,
            "target_chat_id": record.target_chat_id,
            "target_chat_title": record.target_chat_title,
            "status": record.status,
            "member_success_count": record.member_success_count,
            "member_total_count": record.member_total_count,
            "created_at": record.created_at,
            "updated_at": record.updated_at,
        }
        statement = insert(MigrationChatMapModel).values(**values).on_conflict_do_update(
            constraint="uq_migration_chat_map_migration_source_chat",
            set_={
                "source_chat_type": record.source_chat_type,
                "source_chat_title": record.source_chat_title,
                "anchor_cts_host": record.anchor_cts_host,
                "anchor_bot_id": record.anchor_bot_id,
                "target_chat_id": record.target_chat_id,
                "target_chat_title": record.target_chat_title,
                "status": record.status,
                "member_success_count": record.member_success_count,
                "member_total_count": record.member_total_count,
                "updated_at": record.updated_at,
            },
        )
        async with self._session_factory() as session:
            await session.execute(statement)
            await session.commit()

    async def list_by_migration(
        self,
        migration_id: str,
    ) -> list[ChatMappingRecord]:
        statement = (
            select(MigrationChatMapModel)
            .where(MigrationChatMapModel.migration_id == migration_id)
            .order_by(MigrationChatMapModel.source_chat_id.asc())
        )
        async with self._session_factory() as session:
            models = (await session.execute(statement)).scalars().all()
            return [_chat_to_domain(model) for model in models]


class PostgresChatMigrationConfigRepository:
    def __init__(self, session_factory: async_sessionmaker[AsyncSession]) -> None:
        self._session_factory = session_factory

    async def get(
        self,
        migration_id: str,
        source_chat_id: str,
    ) -> ChatMigrationConfigRecord | None:
        statement = select(MigrationChatConfigModel).where(
            MigrationChatConfigModel.migration_id == migration_id,
            MigrationChatConfigModel.source_chat_id == source_chat_id,
        )
        async with self._session_factory() as session:
            model = (await session.execute(statement)).scalar_one_or_none()
            return _chat_config_to_domain(model) if model else None

    async def list_by_migration(
        self,
        migration_id: str,
    ) -> list[ChatMigrationConfigRecord]:
        statement = (
            select(MigrationChatConfigModel)
            .where(MigrationChatConfigModel.migration_id == migration_id)
            .order_by(MigrationChatConfigModel.source_chat_id.asc())
        )
        async with self._session_factory() as session:
            models = (await session.execute(statement)).scalars().all()
            return [_chat_config_to_domain(model) for model in models]

    async def save(
        self,
        record: ChatMigrationConfigRecord,
    ) -> ChatMigrationConfigRecord:
        statement = (
            insert(MigrationChatConfigModel)
            .values(
                migration_id=record.migration_id,
                source_chat_id=record.source_chat_id,
                source_chat_type=record.source_chat_type,
                source_chat_title=record.source_chat_title,
                anchor_cts_host=record.anchor_cts_host,
                anchor_bot_id=record.anchor_bot_id,
                source_backend=record.source_backend,
                target_strategy=record.target_strategy,
                target_title=record.target_title,
                target_chat_id=record.target_chat_id,
                include_from=record.include_from,
                include_to=record.include_to,
                migrate_media=record.migrate_media,
                media_kinds_json=list(record.media_kinds) if record.media_kinds is not None else None,
                service_messages=record.service_messages,
                reply_mode=record.reply_mode,
                output_template=record.output_template,
                identity_policy=record.identity_policy,
                access_strategy=record.access_strategy,
                topic_strategy=record.topic_strategy,
                skip_in_all=record.skip_in_all,
                telegram_chat_id=record.telegram_chat_id,
                source_topic_id=record.source_topic_id,
                source_thread_id=record.source_thread_id,
                source_thread_title=record.source_thread_title,
                updated_by_huid=record.updated_by_huid,
                created_at=record.created_at,
                updated_at=record.updated_at,
            )
            .on_conflict_do_update(
                constraint="uq_migration_chat_config_key",
                set_={
                    "source_chat_type": record.source_chat_type,
                    "source_chat_title": record.source_chat_title,
                    "anchor_cts_host": record.anchor_cts_host,
                    "anchor_bot_id": record.anchor_bot_id,
                    "source_backend": record.source_backend,
                    "target_strategy": record.target_strategy,
                    "target_title": record.target_title,
                    "target_chat_id": record.target_chat_id,
                    "include_from": record.include_from,
                    "include_to": record.include_to,
                    "migrate_media": record.migrate_media,
                    "media_kinds_json": (
                        list(record.media_kinds) if record.media_kinds is not None else None
                    ),
                    "service_messages": record.service_messages,
                    "reply_mode": record.reply_mode,
                    "output_template": record.output_template,
                    "identity_policy": record.identity_policy,
                    "access_strategy": record.access_strategy,
                    "topic_strategy": record.topic_strategy,
                    "skip_in_all": record.skip_in_all,
                    "telegram_chat_id": record.telegram_chat_id,
                    "source_topic_id": record.source_topic_id,
                    "source_thread_id": record.source_thread_id,
                    "source_thread_title": record.source_thread_title,
                    "updated_by_huid": record.updated_by_huid,
                    "updated_at": record.updated_at,
                },
                where=(
                    MigrationChatConfigModel.updated_by_huid == record.updated_by_huid
                ),
            )
            .returning(MigrationChatConfigModel)
        )
        async with self._session_factory() as session:
            model = (await session.execute(statement)).scalar_one_or_none()
            if model is None:
                model = (
                    await session.execute(
                        select(MigrationChatConfigModel).where(
                            MigrationChatConfigModel.migration_id == record.migration_id,
                            MigrationChatConfigModel.source_chat_id == record.source_chat_id,
                        ),
                    )
                ).scalar_one()
            await session.commit()
            return _chat_config_to_domain(model)

    async def delete(
        self,
        migration_id: str,
        source_chat_id: str,
    ) -> bool:
        statement = (
            delete(MigrationChatConfigModel)
            .where(
                MigrationChatConfigModel.migration_id == migration_id,
                MigrationChatConfigModel.source_chat_id == source_chat_id,
            )
            .returning(MigrationChatConfigModel.id)
        )
        async with self._session_factory() as session:
            deleted_id = (await session.execute(statement)).scalar_one_or_none()
            await session.commit()
            return deleted_id is not None


class PostgresOperatorMigrationDefaultsRepository:
    def __init__(self, session_factory: async_sessionmaker[AsyncSession]) -> None:
        self._session_factory = session_factory

    async def get(
        self,
        migration_id: str,
        operator_huid: str,
    ) -> OperatorMigrationDefaultsRecord | None:
        statement = select(MigrationOperatorDefaultsModel).where(
            MigrationOperatorDefaultsModel.migration_id == migration_id,
            MigrationOperatorDefaultsModel.operator_huid == operator_huid,
        )
        async with self._session_factory() as session:
            model = (await session.execute(statement)).scalar_one_or_none()
            return _operator_defaults_to_domain(model) if model else None

    async def save(
        self,
        record: OperatorMigrationDefaultsRecord,
    ) -> OperatorMigrationDefaultsRecord:
        statement = (
            insert(MigrationOperatorDefaultsModel)
            .values(
                migration_id=record.migration_id,
                operator_huid=record.operator_huid,
                include_from=record.include_from,
                include_to=record.include_to,
                migrate_media=record.migrate_media,
                media_kinds_json=list(record.media_kinds) if record.media_kinds is not None else None,
                service_messages=record.service_messages,
                reply_mode=record.reply_mode,
                output_template=record.output_template,
                access_strategy=record.access_strategy,
                topic_strategy=record.topic_strategy,
                created_at=record.created_at,
                updated_at=record.updated_at,
            )
            .on_conflict_do_update(
                constraint="uq_migration_operator_defaults_key",
                set_={
                    "include_from": record.include_from,
                    "include_to": record.include_to,
                    "migrate_media": record.migrate_media,
                    "media_kinds_json": (
                        list(record.media_kinds) if record.media_kinds is not None else None
                    ),
                    "service_messages": record.service_messages,
                    "reply_mode": record.reply_mode,
                    "output_template": record.output_template,
                    "access_strategy": record.access_strategy,
                    "topic_strategy": record.topic_strategy,
                    "updated_at": record.updated_at,
                },
            )
            .returning(MigrationOperatorDefaultsModel)
        )
        async with self._session_factory() as session:
            model = (await session.execute(statement)).scalar_one()
            await session.commit()
            return _operator_defaults_to_domain(model)


class PostgresMessageMappingRepository:
    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        *,
        persist_message_bodies: bool = False,
        payload_storage_mode: str = _REDACTED_PAYLOAD_STORAGE_MODE,
        processing_stale_after_seconds: float = 30.0,
    ) -> None:
        self._session_factory = session_factory
        self._persist_message_bodies = persist_message_bodies
        self._payload_storage_mode = payload_storage_mode
        self._processing_stale_after = timedelta(
            seconds=max(processing_stale_after_seconds, 1.0),
        )

    async def claim(self, record: MessageMappingRecord) -> MessageClaimResult:
        statement = (
            insert(MigrationMessageMapModel)
            .values(
                migration_id=record.migration_id,
                source_chat_id=record.source_chat_id,
                source_message_id=record.source_message_id,
                source_sent_at=record.source_sent_at,
                target_chat_id=record.target_chat_id,
                checksum=record.checksum,
                import_status=record.import_status.value,
                imported_at=record.imported_at,
                target_sync_id=record.target_sync_id,
                rendered_body=self._sanitize_rendered_body(record.rendered_body),
                last_error_code=record.last_error_code,
                last_error_payload=self._sanitize_json_payload(record.last_error_payload),
                updated_at=_now(),
            )
            .on_conflict_do_nothing(
                constraint="uq_migration_message_map_dedup_key",
            )
            .returning(MigrationMessageMapModel)
        )
        async with self._session_factory() as session:
            inserted = (await session.execute(statement)).scalar_one_or_none()
            if inserted:
                await session.commit()
                return MessageClaimResult(
                    state=ClaimState.CLAIMED,
                    record=_message_to_domain(inserted),
                )

            existing = await self._get_model(
                session,
                record.migration_id,
                record.source_chat_id,
                record.source_message_id,
            )
            if existing is None:
                await session.rollback()
                # Should not happen with unique key semantics.
                return MessageClaimResult(state=ClaimState.AMBIGUOUS, record=record)
            if existing.import_status == MessageImportStatus.PROCESSING.value:
                reclaimed = await self._reclaim_stale_processing(
                    session=session,
                    existing=existing,
                    record=record,
                    force=_is_retryable_cancel_error_code(existing.last_error_code),
                )
                if reclaimed is not None:
                    await session.commit()
                    return MessageClaimResult(
                        state=ClaimState.CLAIMED,
                        record=_message_to_domain(reclaimed),
                    )
            if (
                existing.import_status == MessageImportStatus.AMBIGUOUS.value
                and _is_retryable_ambiguous_error_code(
                    existing.last_error_code,
                    target_sync_id=existing.target_sync_id,
                )
            ):
                reclaimed = await self._reclaim_stale_ambiguous(
                    session=session,
                    existing=existing,
                    record=record,
                )
                if reclaimed is not None:
                    await session.commit()
                    return MessageClaimResult(
                        state=ClaimState.CLAIMED,
                        record=_message_to_domain(reclaimed),
                    )
            if (
                existing.import_status == MessageImportStatus.FAILED.value
                and _is_retryable_cancel_error_code(existing.last_error_code)
            ):
                updated = await session.execute(
                    update(MigrationMessageMapModel)
                    .where(
                        MigrationMessageMapModel.migration_id == record.migration_id,
                        MigrationMessageMapModel.source_chat_id == record.source_chat_id,
                        MigrationMessageMapModel.source_message_id == record.source_message_id,
                    )
                    .values(
                        source_sent_at=record.source_sent_at,
                        target_chat_id=record.target_chat_id,
                        checksum=record.checksum,
                        import_status=MessageImportStatus.PROCESSING.value,
                        imported_at=None,
                        target_sync_id=None,
                        rendered_body=self._sanitize_rendered_body(record.rendered_body),
                        last_error_code=None,
                        last_error_payload=None,
                        updated_at=_now(),
                    )
                    .returning(MigrationMessageMapModel),
                )
                model = updated.scalar_one()
                await session.commit()
                return MessageClaimResult(
                    state=ClaimState.CLAIMED,
                    record=_message_to_domain(model),
                )
            existing_record = _message_to_domain(existing)
            await session.rollback()

            if existing_record.import_status is MessageImportStatus.AMBIGUOUS:
                return MessageClaimResult(
                    state=ClaimState.AMBIGUOUS,
                    record=existing_record,
                )
            if existing_record.import_status is MessageImportStatus.IMPORTED:
                return MessageClaimResult(
                    state=ClaimState.IMPORTED,
                    record=existing_record,
                )
            if existing_record.import_status is MessageImportStatus.FAILED:
                return MessageClaimResult(
                    state=ClaimState.FAILED,
                    record=existing_record,
                )
            return MessageClaimResult(
                state=ClaimState.AMBIGUOUS,
                record=existing_record,
            )

    async def _reclaim_stale_processing(
        self,
        *,
        session: AsyncSession,
        existing: MigrationMessageMapModel,
        record: MessageMappingRecord,
        force: bool = False,
    ) -> MigrationMessageMapModel | None:
        if not force and existing.updated_at > _now() - self._processing_stale_after:
            return None
        statement = (
            update(MigrationMessageMapModel)
            .where(
                MigrationMessageMapModel.migration_id == record.migration_id,
                MigrationMessageMapModel.source_chat_id == record.source_chat_id,
                MigrationMessageMapModel.source_message_id == record.source_message_id,
                MigrationMessageMapModel.import_status == MessageImportStatus.PROCESSING.value,
                MigrationMessageMapModel.updated_at == existing.updated_at,
            )
            .values(
                source_sent_at=record.source_sent_at,
                target_chat_id=record.target_chat_id,
                checksum=record.checksum,
                import_status=MessageImportStatus.PROCESSING.value,
                imported_at=None,
                target_sync_id=None,
                rendered_body=self._sanitize_rendered_body(record.rendered_body),
                last_error_code=None,
                last_error_payload=None,
                updated_at=_now(),
            )
            .returning(MigrationMessageMapModel)
        )
        return (await session.execute(statement)).scalar_one_or_none()

    async def _reclaim_stale_ambiguous(
        self,
        *,
        session: AsyncSession,
        existing: MigrationMessageMapModel,
        record: MessageMappingRecord,
    ) -> MigrationMessageMapModel | None:
        if existing.updated_at > _now() - self._processing_stale_after:
            return None
        statement = (
            update(MigrationMessageMapModel)
            .where(
                MigrationMessageMapModel.migration_id == record.migration_id,
                MigrationMessageMapModel.source_chat_id == record.source_chat_id,
                MigrationMessageMapModel.source_message_id == record.source_message_id,
                MigrationMessageMapModel.import_status == MessageImportStatus.AMBIGUOUS.value,
                MigrationMessageMapModel.updated_at == existing.updated_at,
            )
            .values(
                source_sent_at=record.source_sent_at,
                target_chat_id=record.target_chat_id,
                checksum=record.checksum,
                import_status=MessageImportStatus.PROCESSING.value,
                imported_at=None,
                target_sync_id=None,
                rendered_body=self._sanitize_rendered_body(record.rendered_body),
                last_error_code=None,
                last_error_payload=None,
                updated_at=_now(),
            )
            .returning(MigrationMessageMapModel)
        )
        return (await session.execute(statement)).scalar_one_or_none()

    async def get(
        self,
        migration_id: str,
        source_chat_id: str,
        source_message_id: str,
    ) -> MessageMappingRecord | None:
        async with self._session_factory() as session:
            model = await self._get_model(
                session,
                migration_id,
                source_chat_id,
                source_message_id,
            )
            return _message_to_domain(model) if model else None

    async def mark_imported(
        self,
        migration_id: str,
        source_chat_id: str,
        source_message_id: str,
        *,
        target_sync_id: str,
        rendered_body: str,
    ) -> MessageMappingRecord:
        statement = (
            update(MigrationMessageMapModel)
            .where(
                MigrationMessageMapModel.migration_id == migration_id,
                MigrationMessageMapModel.source_chat_id == source_chat_id,
                MigrationMessageMapModel.source_message_id == source_message_id,
            )
            .values(
                import_status=MessageImportStatus.IMPORTED.value,
                target_sync_id=target_sync_id,
                rendered_body=self._sanitize_rendered_body(rendered_body),
                imported_at=_now(),
                last_error_code=None,
                last_error_payload=None,
                updated_at=_now(),
            )
            .returning(MigrationMessageMapModel)
        )
        async with self._session_factory() as session:
            model = (await session.execute(statement)).scalar_one()
            await session.commit()
            return _message_to_domain(model)

    async def mark_ambiguous(
        self,
        migration_id: str,
        source_chat_id: str,
        source_message_id: str,
        *,
        error_code: str,
        error_payload: dict[str, object],
    ) -> MessageMappingRecord:
        statement = (
            update(MigrationMessageMapModel)
            .where(
                MigrationMessageMapModel.migration_id == migration_id,
                MigrationMessageMapModel.source_chat_id == source_chat_id,
                MigrationMessageMapModel.source_message_id == source_message_id,
            )
            .values(
                import_status=MessageImportStatus.AMBIGUOUS.value,
                imported_at=None,
                last_error_code=error_code,
                last_error_payload=self._sanitize_json_payload(error_payload),
                updated_at=_now(),
            )
            .returning(MigrationMessageMapModel)
        )
        async with self._session_factory() as session:
            model = (await session.execute(statement)).scalar_one()
            await session.commit()
            return _message_to_domain(model)

    async def list_failed(
        self,
        migration_id: str,
        *,
        source_chat_id: str | None = None,
        limit: int = 100,
    ) -> list[MessageMappingRecord]:
        filters = [
            MigrationMessageMapModel.migration_id == migration_id,
            MigrationMessageMapModel.import_status == MessageImportStatus.FAILED.value,
        ]
        if source_chat_id:
            filters.append(MigrationMessageMapModel.source_chat_id == source_chat_id)

        statement = (
            select(MigrationMessageMapModel)
            .where(*filters)
            .order_by(
                MigrationMessageMapModel.source_sent_at.asc(),
                MigrationMessageMapModel.source_message_id.asc(),
            )
            .limit(limit)
        )
        async with self._session_factory() as session:
            result = await session.execute(statement)
            models = result.scalars().all()
            return [_message_to_domain(model) for model in models]

    async def requeue_failed(
        self,
        migration_id: str,
        *,
        source_chat_id: str | None = None,
        limit: int = 100,
    ) -> list[MessageMappingRecord]:
        failed = await self.list_failed(
            migration_id,
            source_chat_id=source_chat_id,
            limit=limit,
        )
        if not failed:
            return []

        keys = [
            (item.source_chat_id, item.source_message_id)
            for item in failed
        ]
        statement = (
            update(MigrationMessageMapModel)
            .where(
                MigrationMessageMapModel.migration_id == migration_id,
                MigrationMessageMapModel.import_status == MessageImportStatus.FAILED.value,
                tuple_(
                    MigrationMessageMapModel.source_chat_id,
                    MigrationMessageMapModel.source_message_id,
                ).in_(
                    keys,
                ),
            )
            .values(
                import_status=MessageImportStatus.PROCESSING.value,
                imported_at=None,
                last_error_code=None,
                last_error_payload=None,
                updated_at=_now(),
            )
            .returning(MigrationMessageMapModel)
        )
        async with self._session_factory() as session:
            result = await session.execute(statement)
            await session.commit()
            return [_message_to_domain(model) for model in result.scalars().all()]

    async def summarize_by_chat(
        self,
        migration_id: str,
    ) -> list[MessageStatusSummary]:
        statement = (
            select(
                MigrationMessageMapModel.source_chat_id,
                MigrationMessageMapModel.import_status,
                func.count().label("count"),
            )
            .where(MigrationMessageMapModel.migration_id == migration_id)
            .group_by(
                MigrationMessageMapModel.source_chat_id,
                MigrationMessageMapModel.import_status,
            )
            .order_by(
                MigrationMessageMapModel.source_chat_id.asc(),
                MigrationMessageMapModel.import_status.asc(),
            )
        )
        async with self._session_factory() as session:
            rows = (await session.execute(statement)).all()
            return [
                MessageStatusSummary(
                    migration_id=migration_id,
                    source_chat_id=row.source_chat_id,
                    import_status=MessageImportStatus(row.import_status),
                    count=int(row.count),
                )
                for row in rows
            ]

    async def mark_failed(
        self,
        migration_id: str,
        source_chat_id: str,
        source_message_id: str,
        *,
        error_code: str,
        error_payload: dict[str, object],
    ) -> MessageMappingRecord:
        statement = (
            update(MigrationMessageMapModel)
            .where(
                MigrationMessageMapModel.migration_id == migration_id,
                MigrationMessageMapModel.source_chat_id == source_chat_id,
                MigrationMessageMapModel.source_message_id == source_message_id,
            )
            .values(
                import_status=MessageImportStatus.FAILED.value,
                last_error_code=error_code,
                last_error_payload=self._sanitize_json_payload(error_payload),
                updated_at=_now(),
            )
            .returning(MigrationMessageMapModel)
        )
        async with self._session_factory() as session:
            model = (await session.execute(statement)).scalar_one()
            await session.commit()
            return _message_to_domain(model)

    async def _get_model(
        self,
        session: AsyncSession,
        migration_id: str,
        source_chat_id: str,
        source_message_id: str,
    ) -> MigrationMessageMapModel | None:
        result = await session.execute(
            select(MigrationMessageMapModel).where(
                MigrationMessageMapModel.migration_id == migration_id,
                MigrationMessageMapModel.source_chat_id == source_chat_id,
                MigrationMessageMapModel.source_message_id == source_message_id,
            ),
        )
        return result.scalar_one_or_none()

    def _sanitize_rendered_body(self, rendered_body: str | None) -> str | None:
        if not self._persist_message_bodies:
            return None
        return rendered_body

    def _sanitize_json_payload(
        self,
        payload: dict[str, object] | None,
    ) -> dict[str, object]:
        return _sanitize_payload(payload, mode=self._payload_storage_mode)


class PostgresAttachmentMappingRepository:
    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        *,
        payload_storage_mode: str = _REDACTED_PAYLOAD_STORAGE_MODE,
    ) -> None:
        self._session_factory = session_factory
        self._payload_storage_mode = payload_storage_mode

    async def claim(
        self,
        record: AttachmentMappingRecord,
        *,
        retry_failed: bool = False,
    ) -> AttachmentClaimResult:
        now = _now()
        statement = (
            insert(MigrationAttachmentMapModel)
            .values(
                migration_id=record.migration_id,
                source_chat_id=record.source_chat_id,
                source_message_id=record.source_message_id,
                attachment_index=record.attachment_index,
                source_file_id=record.source_file_id,
                source_filename=record.source_filename,
                media_kind=record.media_kind,
                checksum=record.checksum,
                size_bytes=record.size_bytes,
                target_chat_id=record.target_chat_id,
                target_sync_id=record.target_sync_id,
                import_status=record.import_status.value,
                imported_at=record.imported_at,
                last_error_code=record.last_error_code,
                last_error_payload=self._sanitize_json_payload(record.last_error_payload),
                created_at=now,
                updated_at=now,
            )
            .on_conflict_do_nothing(
                constraint="uq_migration_attachment_map_key",
            )
            .returning(MigrationAttachmentMapModel)
        )
        async with self._session_factory() as session:
            inserted = (await session.execute(statement)).scalar_one_or_none()
            if inserted:
                await session.commit()
                return AttachmentClaimResult(
                    state=ClaimState.CLAIMED,
                    record=_attachment_to_domain(inserted),
                )

            existing = await self._get_model(
                session,
                record.migration_id,
                record.source_chat_id,
                record.source_message_id,
                record.attachment_index,
            )
            if existing is None:
                await session.rollback()
                return AttachmentClaimResult(state=ClaimState.AMBIGUOUS, record=record)
            existing_record = _attachment_to_domain(existing)
            await session.rollback()

            if existing_record.import_status is AttachmentImportStatus.FAILED and retry_failed:
                updated = await session.execute(
                    update(MigrationAttachmentMapModel)
                    .where(
                        MigrationAttachmentMapModel.migration_id == record.migration_id,
                        MigrationAttachmentMapModel.source_chat_id == record.source_chat_id,
                        MigrationAttachmentMapModel.source_message_id == record.source_message_id,
                        MigrationAttachmentMapModel.attachment_index == record.attachment_index,
                    )
                    .values(
                        import_status=AttachmentImportStatus.PROCESSING.value,
                        imported_at=None,
                        last_error_code=None,
                        last_error_payload=None,
                        updated_at=_now(),
                    )
                    .returning(MigrationAttachmentMapModel),
                )
                model = updated.scalar_one()
                await session.commit()
                return AttachmentClaimResult(
                    state=ClaimState.CLAIMED,
                    record=_attachment_to_domain(model),
                )

            if (
                existing_record.import_status is AttachmentImportStatus.FAILED
                and _is_retryable_cancel_error_code(existing_record.last_error_code)
            ):
                updated = await session.execute(
                    update(MigrationAttachmentMapModel)
                    .where(
                        MigrationAttachmentMapModel.migration_id == record.migration_id,
                        MigrationAttachmentMapModel.source_chat_id == record.source_chat_id,
                        MigrationAttachmentMapModel.source_message_id == record.source_message_id,
                        MigrationAttachmentMapModel.attachment_index == record.attachment_index,
                    )
                    .values(
                        import_status=AttachmentImportStatus.PROCESSING.value,
                        imported_at=None,
                        last_error_code=None,
                        last_error_payload=None,
                        updated_at=_now(),
                    )
                    .returning(MigrationAttachmentMapModel),
                )
                model = updated.scalar_one()
                await session.commit()
                return AttachmentClaimResult(
                    state=ClaimState.CLAIMED,
                    record=_attachment_to_domain(model),
                )

            if existing_record.import_status is AttachmentImportStatus.IMPORTED:
                return AttachmentClaimResult(
                    state=ClaimState.IMPORTED,
                    record=existing_record,
                )
            if existing_record.import_status is AttachmentImportStatus.FAILED:
                return AttachmentClaimResult(
                    state=ClaimState.FAILED,
                    record=existing_record,
                )
            if existing_record.import_status is AttachmentImportStatus.AMBIGUOUS:
                return AttachmentClaimResult(
                    state=ClaimState.AMBIGUOUS,
                    record=existing_record,
                )
            if existing_record.import_status is AttachmentImportStatus.SKIPPED:
                return AttachmentClaimResult(
                    state=ClaimState.AMBIGUOUS,
                    record=existing_record,
                )
            return AttachmentClaimResult(
                state=ClaimState.AMBIGUOUS,
                record=existing_record,
            )

    async def get(
        self,
        migration_id: str,
        source_chat_id: str,
        source_message_id: str,
        attachment_index: int,
    ) -> AttachmentMappingRecord | None:
        async with self._session_factory() as session:
            model = await self._get_model(
                session,
                migration_id,
                source_chat_id,
                source_message_id,
                attachment_index,
            )
            return _attachment_to_domain(model) if model else None

    async def list_by_message(
        self,
        migration_id: str,
        source_chat_id: str,
        source_message_id: str,
    ) -> list[AttachmentMappingRecord]:
        statement = (
            select(MigrationAttachmentMapModel)
            .where(
                MigrationAttachmentMapModel.migration_id == migration_id,
                MigrationAttachmentMapModel.source_chat_id == source_chat_id,
                MigrationAttachmentMapModel.source_message_id == source_message_id,
            )
            .order_by(MigrationAttachmentMapModel.attachment_index.asc())
        )
        async with self._session_factory() as session:
            result = await session.execute(statement)
            models = result.scalars().all()
            return [_attachment_to_domain(model) for model in models]

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
        statement = (
            update(MigrationAttachmentMapModel)
            .where(
                MigrationAttachmentMapModel.migration_id == migration_id,
                MigrationAttachmentMapModel.source_chat_id == source_chat_id,
                MigrationAttachmentMapModel.source_message_id == source_message_id,
                MigrationAttachmentMapModel.attachment_index == attachment_index,
            )
            .values(
                source_file_id=attachment.source_file_id,
                source_filename=attachment.filename,
                media_kind=attachment.media_kind,
                checksum=attachment.sha256,
                size_bytes=attachment.size_bytes,
                import_status=AttachmentImportStatus.IMPORTED.value,
                imported_at=_now(),
                target_sync_id=target_sync_id,
                last_error_code=None,
                last_error_payload=None,
                updated_at=_now(),
            )
            .returning(MigrationAttachmentMapModel)
        )
        async with self._session_factory() as session:
            model = (await session.execute(statement)).scalar_one()
            await session.commit()
            return _attachment_to_domain(model)

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
        statement = (
            update(MigrationAttachmentMapModel)
            .where(
                MigrationAttachmentMapModel.migration_id == migration_id,
                MigrationAttachmentMapModel.source_chat_id == source_chat_id,
                MigrationAttachmentMapModel.source_message_id == source_message_id,
                MigrationAttachmentMapModel.attachment_index == attachment_index,
            )
            .values(
                source_file_id=attachment.source_file_id if attachment else MigrationAttachmentMapModel.source_file_id,
                source_filename=attachment.filename if attachment else MigrationAttachmentMapModel.source_filename,
                media_kind=attachment.media_kind if attachment else MigrationAttachmentMapModel.media_kind,
                checksum=attachment.sha256 if attachment else MigrationAttachmentMapModel.checksum,
                size_bytes=attachment.size_bytes if attachment else MigrationAttachmentMapModel.size_bytes,
                import_status=AttachmentImportStatus.FAILED.value,
                imported_at=None,
                last_error_code=error_code,
                last_error_payload=self._sanitize_json_payload(error_payload),
                updated_at=_now(),
            )
            .returning(MigrationAttachmentMapModel)
        )
        async with self._session_factory() as session:
            model = (await session.execute(statement)).scalar_one()
            await session.commit()
            return _attachment_to_domain(model)

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
        statement = (
            update(MigrationAttachmentMapModel)
            .where(
                MigrationAttachmentMapModel.migration_id == migration_id,
                MigrationAttachmentMapModel.source_chat_id == source_chat_id,
                MigrationAttachmentMapModel.source_message_id == source_message_id,
                MigrationAttachmentMapModel.attachment_index == attachment_index,
            )
            .values(
                source_file_id=attachment.source_file_id if attachment else MigrationAttachmentMapModel.source_file_id,
                source_filename=attachment.filename if attachment else MigrationAttachmentMapModel.source_filename,
                media_kind=attachment.media_kind if attachment else MigrationAttachmentMapModel.media_kind,
                checksum=attachment.sha256 if attachment else MigrationAttachmentMapModel.checksum,
                size_bytes=attachment.size_bytes if attachment else MigrationAttachmentMapModel.size_bytes,
                import_status=AttachmentImportStatus.AMBIGUOUS.value,
                imported_at=None,
                last_error_code=error_code,
                last_error_payload=self._sanitize_json_payload(error_payload),
                updated_at=_now(),
            )
            .returning(MigrationAttachmentMapModel)
        )
        async with self._session_factory() as session:
            model = (await session.execute(statement)).scalar_one()
            await session.commit()
            return _attachment_to_domain(model)

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
        statement = (
            update(MigrationAttachmentMapModel)
            .where(
                MigrationAttachmentMapModel.migration_id == migration_id,
                MigrationAttachmentMapModel.source_chat_id == source_chat_id,
                MigrationAttachmentMapModel.source_message_id == source_message_id,
                MigrationAttachmentMapModel.attachment_index == attachment_index,
            )
            .values(
                source_file_id=attachment.source_file_id,
                source_filename=attachment.filename,
                media_kind=attachment.media_kind,
                checksum=attachment.sha256,
                size_bytes=attachment.size_bytes,
                import_status=AttachmentImportStatus.SKIPPED.value,
                imported_at=None,
                last_error_code="skipped",
                last_error_payload=self._sanitize_json_payload({"reason": reason}),
                updated_at=_now(),
            )
            .returning(MigrationAttachmentMapModel)
        )
        async with self._session_factory() as session:
            model = (await session.execute(statement)).scalar_one()
            await session.commit()
            return _attachment_to_domain(model)

    async def summarize_by_chat(
        self,
        migration_id: str,
    ) -> list[AttachmentStatusSummary]:
        statement = (
            select(
                MigrationAttachmentMapModel.source_chat_id,
                MigrationAttachmentMapModel.import_status,
                func.count().label("count"),
            )
            .where(MigrationAttachmentMapModel.migration_id == migration_id)
            .group_by(
                MigrationAttachmentMapModel.source_chat_id,
                MigrationAttachmentMapModel.import_status,
            )
            .order_by(
                MigrationAttachmentMapModel.source_chat_id.asc(),
                MigrationAttachmentMapModel.import_status.asc(),
            )
        )
        async with self._session_factory() as session:
            rows = (await session.execute(statement)).all()
            return [
                AttachmentStatusSummary(
                    migration_id=migration_id,
                    source_chat_id=row.source_chat_id,
                    import_status=AttachmentImportStatus(row.import_status),
                    count=int(row.count),
                )
                for row in rows
            ]

    def _sanitize_json_payload(
        self,
        payload: dict[str, object] | None,
    ) -> dict[str, object]:
        return _sanitize_payload(payload, mode=self._payload_storage_mode)

    async def _get_model(
        self,
        session: AsyncSession,
        migration_id: str,
        source_chat_id: str,
        source_message_id: str,
        attachment_index: int,
    ) -> MigrationAttachmentMapModel | None:
        result = await session.execute(
            select(MigrationAttachmentMapModel).where(
                MigrationAttachmentMapModel.migration_id == migration_id,
                MigrationAttachmentMapModel.source_chat_id == source_chat_id,
                MigrationAttachmentMapModel.source_message_id == source_message_id,
                MigrationAttachmentMapModel.attachment_index == attachment_index,
            ),
        )
        return result.scalar_one_or_none()


class PostgresAttachmentStageRepository:
    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        *,
        payload_storage_mode: str = _REDACTED_PAYLOAD_STORAGE_MODE,
        processing_stale_after_seconds: float = 30.0,
    ) -> None:
        self._session_factory = session_factory
        self._payload_storage_mode = payload_storage_mode
        self._processing_stale_after = timedelta(
            seconds=max(processing_stale_after_seconds, 1.0),
        )

    async def claim(
        self,
        record: AttachmentStageRecord,
        *,
        retry_failed: bool = False,
    ) -> AttachmentStageClaimResult:
        now = _now()
        statement = (
            insert(AttachmentStageModel)
            .values(
                migration_id=record.migration_id,
                source_chat_id=record.source_chat_id,
                source_message_id=record.source_message_id,
                attachment_index=record.attachment_index,
                target_chat_id=record.target_chat_id,
                source_locator=record.source_locator,
                media_kind=record.media_kind,
                checksum=record.checksum,
                size_bytes=record.size_bytes,
                status=record.status.value,
                express_file_id=record.express_file_id,
                express_file_payload=self._sanitize_json_payload(record.express_file_payload),
                target_sync_id=record.target_sync_id,
                last_error_code=record.last_error_code,
                last_error_payload=self._sanitize_json_payload(record.last_error_payload),
                created_at=now,
                updated_at=now,
            )
            .on_conflict_do_nothing(constraint="uq_attachment_stage_key")
            .returning(AttachmentStageModel)
        )
        async with self._session_factory() as session:
            inserted = (await session.execute(statement)).scalar_one_or_none()
            if inserted is not None:
                await session.commit()
                return AttachmentStageClaimResult(
                    state=ClaimState.CLAIMED,
                    record=_attachment_stage_to_domain(inserted),
                )

            existing = await self._get_model(
                session,
                record.migration_id,
                record.source_chat_id,
                record.source_message_id,
                record.attachment_index,
                record.target_chat_id,
            )
            if existing is None:
                await session.rollback()
                return AttachmentStageClaimResult(
                    state=ClaimState.AMBIGUOUS,
                    record=record,
                )
            if existing.status == AttachmentStageStatus.PROCESSING.value:
                reclaimed = await self._reclaim_stale_processing(
                    session=session,
                    existing=existing,
                    record=record,
                    force=_is_retryable_cancel_error_code(existing.last_error_code),
                )
                if reclaimed is not None:
                    await session.commit()
                    return AttachmentStageClaimResult(
                        state=ClaimState.CLAIMED,
                        record=_attachment_stage_to_domain(reclaimed),
                    )
            existing_record = _attachment_stage_to_domain(existing)
            await session.rollback()

            if existing_record.status is AttachmentStageStatus.FAILED and retry_failed:
                updated = await session.execute(
                    update(AttachmentStageModel)
                    .where(
                        AttachmentStageModel.migration_id == record.migration_id,
                        AttachmentStageModel.source_chat_id == record.source_chat_id,
                        AttachmentStageModel.source_message_id == record.source_message_id,
                        AttachmentStageModel.attachment_index == record.attachment_index,
                        AttachmentStageModel.target_chat_id == record.target_chat_id,
                    )
                    .values(
                        status=AttachmentStageStatus.PROCESSING.value,
                        last_error_code=None,
                        last_error_payload=None,
                        updated_at=_now(),
                    )
                    .returning(AttachmentStageModel),
                )
                model = updated.scalar_one()
                await session.commit()
                return AttachmentStageClaimResult(
                    state=ClaimState.CLAIMED,
                    record=_attachment_stage_to_domain(model),
                )

            if (
                existing_record.status is AttachmentStageStatus.FAILED
                and _is_retryable_cancel_error_code(existing_record.last_error_code)
            ):
                updated = await session.execute(
                    update(AttachmentStageModel)
                    .where(
                        AttachmentStageModel.migration_id == record.migration_id,
                        AttachmentStageModel.source_chat_id == record.source_chat_id,
                        AttachmentStageModel.source_message_id == record.source_message_id,
                        AttachmentStageModel.attachment_index == record.attachment_index,
                        AttachmentStageModel.target_chat_id == record.target_chat_id,
                    )
                    .values(
                        status=AttachmentStageStatus.PROCESSING.value,
                        last_error_code=None,
                        last_error_payload=None,
                        updated_at=_now(),
                    )
                    .returning(AttachmentStageModel),
                )
                model = updated.scalar_one()
                await session.commit()
                return AttachmentStageClaimResult(
                    state=ClaimState.CLAIMED,
                    record=_attachment_stage_to_domain(model),
                )

            if existing_record.status in (
                AttachmentStageStatus.UPLOADED,
                AttachmentStageStatus.ATTACHED,
            ):
                return AttachmentStageClaimResult(
                    state=ClaimState.IMPORTED,
                    record=existing_record,
                )
            if existing_record.status is AttachmentStageStatus.FAILED:
                return AttachmentStageClaimResult(
                    state=ClaimState.FAILED,
                    record=existing_record,
                )
            return AttachmentStageClaimResult(
                state=ClaimState.AMBIGUOUS,
                record=existing_record,
            )

    async def _reclaim_stale_processing(
        self,
        *,
        session: AsyncSession,
        existing: AttachmentStageModel,
        record: AttachmentStageRecord,
        force: bool = False,
    ) -> AttachmentStageModel | None:
        if not force and existing.updated_at > _now() - self._processing_stale_after:
            return None
        statement = (
            update(AttachmentStageModel)
            .where(
                AttachmentStageModel.migration_id == record.migration_id,
                AttachmentStageModel.source_chat_id == record.source_chat_id,
                AttachmentStageModel.source_message_id == record.source_message_id,
                AttachmentStageModel.attachment_index == record.attachment_index,
                AttachmentStageModel.target_chat_id == record.target_chat_id,
                AttachmentStageModel.status == AttachmentStageStatus.PROCESSING.value,
                AttachmentStageModel.updated_at == existing.updated_at,
            )
            .values(
                source_locator=record.source_locator,
                media_kind=record.media_kind,
                checksum=record.checksum,
                size_bytes=record.size_bytes,
                status=AttachmentStageStatus.PROCESSING.value,
                express_file_id=None,
                express_file_payload=None,
                target_sync_id=None,
                last_error_code=None,
                last_error_payload=None,
                updated_at=_now(),
            )
            .returning(AttachmentStageModel)
        )
        return (await session.execute(statement)).scalar_one_or_none()

    async def get(
        self,
        migration_id: str,
        source_chat_id: str,
        source_message_id: str,
        attachment_index: int,
        target_chat_id: str,
    ) -> AttachmentStageRecord | None:
        async with self._session_factory() as session:
            model = await self._get_model(
                session,
                migration_id,
                source_chat_id,
                source_message_id,
                attachment_index,
                target_chat_id,
            )
            return _attachment_stage_to_domain(model) if model else None

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
        statement = (
            update(AttachmentStageModel)
            .where(
                AttachmentStageModel.migration_id == migration_id,
                AttachmentStageModel.source_chat_id == source_chat_id,
                AttachmentStageModel.source_message_id == source_message_id,
                AttachmentStageModel.attachment_index == attachment_index,
                AttachmentStageModel.target_chat_id == target_chat_id,
            )
            .values(
                status=AttachmentStageStatus.UPLOADED.value,
                express_file_id=staged_file.file_id,
                express_file_payload=self._sanitize_json_payload(
                    _express_staged_file_to_payload(staged_file),
                ),
                last_error_code=None,
                last_error_payload=None,
                updated_at=_now(),
            )
            .returning(AttachmentStageModel)
        )
        async with self._session_factory() as session:
            model = (await session.execute(statement)).scalar_one()
            await session.commit()
            return _attachment_stage_to_domain(model)

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
        statement = (
            update(AttachmentStageModel)
            .where(
                AttachmentStageModel.migration_id == migration_id,
                AttachmentStageModel.source_chat_id == source_chat_id,
                AttachmentStageModel.source_message_id == source_message_id,
                AttachmentStageModel.attachment_index == attachment_index,
                AttachmentStageModel.target_chat_id == target_chat_id,
            )
            .values(
                status=AttachmentStageStatus.ATTACHED.value,
                target_sync_id=target_sync_id,
                last_error_code=None,
                last_error_payload=None,
                updated_at=_now(),
            )
            .returning(AttachmentStageModel)
        )
        async with self._session_factory() as session:
            model = (await session.execute(statement)).scalar_one()
            await session.commit()
            return _attachment_stage_to_domain(model)

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
        statement = (
            update(AttachmentStageModel)
            .where(
                AttachmentStageModel.migration_id == migration_id,
                AttachmentStageModel.source_chat_id == source_chat_id,
                AttachmentStageModel.source_message_id == source_message_id,
                AttachmentStageModel.attachment_index == attachment_index,
                AttachmentStageModel.target_chat_id == target_chat_id,
            )
            .values(
                status=AttachmentStageStatus.FAILED.value,
                last_error_code=error_code,
                last_error_payload=self._sanitize_json_payload(error_payload),
                updated_at=_now(),
            )
            .returning(AttachmentStageModel)
        )
        async with self._session_factory() as session:
            model = (await session.execute(statement)).scalar_one()
            await session.commit()
            return _attachment_stage_to_domain(model)

    def _sanitize_json_payload(
        self,
        payload: dict[str, object] | None,
    ) -> dict[str, object]:
        return _sanitize_payload(payload, mode=self._payload_storage_mode)

    async def _get_model(
        self,
        session: AsyncSession,
        migration_id: str,
        source_chat_id: str,
        source_message_id: str,
        attachment_index: int,
        target_chat_id: str,
    ) -> AttachmentStageModel | None:
        result = await session.execute(
            select(AttachmentStageModel).where(
                AttachmentStageModel.migration_id == migration_id,
                AttachmentStageModel.source_chat_id == source_chat_id,
                AttachmentStageModel.source_message_id == source_message_id,
                AttachmentStageModel.attachment_index == attachment_index,
                AttachmentStageModel.target_chat_id == target_chat_id,
            ),
        )
        return result.scalar_one_or_none()


class PostgresCheckpointRepository:
    def __init__(self, session_factory: async_sessionmaker[AsyncSession]) -> None:
        self._session_factory = session_factory

    async def get(
        self,
        migration_id: str,
        source_chat_id: str,
        mode: str,
    ) -> MigrationCheckpoint | None:
        async with self._session_factory() as session:
            result = await session.execute(
                select(MigrationCheckpointModel).where(
                    MigrationCheckpointModel.migration_id == migration_id,
                    MigrationCheckpointModel.source_chat_id == source_chat_id,
                    MigrationCheckpointModel.mode == mode,
                ),
            )
            model = result.scalar_one_or_none()
            return _checkpoint_to_domain(model) if model else None

    async def save(self, checkpoint: MigrationCheckpoint) -> None:
        statement = (
            insert(MigrationCheckpointModel)
            .values(
                migration_id=checkpoint.migration_id,
                source_chat_id=checkpoint.source_chat_id,
                mode=checkpoint.mode,
                cursor_type="source_message_id",
                cursor_value=checkpoint.cursor.last_source_message_id,
                last_source_message_id=checkpoint.cursor.last_source_message_id,
                last_source_sent_at=checkpoint.cursor.last_source_sent_at,
                updated_at=checkpoint.updated_at,
            )
            .on_conflict_do_update(
                constraint="uq_migration_checkpoint_key",
                set_={
                    "cursor_type": "source_message_id",
                    "cursor_value": checkpoint.cursor.last_source_message_id,
                    "last_source_message_id": checkpoint.cursor.last_source_message_id,
                    "last_source_sent_at": checkpoint.cursor.last_source_sent_at,
                    "updated_at": checkpoint.updated_at,
                },
            )
        )
        async with self._session_factory() as session:
            await session.execute(statement)
            await session.commit()

class PostgresInventorySnapshotRepository:
    def __init__(self, session_factory: async_sessionmaker[AsyncSession]) -> None:
        self._session_factory = session_factory

    async def replace_for_migration(
        self,
        migration_id: str,
        snapshots: list[InventorySnapshotRecord],
    ) -> None:
        async with self._session_factory() as session:
            await session.execute(
                delete(MigrationInventorySnapshotModel).where(
                    MigrationInventorySnapshotModel.migration_id == migration_id,
                ),
            )
            for snapshot in snapshots:
                await session.execute(
                    insert(MigrationInventorySnapshotModel).values(
                        migration_id=snapshot.migration_id,
                        source_chat_id=snapshot.source_chat_id,
                        source_chat_type=snapshot.source_chat_type,
                        source_chat_title=snapshot.source_chat_title,
                        messages_count=snapshot.message_count,
                        media_count=snapshot.media_count,
                        approximate_bytes=snapshot.approximate_bytes,
                        snapshot_payload={},
                        created_at=snapshot.captured_at,
                        updated_at=snapshot.captured_at,
                    ),
                )
            await session.commit()

    async def save(self, record: InventorySnapshotRecord) -> InventorySnapshotRecord:
        now = _now()
        statement = (
            insert(MigrationInventorySnapshotModel)
            .values(
                migration_id=record.migration_id,
                source_chat_id=record.source_chat_id,
                source_chat_type=record.source_chat_type,
                source_chat_title=record.source_chat_title,
                messages_count=record.message_count,
                media_count=record.media_count,
                approximate_bytes=record.approximate_bytes,
                snapshot_payload={},
                created_at=record.captured_at or now,
                updated_at=record.captured_at or now,
            )
            .on_conflict_do_update(
                constraint="uq_migration_inventory_snapshot_key",
                set_={
                    "source_chat_type": record.source_chat_type,
                    "source_chat_title": record.source_chat_title,
                    "messages_count": record.message_count,
                    "media_count": record.media_count,
                    "approximate_bytes": record.approximate_bytes,
                    "snapshot_payload": {},
                    "updated_at": record.captured_at or now,
                },
            )
            .returning(MigrationInventorySnapshotModel)
        )
        async with self._session_factory() as session:
            model = (await session.execute(statement)).scalar_one()
            await session.commit()
            return _inventory_to_record(model)

    async def get(
        self,
        migration_id: str,
        source_chat_id: str,
    ) -> InventorySnapshotRecord | None:
        statement = select(MigrationInventorySnapshotModel).where(
            MigrationInventorySnapshotModel.migration_id == migration_id,
            MigrationInventorySnapshotModel.source_chat_id == source_chat_id,
        )
        async with self._session_factory() as session:
            model = (await session.execute(statement)).scalar_one_or_none()
            return _inventory_to_record(model) if model else None

    async def list_by_migration(
        self,
        migration_id: str,
    ) -> list[InventorySnapshotRecord]:
        statement = (
            select(MigrationInventorySnapshotModel)
            .where(MigrationInventorySnapshotModel.migration_id == migration_id)
            .order_by(MigrationInventorySnapshotModel.source_chat_id.asc())
        )
        async with self._session_factory() as session:
            models = (await session.execute(statement)).scalars().all()
            return [_inventory_to_record(model) for model in models]


class PostgresTelegramExportSnapshotRepository:
    def __init__(self, session_factory: async_sessionmaker[AsyncSession]) -> None:
        self._session_factory = session_factory

    async def get(
        self,
        source_chat_id: str,
    ) -> TelegramExportSnapshotRecord | None:
        statement = select(TelegramExportSnapshotModel).where(
            TelegramExportSnapshotModel.source_chat_id == source_chat_id,
        )
        async with self._session_factory() as session:
            model = (await session.execute(statement)).scalar_one_or_none()
            return _telegram_export_snapshot_to_domain(model) if model else None

    async def save(
        self,
        record: TelegramExportSnapshotRecord,
    ) -> TelegramExportSnapshotRecord:
        statement = (
            insert(TelegramExportSnapshotModel)
            .values(
                source_chat_id=record.source_chat_id,
                source_chat_title=record.source_chat_title,
                source_chat_type=record.source_chat_type,
                original_filename=record.original_filename,
                export_chat_id=record.export_chat_id,
                archive_locator=record.archive_locator,
                payload_sha256=record.payload_sha256,
                message_count=record.message_count,
                media_count=record.media_count,
                approximate_bytes=record.approximate_bytes,
                uploaded_by_huid=record.uploaded_by_huid,
                created_at=record.created_at,
                updated_at=record.updated_at,
            )
            .on_conflict_do_update(
                constraint="uq_telegram_export_snapshot_source_chat",
                set_={
                    "source_chat_title": record.source_chat_title,
                    "source_chat_type": record.source_chat_type,
                    "original_filename": record.original_filename,
                    "export_chat_id": record.export_chat_id,
                    "archive_locator": record.archive_locator,
                    "payload_sha256": record.payload_sha256,
                    "message_count": record.message_count,
                    "media_count": record.media_count,
                    "approximate_bytes": record.approximate_bytes,
                    "uploaded_by_huid": record.uploaded_by_huid,
                    "updated_at": record.updated_at,
                },
            )
            .returning(TelegramExportSnapshotModel)
        )
        async with self._session_factory() as session:
            model = (await session.execute(statement)).scalar_one()
            await session.commit()
            return _telegram_export_snapshot_to_domain(model)

    async def delete(
        self,
        source_chat_id: str,
    ) -> bool:
        statement = (
            delete(TelegramExportSnapshotModel)
            .where(TelegramExportSnapshotModel.source_chat_id == source_chat_id)
            .returning(TelegramExportSnapshotModel.id)
        )
        async with self._session_factory() as session:
            deleted_id = (await session.execute(statement)).scalar_one_or_none()
            await session.commit()
            return deleted_id is not None


class PostgresIdentityMappingRepository:
    def __init__(self, session_factory: async_sessionmaker[AsyncSession]) -> None:
        self._session_factory = session_factory

    async def get_by_user_id(
        self,
        telegram_user_id: str,
        *,
        express_host: str | None = None,
    ) -> IdentityMappingRecord | None:
        statement = select(IdentityMappingModel).where(
            IdentityMappingModel.telegram_user_id == telegram_user_id,
            IdentityMappingModel.express_host == express_host,
        )
        async with self._session_factory() as session:
            model = (await session.execute(statement)).scalar_one_or_none()
            return _identity_mapping_to_domain(model) if model else None

    async def get_by_username(
        self,
        telegram_username: str,
        *,
        express_host: str | None = None,
    ) -> IdentityMappingRecord | None:
        statement = select(IdentityMappingModel).where(
            IdentityMappingModel.telegram_username == telegram_username,
            IdentityMappingModel.express_host == express_host,
        )
        async with self._session_factory() as session:
            model = (await session.execute(statement)).scalar_one_or_none()
            return _identity_mapping_to_domain(model) if model else None

    async def save(
        self,
        record: IdentityMappingRecord,
    ) -> IdentityMappingRecord:
        async with self._session_factory() as session:
            normalized_host = record.express_host.strip().lower() if record.express_host else None
            normalized_user_id = record.telegram_user_id.strip() if record.telegram_user_id else None
            normalized_username = (
                record.telegram_username.strip().lstrip("@").lower()
                if record.telegram_username
                else None
            )
            by_user_id = None
            by_username = None
            if normalized_user_id is not None:
                by_user_id = (
                    await session.execute(
                        select(IdentityMappingModel).where(
                            IdentityMappingModel.express_host == normalized_host,
                            IdentityMappingModel.telegram_user_id == normalized_user_id,
                        ),
                    )
                ).scalar_one_or_none()
            if normalized_username is not None:
                by_username = (
                    await session.execute(
                        select(IdentityMappingModel).where(
                            IdentityMappingModel.express_host == normalized_host,
                            IdentityMappingModel.telegram_username == normalized_username,
                        ),
                    )
                ).scalar_one_or_none()

            model = by_user_id or by_username
            if by_user_id is not None and by_username is not None and by_user_id.id != by_username.id:
                primary = by_user_id
                secondary = by_username
                primary.express_host = normalized_host or primary.express_host or secondary.express_host
                primary.telegram_user_id = normalized_user_id or primary.telegram_user_id
                primary.telegram_username = normalized_username or primary.telegram_username
                primary.telegram_display_name = (
                    record.telegram_display_name or primary.telegram_display_name or secondary.telegram_display_name
                )
                primary.corporate_email = record.corporate_email
                primary.target_huid = record.target_huid
                primary.last_resolved_at = record.last_resolved_at
                primary.created_at = primary.created_at or secondary.created_at or record.created_at or _now()
                primary.updated_at = record.updated_at or _now()
                await session.delete(secondary)
                model = primary
            elif model is None:
                model = IdentityMappingModel(
                    express_host=normalized_host,
                    telegram_user_id=normalized_user_id,
                    telegram_username=normalized_username,
                    telegram_display_name=record.telegram_display_name,
                    corporate_email=record.corporate_email,
                    target_huid=record.target_huid,
                    last_resolved_at=record.last_resolved_at,
                    created_at=record.created_at or _now(),
                    updated_at=record.updated_at or _now(),
                )
                session.add(model)
            else:
                model.express_host = normalized_host or model.express_host
                model.telegram_user_id = normalized_user_id or model.telegram_user_id
                model.telegram_username = normalized_username or model.telegram_username
                model.telegram_display_name = (
                    record.telegram_display_name or model.telegram_display_name
                )
                model.corporate_email = record.corporate_email
                model.target_huid = record.target_huid
                model.last_resolved_at = record.last_resolved_at
                model.updated_at = record.updated_at or _now()
            await session.commit()
            await session.refresh(model)
            return _identity_mapping_to_domain(model)


class PostgresExpressUserCtsBindingRepository:
    def __init__(self, session_factory: async_sessionmaker[AsyncSession]) -> None:
        self._session_factory = session_factory

    async def get_by_target_huid(
        self,
        target_huid: str,
    ) -> ExpressUserCtsBindingRecord | None:
        normalized_huid = target_huid.strip()
        if not normalized_huid:
            return None
        statement = select(ExpressUserCtsBindingModel).where(
            ExpressUserCtsBindingModel.target_huid == normalized_huid,
        )
        async with self._session_factory() as session:
            model = (await session.execute(statement)).scalar_one_or_none()
            return _express_user_cts_binding_to_domain(model) if model else None

    async def get_by_email(
        self,
        corporate_email: str,
    ) -> ExpressUserCtsBindingRecord | None:
        normalized_email = corporate_email.strip().lower()
        if not normalized_email:
            return None
        statement = select(ExpressUserCtsBindingModel).where(
            ExpressUserCtsBindingModel.corporate_email == normalized_email,
        )
        async with self._session_factory() as session:
            model = (await session.execute(statement)).scalar_one_or_none()
            return _express_user_cts_binding_to_domain(model) if model else None

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
        now = _now()
        async with self._session_factory() as session:
            by_huid = None
            by_email = None
            if normalized_huid is not None:
                by_huid = (
                    await session.execute(
                        select(ExpressUserCtsBindingModel).where(
                            ExpressUserCtsBindingModel.target_huid == normalized_huid,
                        ),
                    )
                ).scalar_one_or_none()
            if normalized_email is not None:
                by_email = (
                    await session.execute(
                        select(ExpressUserCtsBindingModel).where(
                            ExpressUserCtsBindingModel.corporate_email == normalized_email,
                        ),
                    )
                ).scalar_one_or_none()

            model = by_huid or by_email
            if by_huid is not None and by_email is not None and by_huid.id != by_email.id:
                primary = by_huid
                secondary = by_email
                primary.target_huid = normalized_huid or primary.target_huid or secondary.target_huid
                primary.corporate_email = normalized_email or primary.corporate_email or secondary.corporate_email
                primary.cts_host = normalized_cts_host
                primary.last_verified_at = record.last_verified_at
                primary.created_at = primary.created_at or secondary.created_at or record.created_at or now
                primary.updated_at = record.updated_at or now
                await session.delete(secondary)
                model = primary
            elif model is None:
                model = ExpressUserCtsBindingModel(
                    target_huid=normalized_huid,
                    corporate_email=normalized_email,
                    cts_host=normalized_cts_host,
                    last_verified_at=record.last_verified_at,
                    created_at=record.created_at or now,
                    updated_at=record.updated_at or now,
                )
                session.add(model)
            else:
                model.target_huid = normalized_huid or model.target_huid
                model.corporate_email = normalized_email or model.corporate_email
                model.cts_host = normalized_cts_host
                model.last_verified_at = record.last_verified_at
                model.updated_at = record.updated_at or now
            await session.commit()
            await session.refresh(model)
            return _express_user_cts_binding_to_domain(model)


class PostgresExpressBotHuidBindingRepository:
    def __init__(self, session_factory: async_sessionmaker[AsyncSession]) -> None:
        self._session_factory = session_factory

    async def get_by_bot_id(
        self,
        bot_id: str,
    ) -> ExpressBotHuidBindingRecord | None:
        normalized_bot_id = bot_id.strip()
        if not normalized_bot_id:
            return None
        statement = select(ExpressBotHuidBindingModel).where(
            ExpressBotHuidBindingModel.bot_id == normalized_bot_id,
        )
        async with self._session_factory() as session:
            model = (await session.execute(statement)).scalar_one_or_none()
            return _express_bot_huid_binding_to_domain(model) if model else None

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
        now = _now()
        async with self._session_factory() as session:
            model = (
                await session.execute(
                    select(ExpressBotHuidBindingModel).where(
                        ExpressBotHuidBindingModel.bot_id == normalized_bot_id,
                    ),
                )
            ).scalar_one_or_none()
            if model is None:
                model = ExpressBotHuidBindingModel(
                    bot_id=normalized_bot_id,
                    cts_host=normalized_cts_host,
                    bot_huid=normalized_bot_huid,
                    last_learned_at=record.last_learned_at or now,
                    created_at=record.created_at or now,
                    updated_at=record.updated_at or now,
                )
                session.add(model)
            else:
                model.cts_host = normalized_cts_host
                model.bot_huid = normalized_bot_huid
                model.last_learned_at = record.last_learned_at or now
                model.updated_at = record.updated_at or now
            await session.commit()
            await session.refresh(model)
            return _express_bot_huid_binding_to_domain(model)


class PostgresMigrationStateRepository:
    def __init__(self, session_factory: async_sessionmaker[AsyncSession]) -> None:
        self._session_factory = session_factory

    async def get(
        self,
        migration_id: str,
    ) -> MigrationStateRecord | None:
        statement = select(MigrationStateModel).where(
            MigrationStateModel.migration_id == migration_id,
        )
        async with self._session_factory() as session:
            model = (await session.execute(statement)).scalar_one_or_none()
            return _migration_state_to_domain(model) if model else None

    async def save(
        self,
        record: MigrationStateRecord,
    ) -> MigrationStateRecord:
        statement = (
            insert(MigrationStateModel)
            .values(
                migration_id=record.migration_id,
                status=record.status.value,
                freeze_started_at=record.freeze_started_at,
                finalized_at=record.finalized_at,
                last_reconcile_at=record.last_reconcile_at,
                last_blockers_json={"items": list(record.last_blockers)},
                created_at=record.created_at,
                updated_at=record.updated_at,
            )
            .on_conflict_do_update(
                index_elements=[MigrationStateModel.migration_id],
                set_={
                    "status": record.status.value,
                    "freeze_started_at": record.freeze_started_at,
                    "finalized_at": record.finalized_at,
                    "last_reconcile_at": record.last_reconcile_at,
                    "last_blockers_json": {"items": list(record.last_blockers)},
                    "updated_at": record.updated_at,
                },
            )
            .returning(MigrationStateModel)
        )
        async with self._session_factory() as session:
            model = (await session.execute(statement)).scalar_one()
            await session.commit()
            return _migration_state_to_domain(model)


class PostgresMigrationJobRepository:
    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        *,
        payload_storage_mode: str = _REDACTED_PAYLOAD_STORAGE_MODE,
    ) -> None:
        self._session_factory = session_factory
        self._payload_storage_mode = payload_storage_mode

    async def enqueue(
        self,
        record: MigrationJobRecord,
        *,
        outbox_events: tuple[IntegrationOutboxEventRecord, ...] = (),
    ) -> bool:
        job_statement = (
            insert(MigrationJobModel)
            .values(
                job_key=record.job_key,
                migration_id=record.migration_id,
                operation=record.operation,
                operator_huid=record.operator_huid,
                source_chat_ids=list(record.source_chat_ids),
                anchor_cts_host=record.anchor_cts_host,
                anchor_bot_id=record.anchor_bot_id,
                batch_size=record.batch_size,
                status=record.status.value,
                worker_id=record.worker_id,
                attempts=record.attempts,
                last_error_code=record.last_error_code,
                last_error_payload=_sanitize_payload(
                    record.last_error_payload,
                    mode=self._payload_storage_mode,
                ),
                requested_at=record.requested_at,
                started_at=record.started_at,
                finished_at=record.finished_at,
                heartbeat_at=record.heartbeat_at,
                lease_expires_at=record.lease_expires_at,
                updated_at=record.requested_at,
            )
            .on_conflict_do_nothing(index_elements=[MigrationJobModel.job_key])
            .returning(MigrationJobModel.job_key)
        )
        async with self._session_factory() as session:
            inserted_job_key = (await session.execute(job_statement)).scalar_one_or_none()
            if inserted_job_key is not None and outbox_events:
                await session.execute(
                    insert(IntegrationOutboxModel).values(
                        [
                            _integration_outbox_insert_values(
                                event,
                                payload_storage_mode=self._payload_storage_mode,
                            )
                            for event in outbox_events
                        ],
                    ),
                )
            await session.commit()
            return inserted_job_key is not None

    async def get(self, job_key: str) -> MigrationJobRecord | None:
        statement = select(MigrationJobModel).where(MigrationJobModel.job_key == job_key)
        async with self._session_factory() as session:
            model = (await session.execute(statement)).scalar_one_or_none()
            return _migration_job_to_domain(model) if model else None

    async def list_active(
        self,
        migration_id: str,
    ) -> list[MigrationJobRecord]:
        statement = (
            select(MigrationJobModel)
            .where(
                MigrationJobModel.migration_id == migration_id,
                MigrationJobModel.status.in_(
                    [MigrationJobStatus.QUEUED.value, MigrationJobStatus.RUNNING.value],
                ),
            )
            .order_by(MigrationJobModel.requested_at.asc())
        )
        async with self._session_factory() as session:
            models = (await session.execute(statement)).scalars().all()
            return [_migration_job_to_domain(model) for model in models]

    async def claim(
        self,
        *,
        job_key: str,
        worker_id: str,
        lease_duration_seconds: float,
    ) -> MigrationJobRecord | None:
        now = _now()
        lease_expires_at = now + timedelta(seconds=lease_duration_seconds)
        statement = (
            select(MigrationJobModel)
            .where(
                MigrationJobModel.job_key == job_key,
                (MigrationJobModel.status == MigrationJobStatus.QUEUED.value)
                | (
                    (MigrationJobModel.status == MigrationJobStatus.RUNNING.value)
                    & (
                        (MigrationJobModel.last_error_code.is_(None))
                        | (MigrationJobModel.last_error_code != MIGRATION_JOB_CANCEL_REQUESTED_ERROR_CODE)
                    )
                    & (
                        (MigrationJobModel.lease_expires_at.is_(None))
                        | (MigrationJobModel.lease_expires_at < now)
                    )
                ),
            )
            .with_for_update(skip_locked=True)
            .limit(1)
        )
        async with self._session_factory() as session:
            model = (await session.execute(statement)).scalar_one_or_none()
            if model is None:
                await session.rollback()
                return None
            model.status = MigrationJobStatus.RUNNING.value
            model.worker_id = worker_id
            model.started_at = model.started_at or now
            model.heartbeat_at = now
            model.lease_expires_at = lease_expires_at
            model.finished_at = None
            model.attempts = (model.attempts or 0) + 1
            model.last_error_code = None
            model.last_error_payload = None
            model.updated_at = now
            await session.commit()
            return _migration_job_to_domain(model)

    async def acquire_next(
        self,
        *,
        worker_id: str,
        lease_duration_seconds: float,
    ) -> MigrationJobRecord | None:
        now = _now()
        lease_expires_at = now + timedelta(seconds=lease_duration_seconds)
        statement = (
            select(MigrationJobModel)
            .where(
                (MigrationJobModel.status == MigrationJobStatus.QUEUED.value)
                | (
                    (MigrationJobModel.status == MigrationJobStatus.RUNNING.value)
                    & (
                        (MigrationJobModel.last_error_code.is_(None))
                        | (MigrationJobModel.last_error_code != MIGRATION_JOB_CANCEL_REQUESTED_ERROR_CODE)
                    )
                    & (
                        (MigrationJobModel.lease_expires_at.is_(None))
                        | (MigrationJobModel.lease_expires_at < now)
                    )
                ),
            )
            .order_by(MigrationJobModel.requested_at.asc())
            .with_for_update(skip_locked=True)
            .limit(1)
        )
        async with self._session_factory() as session:
            model = (await session.execute(statement)).scalar_one_or_none()
            if model is None:
                await session.rollback()
                return None
            model.status = MigrationJobStatus.RUNNING.value
            model.worker_id = worker_id
            model.started_at = model.started_at or now
            model.heartbeat_at = now
            model.lease_expires_at = lease_expires_at
            model.finished_at = None
            model.attempts = (model.attempts or 0) + 1
            model.last_error_code = None
            model.last_error_payload = None
            model.updated_at = now
            await session.commit()
            return _migration_job_to_domain(model)

    async def heartbeat(
        self,
        *,
        job_key: str,
        worker_id: str,
        lease_duration_seconds: float,
    ) -> bool:
        now = _now()
        statement = (
            update(MigrationJobModel)
            .where(
                MigrationJobModel.job_key == job_key,
                MigrationJobModel.worker_id == worker_id,
                MigrationJobModel.status == MigrationJobStatus.RUNNING.value,
            )
            .values(
                heartbeat_at=now,
                lease_expires_at=now + timedelta(seconds=lease_duration_seconds),
                updated_at=now,
            )
            .returning(MigrationJobModel.job_key)
        )
        async with self._session_factory() as session:
            updated_job_key = (await session.execute(statement)).scalar_one_or_none()
            await session.commit()
            return updated_job_key is not None

    async def mark_completed(
        self,
        *,
        job_key: str,
        worker_id: str,
    ) -> MigrationJobRecord | None:
        now = _now()
        statement = (
            update(MigrationJobModel)
            .where(
                MigrationJobModel.job_key == job_key,
                MigrationJobModel.worker_id == worker_id,
                MigrationJobModel.status == MigrationJobStatus.RUNNING.value,
            )
            .values(
                status=MigrationJobStatus.COMPLETED.value,
                finished_at=now,
                heartbeat_at=now,
                lease_expires_at=None,
                updated_at=now,
            )
            .returning(MigrationJobModel)
        )
        async with self._session_factory() as session:
            model = (await session.execute(statement)).scalar_one_or_none()
            await session.commit()
            return _migration_job_to_domain(model) if model else None

    async def mark_failed(
        self,
        *,
        job_key: str,
        worker_id: str,
        error_code: str,
        error_payload: dict[str, object],
    ) -> MigrationJobRecord | None:
        now = _now()
        statement = (
            update(MigrationJobModel)
            .where(
                MigrationJobModel.job_key == job_key,
                MigrationJobModel.worker_id == worker_id,
                MigrationJobModel.status == MigrationJobStatus.RUNNING.value,
            )
            .values(
                status=MigrationJobStatus.FAILED.value,
                finished_at=now,
                heartbeat_at=now,
                lease_expires_at=None,
                updated_at=now,
                last_error_code=error_code,
                last_error_payload=_sanitize_payload(
                    error_payload,
                    mode=self._payload_storage_mode,
                ),
            )
            .returning(MigrationJobModel)
        )
        async with self._session_factory() as session:
            model = (await session.execute(statement)).scalar_one_or_none()
            await session.commit()
            return _migration_job_to_domain(model) if model else None

    async def request_cancel(
        self,
        *,
        job_key: str,
        operator_huid: str,
    ) -> MigrationJobRecord | None:
        now = _now()
        payload = _sanitize_payload(
            {"requested_by": operator_huid},
            mode=self._payload_storage_mode,
        )
        async with self._session_factory() as session:
            queued_statement = (
                update(MigrationJobModel)
                .where(
                    MigrationJobModel.job_key == job_key,
                    MigrationJobModel.operator_huid == operator_huid,
                    MigrationJobModel.status == MigrationJobStatus.QUEUED.value,
                )
                .values(
                    status=MigrationJobStatus.FAILED.value,
                    finished_at=now,
                    heartbeat_at=now,
                    lease_expires_at=None,
                    updated_at=now,
                    last_error_code=MIGRATION_JOB_CANCELLED_ERROR_CODE,
                    last_error_payload=payload,
                )
                .returning(MigrationJobModel)
            )
            model = (await session.execute(queued_statement)).scalar_one_or_none()
            if model is None:
                running_statement = (
                    update(MigrationJobModel)
                    .where(
                        MigrationJobModel.job_key == job_key,
                        MigrationJobModel.operator_huid == operator_huid,
                        MigrationJobModel.status == MigrationJobStatus.RUNNING.value,
                    )
                    .values(
                        updated_at=now,
                        last_error_code=MIGRATION_JOB_CANCEL_REQUESTED_ERROR_CODE,
                        last_error_payload=payload,
                    )
                    .returning(MigrationJobModel)
                )
                model = (await session.execute(running_statement)).scalar_one_or_none()
            await session.commit()
            return _migration_job_to_domain(model) if model else None

    async def count_by_status(self) -> dict[MigrationJobStatus, int]:
        statement = (
            select(MigrationJobModel.status, func.count(MigrationJobModel.job_key))
            .group_by(MigrationJobModel.status)
        )
        async with self._session_factory() as session:
            rows = (await session.execute(statement)).all()
            return {
                MigrationJobStatus(status): int(count)
                for status, count in rows
            }


class PostgresOutboxRepository:
    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        *,
        payload_storage_mode: str = _REDACTED_PAYLOAD_STORAGE_MODE,
    ) -> None:
        self._session_factory = session_factory
        self._payload_storage_mode = payload_storage_mode

    async def append_many(
        self,
        events: tuple[IntegrationOutboxEventRecord, ...],
    ) -> None:
        if not events:
            return
        statement = insert(IntegrationOutboxModel).values(
            [
                _integration_outbox_insert_values(
                    event,
                    payload_storage_mode=self._payload_storage_mode,
                )
                for event in events
            ],
        )
        async with self._session_factory() as session:
            await session.execute(statement)
            await session.commit()

    async def get(self, event_id: str) -> IntegrationOutboxEventRecord | None:
        statement = select(IntegrationOutboxModel).where(IntegrationOutboxModel.event_id == event_id)
        async with self._session_factory() as session:
            model = (await session.execute(statement)).scalar_one_or_none()
            return _integration_outbox_to_domain(model) if model else None

    async def lease_batch(
        self,
        *,
        publisher_id: str,
        limit: int,
        lease_duration_seconds: float,
    ) -> list[IntegrationOutboxEventRecord]:
        now = _now()
        lease_expires_at = now + timedelta(seconds=lease_duration_seconds)
        statement = (
            select(IntegrationOutboxModel)
            .where(
                (IntegrationOutboxModel.status == IntegrationOutboxStatus.PENDING.value)
                | (
                    (IntegrationOutboxModel.status == IntegrationOutboxStatus.LEASED.value)
                    & (
                        (IntegrationOutboxModel.lease_expires_at.is_(None))
                        | (IntegrationOutboxModel.lease_expires_at < now)
                    )
                ),
            )
            .order_by(IntegrationOutboxModel.created_at.asc())
            .with_for_update(skip_locked=True)
            .limit(limit)
        )
        async with self._session_factory() as session:
            models = list((await session.execute(statement)).scalars().all())
            if not models:
                await session.rollback()
                return []
            for model in models:
                model.status = IntegrationOutboxStatus.LEASED.value
                model.leased_by = publisher_id
                model.leased_at = now
                model.lease_expires_at = lease_expires_at
                model.attempts = (model.attempts or 0) + 1
                model.updated_at = now
            await session.commit()
            return [_integration_outbox_to_domain(model) for model in models]

    async def mark_published(
        self,
        *,
        event_id: str,
        publisher_id: str,
        result: PublishedIntegrationEvent,
    ) -> IntegrationOutboxEventRecord | None:
        now = _now()
        statement = (
            update(IntegrationOutboxModel)
            .where(
                IntegrationOutboxModel.event_id == event_id,
                IntegrationOutboxModel.leased_by == publisher_id,
                IntegrationOutboxModel.status == IntegrationOutboxStatus.LEASED.value,
            )
            .values(
                status=IntegrationOutboxStatus.PUBLISHED.value,
                published_at=result.published_at,
                broker_topic=result.broker_topic,
                broker_partition=result.broker_partition,
                broker_offset=result.broker_offset,
                leased_by=None,
                leased_at=None,
                lease_expires_at=None,
                updated_at=now,
            )
            .returning(IntegrationOutboxModel)
        )
        async with self._session_factory() as session:
            model = (await session.execute(statement)).scalar_one_or_none()
            await session.commit()
            return _integration_outbox_to_domain(model) if model else None

    async def mark_failed(
        self,
        *,
        event_id: str,
        publisher_id: str,
        error_code: str,
        error_payload: dict[str, object],
    ) -> IntegrationOutboxEventRecord | None:
        now = _now()
        statement = (
            update(IntegrationOutboxModel)
            .where(
                IntegrationOutboxModel.event_id == event_id,
                IntegrationOutboxModel.leased_by == publisher_id,
                IntegrationOutboxModel.status == IntegrationOutboxStatus.LEASED.value,
            )
            .values(
                status=IntegrationOutboxStatus.FAILED.value,
                leased_by=None,
                leased_at=None,
                lease_expires_at=None,
                updated_at=now,
                last_error_code=error_code,
                last_error_payload=_sanitize_payload(
                    error_payload,
                    mode=self._payload_storage_mode,
                ),
            )
            .returning(IntegrationOutboxModel)
        )
        async with self._session_factory() as session:
            model = (await session.execute(statement)).scalar_one_or_none()
            await session.commit()
            return _integration_outbox_to_domain(model) if model else None

    async def count_by_status(self) -> dict[IntegrationOutboxStatus, int]:
        statement = (
            select(IntegrationOutboxModel.status, func.count(IntegrationOutboxModel.event_id))
            .group_by(IntegrationOutboxModel.status)
        )
        async with self._session_factory() as session:
            rows = (await session.execute(statement)).all()
            return {
                IntegrationOutboxStatus(status): int(count)
                for status, count in rows
            }


class PostgresInboxRepository:
    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        *,
        payload_storage_mode: str = _REDACTED_PAYLOAD_STORAGE_MODE,
    ) -> None:
        self._session_factory = session_factory
        self._payload_storage_mode = payload_storage_mode

    async def is_processed(self, *, consumer_name: str, message_id: str) -> bool:
        statement = select(ConsumerInboxModel).where(
            ConsumerInboxModel.consumer_name == consumer_name,
            ConsumerInboxModel.message_id == message_id,
        )
        async with self._session_factory() as session:
            model = (await session.execute(statement)).scalar_one_or_none()
            return model is not None

    async def mark_processed(self, record: ConsumerInboxRecord) -> bool:
        statement = (
            insert(ConsumerInboxModel)
            .values(
                consumer_name=record.consumer_name,
                message_id=record.message_id,
                processed_at=record.processed_at,
                result_code=record.result_code,
                result_payload=_sanitize_payload(
                    record.result_payload,
                    mode=self._payload_storage_mode,
                ),
            )
            .on_conflict_do_nothing(
                index_elements=[ConsumerInboxModel.consumer_name, ConsumerInboxModel.message_id],
            )
            .returning(ConsumerInboxModel.message_id)
        )
        async with self._session_factory() as session:
            inserted = (await session.execute(statement)).scalar_one_or_none()
            await session.commit()
            return inserted is not None


class PostgresServiceWatermarkRepository:
    def __init__(self, session_factory: async_sessionmaker[AsyncSession]) -> None:
        self._session_factory = session_factory

    async def get(
        self,
        *,
        consumer_name: str,
        logical_stream_key: str,
    ) -> ServiceWatermarkRecord | None:
        statement = select(ServiceWatermarkModel).where(
            ServiceWatermarkModel.consumer_name == consumer_name,
            ServiceWatermarkModel.logical_stream_key == logical_stream_key,
        )
        async with self._session_factory() as session:
            model = (await session.execute(statement)).scalar_one_or_none()
            return _service_watermark_to_domain(model) if model else None

    async def upsert(
        self,
        record: ServiceWatermarkRecord,
    ) -> ServiceWatermarkRecord:
        statement = (
            insert(ServiceWatermarkModel)
            .values(
                consumer_name=record.consumer_name,
                logical_stream_key=record.logical_stream_key,
                watermark_value=record.watermark_value,
                updated_at=record.updated_at,
            )
            .on_conflict_do_update(
                index_elements=[
                    ServiceWatermarkModel.consumer_name,
                    ServiceWatermarkModel.logical_stream_key,
                ],
                set_={
                    "watermark_value": record.watermark_value,
                    "updated_at": record.updated_at,
                },
            )
            .returning(ServiceWatermarkModel)
        )
        async with self._session_factory() as session:
            model = (await session.execute(statement)).scalar_one()
            await session.commit()
            return _service_watermark_to_domain(model)

    async def list_all(self) -> list[ServiceWatermarkRecord]:
        statement = select(ServiceWatermarkModel).order_by(
            ServiceWatermarkModel.consumer_name.asc(),
            ServiceWatermarkModel.logical_stream_key.asc(),
        )
        async with self._session_factory() as session:
            models = (await session.execute(statement)).scalars().all()
            return [_service_watermark_to_domain(model) for model in models]


def _chat_to_domain(model: MigrationChatMapModel) -> ChatMappingRecord:
    return ChatMappingRecord(
        migration_id=model.migration_id,
        source_chat_id=model.source_chat_id,
        source_chat_type=model.source_chat_type,
        source_chat_title=model.source_chat_title,
        target_chat_id=model.target_chat_id,
        target_chat_title=model.target_chat_title,
        status=model.status,
        created_at=model.created_at,
        updated_at=model.updated_at,
        anchor_cts_host=model.anchor_cts_host,
        anchor_bot_id=model.anchor_bot_id,
        member_success_count=model.member_success_count,
        member_total_count=model.member_total_count,
    )


def _chat_config_to_domain(model: MigrationChatConfigModel) -> ChatMigrationConfigRecord:
    return ChatMigrationConfigRecord(
        migration_id=model.migration_id,
        source_chat_id=model.source_chat_id,
        source_chat_type=model.source_chat_type,
        source_chat_title=model.source_chat_title,
        target_strategy=model.target_strategy,
        target_title=model.target_title,
        target_chat_id=model.target_chat_id,
        include_from=model.include_from,
        include_to=model.include_to,
        migrate_media=model.migrate_media,
        media_kinds=tuple(model.media_kinds_json) if model.media_kinds_json is not None else None,
        service_messages=model.service_messages,
        reply_mode=model.reply_mode,
        output_template=model.output_template,
        identity_policy=model.identity_policy,
        updated_by_huid=model.updated_by_huid,
        created_at=model.created_at,
        updated_at=model.updated_at,
        anchor_cts_host=model.anchor_cts_host,
        anchor_bot_id=model.anchor_bot_id,
        topic_strategy=model.topic_strategy,
        source_backend=model.source_backend,
        access_strategy=model.access_strategy,
        skip_in_all=model.skip_in_all,
        telegram_chat_id=model.telegram_chat_id,
        source_topic_id=model.source_topic_id,
        source_thread_id=model.source_thread_id,
        source_thread_title=model.source_thread_title,
    )


def _operator_defaults_to_domain(
    model: MigrationOperatorDefaultsModel,
) -> OperatorMigrationDefaultsRecord:
    return OperatorMigrationDefaultsRecord(
        migration_id=model.migration_id,
        operator_huid=model.operator_huid,
        include_from=model.include_from,
        include_to=model.include_to,
        migrate_media=model.migrate_media,
        media_kinds=tuple(model.media_kinds_json) if model.media_kinds_json is not None else None,
        service_messages=model.service_messages,
        reply_mode=model.reply_mode,
        output_template=model.output_template,
        access_strategy=model.access_strategy,
        topic_strategy=model.topic_strategy,
        created_at=model.created_at,
        updated_at=model.updated_at,
    )


def _message_to_domain(model: MigrationMessageMapModel) -> MessageMappingRecord:
    return MessageMappingRecord(
        migration_id=model.migration_id,
        source_chat_id=model.source_chat_id,
        source_message_id=model.source_message_id,
        source_sent_at=model.source_sent_at,
        target_chat_id=model.target_chat_id,
        checksum=model.checksum,
        import_status=MessageImportStatus(model.import_status),
        imported_at=model.imported_at,
        target_sync_id=model.target_sync_id,
        rendered_body=model.rendered_body,
        last_error_code=model.last_error_code,
        last_error_payload=model.last_error_payload,
    )


def _checkpoint_to_domain(model: MigrationCheckpointModel) -> MigrationCheckpoint:
    return MigrationCheckpoint(
        migration_id=model.migration_id,
        source_chat_id=model.source_chat_id,
        mode=model.mode,
        cursor=HistoryCursor(
            last_source_message_id=model.last_source_message_id,
            last_source_sent_at=model.last_source_sent_at,
        ),
        updated_at=model.updated_at,
    )


def _now() -> datetime:
    return datetime.now(tz=UTC)


def _inventory_to_record(model: MigrationInventorySnapshotModel) -> InventorySnapshotRecord:
    return InventorySnapshotRecord(
        migration_id=model.migration_id,
        source_chat_id=model.source_chat_id,
        source_chat_type=model.source_chat_type,
        source_chat_title=model.source_chat_title,
        message_count=model.messages_count,
        media_count=model.media_count,
        approximate_bytes=model.approximate_bytes,
        captured_at=model.updated_at,
    )


def _telegram_export_snapshot_to_domain(
    model: TelegramExportSnapshotModel,
) -> TelegramExportSnapshotRecord:
    return TelegramExportSnapshotRecord(
        source_chat_id=model.source_chat_id,
        source_chat_title=model.source_chat_title,
        source_chat_type=model.source_chat_type,
        original_filename=model.original_filename,
        export_chat_id=model.export_chat_id,
        archive_locator=model.archive_locator,
        payload_sha256=model.payload_sha256,
        message_count=model.message_count,
        media_count=model.media_count,
        approximate_bytes=model.approximate_bytes,
        uploaded_by_huid=model.uploaded_by_huid,
        created_at=model.created_at,
        updated_at=model.updated_at,
    )


def _migration_state_to_domain(model: MigrationStateModel) -> MigrationStateRecord:
    blockers_payload = model.last_blockers_json or {}
    raw_items = blockers_payload.get("items", [])
    return MigrationStateRecord(
        migration_id=model.migration_id,
        status=MigrationLifecycleStatus(model.status),
        created_at=model.created_at,
        updated_at=model.updated_at,
        freeze_started_at=model.freeze_started_at,
        finalized_at=model.finalized_at,
        last_reconcile_at=model.last_reconcile_at,
        last_blockers=[str(item) for item in raw_items],
    )


def _migration_job_to_domain(model: MigrationJobModel) -> MigrationJobRecord:
    return MigrationJobRecord(
        job_key=model.job_key,
        migration_id=model.migration_id,
        operation=model.operation,
        operator_huid=model.operator_huid,
        source_chat_ids=tuple(model.source_chat_ids or []),
        anchor_cts_host=model.anchor_cts_host,
        anchor_bot_id=model.anchor_bot_id,
        batch_size=model.batch_size,
        status=MigrationJobStatus(model.status),
        requested_at=model.requested_at,
        started_at=model.started_at,
        finished_at=model.finished_at,
        heartbeat_at=model.heartbeat_at,
        lease_expires_at=model.lease_expires_at,
        worker_id=model.worker_id,
        attempts=model.attempts,
        last_error_code=model.last_error_code,
        last_error_payload=model.last_error_payload,
    )


def _integration_outbox_insert_values(
    record: IntegrationOutboxEventRecord,
    *,
    payload_storage_mode: str,
) -> dict[str, object]:
    return {
        "event_id": record.event_id,
        "aggregate_type": record.aggregate_type,
        "aggregate_id": record.aggregate_id,
        "event_type": record.event_type,
        "payload_json": _sanitize_payload(record.payload_json, mode=payload_storage_mode),
        "headers_json": _sanitize_payload(record.headers_json, mode=payload_storage_mode),
        "status": record.status.value,
        "leased_by": record.leased_by,
        "leased_at": record.leased_at,
        "lease_expires_at": record.lease_expires_at,
        "published_at": record.published_at,
        "broker_topic": record.broker_topic,
        "broker_partition": record.broker_partition,
        "broker_offset": record.broker_offset,
        "attempts": record.attempts,
        "last_error_code": record.last_error_code,
        "last_error_payload": _sanitize_payload(
            record.last_error_payload,
            mode=payload_storage_mode,
        ),
        "created_at": record.created_at,
        "updated_at": record.created_at,
    }


def _integration_outbox_to_domain(model: IntegrationOutboxModel) -> IntegrationOutboxEventRecord:
    return IntegrationOutboxEventRecord(
        event_id=model.event_id,
        aggregate_type=model.aggregate_type,
        aggregate_id=model.aggregate_id,
        event_type=model.event_type,
        payload_json=model.payload_json or {},
        headers_json=model.headers_json or {},
        status=IntegrationOutboxStatus(model.status),
        leased_by=model.leased_by,
        leased_at=model.leased_at,
        lease_expires_at=model.lease_expires_at,
        published_at=model.published_at,
        broker_topic=model.broker_topic,
        broker_partition=model.broker_partition,
        broker_offset=model.broker_offset,
        attempts=model.attempts,
        last_error_code=model.last_error_code,
        last_error_payload=model.last_error_payload,
        created_at=model.created_at,
    )


def _identity_mapping_to_domain(model: IdentityMappingModel) -> IdentityMappingRecord:
    return IdentityMappingRecord(
        express_host=model.express_host,
        telegram_user_id=model.telegram_user_id,
        telegram_username=model.telegram_username,
        telegram_display_name=model.telegram_display_name,
        corporate_email=model.corporate_email,
        target_huid=model.target_huid,
        created_at=model.created_at,
        updated_at=model.updated_at,
        last_resolved_at=model.last_resolved_at,
    )


def _express_user_cts_binding_to_domain(
    model: ExpressUserCtsBindingModel,
) -> ExpressUserCtsBindingRecord:
    return ExpressUserCtsBindingRecord(
        target_huid=model.target_huid,
        corporate_email=model.corporate_email,
        cts_host=model.cts_host,
        created_at=model.created_at,
        updated_at=model.updated_at,
        last_verified_at=model.last_verified_at,
    )


def _express_bot_huid_binding_to_domain(
    model: ExpressBotHuidBindingModel,
) -> ExpressBotHuidBindingRecord:
    return ExpressBotHuidBindingRecord(
        bot_id=model.bot_id,
        cts_host=model.cts_host,
        bot_huid=model.bot_huid,
        created_at=model.created_at,
        updated_at=model.updated_at,
        last_learned_at=model.last_learned_at,
    )


def _service_watermark_to_domain(model: ServiceWatermarkModel) -> ServiceWatermarkRecord:
    return ServiceWatermarkRecord(
        consumer_name=model.consumer_name,
        logical_stream_key=model.logical_stream_key,
        watermark_value=model.watermark_value,
        updated_at=model.updated_at,
    )


def _attachment_to_domain(model: MigrationAttachmentMapModel) -> AttachmentMappingRecord:
    return AttachmentMappingRecord(
        migration_id=model.migration_id,
        source_chat_id=model.source_chat_id,
        source_message_id=model.source_message_id,
        attachment_index=model.attachment_index,
        source_file_id=model.source_file_id,
        source_filename=model.source_filename,
        media_kind=model.media_kind,
        checksum=model.checksum,
        size_bytes=model.size_bytes,
        target_chat_id=model.target_chat_id,
        import_status=AttachmentImportStatus(model.import_status),
        imported_at=model.imported_at,
        target_sync_id=model.target_sync_id,
        last_error_code=model.last_error_code,
        last_error_payload=model.last_error_payload,
    )


def _attachment_stage_to_domain(model: AttachmentStageModel) -> AttachmentStageRecord:
    return AttachmentStageRecord(
        migration_id=model.migration_id,
        source_chat_id=model.source_chat_id,
        source_message_id=model.source_message_id,
        attachment_index=model.attachment_index,
        target_chat_id=model.target_chat_id,
        source_locator=model.source_locator,
        media_kind=model.media_kind,
        checksum=model.checksum,
        size_bytes=model.size_bytes,
        status=AttachmentStageStatus(model.status),
        express_file_id=model.express_file_id,
        express_file_payload=model.express_file_payload,
        target_sync_id=model.target_sync_id,
        last_error_code=model.last_error_code,
        last_error_payload=model.last_error_payload,
        created_at=model.created_at,
        updated_at=model.updated_at,
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
