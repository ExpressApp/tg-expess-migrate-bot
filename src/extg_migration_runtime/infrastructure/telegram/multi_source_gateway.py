from __future__ import annotations

import asyncio
import inspect
from collections.abc import AsyncIterator

from extg_migration_runtime.infrastructure.archive.telegram_export_archive_parser import (
    TelegramExportArchiveParser,
)
from extg_shared.contracts.errors import FatalItemError
from extg_shared.contracts.manifest import MigrationManifest
from extg_shared.contracts.models import (
    CanonicalAttachment,
    DownloadedAttachment,
    HistoryBatch,
    HistoryCursor,
    SourceChannelAccessProfile,
    SourceDialog,
    SourceParticipant,
    SourceTopic,
    TelegramDeltaEvent,
    TelegramExportSnapshotRecord,
    TelegramSourceMessage,
)
from extg_shared.contracts.ports import TelegramExportSnapshotRepository, TelegramGateway
from extg_shared.contracts.ports import TelegramExportArchiveStageStore


class TelegramExportSnapshotGateway:
    def __init__(
        self,
        *,
        snapshot_repository: TelegramExportSnapshotRepository,
        parser: TelegramExportArchiveParser,
        stage_store: TelegramExportArchiveStageStore,
    ) -> None:
        self._snapshot_repository = snapshot_repository
        self._parser = parser
        self._stage_store = stage_store
        self._cache_lock = asyncio.Lock()
        self._messages_cache: dict[str, tuple[TelegramSourceMessage, ...]] = {}
        self._message_index_cache: dict[str, dict[str, int]] = {}

    async def list_available_dialogs(
        self,
        *,
        limit: int = 100,
        query: str | None = None,
        source_backend: str = "telegram_export_archive",
    ) -> list[SourceDialog]:
        return []

    async def list_dialogs(self, manifest: MigrationManifest) -> list[SourceDialog]:
        dialogs: list[SourceDialog] = []
        for dialog in manifest.dialogs:
            if dialog.source_backend != "telegram_export_archive":
                continue
            snapshot = await self._snapshot_repository.get(dialog.source_chat_id)
            if snapshot is None:
                dialogs.append(
                    SourceDialog(
                        dialog_id=dialog.source_chat_id,
                        chat_type=dialog.source_chat_type,
                        title=dialog.source_chat_title or dialog.target_title or dialog.source_chat_id,
                    ),
                )
                continue
            dialogs.append(self._source_dialog_from_snapshot(snapshot))
        return dialogs

    async def fetch_history(
        self,
        dialog_id: str,
        cursor: HistoryCursor | None,
        limit: int,
        *,
        source_backend: str = "telegram_export_archive",
    ) -> HistoryBatch:
        snapshot = await self._require_snapshot(dialog_id)
        messages = await self._messages_for(dialog_id, snapshot)
        start_index = 0
        if cursor is not None and cursor.last_source_message_id:
            message_index = self._message_index_cache.get(dialog_id, {})
            previous_index = message_index.get(cursor.last_source_message_id)
            if previous_index is not None:
                start_index = previous_index + 1
        batch_messages = list(messages[start_index : start_index + max(1, limit)])
        next_index = start_index + len(batch_messages)
        has_more = next_index < len(messages)
        next_cursor = None
        if batch_messages:
            last_message = batch_messages[-1]
            next_cursor = HistoryCursor(
                last_source_message_id=last_message.message_id,
                last_source_sent_at=last_message.sent_at_utc,
            )
        return HistoryBatch(
            dialog_id=dialog_id,
            messages=batch_messages,
            next_cursor=next_cursor,
            has_more=has_more,
            dialog_title=snapshot.source_chat_title,
        )

    async def download_attachment(
        self,
        attachment: CanonicalAttachment,
        *,
        source_backend: str = "telegram_export_archive",
    ) -> DownloadedAttachment:
        raise FatalItemError(
            "Telegram export archive import does not support attachment download",
        )

    async def list_participants(
        self,
        dialog_id: str,
        *,
        source_backend: str = "telegram_export_archive",
    ) -> list[SourceParticipant]:
        return []

    async def list_topics(
        self,
        dialog_id: str,
        *,
        source_backend: str = "telegram_export_archive",
    ) -> list[SourceTopic]:
        return []

    async def get_channel_access_profile(
        self,
        dialog_id: str,
        *,
        source_backend: str = "telegram_export_archive",
    ) -> SourceChannelAccessProfile:
        return SourceChannelAccessProfile(
            dialog_id=dialog_id,
            is_admin=True,
            can_list_participants=False,
        )

    async def subscribe_delta(
        self,
        dialog_ids: list[str],
        *,
        source_backend: str = "telegram_export_archive",
    ) -> AsyncIterator[TelegramDeltaEvent]:
        if False:
            yield

    async def _messages_for(
        self,
        dialog_id: str,
        snapshot: TelegramExportSnapshotRecord,
    ) -> tuple[TelegramSourceMessage, ...]:
        cached = self._messages_cache.get(dialog_id)
        if cached is not None:
            return cached
        async with self._cache_lock:
            cached = self._messages_cache.get(dialog_id)
            if cached is not None:
                return cached
            messages = await self._parser.load_messages(
                dialog_id,
                self._snapshot_repository,
                self._stage_store,
            )
            self._messages_cache[dialog_id] = messages
            self._message_index_cache[dialog_id] = {
                message.message_id: index
                for index, message in enumerate(messages)
            }
            return messages

    async def _require_snapshot(
        self,
        source_chat_id: str,
    ) -> TelegramExportSnapshotRecord:
        snapshot = await self._snapshot_repository.get(source_chat_id)
        if snapshot is None:
            raise FatalItemError(
                f"staged Telegram export archive was not found for source_chat_id={source_chat_id}",
            )
        return snapshot

    def _source_dialog_from_snapshot(
        self,
        snapshot: TelegramExportSnapshotRecord,
    ) -> SourceDialog:
        return SourceDialog(
            dialog_id=snapshot.source_chat_id,
            chat_type=snapshot.source_chat_type,
            title=snapshot.source_chat_title,
            message_count=snapshot.message_count,
            media_count=snapshot.media_count,
            approximate_bytes=snapshot.approximate_bytes,
            has_topics=False,
        )


class MultiSourceTelegramGateway:
    def __init__(
        self,
        *,
        primary_gateway: TelegramGateway,
        archive_gateway: TelegramGateway,
    ) -> None:
        self._primary_gateway = primary_gateway
        self._archive_gateway = archive_gateway

    async def list_available_dialogs(
        self,
        *,
        limit: int = 100,
        query: str | None = None,
        source_backend: str = "telethon_user_session",
    ) -> list[SourceDialog]:
        gateway = self._gateway_for(source_backend)
        return await gateway.list_available_dialogs(
            limit=limit,
            query=query,
            source_backend=source_backend,
        )

    async def list_dialogs(self, manifest: MigrationManifest) -> list[SourceDialog]:
        grouped: dict[str, list] = {}
        for dialog in manifest.dialogs:
            grouped.setdefault(dialog.source_backend, []).append(dialog)
        dialogs: list[SourceDialog] = []
        for source_backend, backend_dialogs in grouped.items():
            gateway = self._gateway_for(source_backend)
            dialogs.extend(
                await gateway.list_dialogs(
                    MigrationManifest(
                        migration_id=manifest.migration_id,
                        mode=manifest.mode,
                        defaults=manifest.defaults,
                        dialogs=backend_dialogs,
                    ),
                ),
            )
        return dialogs

    async def fetch_history(
        self,
        dialog_id: str,
        cursor: HistoryCursor | None,
        limit: int,
        *,
        source_backend: str = "telethon_user_session",
    ) -> HistoryBatch:
        return await self._gateway_for(source_backend).fetch_history(
            dialog_id,
            cursor,
            limit,
            source_backend=source_backend,
        )

    async def download_attachment(
        self,
        attachment: CanonicalAttachment,
        *,
        source_backend: str = "telethon_user_session",
    ) -> DownloadedAttachment:
        return await self._gateway_for(source_backend).download_attachment(
            attachment,
            source_backend=source_backend,
        )

    async def list_participants(
        self,
        dialog_id: str,
        *,
        source_backend: str = "telethon_user_session",
    ) -> list[SourceParticipant]:
        return await self._gateway_for(source_backend).list_participants(
            dialog_id,
            source_backend=source_backend,
        )

    async def list_topics(
        self,
        dialog_id: str,
        *,
        source_backend: str = "telethon_user_session",
    ) -> list[SourceTopic]:
        return await self._gateway_for(source_backend).list_topics(
            dialog_id,
            source_backend=source_backend,
        )

    async def get_channel_access_profile(
        self,
        dialog_id: str,
        *,
        source_backend: str = "telethon_user_session",
    ) -> SourceChannelAccessProfile:
        return await self._gateway_for(source_backend).get_channel_access_profile(
            dialog_id,
            source_backend=source_backend,
        )

    async def subscribe_delta(
        self,
        dialog_ids: list[str],
        *,
        source_backend: str = "telethon_user_session",
    ) -> AsyncIterator[TelegramDeltaEvent]:
        async for event in self._gateway_for(source_backend).subscribe_delta(
            dialog_ids,
            source_backend=source_backend,
        ):
            yield event

    async def close(self) -> None:
        seen: set[int] = set()
        for gateway in (self._primary_gateway, self._archive_gateway):
            identity = id(gateway)
            if identity in seen:
                continue
            seen.add(identity)
            close_method = getattr(gateway, "close", None)
            if close_method is None:
                continue
            result = close_method()
            if inspect.isawaitable(result):
                await result

    def _gateway_for(self, source_backend: str | None) -> TelegramGateway:
        normalized = (source_backend or "telethon_user_session").strip()
        if normalized == "telegram_export_archive":
            return self._archive_gateway
        return self._primary_gateway
