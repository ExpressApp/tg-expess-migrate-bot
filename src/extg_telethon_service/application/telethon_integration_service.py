from __future__ import annotations

from contextlib import asynccontextmanager

from extg_telethon_service.application.telegram_session_context import TelegramSessionContext
from extg_telethon_service.application.telegram_session_service import TelegramSessionService
from extg_shared.contracts.ports import TelegramGateway
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
)


class TelethonIntegrationService:
    def __init__(
        self,
        *,
        telegram_gateway: TelegramGateway,
        telegram_session_service: TelegramSessionService,
        telegram_session_context: TelegramSessionContext,
    ) -> None:
        self._telegram_gateway = telegram_gateway
        self._telegram_session_service = telegram_session_service
        self._telegram_session_context = telegram_session_context

    async def list_available_dialogs(
        self,
        *,
        operator_huid: str,
        limit: int = 100,
        query: str | None = None,
        source_backend: str = "telethon_user_session",
    ) -> list[SourceDialog]:
        async with self._bind_operator(operator_huid, mark_used=True):
            return await self._telegram_gateway.list_available_dialogs(
                limit=limit,
                query=query,
                source_backend=source_backend,
            )

    async def get_source_dialog(
        self,
        *,
        operator_huid: str,
        dialog_id: str,
        source_backend: str = "telethon_user_session",
    ) -> SourceDialog | None:
        async with self._bind_operator(operator_huid, mark_used=True):
            return await self._telegram_gateway.get_source_dialog(
                dialog_id,
                source_backend=source_backend,
            )

    async def list_dialogs(
        self,
        *,
        operator_huid: str,
        manifest: MigrationManifest,
    ) -> list[SourceDialog]:
        async with self._bind_operator(operator_huid, mark_used=True):
            return await self._telegram_gateway.list_dialogs(manifest)

    async def fetch_history(
        self,
        *,
        operator_huid: str,
        dialog_id: str,
        cursor: HistoryCursor | None,
        limit: int,
        source_backend: str = "telethon_user_session",
        thread_id: str | None = None,
    ) -> HistoryBatch:
        async with self._bind_operator(operator_huid, mark_used=True):
            return await self._telegram_gateway.fetch_history(
                dialog_id,
                cursor,
                limit,
                source_backend=source_backend,
                thread_id=thread_id,
            )

    async def download_attachment(
        self,
        *,
        operator_huid: str,
        attachment: CanonicalAttachment,
        source_backend: str = "telethon_user_session",
    ) -> DownloadedAttachment:
        async with self._bind_operator(operator_huid, mark_used=True):
            return await self._telegram_gateway.download_attachment(
                attachment,
                source_backend=source_backend,
            )

    async def list_participants(
        self,
        *,
        operator_huid: str,
        dialog_id: str,
        source_backend: str = "telethon_user_session",
    ) -> list[SourceParticipant]:
        async with self._bind_operator(operator_huid, mark_used=True):
            return await self._telegram_gateway.list_participants(
                dialog_id,
                source_backend=source_backend,
            )

    async def list_topics(
        self,
        *,
        operator_huid: str,
        dialog_id: str,
        source_backend: str = "telethon_user_session",
    ) -> list[SourceTopic]:
        async with self._bind_operator(operator_huid, mark_used=True):
            return await self._telegram_gateway.list_topics(
                dialog_id,
                source_backend=source_backend,
            )

    async def get_channel_access_profile(
        self,
        *,
        operator_huid: str,
        dialog_id: str,
        source_backend: str = "telethon_user_session",
    ) -> SourceChannelAccessProfile:
        async with self._bind_operator(operator_huid, mark_used=True):
            return await self._telegram_gateway.get_channel_access_profile(
                dialog_id,
                source_backend=source_backend,
            )

    async def ensure_session_binding(
        self,
        *,
        operator_huid: str,
        mark_used: bool = True,
    ) -> None:
        await self._telegram_session_service.resolve_session_string(
            operator_huid=operator_huid,
            mark_used=mark_used,
        )

    @asynccontextmanager
    async def _bind_operator(
        self,
        operator_huid: str,
        *,
        mark_used: bool,
    ):
        session_string = await self._telegram_session_service.resolve_session_string(
            operator_huid=operator_huid,
            mark_used=mark_used,
        )
        with self._telegram_session_context.use(
            session_string,
            operator_huid=operator_huid,
        ):
            yield
