from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
import hashlib

from extg_shared.contracts.errors import ConfigurationError
from extg_telethon_service.application.telegram_session_context import TelegramSessionContext
from extg_shared.contracts.manifest import MigrationManifest
from extg_shared.contracts.models import (
    CanonicalAttachment,
    DownloadedAttachment,
    HistoryBatch,
    HistoryCursor,
    SourceChannelAccessProfile,
    SourceDialog,
    SourceTopic,
    SourceParticipant,
    TelegramDeltaEvent,
)
from extg_telethon_service.infrastructure.telegram.telethon_gateway import TelethonTelegramGateway


class SessionAwareTelegramGateway:
    def __init__(
        self,
        *,
        session_context: TelegramSessionContext,
        api_id: int | None,
        api_hash: str | None,
        request_timeout_seconds: float = 30.0,
        connection_retries: int = 5,
        default_attachment_dir: str | None = None,
        delta_queue_size: int = 1000,
    ) -> None:
        self._session_context = session_context
        self._api_id = api_id
        self._api_hash = api_hash
        self._request_timeout_seconds = request_timeout_seconds
        self._connection_retries = connection_retries
        self._default_attachment_dir = default_attachment_dir
        self._delta_queue_size = delta_queue_size
        self._gateways: dict[str, TelethonTelegramGateway] = {}
        self._lock = asyncio.Lock()

    async def list_available_dialogs(
        self,
        *,
        limit: int = 100,
        query: str | None = None,
        source_backend: str = "telethon_user_session",
    ) -> list[SourceDialog]:
        gateway = await self._gateway()
        return await gateway.list_available_dialogs(
            limit=limit,
            query=query,
            source_backend=source_backend,
        )

    async def get_source_dialog(
        self,
        dialog_id: str,
        *,
        source_backend: str = "telethon_user_session",
    ) -> SourceDialog | None:
        gateway = await self._gateway()
        return await gateway.get_source_dialog(
            dialog_id,
            source_backend=source_backend,
        )

    async def list_dialogs(self, manifest: MigrationManifest) -> list[SourceDialog]:
        gateway = await self._gateway()
        return await gateway.list_dialogs(manifest)

    async def fetch_history(
        self,
        dialog_id: str,
        cursor: HistoryCursor | None,
        limit: int,
        *,
        source_backend: str = "telethon_user_session",
        thread_id: str | None = None,
    ) -> HistoryBatch:
        gateway = await self._gateway()
        return await gateway.fetch_history(
            dialog_id,
            cursor,
            limit,
            source_backend=source_backend,
            thread_id=thread_id,
        )

    async def download_attachment(
        self,
        attachment: CanonicalAttachment,
        *,
        source_backend: str = "telethon_user_session",
    ) -> DownloadedAttachment:
        gateway = await self._gateway()
        return await gateway.download_attachment(
            attachment,
            source_backend=source_backend,
        )

    async def list_participants(
        self,
        dialog_id: str,
        *,
        source_backend: str = "telethon_user_session",
    ) -> list[SourceParticipant]:
        gateway = await self._gateway()
        return await gateway.list_participants(
            dialog_id,
            source_backend=source_backend,
        )

    async def list_topics(
        self,
        dialog_id: str,
        *,
        source_backend: str = "telethon_user_session",
    ) -> list[SourceTopic]:
        gateway = await self._gateway()
        return await gateway.list_topics(
            dialog_id,
            source_backend=source_backend,
        )

    async def get_channel_access_profile(
        self,
        dialog_id: str,
        *,
        source_backend: str = "telethon_user_session",
    ) -> SourceChannelAccessProfile:
        gateway = await self._gateway()
        return await gateway.get_channel_access_profile(
            dialog_id,
            source_backend=source_backend,
        )

    async def subscribe_delta(
        self,
        dialog_ids: list[str],
        *,
        source_backend: str = "telethon_user_session",
    ) -> AsyncIterator[TelegramDeltaEvent]:
        gateway = await self._gateway()
        async for event in gateway.subscribe_delta(
            dialog_ids,
            source_backend=source_backend,
        ):
            yield event

    async def close_session(self, session_string: str | None) -> None:
        if session_string is None:
            return
        async with self._lock:
            gateway = self._gateways.pop(self._cache_key(session_string), None)
        if gateway is not None:
            await gateway.close()

    async def close(self) -> None:
        async with self._lock:
            gateways = list(self._gateways.values())
            self._gateways.clear()
        for gateway in gateways:
            await gateway.close()

    async def _gateway(self) -> TelethonTelegramGateway:
        session_string = self._session_context.current_session_path()
        if session_string is None:
            raise ConfigurationError(
                "telegram user session is required; connect it via /connect before migration operations",
            )
        key = self._cache_key(session_string)
        gateway = self._gateways.get(key)
        if gateway is not None:
            return gateway

        async with self._lock:
            gateway = self._gateways.get(key)
            if gateway is not None:
                return gateway
            gateway = TelethonTelegramGateway(
                api_id=self._api_id,
                api_hash=self._api_hash,
                session_string=session_string,
                request_timeout_seconds=self._request_timeout_seconds,
                connection_retries=self._connection_retries,
                default_attachment_dir=self._default_attachment_dir,
                delta_queue_size=self._delta_queue_size,
            )
            self._gateways[key] = gateway
            return gateway

    def _cache_key(self, session_string: str) -> str:
        return hashlib.sha256(session_string.encode("utf-8")).hexdigest()
