from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
import itertools
import hashlib
import os
from dataclasses import replace
from datetime import UTC, datetime
from enum import Enum
from pathlib import Path
import tempfile
from typing import Any
from urllib.parse import urlparse

from telethon import TelegramClient, utils
from telethon import events
from telethon.errors import FloodWaitError, RPCError
from telethon.sessions import StringSession
from telethon.tl.functions.messages import GetForumTopicsRequest, SearchRequest
from telethon.tl.custom.dialog import Dialog
from telethon.tl.custom.message import Message
from telethon.tl.types import Channel, Chat, User

from extg_shared.contracts.errors import (
    ConfigurationError,
    FatalItemError,
    RecoverableItemError,
)
from extg_shared.contracts.manifest import ManifestDialog, MigrationManifest
from extg_shared.contracts.models import (
    CanonicalAttachment,
    CanonicalEntity,
    ContentType,
    DownloadedAttachment,
    HistoryBatch,
    HistoryCursor,
    SourceChannelAccessProfile,
    SourceDialog,
    SourceTopic,
    SourceParticipant,
    TelegramDeltaEvent,
    TelegramAuthor,
    TelegramSourceMessage,
)
from telethon.tl.types import (
    InputMessageEntityMentionName,
    InputMessagesFilterEmpty,
    MessageEntityMention,
    MessageEntityMentionName,
    MessageEntityTextUrl,
    MessageEmpty,
)


class TelethonTelegramGateway:
    """Real Telegram adapter based on Telethon."""

    _SEARCH_CHUNK_SIZE = 100

    def __init__(
        self,
        *,
        api_id: int | None,
        api_hash: str | None,
        session_string: str | None,
        request_timeout_seconds: float = 30.0,
        connection_retries: int = 5,
        default_attachment_dir: str | None = None,
        delta_queue_size: int = 1000,
        client: TelegramClient | None = None,
    ) -> None:
        if client is None:
            if api_id is None or not api_hash:
                raise ConfigurationError(
                    "telegram api_id/api_hash are required for TelethonTelegramGateway",
                )
            if not session_string:
                raise ConfigurationError(
                    "telegram session_string is required for TelethonTelegramGateway",
                )
            self._client = TelegramClient(
                session=StringSession(session_string),
                api_id=api_id,
                api_hash=api_hash,
                timeout=request_timeout_seconds,
                request_retries=connection_retries,
                connection_retries=connection_retries,
            )
        else:
            self._client = client
        self._start_lock = asyncio.Lock()
        self._entity_cache: dict[str, Any] = {}
        self._default_attachment_dir = Path(
            default_attachment_dir or tempfile.gettempdir(),
        )
        self._delta_queue_size = max(delta_queue_size, 1)

    async def list_available_dialogs(
        self,
        *,
        limit: int = 100,
        query: str | None = None,
        source_backend: str = "telethon_user_session",
    ) -> list[SourceDialog]:
        if limit <= 0:
            return []

        async def operation() -> list[SourceDialog]:
            normalized_query = self._normalize_dialog_query(query)
            results: list[SourceDialog] = []
            iter_limit = limit if normalized_query is None else None

            async for dialog in self._client.iter_dialogs(limit=iter_limit):
                source_dialog = self._source_dialog_from_dialog(dialog)
                for alias in self._dialog_aliases(dialog):
                    self._entity_cache.setdefault(alias, dialog.entity)
                if not self._matches_dialog_query(source_dialog, normalized_query):
                    continue
                results.append(source_dialog)
                if len(results) >= limit:
                    break
            return results

        return await self._run_client_operation(
            operation,
            context="listing dialogs",
        )

    async def get_source_dialog(
        self,
        dialog_id: str,
        *,
        source_backend: str = "telethon_user_session",
    ) -> SourceDialog | None:
        del source_backend

        async def operation() -> SourceDialog | None:
            try:
                entity = await self._resolve_entity(dialog_id)
            except FatalItemError as error:
                if "Telegram dialog was not found" in str(error):
                    return None
                raise
            return SourceDialog(
                dialog_id=str(dialog_id),
                chat_type=self._dialog_type(entity),
                title=self._dialog_title(entity),
                has_topics=bool(getattr(entity, "forum", False)),
            )

        return await self._run_client_operation(
            operation,
            context=f"getting dialog {dialog_id}",
        )

    async def list_dialogs(self, manifest: MigrationManifest) -> list[SourceDialog]:
        await self._ensure_started()
        discovered = await self._discover_manifest_dialogs()
        results: list[SourceDialog] = []
        for dialog in manifest.dialogs:
            physical_chat_id = dialog.physical_source_chat_id
            existing = discovered.get(physical_chat_id)
            entity = await self._resolve_entity(physical_chat_id)
            title = existing.title if existing is not None else self._dialog_title(entity)
            chat_type = existing.chat_type if existing is not None else self._dialog_type(entity)
            results.append(
                await self._estimate_dialog_inventory(
                    manifest_dialog=dialog,
                    dialog_id=physical_chat_id,
                    entity=entity,
                    title=dialog.source_thread_title or title,
                    chat_type=chat_type,
                ),
            )
        return results

    async def fetch_history(
        self,
        dialog_id: str,
        cursor: HistoryCursor | None,
        limit: int,
        *,
        source_backend: str = "telethon_user_session",
        thread_id: str | None = None,
    ) -> HistoryBatch:
        entity = await self._resolve_entity(dialog_id)

        min_id = 0
        if cursor and cursor.last_source_message_id:
            try:
                min_id = int(cursor.last_source_message_id)
            except ValueError as error:
                raise ConfigurationError(
                    f"invalid history cursor id: {cursor.last_source_message_id}",
                ) from error

        async def operation() -> HistoryBatch:
            raw_messages = await self._fetch_history_messages(
                entity=entity,
                min_id=min_id,
                limit=limit,
                thread_id=thread_id,
            )

            has_more = len(raw_messages) > limit
            selected = raw_messages[:limit]
            messages = [
                self._to_source_message(
                    dialog_id=dialog_id,
                    entity=entity,
                    message=item,
                    thread_id_override=thread_id,
                )
                for item in selected
            ]
            next_cursor = None
            if messages:
                last_message = messages[-1]
                next_cursor = HistoryCursor(
                    last_source_message_id=last_message.message_id,
                    last_source_sent_at=last_message.sent_at_utc,
                )

            return HistoryBatch(
                dialog_id=dialog_id,
                messages=messages,
                next_cursor=next_cursor,
                has_more=has_more,
                dialog_title=self._dialog_title(entity),
            )

        return await self._run_client_operation(
            operation,
            context=f"fetching history for dialog {dialog_id}",
        )

    async def close(self) -> None:
        if self._client.is_connected():
            await self._client.disconnect()

    async def download_attachment(
        self,
        attachment: CanonicalAttachment,
        *,
        source_backend: str = "telethon_user_session",
    ) -> DownloadedAttachment:
        """Download attachment using `telethon://<dialog_id>/<message_id>` locator."""
        locator = self._parse_download_locator(attachment.download_url)
        entity = await self._resolve_entity(locator["dialog_id"])
        try:
            return await self._download_attachment_with_entity(
                attachment=attachment,
                locator=locator,
                entity=entity,
            )
        except Exception as error:
            if not self._is_entity_resolution_error(error):
                raise
            refreshed_entity = await self._resolve_entity(
                locator["dialog_id"],
                force_refresh=True,
            )
            try:
                return await self._download_attachment_with_entity(
                    attachment=attachment,
                    locator=locator,
                    entity=refreshed_entity,
                )
            except Exception as retry_error:
                if self._is_entity_resolution_error(retry_error):
                    raise FatalItemError(
                        "telegram attachment entity resolution failed for dialog "
                        f"{locator['dialog_id']}: {retry_error}",
                    ) from retry_error
                raise

    def _ensure_private_attachment_dir(self, target_dir: Path) -> Path:
        target_dir.mkdir(parents=True, exist_ok=True, mode=0o700)
        try:
            target_dir.chmod(0o700)
        except OSError:
            pass
        return target_dir

    def _create_private_attachment_path(
        self,
        *,
        target_dir: Path,
        filename: str,
    ) -> Path:
        suffix = Path(filename).suffix
        file_descriptor, raw_path = tempfile.mkstemp(
            prefix="extg-tg-",
            suffix=suffix,
            dir=str(target_dir),
        )
        os.close(file_descriptor)
        return Path(raw_path)

    async def list_participants(
        self,
        dialog_id: str,
        *,
        source_backend: str = "telethon_user_session",
    ) -> list[SourceParticipant]:
        entity = await self._resolve_entity(dialog_id)

        async def operation() -> list[SourceParticipant]:
            participants: list[SourceParticipant] = []
            async for participant in self._client.iter_participants(entity):
                if not isinstance(participant, User):
                    continue
                full_name = " ".join(
                    part
                    for part in [participant.first_name, participant.last_name]
                    if part
                ).strip()
                display_name = full_name or participant.username or f"telegram_user_{participant.id}"
                participants.append(
                    SourceParticipant(
                        external_id=self._optional_str(participant.id),
                        username=participant.username,
                        display_name=display_name,
                        is_self=bool(getattr(participant, "is_self", False)),
                    ),
                )
            return participants

        return await self._run_client_operation(
            operation,
            context=f"listing participants for {dialog_id}",
        )

    async def list_topics(
        self,
        dialog_id: str,
        *,
        source_backend: str = "telethon_user_session",
    ) -> list[SourceTopic]:
        entity = await self._resolve_entity(dialog_id)

        async def operation() -> list[SourceTopic]:
            if not isinstance(entity, Channel) or not bool(getattr(entity, "forum", False)):
                return []
            offset_date = None
            offset_id = 0
            offset_topic = 0
            topics: list[SourceTopic] = []

            while True:
                response = await self._client(
                    GetForumTopicsRequest(
                        peer=entity,
                        offset_date=offset_date,
                        offset_id=offset_id,
                        offset_topic=offset_topic,
                        limit=100,
                    ),
                )
                batch = getattr(response, "topics", []) or []
                for topic in batch:
                    topic_id = self._optional_str(getattr(topic, "id", None))
                    if topic_id is None:
                        continue
                    topics.append(
                        SourceTopic(
                            topic_id=topic_id,
                            title=str(getattr(topic, "title", None) or f"topic_{topic_id}"),
                            # For forum topics, Telegram routes thread history by topic id.
                            # `top_message` points to the latest message in the topic and is
                            # not suitable as a stable thread key for history fetches.
                            top_message_id=topic_id,
                        ),
                    )
                if not batch or len(batch) < 100:
                    break
                last_topic = batch[-1]
                offset_date = getattr(last_topic, "date", None)
                offset_id = int(getattr(last_topic, "top_message", 0) or 0)
                offset_topic = int(getattr(last_topic, "id", 0) or 0)
            return topics

        return await self._run_client_operation(
            operation,
            context=f"listing forum topics for {dialog_id}",
        )

    async def get_channel_access_profile(
        self,
        dialog_id: str,
        *,
        source_backend: str = "telethon_user_session",
    ) -> SourceChannelAccessProfile:
        entity = await self._resolve_entity(dialog_id)

        async def operation() -> SourceChannelAccessProfile:
            me = await self._client.get_me(input_peer=True)
            permissions = await self._client.get_permissions(entity, me)
            is_admin = bool(
                getattr(permissions, "is_admin", False)
                or getattr(permissions, "is_creator", False)
            )
            return SourceChannelAccessProfile(
                dialog_id=dialog_id,
                is_admin=is_admin,
                can_list_participants=is_admin,
            )

        return await self._run_client_operation(
            operation,
            context=f"checking channel permissions for {dialog_id}",
        )

    async def subscribe_delta(
        self,
        dialog_ids: list[str],
        *,
        source_backend: str = "telethon_user_session",
    ) -> AsyncIterator[TelegramDeltaEvent]:
        """Yield new Telegram messages for selected dialogs."""
        await self._ensure_started()
        entities: list[Any] = []
        peer_to_dialog_id: dict[int, str] = {}
        for dialog_id in dialog_ids:
            entity = await self._resolve_entity(dialog_id)
            entities.append(entity)
            peer_to_dialog_id[utils.get_peer_id(entity)] = dialog_id

        queue: asyncio.Queue[TelegramDeltaEvent] = asyncio.Queue(
            maxsize=self._delta_queue_size,
        )

        async def on_new_message(event) -> None:
            message = getattr(event, "message", None)
            if message is None:
                return
            peer_id = getattr(event, "chat_id", None)
            if peer_id is None:
                try:
                    peer_id = utils.get_peer_id(message.peer_id)
                except Exception:
                    return
            dialog_id = peer_to_dialog_id.get(int(peer_id))
            if dialog_id is None:
                return
            entity = self._entity_cache.get(dialog_id)
            if entity is None:
                entity = await self._resolve_entity(dialog_id)
            source_message = self._to_source_message(
                dialog_id=dialog_id,
                entity=entity,
                message=message,
            )
            await queue.put(
                TelegramDeltaEvent(
                    source_chat_id=dialog_id,
                    source_message=source_message,
                    occurred_at=self._to_utc(getattr(message, "date", None)) or datetime.now(tz=UTC),
                ),
            )

        event_filter = events.NewMessage(chats=entities)
        self._client.add_event_handler(on_new_message, event_filter)
        try:
            while True:
                yield await queue.get()
        finally:
            self._client.remove_event_handler(on_new_message, event_filter)

    async def _ensure_started(self) -> None:
        if self._client.is_connected():
            return
        async with self._start_lock:
            if self._client.is_connected():
                return
            await self._client.connect()
            is_authorized = await self._client.is_user_authorized()
            if is_authorized:
                return
            raise ConfigurationError(
                "telegram session is not authorized; reconnect the user via /connect",
            )

    async def _discover_manifest_dialogs(
        self,
        *,
        force_refresh: bool = False,
    ) -> dict[str, SourceDialog]:
        async def operation() -> dict[str, SourceDialog]:
            dialogs: dict[str, SourceDialog] = {}
            async for dialog in self._client.iter_dialogs():
                source_dialog = self._source_dialog_from_dialog(dialog)
                for alias in self._dialog_aliases(dialog):
                    dialogs.setdefault(alias, source_dialog)
                    if force_refresh:
                        self._entity_cache[alias] = dialog.entity
                    else:
                        self._entity_cache.setdefault(alias, dialog.entity)
            return dialogs

        return await self._run_client_operation(
            operation,
            context="listing dialogs",
        )

    async def _estimate_dialog_inventory(
        self,
        *,
        manifest_dialog: ManifestDialog,
        dialog_id: str,
        entity: Any,
        title: str,
        chat_type: str,
    ) -> SourceDialog:
        async def operation() -> SourceDialog:
            message_count = 0
            media_count = 0
            approximate_bytes = 0
            async for message in self._iter_inventory_messages(
                entity=entity,
                manifest_dialog=manifest_dialog,
            ):
                if not getattr(message, "id", None):
                    continue
                source_message_id = str(message.id)
                source_thread_id = (
                    manifest_dialog.source_thread_id
                    or self._message_thread_id(message)
                )
                if not manifest_dialog.matches_source_message(
                    source_message_id=source_message_id,
                    source_thread_id=source_thread_id,
                ):
                    continue
                message_count += 1
                body = getattr(message, "message", None) or ""
                approximate_bytes += len(body.encode("utf-8"))
                file_info = getattr(message, "file", None)
                if file_info:
                    media_count += 1
                    size = getattr(file_info, "size", None)
                    if isinstance(size, int):
                        approximate_bytes += size
            return SourceDialog(
                dialog_id=manifest_dialog.source_chat_id,
                chat_type=chat_type,
                title=title,
                message_count=message_count,
                media_count=media_count,
                approximate_bytes=approximate_bytes,
                has_topics=bool(getattr(entity, "forum", False)),
            )

        return await self._run_client_operation(
            operation,
            context=f"estimating dialog {dialog_id}",
        )

    async def _fetch_history_messages(
        self,
        *,
        entity: Any,
        min_id: int,
        limit: int,
        thread_id: str | None,
    ) -> list[Message]:
        if thread_id is None:
            raw_messages: list[Message] = []
            async for message in self._client.iter_messages(
                entity,
                limit=limit + 1,
                min_id=min_id,
                reverse=True,
            ):
                if not getattr(message, "id", None):
                    continue
                raw_messages.append(message)
            return raw_messages

        thread_root_id = self._parse_thread_id(thread_id)
        raw_messages: list[Message] = []
        target_count = max(1, limit) + 1

        async for message in self._iter_topic_messages(
            entity=entity,
            thread_root_id=thread_root_id,
            min_id=min_id,
            limit=target_count,
        ):
            raw_messages.append(message)
            if len(raw_messages) >= target_count:
                break
        return raw_messages

    async def _iter_inventory_messages(
        self,
        *,
        entity: Any,
        manifest_dialog: ManifestDialog,
    ) -> AsyncIterator[Message]:
        if manifest_dialog.source_thread_id is None:
            async for message in self._client.iter_messages(entity, reverse=True):
                yield message
            return

        thread_root_id = self._parse_thread_id(manifest_dialog.source_thread_id)
        async for message in self._iter_topic_messages(
            entity=entity,
            thread_root_id=thread_root_id,
            min_id=0,
            limit=None,
        ):
            yield message

    async def _iter_topic_messages(
        self,
        *,
        entity: Any,
        thread_root_id: int,
        min_id: int,
        limit: int | None,
    ) -> AsyncIterator[Message]:
        yielded = 0
        seen_ids: set[int] = set()
        if min_id < thread_root_id:
            root_message = await self._get_message_by_id(
                entity=entity,
                message_id=thread_root_id,
            )
            if root_message is not None and getattr(root_message, "id", None):
                seen_ids.add(root_message.id)
                yield root_message
                yielded += 1
                if limit is not None and yielded >= limit:
                    return

        offset_id = max(min_id, thread_root_id) + 1
        while True:
            remaining = None if limit is None else limit - yielded
            if remaining is not None and remaining <= 0:
                return
            request_limit = (
                self._SEARCH_CHUNK_SIZE
                if remaining is None
                else min(remaining, self._SEARCH_CHUNK_SIZE)
            )
            request = SearchRequest(
                peer=entity,
                q="",
                filter=InputMessagesFilterEmpty(),
                min_date=None,
                max_date=None,
                offset_id=offset_id,
                add_offset=-request_limit,
                limit=request_limit,
                max_id=0,
                min_id=0,
                hash=0,
                top_msg_id=thread_root_id,
            )
            response = await self._client(request)
            messages = list(getattr(response, "messages", []))
            if not messages:
                return

            entities = {
                utils.get_peer_id(item): item
                for item in itertools.chain(
                    getattr(response, "users", []),
                    getattr(response, "chats", []),
                )
            }
            last_emitted_id: int | None = None
            for message in reversed(messages):
                if isinstance(message, MessageEmpty):
                    continue
                message_id = getattr(message, "id", None)
                if not isinstance(message_id, int):
                    continue
                if message_id <= min_id or message_id in seen_ids:
                    continue
                self._finish_message_init(
                    message=message,
                    entities=entities,
                    entity=entity,
                )
                seen_ids.add(message_id)
                last_emitted_id = message_id
                yield message
                yielded += 1
                if limit is not None and yielded >= limit:
                    return
            if last_emitted_id is None:
                return
            offset_id = last_emitted_id + 1

    async def _get_message_by_id(self, *, entity: Any, message_id: int) -> Message | None:
        message = await self._client.get_messages(entity, ids=message_id)
        if isinstance(message, list):
            return message[0] if message else None
        return message if getattr(message, "id", None) else None

    def _finish_message_init(
        self,
        *,
        message: Message,
        entities: dict[int, Any],
        entity: Any,
    ) -> None:
        finish_init = getattr(message, "_finish_init", None)
        if callable(finish_init):
            finish_init(self._client, entities, entity)

    def _message_thread_id(self, message: Message) -> str | None:
        direct_thread_id = self._optional_str(getattr(message, "reply_to_top_id", None))
        if direct_thread_id is not None:
            return direct_thread_id
        reply_header = getattr(message, "reply_to", None)
        if reply_header is None:
            return None
        top_id = self._optional_str(getattr(reply_header, "reply_to_top_id", None))
        if top_id is not None:
            return top_id
        if bool(getattr(reply_header, "forum_topic", False)):
            return self._optional_str(getattr(reply_header, "reply_to_msg_id", None))
        return None

    def _parse_thread_id(self, thread_id: str) -> int:
        try:
            return int(thread_id)
        except ValueError as error:
            raise ConfigurationError(
                f"invalid source_thread_id: {thread_id}",
            ) from error

    async def _resolve_entity(self, dialog_id: str, *, force_refresh: bool = False) -> Any:
        if not force_refresh and dialog_id in self._entity_cache:
            return self._entity_cache[dialog_id]
        if force_refresh:
            await self._discover_manifest_dialogs(force_refresh=True)
            cached = self._entity_cache.get(dialog_id)
            if cached is not None:
                return cached

        async def operation() -> Any:
            candidate: Any
            try:
                candidate = int(dialog_id)
            except ValueError:
                candidate = dialog_id
            return await self._client.get_entity(candidate)

        try:
            entity = await self._run_client_operation(
                operation,
                context=f"resolving entity {dialog_id}",
            )
        except Exception as error:
            if not self._is_entity_resolution_error(error):
                raise
            await self._discover_manifest_dialogs(force_refresh=True)
            cached = self._entity_cache.get(dialog_id)
            if cached is not None:
                return cached
            raise FatalItemError(
                f"telegram entity resolution failed for dialog {dialog_id}: {error}",
            ) from error
        self._entity_cache[dialog_id] = entity
        try:
            self._entity_cache.setdefault(str(utils.get_peer_id(entity)), entity)
        except Exception:
            pass
        return entity

    async def _download_attachment_with_entity(
        self,
        *,
        attachment: CanonicalAttachment,
        locator: dict[str, str],
        entity: Any,
    ) -> DownloadedAttachment:
        message_id = int(locator["message_id"])

        async def operation() -> DownloadedAttachment:
            message = await self._client.get_messages(entity, ids=message_id)
            if message is None:
                raise FatalItemError(
                    f"source message not found for attachment locator {attachment.download_url}",
                )
            target_dir = self._ensure_private_attachment_dir(self._default_attachment_dir)
            filename = attachment.filename or f"tg_{locator['dialog_id']}_{message_id}.bin"
            file_path = self._create_private_attachment_path(
                target_dir=target_dir,
                filename=filename,
            )
            downloaded = await self._client.download_media(message, file=str(file_path))

            if downloaded is None:
                raise RecoverableItemError("telegram returned empty attachment payload")
            if isinstance(downloaded, bytes):
                file_path.write_bytes(downloaded)
            final_path = Path(downloaded) if isinstance(downloaded, str) else file_path
            sha256 = self._sha256_file(final_path)
            size_bytes = final_path.stat().st_size
            return DownloadedAttachment(
                attachment=replace(
                    attachment,
                    temp_local_path=str(final_path),
                    sha256=sha256,
                    size_bytes=size_bytes,
                ),
                local_path=str(final_path),
                size_bytes=size_bytes,
            )

        return await self._run_client_operation(
            operation,
            context="downloading attachment",
        )

    def _is_entity_resolution_error(self, error: Exception) -> bool:
        message = str(error)
        if not message:
            return False
        normalized = message.casefold()
        return (
            "could not find the input entity" in normalized
            or "could not find any entity corresponding to" in normalized
            or "no user has" in normalized
        )

    async def _run_client_operation(
        self,
        operation,
        *,
        context: str,
    ):
        await self._ensure_started()
        reconnect_attempted = False
        while True:
            try:
                return await operation()
            except FloodWaitError as error:
                raise RecoverableItemError(
                    f"telegram flood wait while {context}: {error.seconds}s",
                ) from error
            except RPCError as error:
                raise FatalItemError(
                    f"telegram RPC error while {context}: {error}",
                ) from error
            except Exception as error:
                if self._is_disconnect_error(error) and not reconnect_attempted:
                    reconnect_attempted = True
                    await self._restart_client()
                    continue
                if isinstance(error, (OSError, TimeoutError, ConnectionError, asyncio.IncompleteReadError)):
                    raise RecoverableItemError(
                        f"telegram temporary error while {context}: {error}",
                    ) from error
                raise

    async def _restart_client(self) -> None:
        async with self._start_lock:
            try:
                await self._client.disconnect()
            except Exception:
                pass
            await self._client.connect()
            is_authorized = await self._client.is_user_authorized()
            if not is_authorized:
                raise ConfigurationError(
                    "telegram session is not authorized; reconnect the user via /connect",
                )

    def _is_disconnect_error(self, error: Exception) -> bool:
        if isinstance(error, asyncio.IncompleteReadError):
            return True
        if isinstance(error, (ConnectionError, BrokenPipeError, ConnectionResetError)):
            return True
        message = str(error).lower()
        return (
            "server closed the connection" in message
            or "0 bytes read" in message
            or "connection reset by peer" in message
        )

    def _to_source_message(
        self,
        *,
        dialog_id: str,
        entity: Any,
        message: Message,
        thread_id_override: str | None = None,
    ) -> TelegramSourceMessage:
        content_type = self._content_type(message)
        reply_to_message_id = getattr(message, "reply_to_msg_id", None)
        if reply_to_message_id is None and getattr(message, "reply_to", None):
            reply_to_message_id = getattr(
                message.reply_to,
                "reply_to_msg_id",
                None,
            )
        return TelegramSourceMessage(
            chat_id=dialog_id,
            message_id=str(message.id),
            sent_at_utc=self._to_utc(message.date),
            author=self._author(message),
            body=getattr(message, "message", None),
            chat_title=self._dialog_title(entity),
            thread_id=(
                self._message_thread_id(message)
                or thread_id_override
            ),
            reply_to_message_id=self._optional_str(reply_to_message_id),
            edited_at_utc=self._to_utc(getattr(message, "edit_date", None)),
            deleted_in_source=False,
            content_type=content_type,
            attachments=self._attachments(dialog_id, message, content_type),
            entities=self._entities(message),
            raw_payload=self._json_safe_payload(message.to_dict()),
            service_payload=self._service_payload(message),
        )

    def _content_type(self, message: Message) -> ContentType:
        if getattr(message, "sticker", None):
            return ContentType.STICKER
        if getattr(message, "photo", None):
            return ContentType.PHOTO
        if getattr(message, "voice", None):
            return ContentType.VOICE
        if getattr(message, "video", None):
            return ContentType.VIDEO
        if getattr(message, "audio", None):
            return ContentType.AUDIO
        if getattr(message, "document", None):
            return ContentType.DOCUMENT
        if getattr(message, "poll", None):
            return ContentType.POLL
        if getattr(message, "action", None):
            return ContentType.SERVICE
        if getattr(message, "message", None):
            return ContentType.TEXT
        return ContentType.UNSUPPORTED

    def _attachments(
        self,
        dialog_id: str,
        message: Message,
        content_type: ContentType,
    ) -> list[CanonicalAttachment]:
        file_info = getattr(message, "file", None)
        if not file_info:
            return []
        source_file_id = self._optional_str(getattr(file_info, "id", None))
        filename = getattr(file_info, "name", None)
        mime_type = getattr(file_info, "mime_type", None)
        size = getattr(file_info, "size", None)
        size_bytes = int(size) if isinstance(size, int) else None
        duration = getattr(file_info, "duration", None)
        duration_seconds = int(duration) if isinstance(duration, (int, float)) else None
        media_kind = content_type.value if content_type is not ContentType.TEXT else "file"
        return [
            CanonicalAttachment(
                source_file_id=source_file_id,
                filename=filename,
                mime_type=mime_type,
                size_bytes=size_bytes,
                media_kind=media_kind,
                duration_seconds=duration_seconds,
                download_url=f"telethon://{dialog_id}/{message.id}",
            ),
        ]

    def _service_payload(self, message: Message) -> dict[str, Any] | None:
        action = getattr(message, "action", None)
        if not action:
            return None
        return {"action_type": type(action).__name__}

    def _json_safe_payload(self, payload: Any) -> dict[str, Any]:
        sanitized = self._json_safe_value(payload)
        if isinstance(sanitized, dict):
            return sanitized
        return {"value": sanitized}

    def _json_safe_value(self, value: Any) -> Any:
        if value is None or isinstance(value, (str, int, float, bool)):
            return value
        if isinstance(value, datetime):
            return self._to_utc(value).isoformat()
        if isinstance(value, Enum):
            return value.value
        if isinstance(value, (bytes, bytearray, memoryview)):
            return {
                "__type__": "bytes",
                "length": len(value),
            }
        if isinstance(value, dict):
            return {
                str(key): self._json_safe_value(item)
                for key, item in value.items()
            }
        if isinstance(value, (list, tuple, set)):
            return [self._json_safe_value(item) for item in value]
        return str(value)

    def _entities(self, message: Message) -> list[CanonicalEntity]:
        body = getattr(message, "message", None) or ""
        raw_entities = getattr(message, "entities", None) or []
        entities: list[CanonicalEntity] = []
        for entity in raw_entities:
            offset = int(getattr(entity, "offset", 0))
            length = int(getattr(entity, "length", 0))
            if offset < 0 or length <= 0 or offset + length > len(body):
                continue
            text = body[offset : offset + length]
            if isinstance(entity, MessageEntityMention):
                entities.append(
                    CanonicalEntity(
                        kind="mention",
                        offset=offset,
                        length=length,
                        text=text,
                        telegram_username=text.lstrip("@"),
                    ),
                )
                continue
            if isinstance(entity, (MessageEntityMentionName, InputMessageEntityMentionName)):
                entities.append(
                    CanonicalEntity(
                        kind="mention",
                        offset=offset,
                        length=length,
                        text=text,
                        telegram_user_id=self._optional_str(getattr(entity, "user_id", None)),
                    ),
                )
                continue
            if isinstance(entity, MessageEntityTextUrl):
                telegram_user_id = self._telegram_user_id_from_text_url(
                    getattr(entity, "url", None),
                )
                if telegram_user_id is None:
                    continue
                entities.append(
                    CanonicalEntity(
                        kind="mention",
                        offset=offset,
                        length=length,
                        text=text,
                        telegram_user_id=telegram_user_id,
                    ),
                )
        return entities

    def _author(self, message: Message) -> TelegramAuthor:
        sender = getattr(message, "sender", None)
        sender_id = self._optional_str(getattr(message, "sender_id", None))
        post_author = getattr(message, "post_author", None)
        if post_author:
            return TelegramAuthor(
                external_id=sender_id,
                display_name=post_author,
                username=None,
            )
        if isinstance(sender, User):
            full_name = " ".join(
                part
                for part in [sender.first_name, sender.last_name]
                if part
            ).strip()
            display_name = full_name or sender.username or f"telegram_user_{sender.id}"
            return TelegramAuthor(
                external_id=self._optional_str(sender.id) or sender_id,
                display_name=display_name,
                username=sender.username,
            )
        if isinstance(sender, (Channel, Chat)):
            return TelegramAuthor(
                external_id=self._optional_str(sender.id) or sender_id,
                display_name=getattr(sender, "title", None) or f"telegram_chat_{sender.id}",
                username=getattr(sender, "username", None),
            )
        if sender_id:
            return TelegramAuthor(
                external_id=sender_id,
                display_name=f"telegram_user_{sender_id}",
                username=None,
            )
        return TelegramAuthor(
            external_id=None,
            display_name="unknown_sender",
            username=None,
        )

    def _dialog_aliases(self, dialog: Dialog) -> set[str]:
        aliases: set[str] = {str(dialog.id)}
        entity = dialog.entity
        raw_entity_id = getattr(entity, "id", None)
        if raw_entity_id is not None:
            aliases.add(str(raw_entity_id))
        try:
            aliases.add(str(utils.get_peer_id(entity)))
        except Exception:
            pass
        return aliases

    def _source_dialog_from_dialog(self, dialog: Dialog) -> SourceDialog:
        return SourceDialog(
            dialog_id=str(dialog.id),
            chat_type=self._dialog_type(dialog.entity),
            title=dialog.title or self._dialog_title(dialog.entity),
            has_topics=bool(getattr(dialog.entity, "forum", False)),
        )

    def _normalize_dialog_query(self, query: str | None) -> str | None:
        if query is None:
            return None
        normalized = query.strip().casefold()
        return normalized or None

    def _matches_dialog_query(
        self,
        dialog: SourceDialog,
        normalized_query: str | None,
    ) -> bool:
        if normalized_query is None:
            return True
        return normalized_query in dialog.title.casefold()

    def _dialog_title(self, entity: Any) -> str:
        title = getattr(entity, "title", None)
        if title:
            return str(title)
        first_name = getattr(entity, "first_name", None)
        last_name = getattr(entity, "last_name", None)
        full_name = " ".join(part for part in [first_name, last_name] if part).strip()
        if full_name:
            return full_name
        username = getattr(entity, "username", None)
        if username:
            return str(username)
        entity_id = self._optional_str(getattr(entity, "id", None))
        return f"telegram_dialog_{entity_id or 'unknown'}"

    def _dialog_type(self, entity: Any) -> str:
        if isinstance(entity, User):
            return "private"
        if isinstance(entity, Channel):
            return "supergroup" if getattr(entity, "megagroup", False) else "channel"
        if isinstance(entity, Chat):
            return "group"
        return "unknown"

    def _parse_download_locator(self, download_url: str | None) -> dict[str, str]:
        if not download_url:
            raise FatalItemError(
                "attachment.download_url is required for telethon download",
            )
        parsed = urlparse(download_url)
        if parsed.scheme != "telethon":
            raise FatalItemError(
                f"unsupported attachment download scheme: {parsed.scheme}",
            )
        path_parts = [part for part in parsed.path.split("/") if part]
        if parsed.netloc:
            path_parts.insert(0, parsed.netloc)
        if len(path_parts) < 2:
            raise FatalItemError(
                f"invalid attachment locator format: {download_url}",
            )
        dialog_id, message_id = path_parts[0], path_parts[1]
        try:
            int(message_id)
        except ValueError as error:
            raise FatalItemError(
                f"invalid message_id in attachment locator: {download_url}",
            ) from error
        return {"dialog_id": dialog_id, "message_id": message_id}

    def _telegram_user_id_from_text_url(self, value: str | None) -> str | None:
        if not value:
            return None
        parsed = urlparse(value)
        if parsed.scheme != "tg" or parsed.netloc != "user":
            return None
        query_items = {
            part.split("=", 1)[0]: part.split("=", 1)[1]
            for part in parsed.query.split("&")
            if "=" in part
        }
        user_id = query_items.get("id")
        return user_id or None

    def _sha256_file(self, path: Path) -> str:
        digest = hashlib.sha256()
        with path.open("rb") as file:
            for chunk in iter(lambda: file.read(1024 * 1024), b""):
                digest.update(chunk)
        return digest.hexdigest()

    def _to_utc(self, value: datetime | None) -> datetime | None:
        if value is None:
            return None
        if value.tzinfo is None:
            return value.replace(tzinfo=UTC)
        return value.astimezone(UTC)

    def _optional_str(self, value: Any) -> str | None:
        if value is None:
            return None
        return str(value)
