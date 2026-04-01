from __future__ import annotations

from collections.abc import AsyncIterator, Iterable
from dataclasses import replace
from hashlib import sha256

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
    TelegramSourceMessage,
)


class FakeTelegramGateway:
    def __init__(
        self,
        *,
        dialogs: Iterable[SourceDialog],
        messages_by_dialog: dict[str, list[TelegramSourceMessage]],
        delta_events_by_dialog: dict[str, Iterable[TelegramDeltaEvent]] | None = None,
        participants_by_dialog: dict[str, Iterable[SourceParticipant]] | None = None,
        topics_by_dialog: dict[str, Iterable[SourceTopic]] | None = None,
        channel_access_profiles_by_dialog: dict[str, SourceChannelAccessProfile] | None = None,
        downloaded_attachment_content_by_url: dict[str, bytes] | None = None,
        download_failures_by_url: dict[str, Exception] | None = None,
    ) -> None:
        self._dialogs = {dialog.dialog_id: dialog for dialog in dialogs}
        self._messages_by_dialog = {
            dialog_id: sorted(
                messages,
                key=lambda item: (item.sent_at_utc, item.message_id),
            )
            for dialog_id, messages in messages_by_dialog.items()
        }
        self._delta_events_by_dialog = {
            dialog_id: sorted(
                events,
                key=lambda event: (
                    event.occurred_at,
                    event.source_message.sent_at_utc,
                    event.source_message.message_id,
                ),
            )
            for dialog_id, events in (delta_events_by_dialog or {}).items()
        }
        self._participants_by_dialog = {
            dialog_id: list(participants)
            for dialog_id, participants in (participants_by_dialog or {}).items()
        }
        self._topics_by_dialog = {
            dialog_id: list(topics)
            for dialog_id, topics in (topics_by_dialog or {}).items()
        }
        self._channel_access_profiles_by_dialog = dict(
            channel_access_profiles_by_dialog or {},
        )
        self._downloaded_attachment_content_by_url = dict(
            downloaded_attachment_content_by_url or {},
        )
        self._download_failures_by_url = dict(download_failures_by_url or {})
        self.fetch_calls = 0
        self.download_calls = 0
        self.delta_subscribe_calls = 0

    async def list_available_dialogs(
        self,
        *,
        limit: int = 100,
        query: str | None = None,
        source_backend: str = "telethon_user_session",
    ) -> list[SourceDialog]:
        if limit <= 0:
            return []

        normalized_query = self._normalize_dialog_query(query)
        dialogs: list[SourceDialog] = []
        for dialog in self._dialogs.values():
            if not self._matches_dialog_query(dialog, normalized_query):
                continue
            dialogs.append(dialog)
            if len(dialogs) >= limit:
                break
        return dialogs

    async def get_source_dialog(
        self,
        dialog_id: str,
        *,
        source_backend: str = "telethon_user_session",
    ) -> SourceDialog | None:
        return self._dialogs.get(dialog_id)

    async def list_dialogs(self, manifest: MigrationManifest) -> list[SourceDialog]:
        results: list[SourceDialog] = []
        for manifest_dialog in manifest.dialogs:
            physical_chat_id = manifest_dialog.physical_source_chat_id
            dialog = self._dialogs.get(physical_chat_id)
            if dialog is None:
                continue
            if manifest_dialog.source_thread_id is None:
                results.append(
                    SourceDialog(
                        dialog_id=manifest_dialog.source_chat_id,
                        chat_type=dialog.chat_type,
                        title=dialog.title,
                        message_count=dialog.message_count,
                        media_count=dialog.media_count,
                        approximate_bytes=dialog.approximate_bytes,
                        has_topics=dialog.has_topics,
                    ),
                )
                continue
            messages = self._messages_by_dialog.get(physical_chat_id, [])
            filtered = [
                message
                for message in messages
                if manifest_dialog.matches_source_message(
                    source_message_id=message.message_id,
                    source_thread_id=message.thread_id,
                )
            ]
            results.append(
                SourceDialog(
                    dialog_id=manifest_dialog.source_chat_id,
                    chat_type=dialog.chat_type,
                    title=manifest_dialog.source_thread_title or dialog.title,
                    message_count=len(filtered),
                    media_count=sum(1 for message in filtered if message.attachments),
                    approximate_bytes=sum(
                        len((message.body or "").encode("utf-8"))
                        for message in filtered
                    ),
                    has_topics=dialog.has_topics,
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
        del thread_id
        self.fetch_calls += 1
        messages = self._messages_by_dialog.get(dialog_id, [])
        start_index = 0
        if cursor and cursor.last_source_message_id:
            for index, message in enumerate(messages):
                if message.message_id == cursor.last_source_message_id:
                    start_index = index + 1
                    break
        selected = messages[start_index : start_index + limit]
        next_cursor = None
        if selected:
            last_message = selected[-1]
            next_cursor = HistoryCursor(
                last_source_message_id=last_message.message_id,
                last_source_sent_at=last_message.sent_at_utc,
            )
        has_more = start_index + limit < len(messages)
        dialog = self._dialogs.get(dialog_id)
        return HistoryBatch(
            dialog_id=dialog_id,
            messages=selected,
            next_cursor=next_cursor,
            has_more=has_more,
            dialog_title=dialog.title if dialog else None,
        )

    async def download_attachment(
        self,
        attachment: CanonicalAttachment,
        *,
        source_backend: str = "telethon_user_session",
    ) -> DownloadedAttachment:
        self.download_calls += 1
        if not attachment.download_url:
            raise FatalItemError("attachment download_url is required")
        failure = self._download_failures_by_url.get(attachment.download_url)
        if failure is not None:
            raise failure

        payload = self._downloaded_attachment_content_by_url.get(attachment.download_url)
        if payload is None:
            payload = (
                attachment.filename
                or attachment.source_file_id
                or attachment.download_url
            ).encode("utf-8")
        size_bytes = len(payload)
        enriched_attachment = replace(
            attachment,
            size_bytes=size_bytes,
            sha256=sha256(payload).hexdigest(),
        )
        return DownloadedAttachment(
            attachment=enriched_attachment,
            content=payload,
            size_bytes=size_bytes,
        )

    async def list_participants(
        self,
        dialog_id: str,
        *,
        source_backend: str = "telethon_user_session",
    ) -> list[SourceParticipant]:
        return list(self._participants_by_dialog.get(dialog_id, []))

    async def list_topics(
        self,
        dialog_id: str,
        *,
        source_backend: str = "telethon_user_session",
    ) -> list[SourceTopic]:
        return list(self._topics_by_dialog.get(dialog_id, []))

    async def get_channel_access_profile(
        self,
        dialog_id: str,
        *,
        source_backend: str = "telethon_user_session",
    ) -> SourceChannelAccessProfile:
        existing = self._channel_access_profiles_by_dialog.get(dialog_id)
        if existing is not None:
            return existing
        return SourceChannelAccessProfile(
            dialog_id=dialog_id,
            is_admin=True,
            can_list_participants=True,
        )

    async def subscribe_delta(
        self,
        dialog_ids: list[str],
        *,
        source_backend: str = "telethon_user_session",
    ) -> AsyncIterator[TelegramDeltaEvent]:
        self.delta_subscribe_calls += 1
        for dialog_id in dialog_ids:
            for event in self._delta_events_by_dialog.get(dialog_id, []):
                yield event

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
