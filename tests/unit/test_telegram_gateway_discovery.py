from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace

import pytest
from telethon.tl.functions.messages import SearchRequest

from extg_shared.contracts.manifest import MigrationManifest
from extg_shared.contracts.models import CanonicalAttachment, ContentType, SourceDialog
from extg_telethon_service.infrastructure.telegram.fake_gateway import (
    FakeTelegramGateway,
)
from extg_telethon_service.infrastructure.telegram.telethon_gateway import (
    TelethonTelegramGateway,
)


class StubTelethonClient:
    def __init__(self, dialogs: list[SimpleNamespace]) -> None:
        self._dialogs = dialogs
        self._connected = False
        self.connect_calls = 0
        self.disconnect_calls = 0
        self.iter_dialogs_limits: list[int | None] = []

    def is_connected(self) -> bool:
        return self._connected

    async def connect(self) -> None:
        self.connect_calls += 1
        self._connected = True

    async def disconnect(self) -> None:
        self.disconnect_calls += 1
        self._connected = False

    async def is_user_authorized(self) -> bool:
        return True

    def iter_dialogs(self, limit: int | None = None):
        self.iter_dialogs_limits.append(limit)
        return StubDialogIterator(self._dialogs, limit)

    async def get_entity(self, candidate):
        candidate_str = str(candidate)
        for dialog in self._dialogs:
            if str(getattr(dialog.entity, "id", "")) == candidate_str:
                return dialog.entity
        raise LookupError(candidate)


class StubDialogIterator:
    def __init__(self, dialogs: list[SimpleNamespace], limit: int | None) -> None:
        self._dialogs = dialogs
        self._limit = limit
        self._index = 0

    def __aiter__(self) -> "StubDialogIterator":
        return self

    async def __anext__(self) -> SimpleNamespace:
        if self._limit is not None and self._index >= self._limit:
            raise StopAsyncIteration
        if self._index >= len(self._dialogs):
            raise StopAsyncIteration

        dialog = self._dialogs[self._index]
        self._index += 1
        return dialog


class StubMessageIterator:
    def __init__(self, messages: list[SimpleNamespace], limit: int) -> None:
        self._messages = messages[:limit]
        self._index = 0

    def __aiter__(self) -> "StubMessageIterator":
        return self

    async def __anext__(self) -> SimpleNamespace:
        if self._index >= len(self._messages):
            raise StopAsyncIteration
        message = self._messages[self._index]
        self._index += 1
        return message


def _dialog_preview(dialog_id: str, title: str) -> SourceDialog:
    return SourceDialog(
        dialog_id=dialog_id,
        chat_type="supergroup",
        title=title,
    )


def _telethon_dialog(dialog_id: int, title: str) -> SimpleNamespace:
    entity = SimpleNamespace(id=dialog_id, title=title)
    return SimpleNamespace(id=dialog_id, title=title, entity=entity)


@pytest.mark.asyncio
async def test_fake_gateway_list_available_dialogs_applies_case_insensitive_query_and_limit():
    gateway = FakeTelegramGateway(
        dialogs=[
            _dialog_preview("1", "Alpha Project"),
            _dialog_preview("2", "project Beta"),
            _dialog_preview("3", "Random Chat"),
        ],
        messages_by_dialog={},
    )

    dialogs = await gateway.list_available_dialogs(limit=1, query="  PROJ  ")

    assert [dialog.dialog_id for dialog in dialogs] == ["1"]
    assert [dialog.title for dialog in dialogs] == ["Alpha Project"]


@pytest.mark.asyncio
async def test_fake_gateway_list_available_dialogs_returns_all_titles_when_query_is_empty():
    gateway = FakeTelegramGateway(
        dialogs=[
            _dialog_preview("1", "Alpha Project"),
            _dialog_preview("2", "project Beta"),
            _dialog_preview("3", "Random Chat"),
        ],
        messages_by_dialog={},
    )

    dialogs = await gateway.list_available_dialogs(limit=2, query="   ")

    assert [dialog.dialog_id for dialog in dialogs] == ["1", "2"]


@pytest.mark.asyncio
async def test_telethon_gateway_list_available_dialogs_applies_case_insensitive_query_and_limit():
    client = StubTelethonClient(
        [
            _telethon_dialog(101, "Alpha Project"),
            _telethon_dialog(102, "project Beta"),
            _telethon_dialog(103, "Random Chat"),
        ],
    )
    gateway = TelethonTelegramGateway(
        api_id=None,
        api_hash=None,
        session_string=None,
        client=client,
    )

    dialogs = await gateway.list_available_dialogs(limit=1, query="proJ")

    assert client.connect_calls == 1
    assert client.iter_dialogs_limits == [None]
    assert [dialog.dialog_id for dialog in dialogs] == ["101"]
    assert dialogs[0].title == "Alpha Project"
    assert dialogs[0].message_count == 0


@pytest.mark.asyncio
async def test_telethon_gateway_list_available_dialogs_uses_limit_without_query():
    client = StubTelethonClient(
        [
            _telethon_dialog(101, "Alpha Project"),
            _telethon_dialog(102, "project Beta"),
            _telethon_dialog(103, "Random Chat"),
        ],
    )
    gateway = TelethonTelegramGateway(
        api_id=None,
        api_hash=None,
        session_string=None,
        client=client,
    )

    dialogs = await gateway.list_available_dialogs(limit=2)

    assert client.iter_dialogs_limits == [2]
    assert [dialog.dialog_id for dialog in dialogs] == ["101", "102"]


class FlakyDialogClient(StubTelethonClient):
    def __init__(self, dialogs: list[SimpleNamespace]) -> None:
        super().__init__(dialogs)
        self.fail_first_iter = True

    def iter_dialogs(self, limit: int | None = None):
        self.iter_dialogs_limits.append(limit)
        if self.fail_first_iter:
            self.fail_first_iter = False
            return FailingDialogIterator()
        return StubDialogIterator(self._dialogs, limit)


class FailingDialogIterator:
    def __aiter__(self) -> "FailingDialogIterator":
        return self

    async def __anext__(self) -> SimpleNamespace:
        raise asyncio.IncompleteReadError(partial=b"", expected=8)


class StubHistoryClient(StubTelethonClient):
    def __init__(
        self,
        dialogs: list[SimpleNamespace],
        messages: list[SimpleNamespace],
        *,
        replies_by_thread_id: dict[int, list[SimpleNamespace]] | None = None,
    ) -> None:
        super().__init__(dialogs)
        self._messages = messages
        self._replies_by_thread_id = replies_by_thread_id or {}

    async def __call__(self, request):
        assert isinstance(request, SearchRequest)
        messages = self._replies_by_thread_id.get(request.top_msg_id or 0, [])
        filtered = [
            message
            for message in messages
            if int(getattr(message, "id", 0) or 0) >= int(request.offset_id or 0)
        ]
        return SimpleNamespace(
            messages=filtered[: request.limit],
            users=[],
            chats=[],
            count=len(filtered),
        )

    def iter_messages(
        self,
        entity,
        limit: int | None = None,
        min_id: int = 0,
        reverse: bool = False,
        reply_to: int | None = None,
    ):
        del entity, reverse
        messages = (
            self._replies_by_thread_id.get(reply_to, [])
            if reply_to is not None
            else self._messages
        )
        filtered = [
            message
            for message in messages
            if int(getattr(message, "id", 0) or 0) > min_id
        ]
        return StubMessageIterator(filtered, limit or len(filtered))

    async def get_messages(self, entity, ids):
        del entity
        for message in self._messages:
            if getattr(message, "id", None) == ids:
                return message
        return None


class StubInventoryClient(StubTelethonClient):
    def __init__(
        self,
        dialogs: list[SimpleNamespace],
        messages_by_entity_id: dict[str, list[SimpleNamespace]],
        *,
        replies_by_entity_and_thread_id: dict[tuple[str, int], list[SimpleNamespace]] | None = None,
    ) -> None:
        super().__init__(dialogs)
        self._messages_by_entity_id = messages_by_entity_id
        self._replies_by_entity_and_thread_id = replies_by_entity_and_thread_id or {}

    async def __call__(self, request):
        assert isinstance(request, SearchRequest)
        entity_id = str(getattr(request.peer, "id", ""))
        messages = self._replies_by_entity_and_thread_id.get(
            (entity_id, request.top_msg_id or 0),
            [],
        )
        filtered = [
            message
            for message in messages
            if int(getattr(message, "id", 0) or 0) >= int(request.offset_id or 0)
        ]
        return SimpleNamespace(
            messages=filtered[: request.limit],
            users=[],
            chats=[],
            count=len(filtered),
        )

    def iter_messages(
        self,
        entity,
        reverse: bool = True,
        reply_to: int | None = None,
    ):
        del reverse
        entity_id = str(getattr(entity, "id", ""))
        messages = (
            self._replies_by_entity_and_thread_id.get((entity_id, reply_to), [])
            if reply_to is not None
            else self._messages_by_entity_id.get(entity_id, [])
        )
        return StubUnboundedMessageIterator(messages)

    async def get_messages(self, entity, ids):
        entity_id = str(getattr(entity, "id", ""))
        for message in self._messages_by_entity_id.get(entity_id, []):
            if getattr(message, "id", None) == ids:
                return message
        return None


class DiscoveringEntityClient(StubTelethonClient):
    def __init__(self, dialogs: list[SimpleNamespace]) -> None:
        super().__init__(dialogs)
        self.get_entity_calls: list[object] = []

    async def get_entity(self, candidate):
        self.get_entity_calls.append(candidate)
        raise ValueError(
            f"Could not find the input entity for PeerUser(user_id={candidate}) (PeerUser).",
        )


class AttachmentRefreshClient(StubTelethonClient):
    def __init__(
        self,
        *,
        stale_dialog: SimpleNamespace,
        fresh_dialog: SimpleNamespace,
    ) -> None:
        super().__init__([fresh_dialog])
        self._stale_dialog = stale_dialog
        self._fresh_dialog = fresh_dialog
        self.get_messages_entities: list[object] = []
        self.download_media_calls = 0

    async def get_entity(self, candidate):
        candidate_str = str(candidate)
        if candidate_str == str(self._stale_dialog.id):
            return self._stale_dialog.entity
        raise ValueError(candidate)

    async def get_messages(self, entity, ids):
        self.get_messages_entities.append(entity)
        return SimpleNamespace(id=ids, entity=entity)

    async def download_media(self, message, file):
        self.download_media_calls += 1
        if message.entity is self._stale_dialog.entity:
            raise ValueError(
                f"Could not find the input entity for PeerUser(user_id={self._stale_dialog.id}) (PeerUser).",
            )
        Path(file).write_bytes(b"payload")
        return file


class StubUnboundedMessageIterator:
    def __init__(self, messages: list[SimpleNamespace]) -> None:
        self._messages = messages
        self._index = 0

    def __aiter__(self) -> "StubUnboundedMessageIterator":
        return self

    async def __anext__(self) -> SimpleNamespace:
        if self._index >= len(self._messages):
            raise StopAsyncIteration
        message = self._messages[self._index]
        self._index += 1
        return message


@pytest.mark.asyncio
async def test_telethon_gateway_reconnects_once_after_disconnect_during_list_dialogs():
    client = FlakyDialogClient(
        [
            _telethon_dialog(101, "Alpha Project"),
        ],
    )
    gateway = TelethonTelegramGateway(
        api_id=None,
        api_hash=None,
        session_string=None,
        client=client,
    )

    dialogs = await gateway.list_available_dialogs(limit=1)

    assert [dialog.dialog_id for dialog in dialogs] == ["101"]
    assert client.connect_calls == 2
    assert client.disconnect_calls == 1


@pytest.mark.asyncio
async def test_telethon_gateway_fetch_history_sanitizes_binary_raw_payload() -> None:
    dialog = _telethon_dialog(101, "Alpha Project")
    message = SimpleNamespace(
        id=1,
        date=datetime(2026, 3, 25, 12, 0, tzinfo=UTC),
        message="hello",
        reply_to_msg_id=None,
        reply_to=None,
        reply_to_top_id=None,
        edit_date=None,
        sticker=None,
        photo=None,
        voice=None,
        video=None,
        audio=None,
        document=None,
        poll=None,
        action=None,
        file=None,
        entities=[],
        sender=None,
        sender_id=42,
        post_author=None,
        to_dict=lambda: {
            "_": "Message",
            "file_reference": b"\xc8\x01\x02",
            "nested": {"blob": bytearray(b"\xe6\x02")},
        },
    )
    client = StubHistoryClient([dialog], [message])
    gateway = TelethonTelegramGateway(
        api_id=None,
        api_hash=None,
        session_string=None,
        client=client,
    )

    batch = await gateway.fetch_history("101", cursor=None, limit=10)

    assert len(batch.messages) == 1
    assert batch.messages[0].body == "hello"
    assert batch.messages[0].raw_payload["file_reference"] == {
        "__type__": "bytes",
        "length": 3,
    }
    assert batch.messages[0].raw_payload["nested"]["blob"] == {
        "__type__": "bytes",
        "length": 2,
    }


@pytest.mark.asyncio
async def test_telethon_gateway_fetch_history_marks_video_note_messages_separately() -> None:
    dialog = _telethon_dialog(101, "Alpha Project")
    message = SimpleNamespace(
        id=2,
        date=datetime(2026, 3, 25, 12, 5, tzinfo=UTC),
        message=None,
        reply_to_msg_id=None,
        reply_to=None,
        reply_to_top_id=None,
        edit_date=None,
        sticker=None,
        photo=None,
        voice=None,
        video_note=SimpleNamespace(round=True),
        video=None,
        audio=None,
        document=None,
        poll=None,
        action=None,
        file=SimpleNamespace(
            id=987,
            name="round.mp4",
            mime_type="video/mp4",
            size=42,
            duration=8,
        ),
        entities=[],
        sender=None,
        sender_id=42,
        post_author=None,
        to_dict=lambda: {"_": "Message"},
    )
    client = StubHistoryClient([dialog], [message])
    gateway = TelethonTelegramGateway(
        api_id=None,
        api_hash=None,
        session_string=None,
        client=client,
    )

    batch = await gateway.fetch_history("101", cursor=None, limit=10)

    assert len(batch.messages) == 1
    assert batch.messages[0].content_type is ContentType.VIDEO_NOTE
    assert batch.messages[0].body is None
    assert batch.messages[0].attachments[0].media_kind == "video_note"
    assert batch.messages[0].attachments[0].duration_seconds == 8


@pytest.mark.asyncio
async def test_telethon_gateway_fetch_history_reads_topic_thread_history() -> None:
    dialog = _telethon_dialog(101, "Forum Chat")
    starter = SimpleNamespace(
        id=10,
        date=datetime(2026, 3, 25, 12, 0, tzinfo=UTC),
        message="Topic starter",
        reply_to_msg_id=None,
        reply_to=None,
        reply_to_top_id=None,
        edit_date=None,
        sticker=None,
        photo=None,
        voice=None,
        video=None,
        audio=None,
        document=None,
        poll=None,
        action=None,
        file=None,
        entities=[],
        sender=None,
        sender_id=42,
        post_author=None,
        to_dict=lambda: {"_": "Message"},
    )
    reply = SimpleNamespace(
        id=11,
        date=datetime(2026, 3, 25, 12, 1, tzinfo=UTC),
        message="Topic reply",
        reply_to_msg_id=10,
        reply_to=SimpleNamespace(reply_to_msg_id=10, reply_to_top_id=10),
        reply_to_top_id=None,
        edit_date=None,
        sticker=None,
        photo=None,
        voice=None,
        video=None,
        audio=None,
        document=None,
        poll=None,
        action=None,
        file=None,
        entities=[],
        sender=None,
        sender_id=42,
        post_author=None,
        to_dict=lambda: {"_": "Message"},
    )
    client = StubHistoryClient(
        [dialog],
        [starter],
        replies_by_thread_id={10: [reply]},
    )
    gateway = TelethonTelegramGateway(
        api_id=None,
        api_hash=None,
        session_string=None,
        client=client,
    )

    batch = await gateway.fetch_history(
        "101",
        cursor=None,
        limit=10,
        thread_id="10",
    )

    assert [message.message_id for message in batch.messages] == ["10", "11"]
    assert [message.thread_id for message in batch.messages] == ["10", "10"]


@pytest.mark.asyncio
async def test_telethon_gateway_extracts_forum_thread_id_from_reply_header() -> None:
    dialog = _telethon_dialog(101, "Forum Chat")
    message = SimpleNamespace(
        id=11,
        date=datetime(2026, 3, 25, 12, 1, tzinfo=UTC),
        message="Topic reply",
        reply_to_msg_id=10,
        reply_to=SimpleNamespace(
            reply_to_msg_id=10,
            reply_to_top_id=None,
            forum_topic=True,
        ),
        reply_to_top_id=None,
        edit_date=None,
        sticker=None,
        photo=None,
        voice=None,
        video=None,
        audio=None,
        document=None,
        poll=None,
        action=None,
        file=None,
        entities=[],
        sender=None,
        sender_id=42,
        post_author=None,
        to_dict=lambda: {"_": "Message"},
    )
    client = StubHistoryClient([dialog], [message])
    gateway = TelethonTelegramGateway(
        api_id=None,
        api_hash=None,
        session_string=None,
        client=client,
    )

    batch = await gateway.fetch_history("101", cursor=None, limit=10)

    assert [item.thread_id for item in batch.messages] == ["10"]


@pytest.mark.asyncio
async def test_telethon_gateway_resolves_private_entity_from_dialog_discovery_cache() -> None:
    dialog = _telethon_dialog(5843438842, "Private Dialog")
    client = DiscoveringEntityClient([dialog])
    gateway = TelethonTelegramGateway(
        api_id=None,
        api_hash=None,
        session_string=None,
        client=client,
    )

    entity = await gateway._resolve_entity("5843438842")

    assert entity is dialog.entity
    assert client.get_entity_calls == [5843438842]
    assert client.iter_dialogs_limits == [None]


@pytest.mark.asyncio
async def test_telethon_gateway_download_attachment_refreshes_stale_private_entity(
    tmp_path: Path,
) -> None:
    stale_dialog = _telethon_dialog(5843438842, "Private Dialog")
    fresh_dialog = _telethon_dialog(5843438842, "Private Dialog")
    client = AttachmentRefreshClient(
        stale_dialog=stale_dialog,
        fresh_dialog=fresh_dialog,
    )
    gateway = TelethonTelegramGateway(
        api_id=None,
        api_hash=None,
        session_string=None,
        client=client,
        default_attachment_dir=str(tmp_path),
    )

    downloaded = await gateway.download_attachment(
        CanonicalAttachment(
            source_file_id="file-1",
            filename="document.txt",
            mime_type="text/plain",
            size_bytes=7,
            media_kind="document",
            download_url="telethon://5843438842/1",
        ),
    )

    assert downloaded.local_path is not None
    assert Path(downloaded.local_path).exists()
    assert Path(downloaded.local_path).read_bytes() == b"payload"
    assert client.download_media_calls == 2
    assert client.get_messages_entities == [stale_dialog.entity, fresh_dialog.entity]
    assert client.iter_dialogs_limits == [None]


@pytest.mark.asyncio
async def test_telethon_gateway_list_dialogs_estimates_manifest_inventory() -> None:
    dialog = _telethon_dialog(101, "Alpha Project")
    messages = [
        SimpleNamespace(
            id=1,
            message="hello",
            reply_to_top_id=None,
            file=None,
        ),
        SimpleNamespace(
            id=2,
            message="photo",
            reply_to_top_id=None,
            file=SimpleNamespace(size=42),
        ),
    ]
    client = StubInventoryClient([dialog], {"101": messages})
    gateway = TelethonTelegramGateway(
        api_id=None,
        api_hash=None,
        session_string=None,
        client=client,
    )
    manifest = MigrationManifest.model_validate(
        {
            "migration_id": "migration-1",
            "mode": "backfill_delta_cutover",
            "dialogs": [
                {
                    "source_chat_id": "101",
                    "source_chat_type": "supergroup",
                    "target_strategy": "create",
                    "target_title": "Imported Chat",
                },
            ],
        },
    )

    dialogs = await gateway.list_dialogs(manifest)

    assert len(dialogs) == 1
    assert dialogs[0].dialog_id == "101"
    assert dialogs[0].title == "Alpha Project"
    assert dialogs[0].message_count == 2
    assert dialogs[0].media_count == 1
    assert dialogs[0].approximate_bytes == len("hello".encode("utf-8")) + len("photo".encode("utf-8")) + 42


@pytest.mark.asyncio
async def test_telethon_gateway_list_dialogs_estimates_topic_inventory_from_thread_history() -> None:
    dialog = _telethon_dialog(101, "Forum Chat")
    starter = SimpleNamespace(
        id=10,
        message="Topic starter",
        reply_to=None,
        reply_to_top_id=None,
        file=None,
    )
    reply = SimpleNamespace(
        id=11,
        message="Topic reply",
        reply_to=SimpleNamespace(reply_to_msg_id=10, reply_to_top_id=10),
        reply_to_top_id=None,
        file=SimpleNamespace(size=7),
    )
    general = SimpleNamespace(
        id=99,
        message="General message",
        reply_to=None,
        reply_to_top_id=None,
        file=None,
    )
    client = StubInventoryClient(
        [dialog],
        {"101": [starter, general]},
        replies_by_entity_and_thread_id={("101", 10): [reply]},
    )
    gateway = TelethonTelegramGateway(
        api_id=None,
        api_hash=None,
        session_string=None,
        client=client,
    )
    manifest = MigrationManifest.model_validate(
        {
            "migration_id": "migration-1",
            "mode": "backfill_delta_cutover",
            "dialogs": [
                {
                    "source_chat_id": "101#topic:201",
                    "source_chat_type": "supergroup",
                    "target_strategy": "create",
                    "target_title": "Forum Chat / Topic",
                    "telegram_chat_id": "101",
                    "source_topic_id": "201",
                    "source_thread_id": "10",
                    "source_thread_title": "Topic",
                },
            ],
        },
    )

    dialogs = await gateway.list_dialogs(manifest)

    assert len(dialogs) == 1
    assert dialogs[0].dialog_id == "101#topic:201"
    assert dialogs[0].title == "Topic"
    assert dialogs[0].message_count == 2
    assert dialogs[0].media_count == 1
    assert dialogs[0].approximate_bytes == (
        len("Topic starter".encode("utf-8"))
        + len("Topic reply".encode("utf-8"))
        + 7
    )
