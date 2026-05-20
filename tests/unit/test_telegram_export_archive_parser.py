import io
import json
import zipfile
from datetime import UTC, datetime

import pytest

from extg_migration_runtime.infrastructure.telegram.multi_source_gateway import (
    TelegramExportSnapshotGateway,
)
from extg_migration_runtime.infrastructure.archive.telegram_export_archive_parser import (
    TelegramExportArchiveParser,
)
from extg_migration_runtime.infrastructure.archive.stage_store import (
    InMemoryTelegramExportArchiveStageStore,
)
from extg_migration_runtime.infrastructure.persistence.in_memory import (
    InMemoryTelegramExportSnapshotRepository,
)
from extg_shared.contracts.models import ContentType


def _build_export_payload() -> dict:
    return {
        "id": 5186712067,
        "name": "Archive Chat",
        "type": "private_group",
        "messages": [
            {
                "id": 1,
                "type": "message",
                "date": "2026-03-17T08:00:00",
                "date_unixtime": "1773734400",
                "from": "Alice",
                "from_id": "user1",
                "text": "hello",
            },
            {
                "id": 2,
                "type": "message",
                "date": "2026-03-17T08:05:00",
                "date_unixtime": "1773734700",
                "from": "Bob",
                "from_id": "user2",
                "media_type": "photo",
                "photo": "photos/file_1.jpg",
                "text": "",
            },
            {
                "id": 3,
                "type": "service",
                "date": "2026-03-17T08:10:00",
                "date_unixtime": "1773735000",
                "actor": "System",
                "text": "Alice joined the chat",
            },
        ],
    }


@pytest.mark.asyncio
async def test_parse_upload_supports_json_and_loads_messages() -> None:
    parser = TelegramExportArchiveParser()
    stage_store = InMemoryTelegramExportArchiveStageStore()
    snapshot_repository = InMemoryTelegramExportSnapshotRepository()
    payload = _build_export_payload()
    raw_bytes = json.dumps(payload).encode("utf-8")

    parsed = await parser.parse_upload(
        filename="result.json",
        content=raw_bytes,
    )
    archive_locator = await stage_store.stage_upload(
        filename="result.json",
        content=raw_bytes,
    )
    snapshot = parsed.to_snapshot_record(
        archive_locator=archive_locator,
        uploaded_by_huid="operator-1",
        created_at=datetime(2026, 3, 27, 12, 0, tzinfo=UTC),
    )
    await snapshot_repository.save(snapshot)
    messages = await parser.load_messages(
        parsed.source_chat_id,
        snapshot_repository,
        stage_store,
    )

    assert parsed.source_chat_id == "archive:5186712067"
    assert parsed.source_chat_type == "group"
    assert parsed.message_count == 3
    assert parsed.media_count == 1
    assert len(messages) == 3
    assert messages[0].body == "hello"
    assert messages[1].body == "[photo omitted from Telegram export]"
    assert messages[1].content_type.value == "text"
    assert messages[2].content_type.value == "service"


@pytest.mark.asyncio
async def test_parse_upload_extracts_result_json_from_zip() -> None:
    parser = TelegramExportArchiveParser()
    payload = _build_export_payload()
    archive_buffer = io.BytesIO()
    with zipfile.ZipFile(archive_buffer, mode="w") as archive:
        archive.writestr("ChatExport_2026-03-17/result.json", json.dumps(payload))

    parsed = await parser.parse_upload(
        filename="ChatExport_2026-03-17.zip",
        content=archive_buffer.getvalue(),
    )

    assert parsed.source_chat_id == "archive:5186712067"
    assert parsed.source_chat_title == "Archive Chat"
    assert parsed.message_count == 3


@pytest.mark.asyncio
async def test_load_messages_marks_zip_video_message_as_video_note_with_attachment() -> None:
    parser = TelegramExportArchiveParser()
    stage_store = InMemoryTelegramExportArchiveStageStore()
    snapshot_repository = InMemoryTelegramExportSnapshotRepository()
    payload = _build_export_payload()
    payload["messages"].append(
        {
            "id": 4,
            "type": "message",
            "date": "2026-03-17T08:15:00",
            "date_unixtime": "1773735300",
            "from": "Charlie",
            "from_id": "user3",
            "media_type": "video message",
            "file": "files/video_note_1.mp4",
            "mime_type": "video/mp4",
            "duration_seconds": 12,
            "text": "",
        },
    )
    archive_buffer = io.BytesIO()
    with zipfile.ZipFile(archive_buffer, mode="w") as archive:
        archive.writestr("ChatExport_2026-03-17/result.json", json.dumps(payload))
        archive.writestr("ChatExport_2026-03-17/files/video_note_1.mp4", b"video-note-payload")
    archive_content = archive_buffer.getvalue()

    parsed = await parser.parse_upload(
        filename="ChatExport_2026-03-17.zip",
        content=archive_content,
    )
    archive_locator = await stage_store.stage_upload(
        filename="ChatExport_2026-03-17.zip",
        content=archive_content,
    )
    snapshot = parsed.to_snapshot_record(
        archive_locator=archive_locator,
        uploaded_by_huid="operator-1",
        created_at=datetime(2026, 3, 27, 12, 0, tzinfo=UTC),
    )
    await snapshot_repository.save(snapshot)

    messages = await parser.load_messages(
        parsed.source_chat_id,
        snapshot_repository,
        stage_store,
    )

    message = messages[-1]
    assert message.content_type is ContentType.VIDEO_NOTE
    assert message.body is None
    assert len(message.attachments) == 1
    attachment = message.attachments[0]
    assert attachment.media_kind == "video_note"
    assert attachment.filename == "video_note_1.mp4"
    assert attachment.mime_type == "video/mp4"
    assert attachment.duration_seconds == 12
    assert attachment.download_url == (
        "tg-export://attachment"
        "?chat_id=archive%3A5186712067&path=files%2Fvideo_note_1.mp4"
    )


@pytest.mark.asyncio
async def test_load_messages_marks_zip_photo_as_photo_attachment() -> None:
    parser = TelegramExportArchiveParser()
    stage_store = InMemoryTelegramExportArchiveStageStore()
    snapshot_repository = InMemoryTelegramExportSnapshotRepository()
    payload = _build_export_payload()
    archive_buffer = io.BytesIO()
    with zipfile.ZipFile(archive_buffer, mode="w") as archive:
        archive.writestr("ChatExport_2026-03-17/result.json", json.dumps(payload))
        archive.writestr("ChatExport_2026-03-17/photos/file_1.jpg", b"photo-payload")
    archive_content = archive_buffer.getvalue()

    parsed = await parser.parse_upload(
        filename="ChatExport_2026-03-17.zip",
        content=archive_content,
    )
    archive_locator = await stage_store.stage_upload(
        filename="ChatExport_2026-03-17.zip",
        content=archive_content,
    )
    snapshot = parsed.to_snapshot_record(
        archive_locator=archive_locator,
        uploaded_by_huid="operator-1",
        created_at=datetime(2026, 3, 27, 12, 0, tzinfo=UTC),
    )
    await snapshot_repository.save(snapshot)

    messages = await parser.load_messages(
        parsed.source_chat_id,
        snapshot_repository,
        stage_store,
    )

    message = messages[1]
    assert message.content_type is ContentType.PHOTO
    assert message.body is None
    assert len(message.attachments) == 1
    attachment = message.attachments[0]
    assert attachment.media_kind == "photo"
    assert attachment.filename == "file_1.jpg"
    assert attachment.download_url == (
        "tg-export://attachment"
        "?chat_id=archive%3A5186712067&path=photos%2Ffile_1.jpg"
    )


@pytest.mark.asyncio
async def test_archive_gateway_download_attachment_reads_member_from_staged_zip() -> None:
    parser = TelegramExportArchiveParser()
    stage_store = InMemoryTelegramExportArchiveStageStore()
    snapshot_repository = InMemoryTelegramExportSnapshotRepository()
    payload = _build_export_payload()
    payload["messages"].append(
        {
            "id": 4,
            "type": "message",
            "date": "2026-03-17T08:15:00",
            "date_unixtime": "1773735300",
            "from": "Charlie",
            "from_id": "user3",
            "media_type": "video message",
            "file": "files/video_note_1.mp4",
            "mime_type": "video/mp4",
            "text": "",
        },
    )
    archive_buffer = io.BytesIO()
    with zipfile.ZipFile(archive_buffer, mode="w") as archive:
        archive.writestr("ChatExport_2026-03-17/result.json", json.dumps(payload))
        archive.writestr("ChatExport_2026-03-17/files/video_note_1.mp4", b"video-note-payload")
    archive_content = archive_buffer.getvalue()

    parsed = await parser.parse_upload(
        filename="ChatExport_2026-03-17.zip",
        content=archive_content,
    )
    archive_locator = await stage_store.stage_upload(
        filename="ChatExport_2026-03-17.zip",
        content=archive_content,
    )
    snapshot = parsed.to_snapshot_record(
        archive_locator=archive_locator,
        uploaded_by_huid="operator-1",
        created_at=datetime(2026, 3, 27, 12, 0, tzinfo=UTC),
    )
    await snapshot_repository.save(snapshot)

    gateway = TelegramExportSnapshotGateway(
        snapshot_repository=snapshot_repository,
        parser=parser,
        stage_store=stage_store,
    )

    batch = await gateway.fetch_history(parsed.source_chat_id, None, 10)
    downloaded = await gateway.download_attachment(batch.messages[-1].attachments[0])

    assert downloaded.content == b"video-note-payload"
    assert downloaded.size_bytes == len(b"video-note-payload")
