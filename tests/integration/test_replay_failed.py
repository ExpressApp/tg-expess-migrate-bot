from dataclasses import replace
from datetime import UTC, datetime, timedelta

import pytest

from extg_migration_runtime.application.message_delivery import MessageDeliveryService
from extg_migration_runtime.application.identity import UsernameEmailIdentityDirectory
from extg_migration_runtime.application.normalizer import (
    DirectoryBackedIdentityResolver,
    TelegramMessageNormalizer,
)
from extg_migration_runtime.application.renderer import MessageRenderer
from extg_migration_runtime.application.use_cases.replay_failed import (
    ReplayFailedCommand,
    ReplayFailedUseCase,
)
from extg_shared.contracts.manifest import MigrationManifest
from extg_shared.contracts.models import (
    AttachmentImportStatus,
    AttachmentMappingRecord,
    CanonicalAttachment,
    ChatMappingRecord,
    ContentType,
    MessageImportStatus,
    MessageMappingRecord,
    SourceDialog,
    TelegramAuthor,
    TelegramSourceMessage,
)
from extg_migration_runtime.infrastructure.express.fake_gateway import (
    FakeExpressFileStore,
    FakeExpressGateway,
)
from extg_migration_runtime.infrastructure.persistence.in_memory import (
    InMemoryAttachmentMappingRepository,
    InMemoryAttachmentStageRepository,
    InMemoryChatMappingRepository,
    InMemoryIdentityMappingRepository,
    InMemoryMessageMappingRepository,
)
from extg_shared.utils.retry import AsyncRetryPolicy
from extg_telethon_service.infrastructure.persistence.in_memory import (
    InMemoryAuditRepository,
)
from extg_telethon_service.infrastructure.telegram.fake_gateway import (
    FakeTelegramGateway,
)


def build_manifest() -> MigrationManifest:
    return MigrationManifest.model_validate(
        {
            "migration_id": "migration-1",
            "mode": "backfill_delta_cutover",
            "dialogs": [
                {
                    "source_chat_id": "chat-1",
                    "source_chat_type": "supergroup",
                    "target_strategy": "bind",
                    "target_chat_id": "express-chat-1",
                },
            ],
        },
    )


def build_message(message_id: str, minute_offset: int, **overrides) -> TelegramSourceMessage:
    sent_at = datetime(2026, 3, 17, 10, 0, tzinfo=UTC) + timedelta(minutes=minute_offset)
    payload = TelegramSourceMessage(
        chat_id="chat-1",
        message_id=message_id,
        sent_at_utc=sent_at,
        author=TelegramAuthor(
            external_id="tg-user-1",
            display_name="Иван Петров",
            username="ivan.petrov",
        ),
        body=f"Сообщение {message_id}",
        chat_title="Telegram Project Chat",
        content_type=ContentType.TEXT,
    )
    if not overrides:
        return payload
    return replace(payload, **overrides)


def build_failed_mapping(*, message_id: str, minute_offset: int) -> MessageMappingRecord:
    sent_at = datetime(2026, 3, 17, 10, 0, tzinfo=UTC) + timedelta(minutes=minute_offset)
    return MessageMappingRecord(
        migration_id="migration-1",
        source_chat_id="chat-1",
        source_message_id=message_id,
        source_sent_at=sent_at,
        target_chat_id="express-chat-1",
        checksum="checksum",
        import_status=MessageImportStatus.FAILED,
        last_error_code="RecoverableItemError",
        last_error_payload={"message": "planned failure"},
    )


def build_use_case(
    *,
    telegram_gateway,
    express_gateway,
    chat_mapping_repository=None,
    attachment_mapping_repository,
    message_mapping_repository,
    audit_repository,
    identity_repo=None,
    attachment_stage_repository=None,
    express_file_store=None,
) -> ReplayFailedUseCase:
    retry_policy = AsyncRetryPolicy(
        max_attempts=2,
        base_delay_seconds=0.0,
        max_delay_seconds=0.0,
        jitter_seconds=0.0,
    )
    identity_directory = UsernameEmailIdentityDirectory(
        identity_mapping_repository=identity_repo or InMemoryIdentityMappingRepository(),
        express_gateway=express_gateway,
    )
    chat_mapping_repository = chat_mapping_repository or InMemoryChatMappingRepository()
    attachment_stage_repository = (
        attachment_stage_repository or InMemoryAttachmentStageRepository()
    )
    express_file_store = express_file_store or FakeExpressFileStore()
    return ReplayFailedUseCase(
        telegram_gateway=telegram_gateway,
        chat_mapping_repository=chat_mapping_repository,
        message_mapping_repository=message_mapping_repository,
        audit_repository=audit_repository,
        normalizer=TelegramMessageNormalizer(
            DirectoryBackedIdentityResolver(identity_directory),
            identity_directory=identity_directory,
        ),
        message_delivery_service=MessageDeliveryService(
            telegram_gateway=telegram_gateway,
            express_gateway=express_gateway,
            express_file_store=express_file_store,
            attachment_mapping_repository=attachment_mapping_repository,
            attachment_stage_repository=attachment_stage_repository,
            audit_repository=audit_repository,
            renderer=MessageRenderer(timezone_name="Europe/Moscow"),
            retry_policy=retry_policy,
        ),
        retry_policy=retry_policy,
    )


@pytest.mark.asyncio
async def test_replay_failed_recovers_failed_message():
    manifest = build_manifest()
    dialog = SourceDialog(dialog_id="chat-1", chat_type="supergroup", title="Telegram Project Chat")
    messages = [
        build_message("1", 0),
        build_message("2", 1),
        build_message("3", 2),
    ]
    telegram_gateway = FakeTelegramGateway(
        dialogs=[dialog],
        messages_by_dialog={"chat-1": messages},
    )
    express_gateway = FakeExpressGateway()
    chat_repo = InMemoryChatMappingRepository()
    message_repo = InMemoryMessageMappingRepository()
    attachment_repo = InMemoryAttachmentMappingRepository()
    audit_repo = InMemoryAuditRepository()
    await chat_repo.save(
        ChatMappingRecord(
            migration_id="migration-1",
            source_chat_id="chat-1",
            source_chat_type="supergroup",
            source_chat_title="Telegram Project Chat",
            target_chat_id="express-chat-1",
            target_chat_title="Imported Chat",
            status="active",
            created_at=datetime(2026, 3, 17, 10, 0, tzinfo=UTC),
            updated_at=datetime(2026, 3, 17, 10, 0, tzinfo=UTC),
        ),
    )

    await message_repo.claim(build_failed_mapping(message_id="2", minute_offset=1))
    await message_repo.mark_failed(
        "migration-1",
        "chat-1",
        "2",
        error_code="RecoverableItemError",
        error_payload={"message": "planned failure"},
    )

    use_case = build_use_case(
        telegram_gateway=telegram_gateway,
        express_gateway=express_gateway,
        chat_mapping_repository=chat_repo,
        attachment_mapping_repository=attachment_repo,
        message_mapping_repository=message_repo,
        audit_repository=audit_repo,
    )
    result = await use_case.execute(
        ReplayFailedCommand(
            manifest=manifest,
            source_chat_id="chat-1",
            limit=10,
            batch_size=10,
        ),
    )

    mapping = await message_repo.get("migration-1", "chat-1", "2")
    chat_mapping = await chat_repo.get("migration-1", "chat-1")
    audit_events = await audit_repo.list_all()
    sent_messages = express_gateway.messages_for_chat("express-chat-1")

    assert result.requested_count == 1
    assert result.recovered_count == 1
    assert result.failed_count == 0
    assert result.missing_count == 0
    assert mapping is not None
    assert mapping.import_status is MessageImportStatus.IMPORTED
    assert chat_mapping is not None
    assert chat_mapping.status == "completed"
    assert len(sent_messages) == 1
    assert sent_messages[0].body.splitlines()[-1] == "Сообщение 2"
    assert any(event.event_type == "failed_message_replay_imported" for event in audit_events)


@pytest.mark.asyncio
async def test_replay_failed_marks_missing_source_message():
    manifest = build_manifest()
    dialog = SourceDialog(dialog_id="chat-1", chat_type="supergroup", title="Telegram Project Chat")
    telegram_gateway = FakeTelegramGateway(
        dialogs=[dialog],
        messages_by_dialog={"chat-1": [build_message("1", 0)]},
    )
    express_gateway = FakeExpressGateway()
    express_file_store = FakeExpressFileStore()
    message_repo = InMemoryMessageMappingRepository()
    attachment_repo = InMemoryAttachmentMappingRepository()
    audit_repo = InMemoryAuditRepository()

    await message_repo.claim(build_failed_mapping(message_id="99", minute_offset=99))
    await message_repo.mark_failed(
        "migration-1",
        "chat-1",
        "99",
        error_code="RecoverableItemError",
        error_payload={"message": "planned failure"},
    )

    use_case = build_use_case(
        telegram_gateway=telegram_gateway,
        express_gateway=express_gateway,
        attachment_mapping_repository=attachment_repo,
        message_mapping_repository=message_repo,
        audit_repository=audit_repo,
        express_file_store=express_file_store,
    )
    result = await use_case.execute(
        ReplayFailedCommand(
            manifest=manifest,
            source_chat_id="chat-1",
            limit=10,
            batch_size=10,
        ),
    )

    mapping = await message_repo.get("migration-1", "chat-1", "99")
    audit_events = await audit_repo.list_all()

    assert result.requested_count == 1
    assert result.recovered_count == 0
    assert result.missing_count == 1
    assert mapping is not None
    assert mapping.import_status is MessageImportStatus.FAILED
    assert any(event.event_type == "failed_message_replay_missing" for event in audit_events)


@pytest.mark.asyncio
async def test_replay_failed_recovers_message_with_attachment():
    manifest = build_manifest()
    dialog = SourceDialog(dialog_id="chat-1", chat_type="supergroup", title="Telegram Project Chat")
    attachment = CanonicalAttachment(
        source_file_id="file-1",
        filename="voice.ogg",
        mime_type="audio/ogg",
        size_bytes=9,
        media_kind="voice",
        download_url="fake://attachment/voice",
    )
    telegram_gateway = FakeTelegramGateway(
        dialogs=[dialog],
        messages_by_dialog={
            "chat-1": [
                build_message("1", 0),
                build_message(
                    "2",
                    1,
                    attachments=[attachment],
                    body=None,
                    content_type=ContentType.VOICE,
                ),
            ],
        },
        downloaded_attachment_content_by_url={"fake://attachment/voice": b"voice-data"},
    )
    express_gateway = FakeExpressGateway()
    express_file_store = FakeExpressFileStore()
    message_repo = InMemoryMessageMappingRepository()
    attachment_repo = InMemoryAttachmentMappingRepository()
    audit_repo = InMemoryAuditRepository()

    await message_repo.claim(build_failed_mapping(message_id="2", minute_offset=1))
    await message_repo.mark_failed(
        "migration-1",
        "chat-1",
        "2",
        error_code="RecoverableItemError",
        error_payload={"message": "planned failure"},
    )

    use_case = build_use_case(
        telegram_gateway=telegram_gateway,
        express_gateway=express_gateway,
        attachment_mapping_repository=attachment_repo,
        message_mapping_repository=message_repo,
        audit_repository=audit_repo,
        express_file_store=express_file_store,
    )

    result = await use_case.execute(
        ReplayFailedCommand(
            manifest=manifest,
            source_chat_id="chat-1",
            limit=10,
            batch_size=10,
        ),
    )

    sent_messages = express_gateway.messages_for_chat("express-chat-1")

    assert result.recovered_count == 1
    assert len(sent_messages) == 1
    assert sent_messages[0].file_filename == "voice.ogg"
    assert sent_messages[0].file_id == "file-1"
    assert sent_messages[0].body.splitlines()[-1] == "[voice]"
    assert len(express_file_store.uploads) == 1


@pytest.mark.asyncio
async def test_replay_failed_preserves_order_for_multiple_failed_attachment_messages():
    manifest = build_manifest()
    dialog = SourceDialog(dialog_id="chat-1", chat_type="supergroup", title="Telegram Project Chat")
    first = CanonicalAttachment(
        source_file_id="file-1",
        filename="first.ogg",
        mime_type="audio/ogg",
        size_bytes=9,
        media_kind="voice",
        download_url="fake://attachment/first",
    )
    second = CanonicalAttachment(
        source_file_id="file-2",
        filename="second.ogg",
        mime_type="audio/ogg",
        size_bytes=10,
        media_kind="voice",
        download_url="fake://attachment/second",
    )
    telegram_gateway = FakeTelegramGateway(
        dialogs=[dialog],
        messages_by_dialog={
            "chat-1": [
                build_message("2", 1, attachments=[first], body=None, content_type=ContentType.VOICE),
                build_message("3", 2, attachments=[second], body=None, content_type=ContentType.VOICE),
            ],
        },
        downloaded_attachment_content_by_url={
            "fake://attachment/first": b"voice-one",
            "fake://attachment/second": b"voice-two",
        },
    )
    express_gateway = FakeExpressGateway()
    express_file_store = FakeExpressFileStore()
    message_repo = InMemoryMessageMappingRepository()
    attachment_repo = InMemoryAttachmentMappingRepository()
    audit_repo = InMemoryAuditRepository()

    for message_id, minute_offset in (("2", 1), ("3", 2)):
        await message_repo.claim(build_failed_mapping(message_id=message_id, minute_offset=minute_offset))
        await message_repo.mark_failed(
            "migration-1",
            "chat-1",
            message_id,
            error_code="RecoverableItemError",
            error_payload={"message": "planned failure"},
        )

    use_case = build_use_case(
        telegram_gateway=telegram_gateway,
        express_gateway=express_gateway,
        attachment_mapping_repository=attachment_repo,
        message_mapping_repository=message_repo,
        audit_repository=audit_repo,
        express_file_store=express_file_store,
    )

    result = await use_case.execute(
        ReplayFailedCommand(
            manifest=manifest,
            source_chat_id="chat-1",
            limit=10,
            batch_size=10,
        ),
    )

    sent_messages = express_gateway.messages_for_chat("express-chat-1")

    assert result.recovered_count == 2
    assert telegram_gateway.download_calls == 2
    assert [message.file_filename for message in sent_messages] == ["first.ogg", "second.ogg"]


@pytest.mark.asyncio
async def test_replay_failed_reuses_primary_message_when_only_attachment_followup_failed():
    manifest = build_manifest()
    dialog = SourceDialog(dialog_id="chat-1", chat_type="supergroup", title="Telegram Project Chat")
    attachment = CanonicalAttachment(
        source_file_id="file-1",
        filename="deck.pdf",
        mime_type="application/pdf",
        size_bytes=9,
        media_kind="document",
        download_url="fake://attachment/deck",
    )
    telegram_gateway = FakeTelegramGateway(
        dialogs=[dialog],
        messages_by_dialog={
            "chat-1": [
                build_message(
                    "7",
                    7,
                    attachments=[attachment],
                ),
            ],
        },
        downloaded_attachment_content_by_url={"fake://attachment/deck": b"pdf-bytes"},
    )
    express_gateway = FakeExpressGateway()
    message_repo = InMemoryMessageMappingRepository()
    attachment_repo = InMemoryAttachmentMappingRepository()
    audit_repo = InMemoryAuditRepository()

    failed_mapping = build_failed_mapping(message_id="7", minute_offset=7)
    await message_repo.claim(failed_mapping)
    await message_repo.mark_imported(
        "migration-1",
        "chat-1",
        "7",
        target_sync_id="sync-primary-existing",
        rendered_body="[2026-03-17 13:07:00 UTC+03:00] Иван Петров (@ivan.petrov) | Source: Telegram Project Chat\nСообщение 7",
    )
    await message_repo.mark_failed(
        "migration-1",
        "chat-1",
        "7",
        error_code="AttachmentDeliveryIncomplete",
        error_payload={"reason": "one or more attachments require replay"},
    )
    await attachment_repo.claim(
        AttachmentMappingRecord(
            migration_id="migration-1",
            source_chat_id="chat-1",
            source_message_id="7",
            attachment_index=0,
            source_file_id=attachment.source_file_id,
            source_filename=attachment.filename,
            media_kind=attachment.media_kind,
            checksum=attachment.sha256,
            size_bytes=attachment.size_bytes,
            target_chat_id="express-chat-1",
            import_status=AttachmentImportStatus.PROCESSING,
        ),
    )
    await attachment_repo.mark_failed(
        "migration-1",
        "chat-1",
        "7",
        0,
        attachment=attachment,
        error_code="RecoverableItemError",
        error_payload={
            "message": "planned temporary eXpress failure",
            "phase": "send",
        },
    )

    use_case = build_use_case(
        telegram_gateway=telegram_gateway,
        express_gateway=express_gateway,
        attachment_mapping_repository=attachment_repo,
        message_mapping_repository=message_repo,
        audit_repository=audit_repo,
    )

    result = await use_case.execute(
        ReplayFailedCommand(
            manifest=manifest,
            source_chat_id="chat-1",
            limit=10,
            batch_size=10,
        ),
    )

    mapping = await message_repo.get("migration-1", "chat-1", "7")
    sent_messages = express_gateway.messages_for_chat("express-chat-1")

    assert result.recovered_count == 1
    assert mapping is not None
    assert mapping.import_status is MessageImportStatus.IMPORTED
    assert mapping.target_sync_id == "sync-primary-existing"
    assert len(sent_messages) == 1
    assert sent_messages[0].file_filename == "deck.pdf"
    assert sent_messages[0].body == "[document] deck.pdf"
