import asyncio
from datetime import UTC, datetime, timedelta

import pytest

from extg_migration_runtime.application.identity import UsernameEmailIdentityDirectory
from extg_migration_runtime.application.message_checksum import MessageChecksumService
from extg_migration_runtime.application.message_delivery import MessageDeliveryService
from extg_migration_runtime.application.normalizer import (
    DirectoryBackedIdentityResolver,
    TelegramMessageNormalizer,
)
from extg_migration_runtime.application.renderer import MessageRenderer
from extg_migration_runtime.application.target_chat_provisioning import (
    TargetChatProvisioningService,
)
from extg_migration_runtime.application.use_cases.backfill_chat import (
    BackfillChatCommand,
    BackfillChatUseCase,
)
from extg_shared.contracts.errors import FatalItemError, RecoverableItemError
from extg_shared.contracts.manifest import MigrationManifest
from extg_shared.contracts.models import (
    AttachmentImportStatus,
    CanonicalAttachment,
    CanonicalEntity,
    ContentType,
    FilePayload,
    IdentityMappingRecord,
    MIGRATION_JOB_CANCELLED_ERROR_CODE,
    MessageImportStatus,
    SourceDialog,
    SourceParticipant,
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
    InMemoryCheckpointRepository,
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


class RecordingLogger:
    def __init__(self) -> None:
        self.records: list[tuple[str, str, dict[str, object]]] = []

    def info(self, event: str, **payload: object) -> None:
        self.records.append(("info", event, payload))

    def warning(self, event: str, **payload: object) -> None:
        self.records.append(("warning", event, payload))

    def error(self, event: str, **payload: object) -> None:
        self.records.append(("error", event, payload))


class PreviewFailureExpressFileStore(FakeExpressFileStore):
    async def upload_file(
        self,
        target_chat_id: str,
        *,
        file: FilePayload,
    ):
        raise FatalItemError("eXpress upload_file failed: get preview failed")


class CancelOnceMessageDeliveryService(MessageDeliveryService):
    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self._cancelled_once = False

    async def deliver(self, *args, **kwargs):  # type: ignore[override]
        if not self._cancelled_once:
            self._cancelled_once = True
            raise asyncio.CancelledError
        return await super().deliver(*args, **kwargs)


def build_manifest() -> MigrationManifest:
    return MigrationManifest.model_validate(
        {
            "migration_id": "migration-1",
            "mode": "backfill_delta_cutover",
            "dialogs": [
                {
                    "source_chat_id": "chat-1",
                    "source_chat_type": "supergroup",
                    "target_strategy": "create",
                    "target_title": "Imported Chat",
                },
            ],
        },
    )


def build_split_topic_manifest() -> MigrationManifest:
    return MigrationManifest.model_validate(
        {
            "migration_id": "migration-1",
            "mode": "backfill_delta_cutover",
            "dialogs": [
                {
                    "source_chat_id": "chat-1#topic:101",
                    "source_chat_type": "supergroup",
                    "target_strategy": "create",
                    "target_title": "Imported Chat / Topic 1",
                    "topic_strategy": "split_by_topic",
                    "telegram_chat_id": "chat-1",
                    "source_topic_id": "101",
                    "source_thread_id": "10",
                    "source_thread_title": "Topic 1",
                },
                {
                    "source_chat_id": "chat-1#topic:102",
                    "source_chat_type": "supergroup",
                    "target_strategy": "create",
                    "target_title": "Imported Chat / Topic 2",
                    "topic_strategy": "split_by_topic",
                    "telegram_chat_id": "chat-1",
                    "source_topic_id": "102",
                    "source_thread_id": "20",
                    "source_thread_title": "Topic 2",
                },
            ],
        },
    )


def build_message(message_id: str, minute_offset: int, **overrides) -> TelegramSourceMessage:
    sent_at = datetime(2026, 3, 17, 10, 0, tzinfo=UTC) + timedelta(minutes=minute_offset)
    payload = {
        "chat_id": "chat-1",
        "message_id": message_id,
        "chat_title": "Telegram Project Chat",
        "sent_at_utc": sent_at,
        "author": TelegramAuthor(
            external_id="tg-user-1",
            display_name="Иван Петров",
            username="ivan.petrov",
        ),
        "body": f"Сообщение {message_id}",
        "content_type": ContentType.TEXT,
    }
    payload.update(overrides)
    return TelegramSourceMessage(**payload)


def build_use_case(
    telegram_gateway,
    express_gateway,
    repositories,
    *,
    max_attempts: int = 3,
    identity_repo: InMemoryIdentityMappingRepository | None = None,
    attachment_stage_repo: InMemoryAttachmentStageRepository | None = None,
    express_file_store: FakeExpressFileStore | None = None,
    logger: RecordingLogger | None = None,
    max_attachment_upload_size_bytes: int = 100 * 1024 * 1024,
):
    chat_repo, message_repo, attachment_repo, checkpoint_repo, audit_repo = repositories
    identity_repo = identity_repo or InMemoryIdentityMappingRepository()
    attachment_stage_repo = attachment_stage_repo or InMemoryAttachmentStageRepository()
    express_file_store = express_file_store or FakeExpressFileStore()
    retry_policy = AsyncRetryPolicy(
        max_attempts=max_attempts,
        base_delay_seconds=0.0,
        max_delay_seconds=0.0,
        jitter_seconds=0.0,
    )
    identity_directory = UsernameEmailIdentityDirectory(
        identity_mapping_repository=identity_repo,
        express_gateway=express_gateway,
    )
    return BackfillChatUseCase(
        telegram_gateway=telegram_gateway,
        express_gateway=express_gateway,
        chat_mapping_repository=chat_repo,
        message_mapping_repository=message_repo,
        checkpoint_repository=checkpoint_repo,
        audit_repository=audit_repo,
        normalizer=TelegramMessageNormalizer(
            DirectoryBackedIdentityResolver(identity_directory),
            identity_directory=identity_directory,
        ),
        message_delivery_service=MessageDeliveryService(
            telegram_gateway=telegram_gateway,
            express_gateway=express_gateway,
            express_file_store=express_file_store,
            attachment_mapping_repository=attachment_repo,
            attachment_stage_repository=attachment_stage_repo,
            audit_repository=audit_repo,
            renderer=MessageRenderer(timezone_name="Europe/Moscow"),
            retry_policy=retry_policy,
            logger=logger,
            max_attachment_upload_size_bytes=max_attachment_upload_size_bytes,
        ),
        target_chat_provisioning_service=TargetChatProvisioningService(
            telegram_gateway=telegram_gateway,
            express_gateway=express_gateway,
            chat_mapping_repository=chat_repo,
            identity_directory=identity_directory,
            audit_repository=audit_repo,
            retry_policy=retry_policy,
        ),
        retry_policy=retry_policy,
        checksum_service=MessageChecksumService(secret="test-message-hmac-secret"),
    )


@pytest.mark.asyncio
async def test_backfill_rerun_does_not_duplicate_messages():
    manifest = build_manifest()
    dialog = SourceDialog(dialog_id="chat-1", chat_type="supergroup", title="Telegram Project Chat")
    messages = [
        build_message("1", 0),
        build_message("2", 1, reply_to_message_id="1"),
        build_message("3", 2),
    ]
    repositories = (
        InMemoryChatMappingRepository(),
        InMemoryMessageMappingRepository(),
        InMemoryAttachmentMappingRepository(),
        InMemoryCheckpointRepository(),
        InMemoryAuditRepository(),
    )
    telegram_gateway = FakeTelegramGateway(
        dialogs=[dialog],
        messages_by_dialog={"chat-1": messages},
    )
    express_gateway = FakeExpressGateway()
    use_case = build_use_case(telegram_gateway, express_gateway, repositories)
    command = BackfillChatCommand(manifest=manifest, source_chat_id="chat-1", batch_size=2)

    first_run = await use_case.execute(command)
    second_run = await use_case.execute(command)

    sent_messages = express_gateway.messages_for_chat(first_run.target_chat_id)
    chat_mapping = await repositories[0].get("migration-1", "chat-1")

    assert first_run.imported_count == 3
    assert second_run.imported_count == 0
    assert second_run.skipped_count == 0
    assert chat_mapping is not None
    assert chat_mapping.status == "completed"


@pytest.mark.asyncio
async def test_backfill_retries_cancelled_message_delivery_on_next_run():
    manifest = build_manifest()
    dialog = SourceDialog(dialog_id="chat-1", chat_type="supergroup", title="Telegram Project Chat")
    messages = [build_message("1", 0)]
    chat_repo = InMemoryChatMappingRepository()
    message_repo = InMemoryMessageMappingRepository()
    attachment_repo = InMemoryAttachmentMappingRepository()
    checkpoint_repo = InMemoryCheckpointRepository()
    audit_repo = InMemoryAuditRepository()
    identity_repo = InMemoryIdentityMappingRepository()
    attachment_stage_repo = InMemoryAttachmentStageRepository()
    telegram_gateway = FakeTelegramGateway(
        dialogs=[dialog],
        messages_by_dialog={"chat-1": messages},
    )
    express_gateway = FakeExpressGateway()
    retry_policy = AsyncRetryPolicy(
        max_attempts=1,
        base_delay_seconds=0.0,
        max_delay_seconds=0.0,
        jitter_seconds=0.0,
    )
    identity_directory = UsernameEmailIdentityDirectory(
        identity_mapping_repository=identity_repo,
        express_gateway=express_gateway,
    )
    message_delivery_service = CancelOnceMessageDeliveryService(
        telegram_gateway=telegram_gateway,
        express_gateway=express_gateway,
        express_file_store=FakeExpressFileStore(),
        attachment_mapping_repository=attachment_repo,
        attachment_stage_repository=attachment_stage_repo,
        audit_repository=audit_repo,
        renderer=MessageRenderer(timezone_name="Europe/Moscow"),
        retry_policy=retry_policy,
        max_attachment_upload_size_bytes=100 * 1024 * 1024,
    )
    use_case = BackfillChatUseCase(
        telegram_gateway=telegram_gateway,
        express_gateway=express_gateway,
        chat_mapping_repository=chat_repo,
        message_mapping_repository=message_repo,
        checkpoint_repository=checkpoint_repo,
        audit_repository=audit_repo,
        normalizer=TelegramMessageNormalizer(
            DirectoryBackedIdentityResolver(identity_directory),
            identity_directory=identity_directory,
        ),
        message_delivery_service=message_delivery_service,
        target_chat_provisioning_service=TargetChatProvisioningService(
            telegram_gateway=telegram_gateway,
            express_gateway=express_gateway,
            chat_mapping_repository=chat_repo,
            identity_directory=identity_directory,
            audit_repository=audit_repo,
            retry_policy=retry_policy,
        ),
        retry_policy=retry_policy,
        checksum_service=MessageChecksumService(secret="test-message-hmac-secret"),
    )
    command = BackfillChatCommand(manifest=manifest, source_chat_id="chat-1", batch_size=10)

    with pytest.raises(asyncio.CancelledError):
        await use_case.execute(command)

    failed_record = await message_repo.get("migration-1", "chat-1", "1")
    assert failed_record is not None
    assert failed_record.import_status is MessageImportStatus.FAILED
    assert failed_record.last_error_code == MIGRATION_JOB_CANCELLED_ERROR_CODE

    rerun = await use_case.execute(command)

    imported_record = await message_repo.get("migration-1", "chat-1", "1")
    assert rerun.imported_count == 1
    assert imported_record is not None
    assert imported_record.import_status is MessageImportStatus.IMPORTED
    assert len(express_gateway.messages_for_chat(rerun.target_chat_id)) == 1


@pytest.mark.asyncio
async def test_backfill_resumes_from_checkpoint_after_partial_progress():
    manifest = build_manifest()
    dialog = SourceDialog(dialog_id="chat-1", chat_type="supergroup", title="Telegram Project Chat")
    all_messages = [
        build_message("1", 0),
        build_message("2", 1),
        build_message("3", 2),
    ]
    repositories = (
        InMemoryChatMappingRepository(),
        InMemoryMessageMappingRepository(),
        InMemoryAttachmentMappingRepository(),
        InMemoryCheckpointRepository(),
        InMemoryAuditRepository(),
    )
    express_gateway = FakeExpressGateway()

    first_gateway = FakeTelegramGateway(
        dialogs=[dialog],
        messages_by_dialog={"chat-1": all_messages[:2]},
    )
    first_use_case = build_use_case(first_gateway, express_gateway, repositories)
    command = BackfillChatCommand(manifest=manifest, source_chat_id="chat-1", batch_size=10)
    first_result = await first_use_case.execute(command)

    second_gateway = FakeTelegramGateway(
        dialogs=[dialog],
        messages_by_dialog={"chat-1": all_messages},
    )
    second_use_case = build_use_case(second_gateway, express_gateway, repositories)
    second_result = await second_use_case.execute(command)

    sent_messages = express_gateway.messages_for_chat(first_result.target_chat_id)

    assert first_result.imported_count == 2
    assert second_result.imported_count == 1
    assert second_result.last_source_message_id == "3"
    assert len(sent_messages) == 3
    assert [message.body.splitlines()[-1] for message in sent_messages] == [
        "Сообщение 1",
        "Сообщение 2",
        "Сообщение 3",
    ]


@pytest.mark.asyncio
async def test_backfill_marks_failed_item_and_continues():
    manifest = build_manifest()
    dialog = SourceDialog(dialog_id="chat-1", chat_type="supergroup", title="Telegram Project Chat")
    messages = [
        build_message("1", 0),
        build_message("2", 1),
        build_message("3", 2),
    ]
    repositories = (
        InMemoryChatMappingRepository(),
        InMemoryMessageMappingRepository(),
        InMemoryAttachmentMappingRepository(),
        InMemoryCheckpointRepository(),
        InMemoryAuditRepository(),
    )
    telegram_gateway = FakeTelegramGateway(
        dialogs=[dialog],
        messages_by_dialog={"chat-1": messages},
    )
    express_gateway = FakeExpressGateway(
        send_failures_before_success={"telegram:chat-1:2": 5},
    )
    use_case = build_use_case(
        telegram_gateway,
        express_gateway,
        repositories,
        max_attempts=2,
    )
    result = await use_case.execute(
        BackfillChatCommand(manifest=manifest, source_chat_id="chat-1", batch_size=10),
    )

    message_repo = repositories[1]
    audit_repo = repositories[4]
    failed_mapping = await message_repo.get("migration-1", "chat-1", "2")
    audit_events = await audit_repo.list_all()

    assert result.imported_count == 2
    assert result.failed_count == 1
    assert failed_mapping is not None
    assert failed_mapping.import_status.value == "failed"
    assert any(event.event_type == "target_chat_participants_resolved" for event in audit_events)
    assert any(event.source_message_id == "2" for event in audit_events)


@pytest.mark.asyncio
async def test_backfill_respects_bind_strategy_and_date_window():
    manifest = MigrationManifest.model_validate(
        {
            "migration_id": "migration-1",
            "mode": "backfill_delta_cutover",
            "dialogs": [
                {
                    "source_chat_id": "chat-1",
                    "source_chat_type": "supergroup",
                    "target_strategy": "bind",
                    "target_chat_id": "express-bound-chat",
                    "include_from": "2026-03-17T10:01:00Z",
                    "include_to": "2026-03-17T10:01:00Z",
                },
            ],
        },
    )
    dialog = SourceDialog(dialog_id="chat-1", chat_type="supergroup", title="Telegram Project Chat")
    messages = [
        build_message("1", 0),
        build_message("2", 1),
        build_message("3", 2),
    ]
    repositories = (
        InMemoryChatMappingRepository(),
        InMemoryMessageMappingRepository(),
        InMemoryAttachmentMappingRepository(),
        InMemoryCheckpointRepository(),
        InMemoryAuditRepository(),
    )
    telegram_gateway = FakeTelegramGateway(
        dialogs=[dialog],
        messages_by_dialog={"chat-1": messages},
    )
    express_gateway = FakeExpressGateway()
    use_case = build_use_case(telegram_gateway, express_gateway, repositories)

    result = await use_case.execute(
        BackfillChatCommand(manifest=manifest, source_chat_id="chat-1", batch_size=10),
    )

    sent_messages = express_gateway.messages_for_chat("express-bound-chat")
    assert result.target_chat_id == "express-bound-chat"
    assert result.imported_count == 1
    assert len(sent_messages) == 1
    assert sent_messages[0].body.splitlines()[-1] == "Сообщение 2"


@pytest.mark.asyncio
async def test_backfill_rejects_manual_review_required_dialog():
    manifest = MigrationManifest.model_validate(
        {
            "migration_id": "migration-1",
            "mode": "backfill_delta_cutover",
            "dialogs": [
                {
                    "source_chat_id": "chat-1",
                    "source_chat_type": "supergroup",
                    "target_strategy": "manual_review_required",
                },
            ],
        },
    )
    dialog = SourceDialog(dialog_id="chat-1", chat_type="supergroup", title="Telegram Project Chat")
    repositories = (
        InMemoryChatMappingRepository(),
        InMemoryMessageMappingRepository(),
        InMemoryAttachmentMappingRepository(),
        InMemoryCheckpointRepository(),
        InMemoryAuditRepository(),
    )
    telegram_gateway = FakeTelegramGateway(
        dialogs=[dialog],
        messages_by_dialog={"chat-1": [build_message("1", 0)]},
    )
    express_gateway = FakeExpressGateway()
    use_case = build_use_case(telegram_gateway, express_gateway, repositories)

    with pytest.raises(FatalItemError):
        await use_case.execute(
            BackfillChatCommand(manifest=manifest, source_chat_id="chat-1", batch_size=10),
        )


@pytest.mark.asyncio
async def test_backfill_creates_personal_chat_for_private_dialog_using_identity_mapping():
    manifest = MigrationManifest.model_validate(
        {
            "migration_id": "migration-1",
            "mode": "backfill_delta_cutover",
            "dialogs": [
                {
                    "source_chat_id": "dm-1",
                    "source_chat_type": "private",
                    "target_strategy": "create",
                    "target_title": "Imported DM",
                    "initiator_huid": "initiator-huid",
                },
            ],
        },
    )
    dialog = SourceDialog(dialog_id="dm-1", chat_type="private", title="Private Dialog")
    repositories = (
        InMemoryChatMappingRepository(),
        InMemoryMessageMappingRepository(),
        InMemoryAttachmentMappingRepository(),
        InMemoryCheckpointRepository(),
        InMemoryAuditRepository(),
    )
    identity_repo = InMemoryIdentityMappingRepository()
    await identity_repo.save(
        IdentityMappingRecord(
            telegram_user_id="peer-1",
            telegram_username="peer.user",
            telegram_display_name="Peer User",
            corporate_email="peer.user@example.com",
        ),
    )
    telegram_gateway = FakeTelegramGateway(
        dialogs=[dialog],
        messages_by_dialog={"dm-1": [build_message("1", 0, chat_id="dm-1")]},
        participants_by_dialog={
            "dm-1": [
                SourceParticipant(
                    external_id="self-1",
                    username="self.user",
                    display_name="Self User",
                    is_self=True,
                ),
                SourceParticipant(
                    external_id="peer-1",
                    username="peer.user",
                    display_name="Peer User",
                ),
            ],
        },
    )
    express_gateway = FakeExpressGateway(
        user_huid_by_email={"peer.user@example.com": "huid-peer-user"},
    )
    use_case = build_use_case(
        telegram_gateway,
        express_gateway,
        repositories,
        identity_repo=identity_repo,
    )

    result = await use_case.execute(
        BackfillChatCommand(manifest=manifest, source_chat_id="dm-1", batch_size=10),
    )

    created_chat = express_gateway.created_chat(result.target_chat_id)
    assert result.target_chat_id == "express-chat-1"
    assert created_chat is not None
    assert created_chat.kind == "group"
    assert created_chat.participant_huids == ["initiator-huid", "huid-peer-user"]


@pytest.mark.asyncio
async def test_backfill_creates_group_chat_with_resolved_participants_and_skips_unmapped():
    manifest = build_manifest()
    dialog = SourceDialog(dialog_id="chat-1", chat_type="supergroup", title="Telegram Project Chat")
    repositories = (
        InMemoryChatMappingRepository(),
        InMemoryMessageMappingRepository(),
        InMemoryAttachmentMappingRepository(),
        InMemoryCheckpointRepository(),
        InMemoryAuditRepository(),
    )
    identity_repo = InMemoryIdentityMappingRepository()
    await identity_repo.save(
        IdentityMappingRecord(
            telegram_user_id="peer-1",
            telegram_username="ivan.petrov",
            telegram_display_name="Иван Петров",
            corporate_email="ivan.petrov@example.com",
        ),
    )
    telegram_gateway = FakeTelegramGateway(
        dialogs=[dialog],
        messages_by_dialog={"chat-1": [build_message("1", 0)]},
        participants_by_dialog={
            "chat-1": [
                SourceParticipant(
                    external_id="self-1",
                    username="self.user",
                    display_name="Self User",
                    is_self=True,
                ),
                SourceParticipant(
                    external_id="peer-1",
                    username="ivan.petrov",
                    display_name="Иван Петров",
                ),
                SourceParticipant(
                    external_id="peer-2",
                    username="unknown.user",
                    display_name="Unknown User",
                ),
            ],
        },
    )
    express_gateway = FakeExpressGateway(
        user_huid_by_email={"ivan.petrov@example.com": "huid-ivan"},
    )
    use_case = build_use_case(
        telegram_gateway,
        express_gateway,
        repositories,
        identity_repo=identity_repo,
    )

    result = await use_case.execute(
        BackfillChatCommand(manifest=manifest, source_chat_id="chat-1", batch_size=10),
    )

    created_chat = express_gateway.created_chat(result.target_chat_id)
    audit_events = await repositories[4].list_all()

    assert created_chat is not None
    assert created_chat.kind == "group"
    assert created_chat.participant_huids == ["huid-ivan"]
    assert any(event.event_type == "participant_skipped_unresolved_identity" for event in audit_events)


@pytest.mark.asyncio
async def test_backfill_uploads_single_attachment():
    manifest = build_manifest()
    dialog = SourceDialog(dialog_id="chat-1", chat_type="supergroup", title="Telegram Project Chat")
    attachment = CanonicalAttachment(
        source_file_id="file-1",
        filename="document.txt",
        mime_type="text/plain",
        size_bytes=12,
        media_kind="document",
        download_url="fake://attachment/1",
    )
    messages = [
        build_message(
            "1",
            0,
            attachments=[attachment],
        ),
    ]
    repositories = (
        InMemoryChatMappingRepository(),
        InMemoryMessageMappingRepository(),
        InMemoryAttachmentMappingRepository(),
        InMemoryCheckpointRepository(),
        InMemoryAuditRepository(),
    )
    telegram_gateway = FakeTelegramGateway(
        dialogs=[dialog],
        messages_by_dialog={"chat-1": messages},
        downloaded_attachment_content_by_url={"fake://attachment/1": b"hello world\n"},
    )
    express_gateway = FakeExpressGateway()
    express_file_store = FakeExpressFileStore()
    use_case = build_use_case(
        telegram_gateway,
        express_gateway,
        repositories,
        express_file_store=express_file_store,
    )

    result = await use_case.execute(
        BackfillChatCommand(manifest=manifest, source_chat_id="chat-1", batch_size=10),
    )

    sent_messages = express_gateway.messages_for_chat(result.target_chat_id)
    audit_events = await repositories[4].list_all()

    assert result.imported_count == 1
    assert len(sent_messages) == 1
    assert sent_messages[0].file_filename == "document.txt"
    assert sent_messages[0].file_id == "file-1"
    assert sent_messages[0].body.splitlines()[-1] == "Сообщение 1"
    assert len(express_file_store.uploads) == 1
    assert express_file_store.uploads[0][1].content == b"hello world\n"
    assert any(event.event_type == "attachment_uploaded_to_files_api" for event in audit_events)
    assert any(event.event_type == "attachment_imported" for event in audit_events)


@pytest.mark.asyncio
async def test_backfill_falls_back_to_direct_file_send_when_preview_generation_fails():
    manifest = build_manifest()
    dialog = SourceDialog(dialog_id="chat-1", chat_type="supergroup", title="Telegram Project Chat")
    attachment = CanonicalAttachment(
        source_file_id="file-1",
        filename="document.txt",
        mime_type="text/plain",
        size_bytes=12,
        media_kind="document",
        download_url="fake://attachment/1",
    )
    messages = [
        build_message(
            "1",
            0,
            attachments=[attachment],
        ),
    ]
    repositories = (
        InMemoryChatMappingRepository(),
        InMemoryMessageMappingRepository(),
        InMemoryAttachmentMappingRepository(),
        InMemoryCheckpointRepository(),
        InMemoryAuditRepository(),
    )
    telegram_gateway = FakeTelegramGateway(
        dialogs=[dialog],
        messages_by_dialog={"chat-1": messages},
        downloaded_attachment_content_by_url={"fake://attachment/1": b"hello world\n"},
    )
    express_gateway = FakeExpressGateway()
    use_case = build_use_case(
        telegram_gateway,
        express_gateway,
        repositories,
        express_file_store=PreviewFailureExpressFileStore(),
    )

    result = await use_case.execute(
        BackfillChatCommand(manifest=manifest, source_chat_id="chat-1", batch_size=10),
    )

    sent_messages = express_gateway.messages_for_chat(result.target_chat_id)
    audit_events = await repositories[4].list_all()

    assert result.imported_count == 1
    assert len(sent_messages) == 1
    assert sent_messages[0].file_filename == "document.txt"
    assert sent_messages[0].file_content == b"hello world\n"
    assert sent_messages[0].file_id is None
    assert any(
        event.event_type == "attachment_upload_fallback_to_direct_send"
        for event in audit_events
    )
    assert any(
        event.event_type == "attachment_imported"
        and event.payload_json.get("delivery_mode") == "inline_primary_direct_file"
        for event in audit_events
    )


@pytest.mark.asyncio
async def test_backfill_logs_inline_attachment_send_failure():
    manifest = build_manifest()
    dialog = SourceDialog(dialog_id="chat-1", chat_type="supergroup", title="Telegram Project Chat")
    attachment = CanonicalAttachment(
        source_file_id="file-1",
        filename="voice.ogg",
        mime_type="audio/ogg",
        media_kind="voice",
        size_bytes=0,
        download_url="fake://attachment/voice",
    )
    repositories = (
        InMemoryChatMappingRepository(),
        InMemoryMessageMappingRepository(),
        InMemoryAttachmentMappingRepository(),
        InMemoryCheckpointRepository(),
        InMemoryAuditRepository(),
    )
    telegram_gateway = FakeTelegramGateway(
        dialogs=[dialog],
        messages_by_dialog={
            "chat-1": [
                build_message(
                    "1",
                    0,
                    content_type=ContentType.VOICE,
                    body=None,
                    attachments=[attachment],
                ),
            ],
        },
        downloaded_attachment_content_by_url={"fake://attachment/voice": b"voice-data"},
    )
    express_gateway = FakeExpressGateway(
        send_failures_before_success={"telegram:chat-1:1": 5},
    )
    logger = RecordingLogger()
    use_case = build_use_case(
        telegram_gateway,
        express_gateway,
        repositories,
        max_attempts=2,
        logger=logger,
    )

    result = await use_case.execute(
        BackfillChatCommand(manifest=manifest, source_chat_id="chat-1", batch_size=10),
    )

    warning_records = [record for record in logger.records if record[0] == "warning"]

    assert result.failed_count == 1
    assert any(event == "primary message delivery failed" for _, event, _ in warning_records)
    assert any(
        payload.get("source_message_id") == "1" and payload.get("has_inline_attachment") is True
        for _, _, payload in warning_records
    )


@pytest.mark.asyncio
async def test_backfill_derives_photo_filename_when_source_name_is_missing():
    manifest = build_manifest()
    dialog = SourceDialog(dialog_id="chat-1", chat_type="supergroup", title="Telegram Project Chat")
    attachment = CanonicalAttachment(
        source_file_id="file-1",
        filename=None,
        mime_type="image/jpeg",
        size_bytes=12,
        media_kind="photo",
        download_url="fake://attachment/photo",
    )
    repositories = (
        InMemoryChatMappingRepository(),
        InMemoryMessageMappingRepository(),
        InMemoryAttachmentMappingRepository(),
        InMemoryCheckpointRepository(),
        InMemoryAuditRepository(),
    )
    telegram_gateway = FakeTelegramGateway(
        dialogs=[dialog],
        messages_by_dialog={
            "chat-1": [
                build_message(
                    "1",
                    0,
                    body=None,
                    content_type=ContentType.PHOTO,
                    attachments=[attachment],
                ),
            ],
        },
        downloaded_attachment_content_by_url={"fake://attachment/photo": b"jpeg-data"},
    )
    express_gateway = FakeExpressGateway()
    use_case = build_use_case(telegram_gateway, express_gateway, repositories)

    result = await use_case.execute(
        BackfillChatCommand(manifest=manifest, source_chat_id="chat-1", batch_size=10),
    )

    sent_messages = express_gateway.messages_for_chat(result.target_chat_id)

    assert result.imported_count == 1
    assert len(sent_messages) == 1
    assert sent_messages[0].file_filename == "photo.jpg"
    assert sent_messages[0].body.splitlines()[-1] == "[photo]"


@pytest.mark.asyncio
async def test_backfill_imports_text_when_attachment_download_fails():
    manifest = build_manifest()
    dialog = SourceDialog(dialog_id="chat-1", chat_type="supergroup", title="Telegram Project Chat")
    attachment = CanonicalAttachment(
        source_file_id="file-1",
        filename="broken.bin",
        mime_type="application/octet-stream",
        size_bytes=7,
        media_kind="document",
        download_url="fake://attachment/broken",
    )
    messages = [
        build_message(
            "1",
            0,
            attachments=[attachment],
        ),
    ]
    repositories = (
        InMemoryChatMappingRepository(),
        InMemoryMessageMappingRepository(),
        InMemoryAttachmentMappingRepository(),
        InMemoryCheckpointRepository(),
        InMemoryAuditRepository(),
    )
    telegram_gateway = FakeTelegramGateway(
        dialogs=[dialog],
        messages_by_dialog={"chat-1": messages},
        download_failures_by_url={
            "fake://attachment/broken": RecoverableItemError("broken attachment"),
        },
    )
    express_gateway = FakeExpressGateway()
    use_case = build_use_case(telegram_gateway, express_gateway, repositories)

    result = await use_case.execute(
        BackfillChatCommand(manifest=manifest, source_chat_id="chat-1", batch_size=10),
    )

    sent_messages = express_gateway.messages_for_chat(result.target_chat_id)
    audit_events = await repositories[4].list_all()

    assert result.imported_count == 1
    assert result.failed_count == 0
    assert sent_messages[0].file_filename is None
    assert "[attachment import failed: broken attachment]" in sent_messages[0].body
    assert any(event.event_type == "attachment_download_failed" for event in audit_events)


@pytest.mark.asyncio
async def test_backfill_skips_media_when_manifest_disables_it():
    manifest = MigrationManifest.model_validate(
        {
            "migration_id": "migration-1",
            "mode": "backfill_delta_cutover",
            "defaults": {"migrate_media": False},
            "dialogs": [
                {
                    "source_chat_id": "chat-1",
                    "source_chat_type": "supergroup",
                    "target_strategy": "create",
                    "target_title": "Imported Chat",
                },
            ],
        },
    )
    dialog = SourceDialog(dialog_id="chat-1", chat_type="supergroup", title="Telegram Project Chat")
    attachment = CanonicalAttachment(
        source_file_id="file-1",
        filename="document.txt",
        mime_type="text/plain",
        size_bytes=12,
        media_kind="document",
        download_url="fake://attachment/1",
    )
    repositories = (
        InMemoryChatMappingRepository(),
        InMemoryMessageMappingRepository(),
        InMemoryAttachmentMappingRepository(),
        InMemoryCheckpointRepository(),
        InMemoryAuditRepository(),
    )
    telegram_gateway = FakeTelegramGateway(
        dialogs=[dialog],
        messages_by_dialog={"chat-1": [build_message("1", 0, attachments=[attachment])]},
    )
    express_gateway = FakeExpressGateway()
    use_case = build_use_case(telegram_gateway, express_gateway, repositories)

    result = await use_case.execute(
        BackfillChatCommand(manifest=manifest, source_chat_id="chat-1", batch_size=10),
    )

    sent_messages = express_gateway.messages_for_chat(result.target_chat_id)
    audit_events = await repositories[4].list_all()

    assert result.imported_count == 1
    assert telegram_gateway.download_calls == 0
    assert sent_messages[0].file_filename is None
    assert any(event.event_type == "attachment_skipped_by_policy" for event in audit_events)


@pytest.mark.asyncio
async def test_backfill_skips_attachment_above_express_size_limit_before_download():
    manifest = build_manifest()
    dialog = SourceDialog(dialog_id="chat-1", chat_type="supergroup", title="Telegram Project Chat")
    attachment = CanonicalAttachment(
        source_file_id="file-1",
        filename="huge.zip",
        mime_type="application/zip",
        size_bytes=120 * 1024 * 1024,
        media_kind="document",
        download_url="fake://attachment/huge",
    )
    repositories = (
        InMemoryChatMappingRepository(),
        InMemoryMessageMappingRepository(),
        InMemoryAttachmentMappingRepository(),
        InMemoryCheckpointRepository(),
        InMemoryAuditRepository(),
    )
    telegram_gateway = FakeTelegramGateway(
        dialogs=[dialog],
        messages_by_dialog={"chat-1": [build_message("1", 0, attachments=[attachment])]},
    )
    express_gateway = FakeExpressGateway()
    use_case = build_use_case(
        telegram_gateway,
        express_gateway,
        repositories,
        max_attachment_upload_size_bytes=100 * 1024 * 1024,
    )

    result = await use_case.execute(
        BackfillChatCommand(manifest=manifest, source_chat_id="chat-1", batch_size=10),
    )

    sent_messages = express_gateway.messages_for_chat(result.target_chat_id)
    audit_events = await repositories[4].list_all()
    attachment_record = await repositories[2].get("migration-1", "chat-1", "1", 0)

    assert result.imported_count == 1
    assert telegram_gateway.download_calls == 0
    assert sent_messages[0].file_filename is None
    assert "[attachment skipped: file exceeds eXpress BotX upload limit" in sent_messages[0].body
    assert attachment_record is not None
    assert attachment_record.import_status is AttachmentImportStatus.SKIPPED
    assert any(event.event_type == "attachment_skipped_by_policy" for event in audit_events)


@pytest.mark.asyncio
async def test_backfill_sends_multiple_attachments_as_followup_messages():
    manifest = build_manifest()
    dialog = SourceDialog(dialog_id="chat-1", chat_type="supergroup", title="Telegram Project Chat")
    first = CanonicalAttachment(
        source_file_id="file-1",
        filename="first.txt",
        mime_type="text/plain",
        size_bytes=5,
        media_kind="document",
        download_url="fake://attachment/1",
    )
    second = CanonicalAttachment(
        source_file_id="file-2",
        filename="second.txt",
        mime_type="text/plain",
        size_bytes=6,
        media_kind="document",
        download_url="fake://attachment/2",
    )
    repositories = (
        InMemoryChatMappingRepository(),
        InMemoryMessageMappingRepository(),
        InMemoryAttachmentMappingRepository(),
        InMemoryCheckpointRepository(),
        InMemoryAuditRepository(),
    )
    telegram_gateway = FakeTelegramGateway(
        dialogs=[dialog],
        messages_by_dialog={"chat-1": [build_message("1", 0, attachments=[first, second])]},
        downloaded_attachment_content_by_url={
            "fake://attachment/1": b"first",
            "fake://attachment/2": b"second",
        },
    )
    express_gateway = FakeExpressGateway()
    use_case = build_use_case(telegram_gateway, express_gateway, repositories)

    result = await use_case.execute(
        BackfillChatCommand(manifest=manifest, source_chat_id="chat-1", batch_size=10),
    )

    sent_messages = express_gateway.messages_for_chat(result.target_chat_id)
    audit_events = await repositories[4].list_all()

    assert result.imported_count == 1
    assert len(sent_messages) == 3
    assert [message.file_filename for message in sent_messages] == [None, "first.txt", "second.txt"]
    assert sent_messages[1].body == "[document] first.txt"
    assert sent_messages[2].body == "[document] second.txt"
    assert len([event for event in audit_events if event.event_type == "attachment_imported"]) == 2


@pytest.mark.asyncio
async def test_backfill_prefetches_next_message_attachments_without_redownloading():
    manifest = build_manifest()
    dialog = SourceDialog(dialog_id="chat-1", chat_type="supergroup", title="Telegram Project Chat")
    first = CanonicalAttachment(
        source_file_id="file-1",
        filename="first.txt",
        mime_type="text/plain",
        size_bytes=5,
        media_kind="document",
        download_url="fake://attachment/1",
    )
    second = CanonicalAttachment(
        source_file_id="file-2",
        filename="second.txt",
        mime_type="text/plain",
        size_bytes=6,
        media_kind="document",
        download_url="fake://attachment/2",
    )
    repositories = (
        InMemoryChatMappingRepository(),
        InMemoryMessageMappingRepository(),
        InMemoryAttachmentMappingRepository(),
        InMemoryCheckpointRepository(),
        InMemoryAuditRepository(),
    )
    telegram_gateway = FakeTelegramGateway(
        dialogs=[dialog],
        messages_by_dialog={
            "chat-1": [
                build_message("1", 0, attachments=[first]),
                build_message("2", 1, attachments=[second]),
            ],
        },
        downloaded_attachment_content_by_url={
            "fake://attachment/1": b"first",
            "fake://attachment/2": b"second",
        },
    )
    express_gateway = FakeExpressGateway()
    use_case = build_use_case(telegram_gateway, express_gateway, repositories)

    result = await use_case.execute(
        BackfillChatCommand(manifest=manifest, source_chat_id="chat-1", batch_size=10),
    )

    sent_messages = express_gateway.messages_for_chat(result.target_chat_id)

    assert result.imported_count == 2
    assert telegram_gateway.download_calls == 2
    assert [message.body.splitlines()[-1] for message in sent_messages] == [
        "Сообщение 1",
        "Сообщение 2",
    ]


@pytest.mark.asyncio
async def test_backfill_renders_mapped_author_and_mentions_as_express_mentions():
    manifest = build_manifest()
    dialog = SourceDialog(dialog_id="chat-1", chat_type="supergroup", title="Telegram Project Chat")
    repositories = (
        InMemoryChatMappingRepository(),
        InMemoryMessageMappingRepository(),
        InMemoryAttachmentMappingRepository(),
        InMemoryCheckpointRepository(),
        InMemoryAuditRepository(),
    )
    identity_repo = InMemoryIdentityMappingRepository()
    await identity_repo.save(
        IdentityMappingRecord(
            telegram_user_id="peer-1",
            telegram_username="peer.user",
            telegram_display_name="Peer User",
            corporate_email="peer.user@example.com",
        ),
    )
    await identity_repo.save(
        IdentityMappingRecord(
            telegram_user_id="alice-1",
            telegram_username="alice",
            telegram_display_name="Alice Smith",
            corporate_email="alice@example.com",
        ),
    )
    telegram_gateway = FakeTelegramGateway(
        dialogs=[dialog],
        messages_by_dialog={
            "chat-1": [
                build_message(
                    "1",
                    0,
                    author=TelegramAuthor(
                        external_id="peer-1",
                        display_name="Peer User",
                        username="peer.user",
                    ),
                    body="Привет, @alice",
                    entities=[
                        CanonicalEntity(
                            kind="mention",
                            offset=8,
                            length=6,
                            text="@alice",
                            telegram_user_id="alice-1",
                            telegram_username="alice",
                        ),
                    ],
                ),
            ],
        },
        participants_by_dialog={
            "chat-1": [
                SourceParticipant(
                    external_id="self-1",
                    username="self.user",
                    display_name="Self User",
                    is_self=True,
                ),
                SourceParticipant(
                    external_id="peer-1",
                    username="peer.user",
                    display_name="Peer User",
                ),
            ],
        },
    )
    express_gateway = FakeExpressGateway(
        user_huid_by_email={
            "peer.user@example.com": "11111111-1111-1111-1111-111111111111",
            "alice@example.com": "22222222-2222-2222-2222-222222222222",
        },
    )
    use_case = build_use_case(
        telegram_gateway,
        express_gateway,
        repositories,
        identity_repo=identity_repo,
    )

    result = await use_case.execute(
        BackfillChatCommand(manifest=manifest, source_chat_id="chat-1", batch_size=10),
    )

    sent = express_gateway.messages_for_chat(result.target_chat_id)[0]

    assert "<embed_mention>USER:11111111-1111-1111-1111-111111111111:Peer User</embed_mention>" in sent.body
    assert "<embed_mention>USER:22222222-2222-2222-2222-222222222222:@alice</embed_mention>" in sent.body


@pytest.mark.asyncio
async def test_backfill_split_by_topic_routes_messages_to_separate_target_chats():
    manifest = build_split_topic_manifest()
    dialog = SourceDialog(dialog_id="chat-1", chat_type="supergroup", title="Forum Chat")
    messages = [
        build_message("10", 0, body="Topic 1 starter"),
        build_message("11", 1, body="Topic 1 reply", thread_id="10"),
        build_message("20", 2, body="Topic 2 starter"),
        build_message("21", 3, body="Topic 2 reply", thread_id="20"),
        build_message("30", 4, body="General message"),
    ]
    repositories = (
        InMemoryChatMappingRepository(),
        InMemoryMessageMappingRepository(),
        InMemoryAttachmentMappingRepository(),
        InMemoryCheckpointRepository(),
        InMemoryAuditRepository(),
    )
    telegram_gateway = FakeTelegramGateway(
        dialogs=[dialog],
        messages_by_dialog={"chat-1": messages},
    )
    express_gateway = FakeExpressGateway()
    use_case = build_use_case(telegram_gateway, express_gateway, repositories)

    topic_one_result = await use_case.execute(
        BackfillChatCommand(
            manifest=manifest,
            source_chat_id="chat-1#topic:101",
            batch_size=20,
        ),
    )
    topic_two_result = await use_case.execute(
        BackfillChatCommand(
            manifest=manifest,
            source_chat_id="chat-1#topic:102",
            batch_size=20,
        ),
    )

    topic_one_messages = express_gateway.messages_for_chat(topic_one_result.target_chat_id)
    topic_two_messages = express_gateway.messages_for_chat(topic_two_result.target_chat_id)

    assert topic_one_result.imported_count == 2
    assert topic_two_result.imported_count == 2
    assert [message.body.splitlines()[-1] for message in topic_one_messages] == [
        "Topic 1 starter",
        "Topic 1 reply",
    ]
    assert [message.body.splitlines()[-1] for message in topic_two_messages] == [
        "Topic 2 starter",
        "Topic 2 reply",
    ]
