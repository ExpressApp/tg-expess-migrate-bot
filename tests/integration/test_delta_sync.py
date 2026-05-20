from __future__ import annotations

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
from extg_shared.contracts.manifest import MigrationManifest
from extg_shared.contracts.models import (
    CanonicalAttachment,
    ContentType,
    HistoryCursor,
    MigrationCheckpoint,
    MigrationLifecycleStatus,
    MigrationStateRecord,
    SourceDialog,
    TelegramAuthor,
    TelegramDeltaEvent,
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
    InMemoryMigrationStateRepository,
)
from extg_shared.utils.retry import AsyncRetryPolicy
from extg_telethon_service.infrastructure.persistence.in_memory import (
    InMemoryAuditRepository,
)
from extg_telethon_service.infrastructure.telegram.fake_gateway import (
    FakeTelegramGateway,
)

delta_sync_module = pytest.importorskip(
    "extg_migration_runtime.application.use_cases.delta_sync",
)
DeltaSyncCommand = delta_sync_module.DeltaSyncCommand
DeltaSyncUseCase = delta_sync_module.DeltaSyncUseCase


def build_manifest(
    *,
    include_from: str | None = None,
    include_to: str | None = None,
) -> MigrationManifest:
    dialog = {
        "source_chat_id": "chat-1",
        "source_chat_type": "supergroup",
        "target_strategy": "create",
        "target_title": "Imported Chat",
    }
    if include_from is not None:
        dialog["include_from"] = include_from
    if include_to is not None:
        dialog["include_to"] = include_to
    return MigrationManifest.model_validate(
        {
            "migration_id": "migration-1",
            "mode": "backfill_delta_cutover",
            "dialogs": [dialog],
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
                    "source_thread_id": "101",
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
                    "source_thread_id": "102",
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
        "sent_at_utc": sent_at,
        "chat_title": "Telegram Project Chat",
        "author": TelegramAuthor(
            external_id=f"tg-user-{message_id}",
            display_name=f"User {message_id}",
        ),
        "body": f"Message {message_id}",
        "content_type": ContentType.TEXT,
    }
    payload.update(overrides)
    return TelegramSourceMessage(**payload)


def build_delta_event(message_id: str, minute_offset: int, **overrides) -> TelegramDeltaEvent:
    message = build_message(message_id, minute_offset, **overrides)
    return TelegramDeltaEvent(
        source_chat_id=message.chat_id,
        source_message=message,
        occurred_at=message.sent_at_utc,
    )


def build_use_case(
    telegram_gateway: FakeTelegramGateway,
    express_gateway: FakeExpressGateway,
    repositories,
    *,
    identity_repo: InMemoryIdentityMappingRepository | None = None,
    attachment_stage_repo: InMemoryAttachmentStageRepository | None = None,
    express_file_store: FakeExpressFileStore | None = None,
) -> DeltaSyncUseCase:
    (
        chat_repo,
        message_repo,
        attachment_repo,
        checkpoint_repo,
        audit_repo,
        migration_state_repo,
    ) = repositories
    identity_repo = identity_repo or InMemoryIdentityMappingRepository()
    attachment_stage_repo = attachment_stage_repo or InMemoryAttachmentStageRepository()
    express_file_store = express_file_store or FakeExpressFileStore()
    retry_policy = AsyncRetryPolicy(
        max_attempts=2,
        base_delay_seconds=0.0,
        max_delay_seconds=0.0,
        jitter_seconds=0.0,
    )
    identity_directory = UsernameEmailIdentityDirectory(
        identity_mapping_repository=identity_repo,
        express_gateway=express_gateway,
    )
    return DeltaSyncUseCase(
        telegram_gateway=telegram_gateway,
        express_gateway=express_gateway,
        chat_mapping_repository=chat_repo,
        message_mapping_repository=message_repo,
        checkpoint_repository=checkpoint_repo,
        migration_state_repository=migration_state_repo,
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


async def seed_backfill_checkpoint(
    checkpoint_repo: InMemoryCheckpointRepository,
    *,
    source_chat_id: str = "chat-1",
    last_source_message_id: str | None,
    last_source_sent_at: datetime | None,
) -> None:
    await checkpoint_repo.save(
        MigrationCheckpoint(
            migration_id="migration-1",
            source_chat_id=source_chat_id,
            mode="backfill",
            cursor=HistoryCursor(
                last_source_message_id=last_source_message_id,
                last_source_sent_at=last_source_sent_at,
            ),
            updated_at=datetime.now(tz=UTC),
        ),
    )


async def seed_migration_state(
    migration_state_repo: InMemoryMigrationStateRepository,
    *,
    status: MigrationLifecycleStatus,
) -> None:
    await migration_state_repo.save(
        MigrationStateRecord(
            migration_id="migration-1",
            status=status,
            created_at=datetime.now(tz=UTC),
            updated_at=datetime.now(tz=UTC),
        ),
    )


@pytest.mark.asyncio
async def test_delta_sync_catches_up_from_backfill_checkpoint_and_processes_live_delta_event():
    manifest = build_manifest()
    dialog = SourceDialog(dialog_id="chat-1", chat_type="supergroup", title="Telegram Project Chat")
    history_messages = [
        build_message("1", 0),
        build_message("2", 1),
    ]
    live_delta_event = build_delta_event("3", 2)

    repositories = (
        InMemoryChatMappingRepository(),
        InMemoryMessageMappingRepository(),
        InMemoryAttachmentMappingRepository(),
        InMemoryCheckpointRepository(),
        InMemoryAuditRepository(),
        InMemoryMigrationStateRepository(),
    )
    await seed_backfill_checkpoint(
        repositories[3],
        last_source_message_id="1",
        last_source_sent_at=history_messages[0].sent_at_utc,
    )

    telegram_gateway = FakeTelegramGateway(
        dialogs=[dialog],
        messages_by_dialog={"chat-1": history_messages},
        delta_events_by_dialog={"chat-1": [live_delta_event]},
    )
    express_gateway = FakeExpressGateway()
    use_case = build_use_case(telegram_gateway, express_gateway, repositories)

    result = await use_case.execute(
        DeltaSyncCommand(manifest=manifest, source_chat_ids=["chat-1"], batch_size=10),
    )
    chat_mapping = await repositories[0].get("migration-1", "chat-1")
    assert chat_mapping is not None
    sent_messages = express_gateway.messages_for_chat(chat_mapping.target_chat_id)
    delta_checkpoint = await repositories[3].get("migration-1", "chat-1", "delta")

    assert result.status == "completed"
    assert result.processed_count == 2
    assert result.imported_count == 2
    assert result.skipped_count == 0
    assert result.failed_count == 0
    assert result.last_source_message_id == "3"
    assert telegram_gateway.delta_subscribe_calls == 1
    assert [message.body.splitlines()[-1] for message in sent_messages] == [
        "Message 2",
        "Message 3",
    ]
    assert delta_checkpoint is not None
    assert delta_checkpoint.cursor.last_source_message_id == "3"


@pytest.mark.asyncio
async def test_delta_sync_catch_up_preserves_order_for_attachment_messages():
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
    history_messages = [
        build_message("1", 0),
        build_message("2", 1, attachments=[first]),
        build_message("3", 2, attachments=[second]),
    ]

    repositories = (
        InMemoryChatMappingRepository(),
        InMemoryMessageMappingRepository(),
        InMemoryAttachmentMappingRepository(),
        InMemoryCheckpointRepository(),
        InMemoryAuditRepository(),
        InMemoryMigrationStateRepository(),
    )
    await seed_backfill_checkpoint(
        repositories[3],
        last_source_message_id="1",
        last_source_sent_at=history_messages[0].sent_at_utc,
    )

    telegram_gateway = FakeTelegramGateway(
        dialogs=[dialog],
        messages_by_dialog={"chat-1": history_messages},
        downloaded_attachment_content_by_url={
            "fake://attachment/1": b"first",
            "fake://attachment/2": b"second",
        },
    )
    express_gateway = FakeExpressGateway()
    use_case = build_use_case(telegram_gateway, express_gateway, repositories)

    result = await use_case.execute(
        DeltaSyncCommand(
            manifest=manifest,
            source_chat_ids=["chat-1"],
            batch_size=10,
            max_events=2,
        ),
    )
    chat_mapping = await repositories[0].get("migration-1", "chat-1")
    assert chat_mapping is not None
    sent_messages = express_gateway.messages_for_chat(chat_mapping.target_chat_id)

    assert result.status == "max_events_reached"
    assert result.imported_count == 2
    assert telegram_gateway.download_calls == 2
    assert [message.file_filename for message in sent_messages] == ["first.txt", "second.txt"]


@pytest.mark.asyncio
async def test_delta_sync_skips_attachment_when_media_kind_is_not_selected():
    manifest = MigrationManifest.model_validate(
        {
            "migration_id": "migration-1",
            "mode": "backfill_delta_cutover",
            "defaults": {"migrate_media": True, "media_kinds": ["photo"]},
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
        InMemoryMigrationStateRepository(),
    )
    await seed_backfill_checkpoint(
        repositories[3],
        last_source_message_id=None,
        last_source_sent_at=None,
    )
    telegram_gateway = FakeTelegramGateway(
        dialogs=[dialog],
        messages_by_dialog={"chat-1": [build_message("1", 0, attachments=[attachment])]},
    )
    express_gateway = FakeExpressGateway()
    use_case = build_use_case(telegram_gateway, express_gateway, repositories)

    result = await use_case.execute(
        DeltaSyncCommand(
            manifest=manifest,
            source_chat_ids=["chat-1"],
            batch_size=10,
            max_events=1,
        ),
    )
    chat_mapping = await repositories[0].get("migration-1", "chat-1")
    assert chat_mapping is not None
    sent_messages = express_gateway.messages_for_chat(chat_mapping.target_chat_id)
    audit_events = await repositories[4].list_all()

    assert result.imported_count == 1
    assert telegram_gateway.download_calls == 0
    assert sent_messages[0].file_filename is None
    assert any(
        event.event_type == "attachment_skipped_by_policy"
        and event.payload_json.get("reason") == "attachment media kind is disabled by manifest"
        for event in audit_events
    )


@pytest.mark.asyncio
async def test_delta_sync_resume_from_delta_checkpoint_skips_duplicate_live_event():
    manifest = build_manifest()
    dialog = SourceDialog(dialog_id="chat-1", chat_type="supergroup", title="Telegram Project Chat")
    initial_history_messages = [
        build_message("1", 0),
        build_message("2", 1),
    ]
    resumed_history_messages = [
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
        InMemoryMigrationStateRepository(),
    )
    await seed_backfill_checkpoint(
        repositories[3],
        last_source_message_id="1",
        last_source_sent_at=initial_history_messages[0].sent_at_utc,
    )

    first_gateway = FakeTelegramGateway(
        dialogs=[dialog],
        messages_by_dialog={"chat-1": initial_history_messages},
        delta_events_by_dialog={"chat-1": [build_delta_event("3", 2)]},
    )
    express_gateway = FakeExpressGateway()
    use_case = build_use_case(first_gateway, express_gateway, repositories)
    first_result = await use_case.execute(
        DeltaSyncCommand(manifest=manifest, source_chat_ids=["chat-1"], batch_size=10),
    )

    resumed_gateway = FakeTelegramGateway(
        dialogs=[dialog],
        messages_by_dialog={"chat-1": resumed_history_messages},
        delta_events_by_dialog={"chat-1": [build_delta_event("3", 2)]},
    )
    resumed_use_case = build_use_case(resumed_gateway, express_gateway, repositories)
    second_result = await resumed_use_case.execute(
        DeltaSyncCommand(manifest=manifest, source_chat_ids=["chat-1"], batch_size=10),
    )

    chat_mapping = await repositories[0].get("migration-1", "chat-1")
    assert chat_mapping is not None
    sent_messages = express_gateway.messages_for_chat(chat_mapping.target_chat_id)

    assert first_result.imported_count == 2
    assert first_result.processed_count == 2
    assert second_result.imported_count == 0
    assert second_result.skipped_count == 1
    assert second_result.processed_count == 1
    assert len(sent_messages) == 2
    assert resumed_gateway.delta_subscribe_calls == 1


@pytest.mark.asyncio
async def test_delta_sync_respects_include_window():
    manifest = build_manifest(
        include_from="2026-03-17T10:01:00Z",
        include_to="2026-03-17T10:01:00Z",
    )
    dialog = SourceDialog(dialog_id="chat-1", chat_type="supergroup", title="Telegram Project Chat")
    history_messages = [
        build_message("1", 0),
        build_message("2", 1),
        build_message("3", 2),
    ]
    delta_event = build_delta_event("4", 3)

    repositories = (
        InMemoryChatMappingRepository(),
        InMemoryMessageMappingRepository(),
        InMemoryAttachmentMappingRepository(),
        InMemoryCheckpointRepository(),
        InMemoryAuditRepository(),
        InMemoryMigrationStateRepository(),
    )
    await seed_backfill_checkpoint(
        repositories[3],
        last_source_message_id=None,
        last_source_sent_at=None,
    )

    telegram_gateway = FakeTelegramGateway(
        dialogs=[dialog],
        messages_by_dialog={"chat-1": history_messages},
        delta_events_by_dialog={"chat-1": [delta_event]},
    )
    express_gateway = FakeExpressGateway()
    use_case = build_use_case(telegram_gateway, express_gateway, repositories)

    result = await use_case.execute(
        DeltaSyncCommand(manifest=manifest, source_chat_ids=["chat-1"], batch_size=10),
    )
    chat_mapping = await repositories[0].get("migration-1", "chat-1")
    assert chat_mapping is not None
    sent_messages = express_gateway.messages_for_chat(chat_mapping.target_chat_id)

    assert result.imported_count == 1
    assert result.skipped_count == 3
    assert result.processed_count == 4
    assert result.last_source_message_id == "4"
    assert [message.body.splitlines()[-1] for message in sent_messages] == ["Message 2"]
    assert telegram_gateway.delta_subscribe_calls == 1


@pytest.mark.asyncio
async def test_delta_sync_stops_after_max_events_without_entering_delta_stream():
    manifest = build_manifest()
    dialog = SourceDialog(dialog_id="chat-1", chat_type="supergroup", title="Telegram Project Chat")
    history_messages = [
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
        InMemoryMigrationStateRepository(),
    )
    await seed_backfill_checkpoint(
        repositories[3],
        last_source_message_id=None,
        last_source_sent_at=None,
    )

    telegram_gateway = FakeTelegramGateway(
        dialogs=[dialog],
        messages_by_dialog={"chat-1": history_messages},
        delta_events_by_dialog={"chat-1": [build_delta_event("4", 3)]},
    )
    express_gateway = FakeExpressGateway()
    use_case = build_use_case(telegram_gateway, express_gateway, repositories)

    result = await use_case.execute(
        DeltaSyncCommand(
            manifest=manifest,
            source_chat_ids=["chat-1"],
            batch_size=10,
            max_events=2,
        ),
    )
    chat_mapping = await repositories[0].get("migration-1", "chat-1")
    assert chat_mapping is not None
    sent_messages = express_gateway.messages_for_chat(chat_mapping.target_chat_id)

    assert result.status == "max_events_reached"
    assert result.imported_count == 2
    assert result.processed_count == 2
    assert result.last_source_message_id == "2"
    assert len(sent_messages) == 2
    assert telegram_gateway.delta_subscribe_calls == 0


@pytest.mark.asyncio
async def test_delta_sync_returns_paused_without_processing_messages():
    manifest = build_manifest()
    dialog = SourceDialog(dialog_id="chat-1", chat_type="supergroup", title="Telegram Project Chat")
    history_messages = [
        build_message("1", 0),
        build_message("2", 1),
    ]
    repositories = (
        InMemoryChatMappingRepository(),
        InMemoryMessageMappingRepository(),
        InMemoryAttachmentMappingRepository(),
        InMemoryCheckpointRepository(),
        InMemoryAuditRepository(),
        InMemoryMigrationStateRepository(),
    )
    await seed_backfill_checkpoint(
        repositories[3],
        last_source_message_id=None,
        last_source_sent_at=None,
    )
    await seed_migration_state(
        repositories[5],
        status=MigrationLifecycleStatus.PAUSED,
    )
    telegram_gateway = FakeTelegramGateway(
        dialogs=[dialog],
        messages_by_dialog={"chat-1": history_messages},
        delta_events_by_dialog={"chat-1": [build_delta_event("3", 2)]},
    )
    express_gateway = FakeExpressGateway()
    use_case = build_use_case(telegram_gateway, express_gateway, repositories)

    result = await use_case.execute(
        DeltaSyncCommand(manifest=manifest, source_chat_ids=["chat-1"], batch_size=10),
    )

    assert result.status == "paused"
    assert result.processed_count == 0
    assert result.migration_state is MigrationLifecycleStatus.PAUSED
    assert result.reason == "migration is paused"
    assert telegram_gateway.fetch_calls == 0
    assert telegram_gateway.delta_subscribe_calls == 0


@pytest.mark.asyncio
async def test_delta_sync_split_by_topic_fans_out_one_physical_subscription_to_multiple_logical_dialogs():
    manifest = build_split_topic_manifest()
    dialog = SourceDialog(dialog_id="chat-1", chat_type="supergroup", title="Forum Chat")
    repositories = (
        InMemoryChatMappingRepository(),
        InMemoryMessageMappingRepository(),
        InMemoryAttachmentMappingRepository(),
        InMemoryCheckpointRepository(),
        InMemoryAuditRepository(),
        InMemoryMigrationStateRepository(),
    )
    await seed_backfill_checkpoint(
        repositories[3],
        source_chat_id="chat-1#topic:101",
        last_source_message_id="0",
        last_source_sent_at=datetime(2026, 3, 17, 9, 59, tzinfo=UTC),
    )
    await seed_backfill_checkpoint(
        repositories[3],
        source_chat_id="chat-1#topic:102",
        last_source_message_id="0",
        last_source_sent_at=datetime(2026, 3, 17, 9, 59, tzinfo=UTC),
    )

    telegram_gateway = FakeTelegramGateway(
        dialogs=[dialog],
        messages_by_dialog={"chat-1": []},
        delta_events_by_dialog={
            "chat-1": [
                build_delta_event("10", 0, thread_id="101"),
                build_delta_event("11", 1, thread_id="101"),
                build_delta_event("20", 2, thread_id="102"),
                build_delta_event("21", 3, thread_id="102"),
            ],
        },
    )
    express_gateway = FakeExpressGateway()
    use_case = build_use_case(telegram_gateway, express_gateway, repositories)

    result = await use_case.execute(
        DeltaSyncCommand(
            manifest=manifest,
            batch_size=10,
            max_events=4,
        ),
    )

    topic_one_mapping = await repositories[0].get("migration-1", "chat-1#topic:101")
    topic_two_mapping = await repositories[0].get("migration-1", "chat-1#topic:102")

    assert topic_one_mapping is not None
    assert topic_two_mapping is not None
    assert telegram_gateway.delta_subscribe_calls == 1
    assert result.imported_count == 4
    assert [
        message.body.splitlines()[-1]
        for message in express_gateway.messages_for_chat(topic_one_mapping.target_chat_id)
    ] == [
        "Message 10",
        "Message 11",
    ]
    assert [
        message.body.splitlines()[-1]
        for message in express_gateway.messages_for_chat(topic_two_mapping.target_chat_id)
    ] == [
        "Message 20",
        "Message 21",
    ]
