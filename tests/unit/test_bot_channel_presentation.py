from datetime import UTC, datetime
from types import SimpleNamespace

import pytest

from extg_bot_ui.application.bot_control import (
    BotChatCheckpointStatus,
    BotAvailableChat,
    BotCancelResult,
    BotChatConfigurationResult,
    BotChatMembersWorkbookRequest,
    BotChannelChatResolutionRequest,
    BotGroupChatResolutionRequest,
    BotIdentityMappingResult,
    BotMigratedChat,
    BotMigrationStatsResult,
    BotChatUserMatrixEntry,
    BotChatUserMatrixResult,
    BotOperationAcceptedResult,
    MigrationBotStatusResult,
    MigrationRunOptions,
)
from extg_migration_runtime.application.use_cases.reconcile_migration import (
    ReconcileMigrationResult,
)
from extg_migration_runtime.application.use_cases.replay_failed import ReplayFailedResult
from extg_telethon_service.application.telegram_session_service import (
    TelegramConnectionChallengeResult,
    TelegramDisconnectResult,
    TelegramSessionStatusResult,
)
from extg_shared.contracts.errors import RecoverableItemError
from extg_shared.contracts.models import MigrationLifecycleStatus
from extg_bot_ui.presentation.bot.handlers import build_handler_collector
from extg_bot_ui.presentation.bot.wizard import (
    MigrationWizardState,
    build_wizard_collector,
)


class FakeBot:
    def __init__(self) -> None:
        self.calls: list[tuple[object, dict[str, object]]] = []

    async def answer_message(self, body, **kwargs):
        self.calls.append((body, dict(kwargs)))
        return None


class FakeFSM:
    def __init__(self, storage: dict[str, object] | None = None) -> None:
        self.storage = SimpleNamespace(**(storage or {}))
        self.change_calls: list[tuple[object, int, dict[str, object]]] = []
        self.dropped = False

    async def change_state(self, state, *, ttl_seconds: int, **payload) -> None:
        self.change_calls.append((state, ttl_seconds, dict(payload)))
        self.storage = SimpleNamespace(**payload)

    async def drop_state(self) -> None:
        self.dropped = True
        self.storage = SimpleNamespace()


class FakeMessageState:
    def __init__(self, storage: dict[str, object] | None = None) -> None:
        self.fsm = FakeFSM(storage)

    @property
    def fsm_storage(self):
        return self.fsm.storage


class FakeMessage:
    def __init__(
        self,
        *,
        argument: str = "",
        body: str = "",
        storage: dict[str, object] | None = None,
        file=None,
    ) -> None:
        self.argument = argument
        self.body = body
        self.file = file
        self.mentions = SimpleNamespace(users=[])
        self.sender = SimpleNamespace(
            huid="operator-1",
            username="operator",
            ad_login="operator",
        )
        self.chat = SimpleNamespace(id="express-chat")
        self.state = FakeMessageState(storage)


class FakeBotControlService:
    def __init__(self) -> None:
        self.available_chats: tuple[BotAvailableChat, ...] = ()
        self.private_request = None
        self.group_request = None
        self.channel_request = None
        self.configuration_result = BotChatConfigurationResult(
            migration_id="migration-1",
            source_chat_id="channel-1",
            source_chat_type="channel",
            source_chat_title="Release Notes",
            source_backend="telethon_user_session",
            target_strategy="create",
            target_title="Release Notes",
            target_chat_id=None,
            include_from=None,
            include_to=None,
            migrate_media=True,
            reply_mode="inline_quote",
            identity_policy="matrix_optional",
            access_strategy="invite_link",
            topic_strategy="single_chat",
            skip_in_all=False,
            telegram_chat_id=None,
            source_thread_id=None,
            source_thread_title=None,
            configured_via="bot",
            progress_hint=None,
        )
        self.list_chat_users_result: BotChatUserMatrixResult | None = None
        self.chat_members_workbook_request = BotChatMembersWorkbookRequest(
            source_chat_id="channel-1",
            source_chat_type="group",
            source_chat_title="Release Notes",
            target_chat_id="target-chat-1",
            target_chat_title="Release Notes",
            access_strategy="direct_add",
            workbook_content=b"xlsx",
            workbook_filename="chat_members_channel-1.xlsx",
            mapped_rows=1,
            unresolved_rows=0,
            note=None,
        )
        self.start_result = BotOperationAcceptedResult(
            status="accepted",
            operation="migrate_chat",
            migration_id="migration-1",
            source_chat_ids=("channel-1",),
            job_key="job-1",
        )
        self.status_result = MigrationBotStatusResult(
            migration_id="migration-1",
            migration_state=MigrationLifecycleStatus.ACTIVE,
            active_jobs=(),
            reconcile=ReconcileMigrationResult(
                migration_id="migration-1",
                chats_total=1,
                attention_chats=0,
                inventory_missing_chats=0,
                source_messages_total_known=10,
                imported_count=8,
                failed_count=1,
                ambiguous_count=0,
                processing_count=0,
                mapped_total=9,
                gap_total_known=0,
                source_media_total_known=2,
                attachment_imported_count=1,
                attachment_failed_count=0,
                attachment_ambiguous_count=0,
                attachment_processing_count=0,
                attachment_skipped_count=0,
                attachment_mapped_total=1,
                chats=[],
            ),
            chat_checkpoints=(
                BotChatCheckpointStatus(
                    source_chat_id="channel-1",
                    backfill_last_source_message_id="17",
                    delta_last_source_message_id=None,
                ),
            ),
        )
        self.stats_result = BotMigrationStatsResult(
            migration_id="migration-1",
            configured_chats=3,
            foreign_managed_chats=0,
            skipped_in_all_chats=1,
            active_jobs=1,
            chats_with_progress=2,
            attention_chats=0,
            imported_messages=8,
            failed_messages=1,
            ambiguous_messages=0,
            imported_attachments=1,
            failed_attachments=0,
        )
        self.identity_mapping_result = BotIdentityMappingResult(
            telegram_user_id="100",
            telegram_username="peer.user",
            telegram_display_name="Peer User",
            corporate_email="peer.user@example.com",
            target_huid="huid-peer-user",
            resolution_source="identity_directory",
            reason=None,
        )
        self.migrated_chats = (
            BotMigratedChat(
                source_chat_id="channel-1",
                source_chat_type="group",
                source_chat_title="Release Notes",
                target_chat_id="target-chat-1",
                target_chat_title="Release Notes",
            ),
        )
        self.archive_import_result = BotOperationAcceptedResult(
            status="accepted",
            operation="import_archive_chat",
            migration_id="migration-1",
            source_chat_ids=("archive:1",),
            job_key="job-archive",
        )
        self.replay_failed_result = ReplayFailedResult(
            migration_id="migration-1",
            requested_count=10,
            recovered_count=8,
            failed_count=1,
            ambiguous_count=0,
            missing_count=1,
            skipped_count=0,
            source_chat_ids=["channel-1"],
        )
        self.calls: list[tuple[str, str, object | None]] = []
        self.start_options: list[MigrationRunOptions] = []
        self.migrate_all_error: Exception | None = None

    async def list_available_chats(self, *, operator, limit=20, query=None, source_backend="telethon_user_session"):
        self.calls.append(("list_available_chats", query or "", source_backend))
        return self.available_chats

    async def prepare_private_chat_resolution(self, *, operator, source_chat_id: str, options: MigrationRunOptions):
        self.calls.append(("prepare_private", source_chat_id, options))
        return self.private_request

    async def prepare_group_chat_resolution(self, *, operator, source_chat_id: str, options: MigrationRunOptions):
        self.calls.append(("prepare_group", source_chat_id, options))
        return self.group_request

    async def prepare_channel_chat_resolution(self, *, operator, source_chat_id: str, options: MigrationRunOptions):
        self.calls.append(("prepare_channel", source_chat_id, options))
        return self.channel_request

    async def start_migrate_chat(self, *, operator, source_chat_id: str, options: MigrationRunOptions):
        self.calls.append(("start_migrate_chat", source_chat_id, options))
        self.start_options.append(options)
        return self.start_result

    async def list_chat_users(self, *, operator, source_chat_id: str):
        self.calls.append(("list_chat_users", source_chat_id, None))
        if self.list_chat_users_result is None:
            raise AssertionError("list_chat_users_result must be configured")
        return self.list_chat_users_result

    async def prepare_chat_members_workbook(self, *, operator, chat_selector: str):
        self.calls.append(("prepare_chat_members_workbook", chat_selector, None))
        return self.chat_members_workbook_request

    async def show_chat(self, *, operator, source_chat_id: str):
        self.calls.append(("show_chat", source_chat_id, None))
        return self.configuration_result

    async def show_or_configure_chat(self, *, operator, source_chat_id: str):
        self.calls.append(("show_or_configure_chat", source_chat_id, None))
        return self.configuration_result

    async def configure_chat(self, *, operator, source_chat_id: str, options: MigrationRunOptions):
        self.calls.append(("configure_chat", source_chat_id, options))
        return self.configuration_result

    async def start_migrate_all(self, *, operator, options: MigrationRunOptions):
        self.calls.append(("start_migrate_all", "", options))
        if self.migrate_all_error is not None:
            raise self.migrate_all_error
        return BotOperationAcceptedResult(
            status="accepted",
            operation="migrate_all",
            migration_id="migration-1",
            source_chat_ids=("chat-1",),
            job_key="job-migrate-all",
        )

    async def remigrate_chat(self, *, operator, source_chat_id: str, options: MigrationRunOptions):
        self.calls.append(("remigrate_chat", source_chat_id, options))
        self.start_options.append(options)
        return BotOperationAcceptedResult(
            status="accepted",
            operation="remigrate_chat",
            migration_id="migration-1",
            source_chat_ids=(source_chat_id,),
            job_key="job-remigrate",
        )

    async def status(self, *, operator, source_chat_id: str | None = None):
        self.calls.append(("status", source_chat_id or "", None))
        return self.status_result

    async def stats(self, *, operator):
        self.calls.append(("stats", "", None))
        return self.stats_result

    async def list_migrated_chats_for_user_addition(self, *, operator):
        self.calls.append(("list_migrated_chats_for_user_addition", "", None))
        return self.migrated_chats

    async def start_archive_import(self, *, operator, archive_content: bytes, archive_filename: str | None):
        self.calls.append(("start_archive_import", archive_filename or "", archive_content))
        return self.archive_import_result

    async def map_identity(
        self,
        *,
        operator,
        telegram_user_id,
        telegram_username,
        telegram_display_name,
        corporate_email,
    ):
        self.calls.append(("map_identity", corporate_email or "", telegram_username))
        return self.identity_mapping_result

    async def replay_failed(self, *, operator, source_chat_id: str | None, limit: int, batch_size: int):
        self.calls.append(("replay_failed", source_chat_id or "", (limit, batch_size)))
        return self.replay_failed_result

    async def cancel_active_jobs(self, *, operator, source_chat_id: str | None = None):
        self.calls.append(("cancel_active_jobs", source_chat_id or "", None))
        return BotCancelResult(
            migration_id="migration-1",
            source_chat_id=source_chat_id,
        )


class FakeTelegramSessionService:
    def __init__(self) -> None:
        self.calls: list[tuple[str, str, str | None]] = []
        self.challenge_result = TelegramConnectionChallengeResult(
            operator_huid="operator-1",
            phone_number="+79990001122",
            challenge_id="challenge-1",
        )
        self.status_result = TelegramSessionStatusResult(
            operator_huid="operator-1",
            connected=True,
            source="telethon_user_session",
            phone_number="+79990001122",
            telegram_user_id="42",
            telegram_username="peer.user",
            telegram_display_name="Peer User",
            session_bound=True,
            updated_at=datetime(2026, 3, 27, 12, 0, tzinfo=UTC),
            last_used_at=datetime(2026, 3, 27, 12, 5, tzinfo=UTC),
        )
        self.disconnect_result = TelegramDisconnectResult(
            operator_huid="operator-1",
            disconnected=True,
        )

    async def start_connection(self, *, operator_huid: str, phone_number: str, force_sms: bool = False):
        self.calls.append(("start_connection", operator_huid, phone_number))
        return self.challenge_result

    async def status(self, *, operator_huid: str):
        self.calls.append(("status", operator_huid, None))
        return self.status_result

    async def disconnect(self, *, operator_huid: str):
        self.calls.append(("disconnect", operator_huid, None))
        return self.disconnect_result


def _fsm_handler(fsm, state: MigrationWizardState):
    command = f"/fsm_{MigrationWizardState.__name__}_{state.name}"
    return fsm._user_commands_handlers[command].handler_func


@pytest.mark.asyncio
async def test_help_handler_renders_help_text():
    service = FakeBotControlService()
    telegram_session_service = FakeTelegramSessionService()
    collector = build_handler_collector(
        service=service,
        telegram_session_service=telegram_session_service,
        default_batch_size=20,
    )
    handler = collector._user_commands_handlers["/help"].handler_func
    message = FakeMessage()
    bot = FakeBot()

    await handler(message, bot)

    rendered = str(bot.calls[0][0])
    assert "/connect" in rendered
    assert "/import_archive" in rendered
    assert "/cancel [source_chat_id]" in rendered


@pytest.mark.asyncio
async def test_connect_handler_without_phone_enters_phone_state():
    service = FakeBotControlService()
    telegram_session_service = FakeTelegramSessionService()
    collector = build_handler_collector(
        service=service,
        telegram_session_service=telegram_session_service,
        default_batch_size=20,
    )
    handler = collector._user_commands_handlers["/connect"].handler_func
    message = FakeMessage()
    bot = FakeBot()

    await handler(message, bot)

    assert telegram_session_service.calls == []
    assert message.state.fsm.change_calls[-1][0].name == "INPUT_PHONE"
    assert "номер телефона" in str(bot.calls[0][0])


@pytest.mark.asyncio
async def test_connect_handler_with_phone_requests_code():
    service = FakeBotControlService()
    telegram_session_service = FakeTelegramSessionService()
    collector = build_handler_collector(
        service=service,
        telegram_session_service=telegram_session_service,
        default_batch_size=20,
    )
    handler = collector._user_commands_handlers["/connect"].handler_func
    message = FakeMessage(argument="+79990001122")
    bot = FakeBot()

    await handler(message, bot)

    assert telegram_session_service.calls == [
        ("start_connection", "operator-1", "+79990001122"),
    ]
    assert message.state.fsm.change_calls[-1][0].name == "INPUT_CODE"
    assert "Код отправлен" in str(bot.calls[0][0])


@pytest.mark.asyncio
async def test_account_handler_formats_telegram_status():
    service = FakeBotControlService()
    telegram_session_service = FakeTelegramSessionService()
    collector = build_handler_collector(
        service=service,
        telegram_session_service=telegram_session_service,
        default_batch_size=20,
    )
    handler = collector._user_commands_handlers["/account"].handler_func
    message = FakeMessage()
    bot = FakeBot()

    await handler(message, bot)

    assert telegram_session_service.calls == [("status", "operator-1", None)]
    rendered = str(bot.calls[0][0])
    assert "connected=yes" in rendered
    assert "telegram_user_id=42" in rendered


@pytest.mark.asyncio
async def test_disconnect_handler_drops_state_and_formats_result():
    service = FakeBotControlService()
    telegram_session_service = FakeTelegramSessionService()
    collector = build_handler_collector(
        service=service,
        telegram_session_service=telegram_session_service,
        default_batch_size=20,
    )
    handler = collector._user_commands_handlers["/disconnect"].handler_func
    message = FakeMessage(storage={"challenge_id": "challenge-1"})
    bot = FakeBot()

    await handler(message, bot)

    assert telegram_session_service.calls == [("disconnect", "operator-1", None)]
    assert message.state.fsm.dropped is True
    assert "Telegram session disconnected." in str(bot.calls[0][0])


@pytest.mark.asyncio
async def test_status_handler_formats_migration_status():
    service = FakeBotControlService()
    collector = build_handler_collector(
        service=service,
        telegram_session_service=FakeTelegramSessionService(),
        default_batch_size=20,
    )
    handler = collector._user_commands_handlers["/status"].handler_func
    message = FakeMessage(argument="channel-1")
    bot = FakeBot()

    await handler(message, bot)

    assert service.calls[-1] == ("status", "channel-1", None)
    rendered = str(bot.calls[0][0])
    assert "migration_id=migration-1" in rendered
    assert "messages imported=8 failed=1" in rendered
    assert "Checkpoints:" in rendered


@pytest.mark.asyncio
async def test_stats_handler_formats_stats():
    service = FakeBotControlService()
    collector = build_handler_collector(
        service=service,
        telegram_session_service=FakeTelegramSessionService(),
        default_batch_size=20,
    )
    handler = collector._user_commands_handlers["/stats"].handler_func
    message = FakeMessage()
    bot = FakeBot()

    await handler(message, bot)

    assert service.calls[-1] == ("stats", "", None)
    rendered = str(bot.calls[0][0])
    assert "configured_chats=3" in rendered
    assert "messages imported=8 failed=1 ambiguous=0" in rendered


@pytest.mark.asyncio
async def test_chats_handler_formats_available_chats():
    service = FakeBotControlService()
    service.available_chats = (
        BotAvailableChat(
            source_chat_id="chat-1",
            source_chat_type="supergroup",
            source_chat_title="Engineering",
            message_count=10,
            media_count=1,
            approximate_bytes=100,
            configured=True,
            has_progress=True,
            imported_count=5,
            mapped_total=10,
            last_source_message_id="5",
        ),
    )
    collector = build_handler_collector(
        service=service,
        telegram_session_service=FakeTelegramSessionService(),
        default_batch_size=20,
    )
    handler = collector._user_commands_handlers["/chats"].handler_func
    message = FakeMessage(argument="query=eng limit=5")
    bot = FakeBot()

    await handler(message, bot)

    assert service.calls[-1] == ("list_available_chats", "eng", "telethon_user_session")
    rendered = str(bot.calls[0][0])
    assert "Доступные чаты:" in rendered
    assert "chat-1 | supergroup | Engineering" in rendered


@pytest.mark.asyncio
async def test_chats_handler_respects_render_limit():
    service = FakeBotControlService()
    service.available_chats = tuple(
        BotAvailableChat(
            source_chat_id=f"chat-{index}",
            source_chat_type="supergroup",
            source_chat_title=f"Chat {index}",
            message_count=index,
            media_count=0,
            approximate_bytes=100,
            configured=False,
            has_progress=False,
            imported_count=0,
            mapped_total=0,
            last_source_message_id=None,
        )
        for index in range(1, 32)
    )
    collector = build_handler_collector(
        service=service,
        telegram_session_service=FakeTelegramSessionService(),
        default_batch_size=20,
    )
    handler = collector._user_commands_handlers["/chats"].handler_func
    message = FakeMessage(argument="limit=30")
    bot = FakeBot()

    await handler(message, bot)

    rendered = str(bot.calls[0][0])
    assert "chat-30 | supergroup | Chat 30" in rendered
    assert "chat-31 | supergroup | Chat 31" not in rendered
    assert "... +1 chats" in rendered


@pytest.mark.asyncio
async def test_migrate_chat_handler_routes_channel_resolution_before_start():
    service = FakeBotControlService()
    service.channel_request = BotChannelChatResolutionRequest(
        source_chat_id="channel-1",
        source_chat_title="Release Notes",
        action="choose_access_strategy",
        can_list_participants=False,
    )
    collector = build_handler_collector(
        service=service,
        telegram_session_service=SimpleNamespace(),
        default_batch_size=20,
    )
    handler = collector._user_commands_handlers["/migrate"].handler_func
    message = FakeMessage(argument="channel-1")
    bot = FakeBot()

    await handler(message, bot)

    assert [call[0] for call in service.calls] == [
        "prepare_private",
        "prepare_group",
        "prepare_channel",
    ]
    assert not service.start_options
    assert message.state.fsm.change_calls[-1][0] is MigrationWizardState.INPUT_CHANNEL_ACCESS_STRATEGY
    assert "direct_add" in str(bot.calls[0][0])
    assert "invite_link" in str(bot.calls[0][0])


@pytest.mark.asyncio
async def test_migrate_chat_handler_renders_all_forum_topics_without_truncation():
    service = FakeBotControlService()
    service.group_request = BotGroupChatResolutionRequest(
        source_chat_id="forum-1",
        source_chat_title="Big Forum",
        source_chat_type="supergroup",
        action="choose_topic_strategy",
        topic_titles=tuple(f"Topic {index}" for index in range(1, 13)),
    )
    collector = build_handler_collector(
        service=service,
        telegram_session_service=SimpleNamespace(),
        default_batch_size=20,
    )
    handler = collector._user_commands_handlers["/migrate"].handler_func
    message = FakeMessage(argument="forum-1")
    bot = FakeBot()

    await handler(message, bot)

    response = str(bot.calls[0][0])
    assert [call[0] for call in service.calls] == ["prepare_private", "prepare_group"]
    assert message.state.fsm.change_calls[-1][0] is MigrationWizardState.INPUT_GROUP_TOPIC_STRATEGY
    assert "- Topic 12" in response
    assert "... +" not in response


@pytest.mark.asyncio
async def test_cancel_handler_routes_source_chat_specific_cancellation():
    service = FakeBotControlService()
    collector = build_handler_collector(
        service=service,
        telegram_session_service=SimpleNamespace(),
        default_batch_size=20,
    )
    handler = collector._user_commands_handlers["/cancel"].handler_func
    message = FakeMessage(argument="chat-42")
    bot = FakeBot()

    await handler(message, bot)

    assert message.state.fsm.dropped is True
    assert service.calls[-1] == ("cancel_active_jobs", "chat-42", None)
    assert "source_chat_id=chat-42" in str(bot.calls[-1][0])


@pytest.mark.asyncio
async def test_list_chat_users_handler_renders_note_and_matrix_entries_for_channel():
    service = FakeBotControlService()
    service.list_chat_users_result = BotChatUserMatrixResult(
        source_chat_id="channel-1",
        source_chat_type="channel",
        source_chat_title="Release Notes",
        entries=(
            BotChatUserMatrixEntry(
                telegram_user_id="100",
                telegram_username="release.user",
                telegram_display_name="Release User",
                is_self=False,
                corporate_email="release.user@example.com",
                target_huid="huid-release-user",
                resolution_source="identity_map_cache",
                reason=None,
            ),
        ),
        note="Список пользователей канала best-effort и может быть неполным.",
    )
    collector = build_handler_collector(
        service=service,
        telegram_session_service=SimpleNamespace(),
        default_batch_size=20,
    )
    handler = collector._user_commands_handlers["/chat_users"].handler_func
    message = FakeMessage(argument="channel-1")
    bot = FakeBot()

    await handler(message, bot)

    rendered = str(bot.calls[0][0])
    assert "best-effort" in rendered
    assert "tg_id=100" in rendered
    assert "release.user@example.com" in rendered


@pytest.mark.asyncio
async def test_configure_handler_without_options_shows_or_bootstraps_configuration():
    service = FakeBotControlService()
    collector = build_handler_collector(
        service=service,
        telegram_session_service=SimpleNamespace(),
        default_batch_size=20,
    )
    handler = collector._user_commands_handlers["/configure"].handler_func
    message = FakeMessage(argument="channel-1")
    bot = FakeBot()

    await handler(message, bot)

    assert service.calls == [("show_or_configure_chat", "channel-1", None)]
    rendered = str(bot.calls[0][0])
    assert "migration_id=migration-1" in rendered
    assert "source_chat_id=channel-1" in rendered


@pytest.mark.asyncio
async def test_configure_handler_without_source_chat_id_lists_available_chats():
    service = FakeBotControlService()
    service.available_chats = (
        BotAvailableChat(
            source_chat_id="channel-1",
            source_chat_type="channel",
            source_chat_title="Release Notes",
            message_count=10,
            media_count=0,
            approximate_bytes=100,
            configured=False,
            has_progress=False,
            imported_count=0,
            mapped_total=0,
            last_source_message_id=None,
        ),
    )
    collector = build_handler_collector(
        service=service,
        telegram_session_service=SimpleNamespace(),
        default_batch_size=20,
    )
    handler = collector._user_commands_handlers["/configure"].handler_func
    message = FakeMessage(argument="")
    bot = FakeBot()

    await handler(message, bot)

    assert service.calls == [("list_available_chats", "", "telethon_user_session")]
    rendered = str(bot.calls[0][0])
    assert "/configure <source_chat_id>" in rendered
    assert "channel-1" in rendered


@pytest.mark.asyncio
async def test_migrate_all_handler_replies_on_recoverable_error():
    service = FakeBotControlService()
    service.migrate_all_error = RecoverableItemError("telethon service timeout: ReadTimeout")
    collector = build_handler_collector(
        service=service,
        telegram_session_service=SimpleNamespace(),
        default_batch_size=20,
    )
    handler = collector._user_commands_handlers["/migrate_all"].handler_func
    message = FakeMessage()
    bot = FakeBot()

    await handler(message, bot)

    assert "telethon service timeout: ReadTimeout" in str(bot.calls[0][0])


@pytest.mark.asyncio
async def test_select_chat_wizard_routes_channel_request_into_channel_state():
    service = FakeBotControlService()
    service.available_chats = (
        BotAvailableChat(
            source_chat_id="channel-1",
            source_chat_type="channel",
            source_chat_title="Release Notes",
            message_count=10,
            media_count=0,
            approximate_bytes=100,
            configured=False,
            has_progress=False,
            imported_count=0,
            mapped_total=0,
            last_source_message_id=None,
        ),
    )
    service.channel_request = BotChannelChatResolutionRequest(
        source_chat_id="channel-1",
        source_chat_title="Release Notes",
        action="choose_access_strategy",
        can_list_participants=True,
    )
    fsm = build_wizard_collector(service=service, default_batch_size=20)
    handler = _fsm_handler(fsm, MigrationWizardState.SELECT_CHAT)
    message = FakeMessage(body="channel-1")
    bot = FakeBot()

    await handler(message, bot)

    assert [call[0] for call in service.calls] == [
        "list_available_chats",
        "prepare_private",
        "prepare_group",
        "prepare_channel",
    ]
    assert message.state.fsm.change_calls[-1][0] is MigrationWizardState.INPUT_CHANNEL_ACCESS_STRATEGY


@pytest.mark.asyncio
async def test_channel_access_strategy_state_starts_command_flow_with_selected_strategy():
    service = FakeBotControlService()
    fsm = build_wizard_collector(service=service, default_batch_size=20)
    handler = _fsm_handler(fsm, MigrationWizardState.INPUT_CHANNEL_ACCESS_STRATEGY)
    message = FakeMessage(
        body="invite_link",
        storage={
            "source_chat_id": "channel-1",
            "flow_mode": "command",
            "identity_policy": "skip_unresolved",
        },
    )
    bot = FakeBot()

    await handler(message, bot)

    assert len(service.start_options) == 1
    assert service.start_options[0].access_strategy == "invite_link"
    assert service.start_options[0].identity_policy == "skip_unresolved"
    assert message.state.fsm.dropped is True
    assert "Wizard запустил перенос." in str(bot.calls[0][0])


@pytest.mark.asyncio
async def test_add_users_handler_with_direct_chat_selector_enters_workbook_state_without_existing_fsm_storage():
    service = FakeBotControlService()
    collector = build_handler_collector(
        service=service,
        telegram_session_service=SimpleNamespace(),
        default_batch_size=20,
    )
    handler = collector._user_commands_handlers["/add_users"].handler_func
    message = FakeMessage(argument="channel-1")
    message.state.fsm.storage = SimpleNamespace()
    bot = FakeBot()

    await handler(message, bot)

    assert service.calls == [("prepare_chat_members_workbook", "channel-1", None)]
    assert message.state.fsm.change_calls[-1][0] is MigrationWizardState.INPUT_CHAT_MEMBERS_WORKBOOK
    rendered = str(bot.calls[0][0])
    assert "Заполни матрицу пользователей" in rendered


@pytest.mark.asyncio
async def test_add_users_handler_without_selector_enters_selection_state():
    service = FakeBotControlService()
    collector = build_handler_collector(
        service=service,
        telegram_session_service=FakeTelegramSessionService(),
        default_batch_size=20,
    )
    handler = collector._user_commands_handlers["/add_users"].handler_func
    message = FakeMessage()
    bot = FakeBot()

    await handler(message, bot)

    assert service.calls == [("list_migrated_chats_for_user_addition", "", None)]
    assert message.state.fsm.change_calls[-1][0] is MigrationWizardState.SELECT_MEMBERS_CHAT
    assert "Добавление пользователей" in str(bot.calls[0][0])


@pytest.mark.asyncio
async def test_import_archive_handler_enters_archive_state():
    service = FakeBotControlService()
    collector = build_handler_collector(
        service=service,
        telegram_session_service=FakeTelegramSessionService(),
        default_batch_size=20,
    )
    handler = collector._user_commands_handlers["/import_archive"].handler_func
    message = FakeMessage()
    bot = FakeBot()

    await handler(message, bot)

    assert message.state.fsm.change_calls[-1][0] is MigrationWizardState.INPUT_ARCHIVE_IMPORT
    rendered = str(bot.calls[0][0])
    assert ".json" in rendered and ".zip" in rendered and ".rar" in rendered


@pytest.mark.asyncio
async def test_map_identity_handler_formats_mapping_result():
    service = FakeBotControlService()
    collector = build_handler_collector(
        service=service,
        telegram_session_service=FakeTelegramSessionService(),
        default_batch_size=20,
    )
    handler = collector._user_commands_handlers["/map_identity"].handler_func
    message = FakeMessage(argument="@peer.user peer.user@example.com")
    bot = FakeBot()

    await handler(message, bot)

    assert service.calls[-1] == ("map_identity", "peer.user@example.com", "@peer.user")
    rendered = str(bot.calls[0][0])
    assert "telegram_username=@peer.user" in rendered
    assert "target_huid=huid-peer-user" in rendered


@pytest.mark.asyncio
async def test_remigrate_handler_forces_resume_policy():
    service = FakeBotControlService()
    collector = build_handler_collector(
        service=service,
        telegram_session_service=FakeTelegramSessionService(),
        default_batch_size=20,
    )
    handler = collector._user_commands_handlers["/remigrate"].handler_func
    message = FakeMessage(argument="channel-1 progress=ask")
    bot = FakeBot()

    await handler(message, bot)

    assert service.calls[-1][0] == "remigrate_chat"
    assert service.start_options[-1].progress_policy == "resume"
    assert "job-remigrate" in str(bot.calls[0][0])


@pytest.mark.asyncio
async def test_retry_failed_handler_formats_result():
    service = FakeBotControlService()
    collector = build_handler_collector(
        service=service,
        telegram_session_service=FakeTelegramSessionService(),
        default_batch_size=20,
    )
    handler = collector._user_commands_handlers["/retry_failed"].handler_func
    message = FakeMessage(argument="channel-1 limit=10 batch=20")
    bot = FakeBot()

    await handler(message, bot)

    assert service.calls[-1] == ("replay_failed", "channel-1", (10, 20))
    rendered = str(bot.calls[0][0])
    assert "requested=10" in rendered
    assert "recovered=8" in rendered
