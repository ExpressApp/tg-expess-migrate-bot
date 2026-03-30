from __future__ import annotations

from dataclasses import dataclass
from enum import Enum, auto
from uuid import UUID

from pybotx import Bot, IncomingMessage, KeyboardMarkup
from pybotx.models.attachments import OutgoingAttachment
from pybotx_fsm import FSMCollector

from extg_bot_ui.application.bot_control import (
    BotAvailableChat,
    BotChatMembersAddResult,
    BotChatMembersWorkbookRequest,
    BotChannelChatResolutionRequest,
    BotGroupChatResolutionRequest,
    BotGroupIdentityImportResult,
    BotMigratedChat,
    BotPrivateChatResolutionRequest,
    BotOperationAcceptedResult,
    BotOperatorContext,
    BotProgressHint,
    MigrationBotControlService,
    MigrationRunOptions,
)
from extg_bot_ui.presentation.bot.command_parser import parse_arguments
from extg_shared.contracts.errors import (
    AmbiguousDeliveryError,
    ConfigurationError,
    FatalItemError,
    RecoverableItemError,
)

WIZARD_STATE_REPO_KEY = "extg_fsm_repo"
WIZARD_TTL_SECONDS = 3600
USER_VISIBLE_WIZARD_ERRORS = (
    ConfigurationError,
    FatalItemError,
    RecoverableItemError,
    AmbiguousDeliveryError,
)


class MigrationWizardState(Enum):
    SELECT_CHAT = auto()
    SELECT_MEMBERS_CHAT = auto()
    INPUT_CHAT_MEMBERS_WORKBOOK = auto()
    INPUT_ARCHIVE_IMPORT = auto()
    INPUT_PRIVATE_PEER = auto()
    INPUT_GROUP_TOPIC_STRATEGY = auto()
    INPUT_GROUP_IDENTITY_MATRIX = auto()
    INPUT_CHANNEL_IDENTITY_MATRIX = auto()
    INPUT_CHANNEL_ACCESS_STRATEGY = auto()
    INPUT_FROM = auto()
    INPUT_TO = auto()
    INPUT_MEDIA = auto()
    INPUT_REPLY = auto()
    CONFIRM = auto()


@dataclass(frozen=True, slots=True)
class PrivatePeerResolutionInput:
    corporate_email: str | None = None
    target_huid: str | None = None
    source: str = "unknown"


def build_wizard_collector(
    service: MigrationBotControlService,
    *,
    default_batch_size: int,
) -> FSMCollector:
    fsm = FSMCollector(MigrationWizardState)

    @fsm.on(MigrationWizardState.SELECT_CHAT)
    async def select_chat(message: IncomingMessage, bot: Bot) -> None:
        body = _message_text(message)
        choice = _normalized_choice(body)
        if _is_cancel(choice):
            await _cancel(message, bot)
            return
        if choice == "list":
            chats = await service.list_available_chats(
                operator=_operator_from_message(message),
                limit=10,
                query=None,
            )
            await bot.answer_message(_build_wizard_intro(chats), wait_callback=False)
            return
        if not body:
            await bot.answer_message(
                "Нужен source_chat_id. Отправь id чата, `list` или `cancel`.",
                wait_callback=False,
            )
            return

        try:
            chats = await service.list_available_chats(
                operator=_operator_from_message(message),
                limit=1000,
                query=None,
            )
        except USER_VISIBLE_WIZARD_ERRORS as error:
            await bot.answer_message(f"Ошибка: {error}", wait_callback=False)
            return
        if not any(chat.source_chat_id == body for chat in chats):
            await bot.answer_message(
                "Такой source_chat_id не найден. Отправь корректный id, `list` или `cancel`.",
                wait_callback=False,
            )
            return
        request = await service.prepare_private_chat_resolution(
            operator=_operator_from_message(message),
            source_chat_id=body,
            options=MigrationRunOptions(progress_policy="ask"),
        )
        if request is not None:
            await begin_private_chat_resolution(
                message=message,
                bot=bot,
                request=request,
                options=None,
            )
            return
        group_request = await service.prepare_group_chat_resolution(
            operator=_operator_from_message(message),
            source_chat_id=body,
            options=MigrationRunOptions(progress_policy="ask"),
        )
        if group_request is not None:
            await begin_group_chat_resolution(
                message=message,
                bot=bot,
                request=group_request,
                options=None,
            )
            return
        channel_request = await service.prepare_channel_chat_resolution(
            operator=_operator_from_message(message),
            source_chat_id=body,
            options=MigrationRunOptions(progress_policy="ask"),
        )
        if channel_request is not None:
            await begin_channel_chat_resolution(
                message=message,
                bot=bot,
                request=channel_request,
                options=None,
            )
            return
        await _continue_wizard_after_private_resolution(
            message=message,
            bot=bot,
            source_chat_id=body,
        )

    @fsm.on(MigrationWizardState.SELECT_MEMBERS_CHAT)
    async def select_members_chat(message: IncomingMessage, bot: Bot) -> None:
        choice = _normalized_choice(_message_text(message))
        if _is_cancel(choice):
            await _cancel(message, bot)
            return
        if choice == "list":
            chats = await service.list_migrated_chats_for_user_addition(
                operator=_operator_from_message(message),
            )
            await bot.answer_message(
                _build_members_wizard_intro(chats),
                wait_callback=False,
            )
            return
        try:
            chat_selector = _parse_chat_selection_message(message)
            request = await service.prepare_chat_members_workbook(
                operator=_operator_from_message(message),
                chat_selector=chat_selector,
            )
        except USER_VISIBLE_WIZARD_ERRORS as error:
            await bot.answer_message(f"Ошибка: {error}", wait_callback=False)
            return
        await begin_chat_members_workbook(
            message=message,
            bot=bot,
            request=request,
        )

    @fsm.on(MigrationWizardState.INPUT_CHAT_MEMBERS_WORKBOOK)
    async def input_chat_members_workbook(message: IncomingMessage, bot: Bot) -> None:
        choice = _normalized_choice(_message_text(message))
        if _is_cancel(choice):
            await _cancel(message, bot)
            return
        if message.file is None:
            await bot.answer_message(
                "Загрузи заполненный Excel-файл или отправь `/cancel`.",
                wait_callback=False,
            )
            return
        try:
            result = await service.add_chat_members_from_workbook(
                operator=_operator_from_message(message),
                chat_selector=message.state.fsm_storage.source_chat_id,
                workbook_content=message.file.content,
            )
        except USER_VISIBLE_WIZARD_ERRORS as error:
            await bot.answer_message(f"Ошибка: {error}", wait_callback=False)
            return
        await message.state.fsm.drop_state()
        await bot.answer_message(
            _format_chat_members_add_result(result),
            wait_callback=False,
        )

    @fsm.on(MigrationWizardState.INPUT_ARCHIVE_IMPORT)
    async def input_archive_import(message: IncomingMessage, bot: Bot) -> None:
        choice = _normalized_choice(_message_text(message))
        if _is_cancel(choice):
            await _cancel(message, bot)
            return
        if message.file is None:
            await bot.answer_message(
                "Загрузи Telegram export `.json`, `.zip` или `.rar`, либо отправь `/cancel`.",
                wait_callback=False,
            )
            return
        try:
            result = await service.start_archive_import(
                operator=_operator_from_message(message),
                archive_content=message.file.content,
                archive_filename=getattr(message.file, "filename", None),
            )
        except USER_VISIBLE_WIZARD_ERRORS as error:
            await bot.answer_message(f"Ошибка: {error}", wait_callback=False)
            return
        await message.state.fsm.drop_state()
        await bot.answer_message(
            (
                "Архив принят.\n"
                "Участники и вложения из архива не переносятся.\n\n"
                f"{_format_background_operation_for_wizard(result)}"
            ),
            wait_callback=False,
        )

    @fsm.on(MigrationWizardState.INPUT_PRIVATE_PEER)
    async def input_private_peer(message: IncomingMessage, bot: Bot) -> None:
        body = _message_text(message)
        choice = _normalized_choice(body)
        flow_mode = getattr(message.state.fsm_storage, "flow_mode", "wizard")
        if _is_skip(choice):
            if flow_mode == "command":
                await _continue_private_chat_command_without_peer(
                    message=message,
                    bot=bot,
                    service=service,
                    default_batch_size=default_batch_size,
                )
                return
            await _continue_wizard_after_private_resolution(
                message=message,
                bot=bot,
                source_chat_id=message.state.fsm_storage.source_chat_id,
            )
            return
        if _is_cancel(choice):
            await _cancel(message, bot)
            return
        try:
            parsed = _parse_private_peer_input(message)
            result = await service.map_private_chat_peer_identity(
                operator=_operator_from_message(message),
                source_chat_id=message.state.fsm_storage.source_chat_id,
                corporate_email=parsed.corporate_email,
                target_huid=parsed.target_huid,
            )
        except USER_VISIBLE_WIZARD_ERRORS as error:
            await bot.answer_message(
                f"Ошибка: {error}",
                wait_callback=False,
            )
            return
        if flow_mode == "command":
            await _continue_private_chat_command_after_mapping(
                message=message,
                bot=bot,
                service=service,
                result=result,
                default_batch_size=default_batch_size,
            )
            return
        await bot.answer_message(
            (
                "Маппинг сохранен.\n"
                f"target_huid={result.target_huid or '-'}\n"
                f"corporate_email={result.corporate_email or '-'}\n\n"
                "Шаг 2/5. Введи `from` в ISO-формате или `skip`."
            ),
            wait_callback=False,
        )
        await message.state.fsm.change_state(
            MigrationWizardState.INPUT_FROM,
            ttl_seconds=WIZARD_TTL_SECONDS,
            **_wizard_state_payload(
                message,
                source_chat_id=message.state.fsm_storage.source_chat_id,
            ),
        )

    @fsm.on(MigrationWizardState.INPUT_GROUP_TOPIC_STRATEGY)
    async def input_group_topic_strategy(message: IncomingMessage, bot: Bot) -> None:
        choice = _normalized_choice(_message_text(message))
        flow_mode = getattr(message.state.fsm_storage, "flow_mode", "wizard")
        if _is_cancel(choice):
            await _cancel(message, bot)
            return
        if choice not in {"single", "single_chat", "one", "one_chat", "split", "split_by_topic"}:
            await bot.answer_message(
                "Ожидаю `single_chat` или `split_by_topic`. Либо `cancel`.",
                wait_callback=False,
            )
            return
        normalized_topic_strategy = (
            "split_by_topic"
            if choice in {"split", "split_by_topic"}
            else "single_chat"
        )
        options = _options_from_flow_state(
            message,
            default_batch_size=default_batch_size,
            topic_strategy=normalized_topic_strategy,
        )
        group_request = await service.prepare_group_chat_resolution(
            operator=_operator_from_message(message),
            source_chat_id=message.state.fsm_storage.source_chat_id,
            options=options,
        )
        if group_request is not None:
            await begin_group_chat_resolution(
                message=message,
                bot=bot,
                request=group_request,
                options=options if flow_mode == "command" else None,
            )
            return
        if flow_mode == "command":
            await _start_command_migration_from_state(
                message=message,
                bot=bot,
                service=service,
                default_batch_size=default_batch_size,
                overrides={"topic_strategy": normalized_topic_strategy},
            )
            return
        await _continue_wizard_after_group_resolution(
            message=message,
            bot=bot,
            source_chat_id=message.state.fsm_storage.source_chat_id,
            topic_strategy=normalized_topic_strategy,
        )

    @fsm.on(MigrationWizardState.INPUT_GROUP_IDENTITY_MATRIX)
    async def input_group_identity_matrix(message: IncomingMessage, bot: Bot) -> None:
        choice = _normalized_choice(_message_text(message))
        flow_mode = getattr(message.state.fsm_storage, "flow_mode", "wizard")
        if _is_skip(choice):
            if flow_mode == "command":
                await _start_command_migration_from_state(
                    message=message,
                    bot=bot,
                    service=service,
                    default_batch_size=default_batch_size,
                    overrides={"identity_policy": "skip_unresolved"},
                )
                return
            await _continue_wizard_after_group_resolution(
                message=message,
                bot=bot,
                source_chat_id=message.state.fsm_storage.source_chat_id,
                identity_policy="skip_unresolved",
            )
            return
        if _is_cancel(choice):
            await _cancel(message, bot)
            return
        if message.file is None:
            await bot.answer_message(
                "Загрузи Excel-файл матрицы, отправь `/skip` для partial migration или `/cancel`.",
                wait_callback=False,
            )
            return
        try:
            import_result = await service.apply_group_identity_matrix(
                operator=_operator_from_message(message),
                source_chat_id=message.state.fsm_storage.source_chat_id,
                workbook_content=message.file.content,
            )
        except USER_VISIBLE_WIZARD_ERRORS as error:
            await bot.answer_message(f"Ошибка: {error}", wait_callback=False)
            return
        if flow_mode == "command":
            await _continue_group_command_after_matrix(
                message=message,
                bot=bot,
                service=service,
                import_result=import_result,
                default_batch_size=default_batch_size,
            )
            return
        await bot.answer_message(
            (
                "Матрица соответствий импортирована.\n"
                f"imported={import_result.imported_count}\n"
                f"skipped={import_result.skipped_count}\n\n"
                "Шаг 2/5. Введи `from` в ISO-формате или `skip`."
            ),
            wait_callback=False,
        )
        await message.state.fsm.change_state(
            MigrationWizardState.INPUT_FROM,
            ttl_seconds=WIZARD_TTL_SECONDS,
            **_wizard_state_payload(
                message,
                source_chat_id=message.state.fsm_storage.source_chat_id,
                identity_policy="matrix_uploaded",
            ),
        )

    @fsm.on(MigrationWizardState.INPUT_CHANNEL_IDENTITY_MATRIX)
    async def input_channel_identity_matrix(message: IncomingMessage, bot: Bot) -> None:
        choice = _normalized_choice(_message_text(message))
        flow_mode = getattr(message.state.fsm_storage, "flow_mode", "wizard")
        if _is_skip(choice):
            if flow_mode == "command":
                access_strategy = getattr(message.state.fsm_storage, "access_strategy", None)
                if access_strategy:
                    await _start_command_migration_from_state(
                        message=message,
                        bot=bot,
                        service=service,
                        default_batch_size=default_batch_size,
                        overrides={"identity_policy": "skip_unresolved"},
                    )
                    return
                await _prompt_channel_access_strategy(
                    message=message,
                    bot=bot,
                    identity_policy="skip_unresolved",
                )
                return
            await _prompt_channel_access_strategy(
                message=message,
                bot=bot,
                identity_policy="skip_unresolved",
            )
            return
        if _is_cancel(choice):
            await _cancel(message, bot)
            return
        if message.file is None:
            await bot.answer_message(
                "Загрузи Excel-файл матрицы, отправь `/skip` для partial migration или `/cancel`.",
                wait_callback=False,
            )
            return
        try:
            import_result = await service.apply_channel_identity_matrix(
                operator=_operator_from_message(message),
                source_chat_id=message.state.fsm_storage.source_chat_id,
                workbook_content=message.file.content,
            )
        except USER_VISIBLE_WIZARD_ERRORS as error:
            await bot.answer_message(f"Ошибка: {error}", wait_callback=False)
            return
        if flow_mode == "command":
            access_strategy = getattr(message.state.fsm_storage, "access_strategy", None)
            if access_strategy:
                await _start_command_migration_from_state(
                    message=message,
                    bot=bot,
                    service=service,
                    default_batch_size=default_batch_size,
                    overrides={"identity_policy": "matrix_uploaded"},
                )
                return
            await bot.answer_message(
                (
                    "Матрица соответствий импортирована.\n"
                    f"imported={import_result.imported_count}\n"
                    f"skipped={import_result.skipped_count}"
                ),
                wait_callback=False,
            )
            await _prompt_channel_access_strategy(
                message=message,
                bot=bot,
                identity_policy="matrix_uploaded",
            )
            return
        await bot.answer_message(
            (
                "Матрица соответствий импортирована.\n"
                f"imported={import_result.imported_count}\n"
                f"skipped={import_result.skipped_count}"
            ),
            wait_callback=False,
        )
        await _prompt_channel_access_strategy(
            message=message,
            bot=bot,
            identity_policy="matrix_uploaded",
        )

    @fsm.on(MigrationWizardState.INPUT_CHANNEL_ACCESS_STRATEGY)
    async def input_channel_access_strategy(message: IncomingMessage, bot: Bot) -> None:
        choice = _normalized_choice(_message_text(message))
        flow_mode = getattr(message.state.fsm_storage, "flow_mode", "wizard")
        if _is_cancel(choice):
            await _cancel(message, bot)
            return
        if choice not in {"direct_add", "direct", "invite_link", "invite", "link"}:
            await bot.answer_message(
                "Ожидаю `direct_add` или `invite_link`. Либо `cancel`.",
                wait_callback=False,
            )
            return
        normalized_access_strategy = (
            "invite_link" if choice in {"invite_link", "invite", "link"} else "direct_add"
        )
        if flow_mode == "command":
            await _start_command_migration_from_state(
                message=message,
                bot=bot,
                service=service,
                default_batch_size=default_batch_size,
                overrides={"access_strategy": normalized_access_strategy},
            )
            return
        await _continue_wizard_after_channel_resolution(
            message=message,
            bot=bot,
            source_chat_id=message.state.fsm_storage.source_chat_id,
            access_strategy=normalized_access_strategy,
        )

    @fsm.on(MigrationWizardState.INPUT_FROM)
    async def input_from(message: IncomingMessage, bot: Bot) -> None:
        body = _message_text(message)
        choice = _normalized_choice(body)
        if _is_cancel(choice):
            await _cancel(message, bot)
            return
        include_from = None if choice in {"", "skip"} else body
        await message.state.fsm.change_state(
            MigrationWizardState.INPUT_TO,
            ttl_seconds=WIZARD_TTL_SECONDS,
            **_wizard_state_payload(
                message,
                source_chat_id=message.state.fsm_storage.source_chat_id,
                include_from=include_from,
            ),
        )
        await bot.answer_message(
            "Шаг 3/5. Введи `to` в ISO-формате или `skip`.",
            wait_callback=False,
        )

    @fsm.on(MigrationWizardState.INPUT_TO)
    async def input_to(message: IncomingMessage, bot: Bot) -> None:
        body = _message_text(message)
        choice = _normalized_choice(body)
        if _is_cancel(choice):
            await _cancel(message, bot)
            return
        include_to = None if choice in {"", "skip"} else body
        await message.state.fsm.change_state(
            MigrationWizardState.INPUT_MEDIA,
            ttl_seconds=WIZARD_TTL_SECONDS,
            **_wizard_state_payload(
                message,
                source_chat_id=message.state.fsm_storage.source_chat_id,
                include_from=getattr(message.state.fsm_storage, "include_from", None),
                include_to=include_to,
            ),
        )
        await bot.answer_message(
            "Шаг 4/5. Переносить вложения? Ответь `on` или `off`.",
            wait_callback=False,
        )

    @fsm.on(MigrationWizardState.INPUT_MEDIA)
    async def input_media(message: IncomingMessage, bot: Bot) -> None:
        body = _message_text(message)
        choice = _normalized_choice(body)
        if _is_cancel(choice):
            await _cancel(message, bot)
            return
        migrate_media = _parse_on_off(choice)
        if migrate_media is None:
            await bot.answer_message(
                "Ожидаю `on` или `off`. Либо `cancel`.",
                wait_callback=False,
            )
            return
        await message.state.fsm.change_state(
            MigrationWizardState.INPUT_REPLY,
            ttl_seconds=WIZARD_TTL_SECONDS,
            **_wizard_state_payload(
                message,
                source_chat_id=message.state.fsm_storage.source_chat_id,
                include_from=getattr(message.state.fsm_storage, "include_from", None),
                include_to=getattr(message.state.fsm_storage, "include_to", None),
                migrate_media=migrate_media,
            ),
        )
        await bot.answer_message(
            "Шаг 5/5. Режим reply: `inline_quote`, `source_id` или `none`.",
            wait_callback=False,
        )

    @fsm.on(MigrationWizardState.INPUT_REPLY)
    async def input_reply(message: IncomingMessage, bot: Bot) -> None:
        body = _message_text(message)
        choice = _normalized_choice(body)
        if _is_cancel(choice):
            await _cancel(message, bot)
            return
        if choice not in {"inline_quote", "source_id", "none"}:
            await bot.answer_message(
                "Ожидаю `inline_quote`, `source_id` или `none`. Либо `cancel`.",
                wait_callback=False,
            )
            return
        await message.state.fsm.change_state(
            MigrationWizardState.CONFIRM,
            ttl_seconds=WIZARD_TTL_SECONDS,
            **_wizard_state_payload(
                message,
                source_chat_id=message.state.fsm_storage.source_chat_id,
                include_from=getattr(message.state.fsm_storage, "include_from", None),
                include_to=getattr(message.state.fsm_storage, "include_to", None),
                migrate_media=getattr(message.state.fsm_storage, "migrate_media", True),
                reply_mode=choice,
            ),
        )
        await bot.answer_message(
            _build_confirmation_prompt(
                source_chat_id=message.state.fsm_storage.source_chat_id,
                include_from=getattr(message.state.fsm_storage, "include_from", None),
                include_to=getattr(message.state.fsm_storage, "include_to", None),
                migrate_media=getattr(message.state.fsm_storage, "migrate_media", True),
                reply_mode=choice,
                access_strategy=getattr(message.state.fsm_storage, "access_strategy", None),
            ),
            wait_callback=False,
        )

    @fsm.on(MigrationWizardState.CONFIRM)
    async def confirm(message: IncomingMessage, bot: Bot) -> None:
        body = _message_text(message)
        choice = _normalized_choice(body)
        if _is_cancel(choice):
            await _cancel(message, bot)
            return
        if choice not in {"start", "resume"}:
            await bot.answer_message(
                "Отправь `start`, чтобы сохранить конфиг и запустить перенос, "
                "`resume`, чтобы явно продолжить найденный прогресс, или `cancel`.",
                wait_callback=False,
            )
            return

        run_options = MigrationRunOptions(
            include_from=getattr(message.state.fsm_storage, "include_from", None),
            include_to=getattr(message.state.fsm_storage, "include_to", None),
            migrate_media=getattr(message.state.fsm_storage, "migrate_media", True),
            reply_mode=getattr(message.state.fsm_storage, "reply_mode", "inline_quote"),
            source_backend=getattr(message.state.fsm_storage, "source_backend", None),
            topic_strategy=getattr(message.state.fsm_storage, "topic_strategy", None),
            identity_policy=getattr(message.state.fsm_storage, "identity_policy", None),
            access_strategy=getattr(message.state.fsm_storage, "access_strategy", None),
            progress_policy="resume" if choice == "resume" else "ask",
            batch_size=default_batch_size,
        )
        operator = _operator_from_message(message)
        source_chat_id = message.state.fsm_storage.source_chat_id

        try:
            await service.configure_chat(
                operator=operator,
                source_chat_id=source_chat_id,
                options=run_options,
            )
            result = await service.start_migrate_chat(
                operator=operator,
                source_chat_id=source_chat_id,
                options=run_options,
            )
        except USER_VISIBLE_WIZARD_ERRORS as error:
            await bot.answer_message(f"Ошибка: {error}", wait_callback=False)
            return

        if result.status == "blocked":
            await bot.answer_message(
                _format_blocked_result(result),
                wait_callback=False,
            )
            return

        await message.state.fsm.drop_state()
        await bot.answer_message(
            _format_start_result(result),
            wait_callback=False,
        )

    return fsm


def _build_wizard_intro(chats: tuple[BotAvailableChat, ...]) -> str:
    if not chats:
        return "Доступные Telegram-чаты не найдены."
    lines = [
        "Wizard переноса чата.",
        "Шаг 1/5. Выбери source_chat_id и отправь его следующим сообщением.",
        "Можно отправить `list`, чтобы показать список еще раз, или `cancel`.",
        "",
        "Доступные чаты:",
    ]
    for chat in chats[:10]:
        lines.append(
            f"- {chat.source_chat_id} | {chat.source_chat_title} "
            f"| configured={'yes' if chat.configured else 'no'} "
            f"| progress={'yes' if chat.has_progress else 'no'}",
        )
    if len(chats) > 10:
        lines.append(f"... +{len(chats) - 10} chats")
    return "\n".join(lines)


def _build_members_wizard_intro(chats: tuple[BotMigratedChat, ...]) -> str:
    if not chats:
        return "У тебя пока нет мигрированных чатов, доступных для добавления участников."
    lines = [
        "Добавление пользователей в уже созданный eXpress чат.",
        "Шаг 1/2. Отправь source_chat_id из списка ниже или mention целевого eXpress чата.",
        "Можно отправить `list`, чтобы показать список еще раз, или `cancel`.",
        "",
        "Доступные чаты:",
    ]
    for chat in chats[:10]:
        lines.append(
            f"- {chat.source_chat_id} | {chat.source_chat_type} | {chat.source_chat_title} "
            f"| target_chat_id={chat.target_chat_id} | target_chat_title={chat.target_chat_title}",
        )
    if len(chats) > 10:
        lines.append(f"... +{len(chats) - 10} chats")
    return "\n".join(lines)


async def begin_chat_members_selection(
    *,
    message: IncomingMessage,
    bot: Bot,
    chats: tuple[BotMigratedChat, ...],
) -> None:
    if not chats:
        await bot.answer_message(
            _build_members_wizard_intro(chats),
            wait_callback=False,
        )
        return
    await message.state.fsm.change_state(
        MigrationWizardState.SELECT_MEMBERS_CHAT,
        ttl_seconds=WIZARD_TTL_SECONDS,
        **_wizard_state_payload(message),
    )
    await bot.answer_message(
        _build_members_wizard_intro(chats),
        wait_callback=False,
    )


async def begin_archive_import(
    *,
    message: IncomingMessage,
    bot: Bot,
) -> None:
    await message.state.fsm.change_state(
        MigrationWizardState.INPUT_ARCHIVE_IMPORT,
        ttl_seconds=WIZARD_TTL_SECONDS,
        **_wizard_state_payload(message),
    )
    await bot.answer_message(
        (
            "Загрузи Telegram export archive или `result.json`.\n"
            "Поддерживаются `.json`, `.zip`, `.rar`.\n"
            "Будут перенесены только сообщения из архива: без участников и без вложений.\n"
            "Инициатор миграции станет администратором созданного чата.\n"
            "`/cancel` остановит flow."
        ),
        wait_callback=False,
    )


async def begin_chat_members_workbook(
    *,
    message: IncomingMessage,
    bot: Bot,
    request: BotChatMembersWorkbookRequest,
) -> None:
    await message.state.fsm.change_state(
        MigrationWizardState.INPUT_CHAT_MEMBERS_WORKBOOK,
        ttl_seconds=WIZARD_TTL_SECONDS,
        **_wizard_state_payload(
            message,
            source_chat_id=request.source_chat_id,
        ),
    )
    lines = [
        "Заполни матрицу пользователей и загрузи ее обратно.",
        f"source_chat_id={request.source_chat_id}",
        f"source_chat_title={request.source_chat_title}",
        f"target_chat_id={request.target_chat_id}",
        f"target_chat_title={request.target_chat_title}",
        f"access_strategy={request.access_strategy}",
        f"mapped_rows={request.mapped_rows}",
        f"unresolved_rows={request.unresolved_rows}",
        "",
        "Можно заполнить одну или несколько колонок: `corporate_email`, `target_huid`, `ad_login`, `other_id`.",
        "Если заполнено несколько, приоритет такой: `target_huid`, затем `corporate_email`, затем `ad_login`, затем `other_id`.",
        "Можно добавлять и новых пользователей: оставь Telegram поля пустыми и заполни только корпоративный идентификатор.",
        "`/cancel` остановит flow.",
    ]
    if request.note:
        lines.extend(["", request.note])
    await bot.answer_message(
        "\n".join(lines),
        wait_callback=False,
        file=OutgoingAttachment(
            content=request.workbook_content,
            filename=request.workbook_filename,
        ),
    )


def _format_chat_members_add_result(result: BotChatMembersAddResult) -> str:
    lines = [
        "Пользователи обработаны.",
        f"source_chat_id={result.source_chat_id}",
        f"target_chat_id={result.target_chat_id}",
        f"target_chat_title={result.target_chat_title}",
        f"requested_access_strategy={result.access_strategy}",
        f"effective_result={result.effective_result}",
    ]
    if result.invite_fallback_used:
        lines.append(
            "note=direct_add could not add all users; invite_link fallback was used",
        )
    lines.extend(
        [
            f"processed_rows={result.processed_rows}",
            f"imported_identity_mappings={result.imported_identity_mappings}",
            f"mapping_skipped_rows={result.mapping_skipped_rows}",
            f"resolved_targets={result.resolved_targets}",
            f"direct_added={result.direct_added}",
            f"invited={result.invited}",
            f"skipped_rows={result.skipped_rows}",
        ],
    )
    if result.failed_targets:
        lines.append(f"failed_to_add={len(result.failed_targets)}")
        lines.extend(
            f"failed_user={label}"
            for label in result.failed_targets
        )
    return "\n".join(lines)


def _format_background_operation_for_wizard(result: BotOperationAcceptedResult) -> str:
    lines = [
        "Задача принята." if result.status == "accepted" else "Новая задача не запущена.",
        f"operation={result.operation}",
        f"migration_id={result.migration_id}",
    ]
    if result.job_key:
        lines.append(f"job_key={result.job_key}")
    if result.source_chat_ids:
        lines.append(f"source_chat_ids={','.join(result.source_chat_ids)}")
    if result.reason:
        lines.extend(["", f"reason={result.reason}"])
    lines.extend(["", "Проверка прогресса: /status"])
    return "\n".join(lines)


async def begin_private_chat_resolution(
    *,
    message: IncomingMessage,
    bot: Bot,
    request: BotPrivateChatResolutionRequest,
    options: MigrationRunOptions | None,
) -> None:
    payload = {
        "source_chat_id": request.source_chat_id,
        "flow_mode": "command" if options is not None else "wizard",
    }
    if options is not None:
        payload.update(
            {
                "include_from": options.include_from,
                "include_to": options.include_to,
                "migrate_media": options.migrate_media,
                "reply_mode": options.reply_mode,
                "source_backend": options.source_backend,
                "target_strategy": options.target_strategy,
                "target_title": options.target_title,
                "target_chat_id": options.target_chat_id,
                "identity_policy": options.identity_policy,
                "progress_policy": options.progress_policy,
                "batch_size": options.batch_size,
            },
        )
    await message.state.fsm.change_state(
        MigrationWizardState.INPUT_PRIVATE_PEER,
        ttl_seconds=WIZARD_TTL_SECONDS,
        **payload,
    )
    username = (
        f"@{request.peer_telegram_username}"
        if request.peer_telegram_username
        else "-"
    )
    await bot.answer_message(
        (
            "Для персонального чата второй участник пока не сопоставлен.\n"
            f"source_chat_id={request.source_chat_id}\n"
            f"peer_name={request.peer_display_name}\n"
            f"peer_username={username}\n"
            f"peer_tg_id={request.peer_telegram_user_id or '-'}\n\n"
            "Отправь mention пользователя в eXpress, corporate email или huid. "
            "Отправь `/skip`, чтобы продолжить перенос без второго участника, "
            "или `/cancel`, чтобы остановить wizard."
        ),
        wait_callback=False,
        keyboard=_private_peer_keyboard(),
    )


async def begin_group_chat_resolution(
    *,
    message: IncomingMessage,
    bot: Bot,
    request: BotGroupChatResolutionRequest,
    options: MigrationRunOptions | None,
) -> None:
    payload = {
        "source_chat_id": request.source_chat_id,
        "flow_mode": "command" if options is not None else "wizard",
    }
    if options is not None:
        payload.update(
            {
                "include_from": options.include_from,
                "include_to": options.include_to,
                "migrate_media": options.migrate_media,
                "reply_mode": options.reply_mode,
                "source_backend": options.source_backend,
                "target_strategy": options.target_strategy,
                "target_title": options.target_title,
                "target_chat_id": options.target_chat_id,
                "identity_policy": options.identity_policy,
                "topic_strategy": options.topic_strategy,
                "progress_policy": options.progress_policy,
                "batch_size": options.batch_size,
            },
        )
    if request.action == "choose_topic_strategy":
        await message.state.fsm.change_state(
            MigrationWizardState.INPUT_GROUP_TOPIC_STRATEGY,
            ttl_seconds=WIZARD_TTL_SECONDS,
            **payload,
        )
        lines = [
            "Для этой супергруппы обнаружены Telegram topics.",
            f"source_chat_id={request.source_chat_id}",
            f"source_chat_title={request.source_chat_title}",
            "",
            "Выбери стратегию: `single_chat` или `split_by_topic`.",
        ]
        if request.topic_titles:
            lines.extend(["", "Темы:"])
            for title in request.topic_titles[:10]:
                lines.append(f"- {title}")
            if len(request.topic_titles) > 10:
                lines.append(f"... +{len(request.topic_titles) - 10} topics")
        await bot.answer_message(
            "\n".join(lines),
            wait_callback=False,
            keyboard=_group_topic_keyboard(),
        )
        return

    await message.state.fsm.change_state(
        MigrationWizardState.INPUT_GROUP_IDENTITY_MATRIX,
        ttl_seconds=WIZARD_TTL_SECONDS,
        **payload,
    )
    file = None
    if request.workbook_content is not None:
        file = OutgoingAttachment(
            content=request.workbook_content,
            filename=request.workbook_filename or "identity_matrix.xlsx",
        )
    await bot.answer_message(
        (
            "Есть участники Telegram-чата, которых пока не удалось сопоставить с eXpress.\n"
            f"source_chat_id={request.source_chat_id}\n"
            f"resolved={request.resolved_count}\n"
            f"unresolved={request.unresolved_count}\n\n"
            "Заполни Excel и загрузи его обратно, или отправь `/skip`, чтобы продолжить partial migration. "
            "`/cancel` остановит flow."
        ),
        wait_callback=False,
        file=file,
        keyboard=_group_identity_keyboard(),
    )


async def begin_channel_chat_resolution(
    *,
    message: IncomingMessage,
    bot: Bot,
    request: BotChannelChatResolutionRequest,
    options: MigrationRunOptions | None,
) -> None:
    payload = {
        "source_chat_id": request.source_chat_id,
        "flow_mode": "command" if options is not None else "wizard",
    }
    if options is not None:
        payload.update(
            {
                "include_from": options.include_from,
                "include_to": options.include_to,
                "migrate_media": options.migrate_media,
                "reply_mode": options.reply_mode,
                "source_backend": options.source_backend,
                "target_strategy": options.target_strategy,
                "target_title": options.target_title,
                "target_chat_id": options.target_chat_id,
                "identity_policy": options.identity_policy,
                "access_strategy": options.access_strategy,
                "topic_strategy": options.topic_strategy,
                "progress_policy": options.progress_policy,
                "batch_size": options.batch_size,
            },
        )
    if request.action == "choose_access_strategy":
        await message.state.fsm.change_state(
            MigrationWizardState.INPUT_CHANNEL_ACCESS_STRATEGY,
            ttl_seconds=WIZARD_TTL_SECONDS,
            **payload,
        )
        lines = [
            "Для Telegram-канала выбери стратегию выдачи доступа.",
            f"source_chat_id={request.source_chat_id}",
            f"source_chat_title={request.source_chat_title}",
            "",
            "`direct_add` — попытаться добавить сопоставленных пользователей напрямую.",
            "`invite_link` — создать канал и подготовить инвайт-ссылку как fallback.",
        ]
        if not request.can_list_participants:
            lines.extend(
                [
                    "",
                    "Telegram не гарантирует полный список подписчиков. "
                    "Доступ будет выдан в best-effort режиме.",
                ],
            )
        await bot.answer_message(
            "\n".join(lines),
            wait_callback=False,
            keyboard=_channel_access_keyboard(),
        )
        return

    await message.state.fsm.change_state(
        MigrationWizardState.INPUT_CHANNEL_IDENTITY_MATRIX,
        ttl_seconds=WIZARD_TTL_SECONDS,
        **payload,
    )
    file = None
    if request.workbook_content is not None:
        file = OutgoingAttachment(
            content=request.workbook_content,
            filename=request.workbook_filename or "channel_identity_matrix.xlsx",
        )
    lines = [
        "Есть пользователи Telegram-канала, которых пока не удалось сопоставить с eXpress.",
        f"source_chat_id={request.source_chat_id}",
        f"resolved={request.resolved_count}",
        f"unresolved={request.unresolved_count}",
        "",
        "Заполни Excel и загрузи его обратно, или отправь `/skip`, чтобы продолжить partial migration.",
        "`/cancel` остановит flow.",
    ]
    if not request.can_list_participants:
        lines.extend(
            [
                "",
                "Список подписчиков канала может быть неполным: Telegram отдает его только в best-effort режиме.",
            ],
        )
    await bot.answer_message(
        "\n".join(lines),
        wait_callback=False,
        file=file,
        keyboard=_group_identity_keyboard(),
    )


def _build_confirmation_prompt(
    *,
    source_chat_id: str,
    include_from: str | None,
    include_to: str | None,
    migrate_media: bool,
    reply_mode: str,
    access_strategy: str | None = None,
) -> str:
    lines = [
        "Проверь параметры:",
        f"source_chat_id={source_chat_id}",
        f"include_from={include_from or '-'}",
        f"include_to={include_to or '-'}",
        f"migrate_media={'on' if migrate_media else 'off'}",
        f"reply_mode={reply_mode}",
    ]
    if access_strategy:
        lines.append(f"access_strategy={access_strategy}")
    lines.extend(
        [
            "",
            "Отправь `start`, чтобы запустить перенос с проверкой existing progress, "
            "`resume`, чтобы явно продолжить уже начатый перенос, или `cancel`.",
        ],
    )
    return "\n".join(lines)


def _format_blocked_result(result: BotOperationAcceptedResult) -> str:
    lines = [
        "Обнаружен существующий прогресс. Wizard не продолжил перенос автоматически.",
        "",
        "Найденный прогресс:",
    ]
    for hint in result.progress_hints:
        lines.append(_format_progress_hint(hint))
    lines.append("")
    lines.append("Отправь `resume`, чтобы продолжить, или `cancel`.")
    return "\n".join(lines)


def _format_start_result(result: BotOperationAcceptedResult) -> str:
    if result.status == "already_migrated":
        lines = [
            "Чат уже был перенесен. Wizard не запускал новую задачу.",
            f"migration_id={result.migration_id}",
            f"source_chat_id={','.join(result.source_chat_ids)}",
        ]
        if result.topic_targets:
            lines.append("target_chats_by_topic:")
            for topic_target in result.topic_targets:
                lines.append(
                    f"- topic={topic_target.topic_title} "
                    f"| target_chat_id={topic_target.target_chat_id} "
                    f"| target_chat_link={topic_target.target_chat_link or '-'}",
                )
        else:
            lines.append(f"target_chat_id={result.target_chat_id or '-'}")
            if result.target_chat_link:
                lines.append(f"target_chat_link={result.target_chat_link}")
        return "\n".join(lines)
    if result.status == "accepted":
        return (
            "Wizard запустил перенос.\n"
            f"migration_id={result.migration_id}\n"
            f"job_key={result.job_key}\n"
            f"source_chat_id={','.join(result.source_chat_ids)}"
        )
    if result.status == "already_running":
        return "Новая задача не запущена: уже есть активный bot-managed job."
    return result.reason or "Операция заблокирована."


def _format_progress_hint(hint: BotProgressHint) -> str:
    return (
        f"- {hint.source_chat_id}: mapped={hint.mapped_total} "
        f"imported={hint.imported_count} "
        f"last_source_message_id={hint.last_source_message_id or '-'}"
    )


def _operator_from_message(message: IncomingMessage) -> BotOperatorContext:
    sender = message.sender
    username = getattr(sender, "username", None)
    ad_login = getattr(sender, "ad_login", None)
    ad_domain = getattr(sender, "ad_domain", None)
    display_name = ad_login or username or str(sender.huid)
    bot = getattr(message, "bot", None)
    bot_id = getattr(bot, "id", None)
    return BotOperatorContext(
        huid=str(sender.huid),
        chat_id=str(message.chat.id),
        username=username,
        ad_login=ad_login,
        ad_domain=ad_domain,
        display_name=display_name,
        current_cts_host=getattr(bot, "host", None),
        current_bot_id=str(bot_id) if bot_id is not None else None,
    )


async def _cancel(message: IncomingMessage, bot: Bot) -> None:
    await message.state.fsm.drop_state()
    await bot.answer_message("Wizard остановлен.", wait_callback=False)


def _message_text(message: IncomingMessage) -> str:
    return (message.body or "").strip()


def _normalized_choice(value: str) -> str:
    return value.strip().lower()


def _parse_on_off(value: str) -> bool | None:
    if value in {"on", "true", "1", "yes"}:
        return True
    if value in {"off", "false", "0", "no"}:
        return False
    return None


def _is_cancel(value: str) -> bool:
    return value in {"cancel", "/cancel"}


def _is_skip(value: str) -> bool:
    return value in {"skip", "/skip"}


async def _continue_wizard_after_private_resolution(
    *,
    message: IncomingMessage,
    bot: Bot,
    source_chat_id: str,
) -> None:
    await message.state.fsm.change_state(
        MigrationWizardState.INPUT_FROM,
        ttl_seconds=WIZARD_TTL_SECONDS,
        **_wizard_state_payload(
            message,
            source_chat_id=source_chat_id,
        ),
    )
    await bot.answer_message(
        "Шаг 2/5. Введи `from` в ISO-формате или `skip`.",
        wait_callback=False,
    )


async def _continue_wizard_after_group_resolution(
    *,
    message: IncomingMessage,
    bot: Bot,
    source_chat_id: str,
    identity_policy: str | None = None,
    topic_strategy: str | None = None,
) -> None:
    await message.state.fsm.change_state(
        MigrationWizardState.INPUT_FROM,
        ttl_seconds=WIZARD_TTL_SECONDS,
        **_wizard_state_payload(
            message,
            source_chat_id=source_chat_id,
            identity_policy=identity_policy or getattr(message.state.fsm_storage, "identity_policy", None),
            topic_strategy=topic_strategy or getattr(message.state.fsm_storage, "topic_strategy", None),
        ),
    )
    await bot.answer_message(
        "Шаг 2/5. Введи `from` в ISO-формате или `skip`.",
        wait_callback=False,
    )


async def _prompt_channel_access_strategy(
    *,
    message: IncomingMessage,
    bot: Bot,
    identity_policy: str | None = None,
) -> None:
    await message.state.fsm.change_state(
        MigrationWizardState.INPUT_CHANNEL_ACCESS_STRATEGY,
        ttl_seconds=WIZARD_TTL_SECONDS,
        **_wizard_state_payload(
            message,
            identity_policy=identity_policy or getattr(message.state.fsm_storage, "identity_policy", None),
        ),
    )
    await bot.answer_message(
        (
            "Выбери стратегию доступа для канала.\n"
            "`direct_add` — попытаться добавить сопоставленных пользователей напрямую.\n"
            "`invite_link` — подготовить инвайт-ссылку и продолжить best-effort migration."
        ),
        wait_callback=False,
        keyboard=_channel_access_keyboard(),
    )


async def _continue_wizard_after_channel_resolution(
    *,
    message: IncomingMessage,
    bot: Bot,
    source_chat_id: str,
    access_strategy: str,
) -> None:
    await message.state.fsm.change_state(
        MigrationWizardState.INPUT_FROM,
        ttl_seconds=WIZARD_TTL_SECONDS,
        **_wizard_state_payload(
            message,
            source_chat_id=source_chat_id,
            access_strategy=access_strategy,
        ),
    )
    await bot.answer_message(
        "Шаг 2/5. Введи `from` в ISO-формате или `skip`.",
        wait_callback=False,
    )


async def _continue_private_chat_command_without_peer(
    *,
    message: IncomingMessage,
    bot: Bot,
    service: MigrationBotControlService,
    default_batch_size: int,
) -> None:
    options = _options_from_flow_state(
        message,
        default_batch_size=default_batch_size,
    )
    try:
        result = await service.start_migrate_chat(
            operator=_operator_from_message(message),
            source_chat_id=message.state.fsm_storage.source_chat_id,
            options=options,
        )
    except USER_VISIBLE_WIZARD_ERRORS as error:
        await bot.answer_message(f"Ошибка: {error}", wait_callback=False)
        return
    await message.state.fsm.drop_state()
    await bot.answer_message(
        _format_blocked_result(result)
        if result.status == "blocked"
        else _format_start_result(result),
        wait_callback=False,
    )


async def _continue_private_chat_command_after_mapping(
    *,
    message: IncomingMessage,
    bot: Bot,
    service: MigrationBotControlService,
    result,
    default_batch_size: int,
) -> None:
    options = _options_from_flow_state(
        message,
        default_batch_size=default_batch_size,
    )
    try:
        start_result = await service.start_migrate_chat(
            operator=_operator_from_message(message),
            source_chat_id=message.state.fsm_storage.source_chat_id,
            options=options,
        )
    except USER_VISIBLE_WIZARD_ERRORS as error:
        await bot.answer_message(f"Ошибка: {error}", wait_callback=False)
        return
    await message.state.fsm.drop_state()
    lines = [
        "Маппинг второго участника сохранен.",
        f"target_huid={result.target_huid or '-'}",
        f"corporate_email={result.corporate_email or '-'}",
        "",
        _format_blocked_result(start_result)
        if start_result.status == "blocked"
        else _format_start_result(start_result),
    ]
    await bot.answer_message("\n".join(lines), wait_callback=False)


async def _continue_group_command_after_matrix(
    *,
    message: IncomingMessage,
    bot: Bot,
    service: MigrationBotControlService,
    import_result: BotGroupIdentityImportResult,
    default_batch_size: int,
) -> None:
    try:
        start_result = await service.start_migrate_chat(
            operator=_operator_from_message(message),
            source_chat_id=message.state.fsm_storage.source_chat_id,
            options=_options_from_flow_state(
                message,
                default_batch_size=default_batch_size,
                identity_policy="matrix_uploaded",
            ),
        )
    except USER_VISIBLE_WIZARD_ERRORS as error:
        await bot.answer_message(f"Ошибка: {error}", wait_callback=False)
        return
    await message.state.fsm.drop_state()
    lines = [
        "Матрица соответствий импортирована.",
        f"imported={import_result.imported_count}",
        f"skipped={import_result.skipped_count}",
        "",
        _format_blocked_result(start_result)
        if start_result.status == "blocked"
        else _format_start_result(start_result),
    ]
    await bot.answer_message("\n".join(lines), wait_callback=False)


async def _start_command_migration_from_state(
    *,
    message: IncomingMessage,
    bot: Bot,
    service: MigrationBotControlService,
    default_batch_size: int,
    overrides: dict[str, object],
) -> None:
    try:
        result = await service.start_migrate_chat(
            operator=_operator_from_message(message),
            source_chat_id=message.state.fsm_storage.source_chat_id,
            options=_options_from_flow_state(
                message,
                default_batch_size=default_batch_size,
                **overrides,
            ),
        )
    except USER_VISIBLE_WIZARD_ERRORS as error:
        await bot.answer_message(f"Ошибка: {error}", wait_callback=False)
        return
    await message.state.fsm.drop_state()
    await bot.answer_message(
        _format_blocked_result(result) if result.status == "blocked" else _format_start_result(result),
        wait_callback=False,
    )


def _options_from_flow_state(
    message: IncomingMessage,
    *,
    default_batch_size: int,
    **overrides: object,
) -> MigrationRunOptions:
    return MigrationRunOptions(
        include_from=overrides.get(
            "include_from",
            getattr(message.state.fsm_storage, "include_from", None),
        ),
        include_to=overrides.get(
            "include_to",
            getattr(message.state.fsm_storage, "include_to", None),
        ),
        migrate_media=overrides.get(
            "migrate_media",
            getattr(message.state.fsm_storage, "migrate_media", None),
        ),
        reply_mode=overrides.get(
            "reply_mode",
            getattr(message.state.fsm_storage, "reply_mode", None),
        ),
        source_backend=overrides.get(
            "source_backend",
            getattr(message.state.fsm_storage, "source_backend", None),
        ),
        target_strategy=overrides.get(
            "target_strategy",
            getattr(message.state.fsm_storage, "target_strategy", None),
        ),
        target_title=overrides.get(
            "target_title",
            getattr(message.state.fsm_storage, "target_title", None),
        ),
        target_chat_id=overrides.get(
            "target_chat_id",
            getattr(message.state.fsm_storage, "target_chat_id", None),
        ),
        identity_policy=overrides.get(
            "identity_policy",
            getattr(message.state.fsm_storage, "identity_policy", None),
        ),
        access_strategy=overrides.get(
            "access_strategy",
            getattr(message.state.fsm_storage, "access_strategy", None),
        ),
        topic_strategy=overrides.get(
            "topic_strategy",
            getattr(message.state.fsm_storage, "topic_strategy", None),
        ),
        progress_policy=str(
            overrides.get(
                "progress_policy",
                getattr(message.state.fsm_storage, "progress_policy", "resume"),
            ),
        ),
        batch_size=int(
            overrides.get(
                "batch_size",
                getattr(message.state.fsm_storage, "batch_size", None) or default_batch_size,
            ),
        ),
    )


def _parse_private_peer_input(
    message: IncomingMessage,
) -> PrivatePeerResolutionInput:
    mention_users = list(getattr(getattr(message, "mentions", None), "users", []))
    if mention_users:
        return PrivatePeerResolutionInput(
            target_huid=str(mention_users[0].entity_id),
            source="mention",
        )
    body = _message_text(message)
    if not body:
        raise ConfigurationError("нужен mention, corporate email, huid, /skip или /cancel")
    parsed = parse_arguments(body)
    email = parsed.options.get("email")
    huid = parsed.options.get("huid") or parsed.options.get("user_huid")
    if email is None and huid is None and parsed.positionals:
        token = parsed.positionals[0]
        if _looks_like_email(token):
            email = token
        else:
            huid = token
    if email and huid:
        raise ConfigurationError("укажи либо email, либо huid/mention")
    if email:
        return PrivatePeerResolutionInput(
            corporate_email=email.strip(),
            source="email",
        )
    if huid:
        try:
            normalized_huid = str(UUID(huid.strip()))
        except ValueError as error:
            raise ConfigurationError("huid должен быть корректным UUID") from error
        return PrivatePeerResolutionInput(
            target_huid=normalized_huid,
            source="huid",
        )
    raise ConfigurationError("ожидаю mention пользователя в eXpress, corporate email или huid")


def _parse_chat_selection_message(message: IncomingMessage) -> str:
    mentions = getattr(message, "mentions", None)
    mention_chats = list(getattr(mentions, "chats", []))
    if mention_chats:
        return str(mention_chats[0].entity_id)
    mention_channels = list(getattr(mentions, "channels", []))
    if mention_channels:
        return str(mention_channels[0].entity_id)
    body = _message_text(message)
    if not body:
        raise ConfigurationError(
            "нужен source_chat_id или mention целевого eXpress чата; можно также отправить `list` или `/cancel`",
        )
    return body


def _looks_like_email(value: str) -> bool:
    candidate = value.strip()
    return "@" in candidate and " " not in candidate


def _private_peer_keyboard() -> KeyboardMarkup:
    keyboard = KeyboardMarkup()
    keyboard.add_button(
        "Пропустить второго участника",
        command="/skip",
        silent=False,
    )
    keyboard.add_button(
        "Отменить wizard",
        command="/cancel",
        silent=False,
        new_row=False,
    )
    return keyboard


def _group_topic_keyboard() -> KeyboardMarkup:
    keyboard = KeyboardMarkup()
    keyboard.add_button("Один чат", command="single_chat", silent=False)
    keyboard.add_button("По топикам", command="split_by_topic", silent=False, new_row=False)
    keyboard.add_button("Отменить", command="/cancel", silent=False)
    return keyboard


def _group_identity_keyboard() -> KeyboardMarkup:
    keyboard = KeyboardMarkup()
    keyboard.add_button("Пропустить и продолжить", command="/skip", silent=False)
    keyboard.add_button("Отменить", command="/cancel", silent=False, new_row=False)
    return keyboard


def _channel_access_keyboard() -> KeyboardMarkup:
    keyboard = KeyboardMarkup()
    keyboard.add_button("Добавить напрямую", command="direct_add", silent=False)
    keyboard.add_button("Через ссылку", command="invite_link", silent=False, new_row=False)
    keyboard.add_button("Отменить", command="/cancel", silent=False)
    return keyboard


def _wizard_state_payload(message: IncomingMessage, **updates: object) -> dict[str, object]:
    fsm_storage = getattr(message.state, "fsm_storage", None)
    payload = {
        "flow_mode": getattr(fsm_storage, "flow_mode", "wizard"),
        "source_backend": getattr(fsm_storage, "source_backend", None),
        "target_strategy": getattr(fsm_storage, "target_strategy", None),
        "target_title": getattr(fsm_storage, "target_title", None),
        "target_chat_id": getattr(fsm_storage, "target_chat_id", None),
        "identity_policy": getattr(fsm_storage, "identity_policy", None),
        "access_strategy": getattr(fsm_storage, "access_strategy", None),
        "topic_strategy": getattr(fsm_storage, "topic_strategy", None),
        "progress_policy": getattr(fsm_storage, "progress_policy", "ask"),
        "batch_size": getattr(fsm_storage, "batch_size", None),
    }
    payload.update(updates)
    return payload
