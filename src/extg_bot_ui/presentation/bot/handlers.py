from __future__ import annotations

from dataclasses import replace
from datetime import UTC

from pybotx import Bot, IncomingMessage
from pybotx.bot.handler_collector import HandlerCollector

from extg_bot_ui.application.bot_control import (
    BotActiveJob,
    BotAvailableChat,
    BotCancelResult,
    BotChatConfigurationResult,
    BotChatMembersAddResult,
    BotChatUserMatrixResult,
    BotIdentityMappingResult,
    BotMigratedChat,
    BotMigrationStatsResult,
    BotOperationAcceptedResult,
    BotOperatorContext,
    BotProgressHint,
    BotChatCheckpointStatus,
    MigrationBotControlService,
    MigrationBotStatusResult,
    MigrationRunOptions,
)
from extg_telethon_service.application.telegram_session_service import (
    TelegramDisconnectResult,
    TelegramSessionService,
    TelegramSessionStatusResult,
)
from extg_migration_runtime.application.use_cases.replay_failed import (
    ReplayFailedResult,
)
from extg_shared.contracts.errors import ConfigurationError, FatalItemError
from extg_shared.contracts.errors import (
    AmbiguousDeliveryError,
    RecoverableItemError,
)
from extg_bot_ui.presentation.bot.command_parser import (
    CommandArgumentError,
    parse_list_chat_users_options,
    parse_list_chats_options,
    parse_map_identity_arguments,
    parse_migrate_all_options,
    parse_migrate_chat_options,
    parse_optional_migrate_chat_options,
    parse_optional_source_chat_id,
    parse_replay_failed_options,
)
from extg_bot_ui.presentation.bot.wizard import (
    begin_archive_import,
    begin_chat_members_selection,
    begin_chat_members_workbook,
    begin_channel_chat_resolution,
    begin_group_chat_resolution,
    begin_private_chat_resolution,
)
from extg_bot_ui.presentation.bot.telegram_session_wizard import (
    begin_telegram_connection,
)

USER_VISIBLE_OPERATION_ERRORS = (
    ConfigurationError,
    FatalItemError,
    RecoverableItemError,
    AmbiguousDeliveryError,
)
USER_VISIBLE_COMMAND_ERRORS = (CommandArgumentError,) + USER_VISIBLE_OPERATION_ERRORS


def build_handler_collector(
    service: MigrationBotControlService,
    *,
    telegram_session_service: TelegramSessionService,
    default_batch_size: int,
) -> HandlerCollector:
    collector = HandlerCollector()

    async def _handle_help(bot: Bot) -> None:
        await _reply(bot, _help_text())

    async def _handle_connect(message: IncomingMessage, bot: Bot) -> None:
        await begin_telegram_connection(
            message=message,
            bot=bot,
            service=telegram_session_service,
            phone_number=(message.argument or "").strip() or None,
        )

    async def _handle_telegram_status(message: IncomingMessage, bot: Bot) -> None:
        try:
            result = await telegram_session_service.status(
                operator_huid=str(message.sender.huid),
            )
        except USER_VISIBLE_OPERATION_ERRORS as error:
            await _reply(bot, f"Ошибка: {error}")
            return
        await _reply(bot, _format_telegram_status_result(result))

    async def _handle_disconnect(message: IncomingMessage, bot: Bot) -> None:
        try:
            result = await telegram_session_service.disconnect(
                operator_huid=str(message.sender.huid),
            )
        except USER_VISIBLE_OPERATION_ERRORS as error:
            await _reply(bot, f"Ошибка: {error}")
            return
        await message.state.fsm.drop_state()
        await _reply(bot, _format_telegram_disconnect_result(result))

    @collector.command("/help", description="Показать команды управления переносом")
    async def help_command(message: IncomingMessage, bot: Bot) -> None:
        await _handle_help(bot)

    @collector.command("/connect", description="Подключить свою Telegram УЗ")
    async def connect_telegram(message: IncomingMessage, bot: Bot) -> None:
        await _handle_connect(message, bot)

    @collector.command("/account", description="Показать статус подключенной Telegram УЗ")
    async def telegram_status(message: IncomingMessage, bot: Bot) -> None:
        await _handle_telegram_status(message, bot)

    @collector.command("/disconnect", description="Отключить свою Telegram УЗ")
    async def disconnect_telegram(message: IncomingMessage, bot: Bot) -> None:
        await _handle_disconnect(message, bot)

    @collector.command("/cancel", description="Остановить активный wizard/flow")
    async def cancel_command(message: IncomingMessage, bot: Bot) -> None:
        await message.state.fsm.drop_state()
        try:
            source_chat_id = parse_optional_source_chat_id(message.argument)
            result = await service.cancel_active_jobs(
                operator=_operator_from_message(message),
                source_chat_id=source_chat_id,
            )
        except USER_VISIBLE_COMMAND_ERRORS as error:
            await _reply(bot, f"Wizard остановлен.\nОшибка: {error}")
            return
        await _reply(bot, _format_cancel_result(result))

    @collector.command("/status", description="Показать статус миграции")
    async def migration_status(message: IncomingMessage, bot: Bot) -> None:
        try:
            source_chat_id = parse_optional_source_chat_id(message.argument)
            result = await service.status(
                operator=_operator_from_message(message),
                source_chat_id=source_chat_id,
            )
        except USER_VISIBLE_COMMAND_ERRORS as error:
            await _reply(bot, f"Ошибка: {error}")
            return
        await _reply(bot, _format_status_result(result))

    @collector.command("/stats", description="Показать общую статистику по миграции")
    async def migration_stats(message: IncomingMessage, bot: Bot) -> None:
        try:
            result = await service.stats(
                operator=_operator_from_message(message),
            )
        except USER_VISIBLE_OPERATION_ERRORS as error:
            await _reply(bot, f"Ошибка: {error}")
            return
        await _reply(bot, _format_stats_result(result))

    @collector.command("/chats", description="Показать список доступных Telegram-чатов")
    async def list_chats(message: IncomingMessage, bot: Bot) -> None:
        try:
            limit, query = parse_list_chats_options(message.argument)
            result = await service.list_available_chats(
                operator=_operator_from_message(message),
                limit=limit,
                query=query,
            )
        except USER_VISIBLE_COMMAND_ERRORS as error:
            await _reply(bot, f"Ошибка: {error}")
            return
        await _reply(bot, _format_available_chats(result, limit=limit))

    @collector.command("/configure", description="Показать или сохранить конфигурацию переноса")
    async def configure_chat(message: IncomingMessage, bot: Bot) -> None:
        try:
            source_chat_id, options = parse_optional_migrate_chat_options(message.argument)
            if source_chat_id is None:
                chats = await service.list_available_chats(
                    operator=_operator_from_message(message),
                    limit=10,
                    query=None,
                )
                await _reply(bot, _format_configure_chat_selector(chats))
                return
            if _is_show_configuration_request(options):
                result = await service.show_or_configure_chat(
                    operator=_operator_from_message(message),
                    source_chat_id=source_chat_id,
                )
            else:
                result = await service.configure_chat(
                    operator=_operator_from_message(message),
                    source_chat_id=source_chat_id,
                    options=options,
                )
        except USER_VISIBLE_COMMAND_ERRORS as error:
            await _reply(bot, f"Ошибка: {error}")
            return
        await _reply(bot, _format_chat_configuration(result))

    @collector.command("/chat_users", description="Показать список пользователей одного Telegram-чата")
    async def list_chat_users(message: IncomingMessage, bot: Bot) -> None:
        try:
            source_chat_id = parse_list_chat_users_options(message.argument)
            result = await service.list_chat_users(
                operator=_operator_from_message(message),
                source_chat_id=source_chat_id,
            )
        except USER_VISIBLE_COMMAND_ERRORS as error:
            await _reply(bot, f"Ошибка: {error}")
            return
        await _reply(bot, _format_chat_user_matrix(result))

    @collector.command("/add_users", description="Добавить пользователей в уже мигрированный чат")
    async def add_users_to_existing_chat(message: IncomingMessage, bot: Bot) -> None:
        try:
            chat_selector = parse_optional_source_chat_id(message.argument)
            if chat_selector is None:
                chats = await service.list_migrated_chats_for_user_addition(
                    operator=_operator_from_message(message),
                )
                await begin_chat_members_selection(
                    message=message,
                    bot=bot,
                    chats=chats,
                )
                return
            request = await service.prepare_chat_members_workbook(
                operator=_operator_from_message(message),
                chat_selector=chat_selector,
            )
        except USER_VISIBLE_COMMAND_ERRORS as error:
            await _reply(bot, f"Ошибка: {error}")
            return
        await begin_chat_members_workbook(
            message=message,
            bot=bot,
            request=request,
        )

    @collector.command(
        "/import_archive",
        description="Импортировать Telegram export archive без Telethon",
    )
    async def import_archive(message: IncomingMessage, bot: Bot) -> None:
        await begin_archive_import(
            message=message,
            bot=bot,
        )

    @collector.command("/map_identity", description="Сохранить соответствие Telegram username и corporate email")
    async def map_identity(message: IncomingMessage, bot: Bot) -> None:
        try:
            mapping = parse_map_identity_arguments(message.argument)
            result = await service.map_identity(
                operator=_operator_from_message(message),
                telegram_user_id=mapping.telegram_user_id,
                telegram_username=mapping.telegram_username,
                telegram_display_name=mapping.telegram_display_name,
                corporate_email=mapping.corporate_email,
            )
        except USER_VISIBLE_COMMAND_ERRORS as error:
            await _reply(bot, f"Ошибка: {error}")
            return
        await _reply(bot, _format_identity_mapping_result(result))

    @collector.command("/migrate_all", description="Запустить перенос всех чатов")
    async def migrate_all(message: IncomingMessage, bot: Bot) -> None:
        try:
            options = parse_migrate_all_options(message.argument)
            result = await service.start_migrate_all(
                operator=_operator_from_message(message),
                options=options,
            )
        except USER_VISIBLE_COMMAND_ERRORS as error:
            await _reply(bot, f"Ошибка: {error}")
            return
        await _reply(
            bot,
            _format_background_operation(
                result,
                resume_hint="/migrate_all progress=resume",
            ),
        )

    @collector.command("/migrate", description="Запустить перенос одного чата")
    async def migrate_chat(message: IncomingMessage, bot: Bot) -> None:
        try:
            source_chat_id, options = parse_migrate_chat_options(message.argument)
            resolution_request = await service.prepare_private_chat_resolution(
                operator=_operator_from_message(message),
                source_chat_id=source_chat_id,
                options=options,
            )
            if resolution_request is not None:
                await begin_private_chat_resolution(
                    message=message,
                    bot=bot,
                    request=resolution_request,
                    options=options,
                )
                return
            group_resolution_request = await service.prepare_group_chat_resolution(
                operator=_operator_from_message(message),
                source_chat_id=source_chat_id,
                options=options,
            )
            if group_resolution_request is not None:
                await begin_group_chat_resolution(
                    message=message,
                    bot=bot,
                    request=group_resolution_request,
                    options=options,
                )
                return
            channel_resolution_request = await service.prepare_channel_chat_resolution(
                operator=_operator_from_message(message),
                source_chat_id=source_chat_id,
                options=options,
            )
            if channel_resolution_request is not None:
                await begin_channel_chat_resolution(
                    message=message,
                    bot=bot,
                    request=channel_resolution_request,
                    options=options,
                )
                return
            result = await service.start_migrate_chat(
                operator=_operator_from_message(message),
                source_chat_id=source_chat_id,
                options=options,
            )
        except USER_VISIBLE_COMMAND_ERRORS as error:
            await _reply(bot, f"Ошибка: {error}")
            return
        await _reply(
            bot,
            _format_background_operation(
                result,
                resume_hint=f"/migrate {source_chat_id}",
            ),
        )

    @collector.command("/remigrate", description="Повторно запустить перенос чата с текущего смещения")
    async def remigrate_chat(message: IncomingMessage, bot: Bot) -> None:
        try:
            source_chat_id, options = parse_migrate_chat_options(message.argument)
            options = options if options.progress_policy == "resume" else replace(options, progress_policy="resume")
            result = await service.remigrate_chat(
                operator=_operator_from_message(message),
                source_chat_id=source_chat_id,
                options=options,
            )
        except USER_VISIBLE_COMMAND_ERRORS as error:
            await _reply(bot, f"Ошибка: {error}")
            return
        await _reply(
            bot,
            _format_background_operation(
                result,
                resume_hint=f"/remigrate {source_chat_id}",
            ),
        )

    @collector.command("/retry_failed", description="Повторно отправить failed сообщения")
    async def replay_failed(message: IncomingMessage, bot: Bot) -> None:
        try:
            source_chat_id, limit, batch_size = parse_replay_failed_options(
                message.argument,
                default_limit=100,
                default_batch_size=default_batch_size,
            )
            result = await service.replay_failed(
                operator=_operator_from_message(message),
                source_chat_id=source_chat_id,
                limit=limit,
                batch_size=batch_size,
            )
        except USER_VISIBLE_COMMAND_ERRORS as error:
            await _reply(bot, f"Ошибка: {error}")
            return
        await _reply(bot, _format_replay_failed_result(result))

    @collector.default_message_handler
    async def default_message(message: IncomingMessage, bot: Bot) -> None:
        await _reply(
            bot,
            "Неизвестная команда.\n\n" + _help_text(),
        )

    return collector


async def _reply(bot: Bot, body: str) -> None:
    await bot.answer_message(body, wait_callback=False)


def _is_show_configuration_request(options: object) -> bool:
    if not isinstance(options, MigrationRunOptions):
        return False
    return (
        options.include_from is None
        and options.include_to is None
        and options.migrate_media is None
        and options.reply_mode is None
        and options.source_backend is None
        and options.target_strategy is None
        and options.target_title is None
        and options.target_chat_id is None
        and options.identity_policy is None
        and options.access_strategy is None
        and options.topic_strategy is None
        and options.skip_in_all is None
        and options.progress_policy == "resume"
        and options.batch_size is None
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


def _format_background_operation(
    result: BotOperationAcceptedResult,
    *,
    resume_hint: str,
) -> str:
    if result.status == "already_migrated":
        lines = [
            "Чат уже был перенесен. Новый запуск не выполнялся.",
            f"migration_id={result.migration_id}",
            f"source_chat_id={','.join(result.source_chat_ids)}",
        ]
        if result.topic_targets:
            lines.append("target_chats_by_topic:")
            for topic_target in result.topic_targets:
                lines.append(
                    f"- topic={topic_target.topic_title} "
                    f"| target_chat_id={topic_target.target_chat_id} "
                    f"| target_chat_title={topic_target.target_chat_title} "
                    f"| target_chat_link={topic_target.target_chat_link or '-'}",
                )
        else:
            lines.append(f"target_chat_id={result.target_chat_id or '-'}")
            if result.target_chat_title:
                lines.append(f"target_chat_title={result.target_chat_title}")
            if result.target_chat_link:
                lines.append(f"target_chat_link={result.target_chat_link}")
        return "\n".join(lines)
    if result.status == "accepted":
        job_keys = result.job_keys or ((result.job_key,) if result.job_key else ())
        accepted_chat_ids = result.accepted_source_chat_ids or result.source_chat_ids
        if len(job_keys) <= 1 and not result.blocked_source_chat_ids and not result.already_running_source_chat_ids:
            chats = ", ".join(result.source_chat_ids) if result.source_chat_ids else "all"
            return (
                f"Задача принята.\n"
                f"operation={result.operation}\n"
                f"migration_id={result.migration_id}\n"
                f"job_key={result.job_key}\n"
                f"source_chat_ids={chats}\n\n"
                "Проверка прогресса: /status"
            )
        lines = [
            "Задачи приняты.",
            f"operation={result.operation}",
            f"migration_id={result.migration_id}",
            f"accepted_jobs={len(job_keys)}",
            f"accepted_source_chat_ids={','.join(accepted_chat_ids) or '-'}",
        ]
        if result.blocked_source_chat_ids:
            lines.append(
                f"blocked_source_chat_ids={','.join(result.blocked_source_chat_ids)}",
            )
        if result.already_running_source_chat_ids:
            lines.append(
                "already_running_source_chat_ids="
                + ",".join(result.already_running_source_chat_ids),
            )
        if job_keys:
            if len(job_keys) == 1:
                lines.append(f"job_key={job_keys[0]}")
            else:
                lines.append("job_keys:")
                for job_key in job_keys[:10]:
                    lines.append(f"- {job_key}")
                if len(job_keys) > 10:
                    lines.append(f"... +{len(job_keys) - 10} jobs")
        if result.progress_hints:
            lines.extend(["", _format_progress_hints(result.progress_hints)])
            lines.append(f"Чтобы явно продолжить заблокированные чаты: {resume_hint}")
        lines.extend(["", "Проверка прогресса: /status"])
        return "\n".join(lines)
    if result.status == "already_running":
        lines = ["Новая задача не запущена: уже есть активный bot-managed job."]
        if result.already_running_source_chat_ids:
            lines.append(
                "already_running_source_chat_ids="
                + ",".join(result.already_running_source_chat_ids),
            )
        if result.active_jobs:
            lines.extend(["", _format_active_jobs(result.active_jobs)])
        if result.blocked_source_chat_ids:
            lines.append("")
            lines.append(
                "Дополнительно заблокированы чаты: "
                + ",".join(result.blocked_source_chat_ids),
            )
        return "\n".join(lines)
    if result.progress_hints:
        return (
            "Обнаружен существующий прогресс. Автозапуск остановлен до подтверждения.\n\n"
            + _format_progress_hints(result.progress_hints)
            + f"\n\nЧтобы явно продолжить: {resume_hint}"
        )
    return f"Операция заблокирована: {result.reason or 'unknown'}"


def _format_available_chats(
    chats: tuple[BotAvailableChat, ...],
    *,
    limit: int = 20,
) -> str:
    if not chats:
        return "Доступные Telegram-чаты не найдены."
    render_limit = max(1, limit)
    lines = ["Доступные чаты:"]
    for chat in chats[:render_limit]:
        lines.append(
            f"- {chat.source_chat_id} | {chat.source_chat_type} | {chat.source_chat_title} "
            f"| configured={'yes' if chat.configured else 'no'} "
            f"| progress={'yes' if chat.has_progress else 'no'} "
            f"| imported={chat.imported_count}/{chat.mapped_total or 0}"
        )
    if len(chats) > render_limit:
        lines.append(f"... +{len(chats) - render_limit} chats")
    return "\n".join(lines)


def _format_chat_configuration(result: BotChatConfigurationResult) -> str:
    lines = [
        f"migration_id={result.migration_id}",
        f"source_chat_id={result.source_chat_id}",
        f"source_chat_type={result.source_chat_type}",
        f"source_chat_title={result.source_chat_title}",
        f"target_strategy={result.target_strategy}",
        f"access={result.access_strategy}",
        f"format={result.reply_mode}",
        f"media={'on' if result.migrate_media else 'off'}",
        f"skip_in_all={'yes' if result.skip_in_all else 'no'}",
        f"target_title={result.target_title or '-'}",
        f"target_chat_id={result.target_chat_id or '-'}",
        f"include_from={result.include_from or '-'}",
        f"include_to={result.include_to or '-'}",
    ]
    if result.topic_strategy != "single_chat":
        lines.append(f"topic_strategy={result.topic_strategy}")
    if result.telegram_chat_id:
        lines.append(f"telegram_chat_id={result.telegram_chat_id}")
    if result.source_thread_id:
        lines.append(f"source_thread_id={result.source_thread_id}")
    if result.source_thread_title:
        lines.append(f"source_thread_title={result.source_thread_title}")
    if result.progress_hint is not None:
        lines.extend(
            [
                "",
                "Найден существующий прогресс:",
                (
                    f"mapped={result.progress_hint.mapped_total} "
                    f"imported={result.progress_hint.imported_count} "
                    f"last_source_message_id={result.progress_hint.last_source_message_id or '-'}"
                ),
            ],
        )
    return "\n".join(lines)


def _format_identity_mapping_result(result: BotIdentityMappingResult) -> str:
    return (
        f"telegram_user_id={result.telegram_user_id or '-'}\n"
        f"telegram_username={('@' + result.telegram_username) if result.telegram_username else '-'}\n"
        f"telegram_display_name={result.telegram_display_name or '-'}\n"
        f"corporate_email={result.corporate_email or '-'}\n"
        f"target_huid={result.target_huid or '-'}\n"
        f"resolution_source={result.resolution_source}\n"
        f"reason={result.reason or '-'}"
    )


def _format_chat_user_matrix(result: BotChatUserMatrixResult) -> str:
    lines = [
        f"source_chat_id={result.source_chat_id}",
        f"source_chat_type={result.source_chat_type}",
        f"source_chat_title={result.source_chat_title}",
        f"users={len(result.entries)}",
    ]
    if result.note:
        lines.extend(["", result.note])
    if result.entries:
        lines.append("")
    for entry in result.entries[:50]:
        username = f"@{entry.telegram_username}" if entry.telegram_username else "-"
        lines.append(
            f"- tg_id={entry.telegram_user_id or '-'} | username={username} | "
            f"name={entry.telegram_display_name} | email={entry.corporate_email or '-'} | "
            f"huid={entry.target_huid or '-'} | mapped={'yes' if entry.target_huid else 'no'}"
        )
        if entry.target_huid is None:
            lines.append(
                "  map: /map_identity "
                f"tg_id={entry.telegram_user_id or '<telegram_user_id>'} "
                f"{'username=' + entry.telegram_username + ' ' if entry.telegram_username else ''}"
                "email=<corporate_email>"
            )
    if len(result.entries) > 50:
        lines.append(f"... +{len(result.entries) - 50} users")
    return "\n".join(lines)


def _format_migrated_chats(chats: tuple[BotMigratedChat, ...]) -> str:
    if not chats:
        return "Мигрированные чаты для добавления пользователей не найдены."
    lines = ["Мигрированные чаты:"]
    for chat in chats[:20]:
        lines.append(
            f"- {chat.source_chat_id} | {chat.source_chat_type} | {chat.source_chat_title} "
            f"| target_chat_id={chat.target_chat_id} | target_chat_title={chat.target_chat_title}",
        )
    if len(chats) > 20:
        lines.append(f"... +{len(chats) - 20} chats")
    return "\n".join(lines)


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


def _format_status_result(result: MigrationBotStatusResult) -> str:
    lines = [
        f"migration_id={result.migration_id}",
        f"migration_state={(result.migration_state.value if result.migration_state else 'active')}",
        (
            f"active_jobs={len(result.active_jobs)}"
            if result.active_jobs
            else "active_jobs=0"
        ),
        (
            f"attention_chats={result.reconcile.attention_chats}/{result.reconcile.chats_total}"
        ),
        (
            f"messages imported={result.reconcile.imported_count} "
            f"failed={result.reconcile.failed_count} "
            f"ambiguous={result.reconcile.ambiguous_count} "
            f"processing={result.reconcile.processing_count}"
        ),
        (
            f"attachments imported={result.reconcile.attachment_imported_count} "
            f"failed={result.reconcile.attachment_failed_count} "
            f"ambiguous={result.reconcile.attachment_ambiguous_count}"
        ),
    ]
    if result.active_jobs:
        lines.extend(["", _format_active_jobs(result.active_jobs)])
    if result.chat_checkpoints:
        lines.extend(["", _format_chat_checkpoints(result.chat_checkpoints)])
    return "\n".join(lines)


def _format_stats_result(result: BotMigrationStatsResult) -> str:
    lines = [
        f"migration_id={result.migration_id}",
        f"configured_chats={result.configured_chats}",
        f"foreign_managed_chats={result.foreign_managed_chats}",
        f"skipped_in_all={result.skipped_in_all_chats}",
        f"chats_with_progress={result.chats_with_progress}",
        f"active_jobs={result.active_jobs}",
        f"attention_chats={result.attention_chats}",
        (
            f"messages imported={result.imported_messages} "
            f"failed={result.failed_messages} ambiguous={result.ambiguous_messages}"
        ),
        f"attachments imported={result.imported_attachments} failed={result.failed_attachments}",
    ]
    if result.foreign_managed_chats:
        lines.append(
            "note=some source chats are already configured by other operators and are excluded from your stats",
        )
    return "\n".join(lines)


def _format_cancel_result(result: BotCancelResult) -> str:
    lines = ["Wizard остановлен."]
    scope_suffix = (
        f" для source_chat_id={result.source_chat_id}"
        if result.source_chat_id
        else ""
    )
    if not result.cancelled_queued_jobs and not result.cancellation_requested_jobs:
        lines.append(f"Активных bot-managed задач пользователя{scope_suffix} не найдено.")
        return "\n".join(lines)
    if result.source_chat_id:
        lines.append(f"Фильтр отмены: source_chat_id={result.source_chat_id}")
    if result.cancelled_queued_jobs:
        lines.append("")
        lines.append("Сняты из очереди:")
        for job in result.cancelled_queued_jobs:
            lines.append(f"- {job.job_key} | chats={','.join(job.source_chat_ids)}")
    if result.cancellation_requested_jobs:
        lines.append("")
        lines.append("Запрошена остановка выполняющихся задач:")
        for job in result.cancellation_requested_jobs:
            lines.append(f"- {job.job_key} | chats={','.join(job.source_chat_ids)}")
        lines.append("Подожди несколько секунд и проверь /status.")
    return "\n".join(lines)


def _format_replay_failed_result(result: ReplayFailedResult) -> str:
    return (
        f"migration_id={result.migration_id}\n"
        f"requested={result.requested_count}\n"
        f"recovered={result.recovered_count}\n"
        f"failed={result.failed_count}\n"
        f"ambiguous={result.ambiguous_count}\n"
        f"missing={result.missing_count}\n"
        f"skipped={result.skipped_count}"
    )


def _format_configure_chat_selector(chats: tuple[BotAvailableChat, ...]) -> str:
    if not chats:
        return "Доступные Telegram-чаты не найдены. Проверь /connect и затем попробуй /chats."
    lines = [
        "Укажи source_chat_id: `/configure <source_chat_id>`.",
        "Первые доступные чаты:",
    ]
    for chat in chats[:10]:
        lines.append(
            f"- {chat.source_chat_id} | {chat.source_chat_title} "
            f"| configured={'yes' if chat.configured else 'no'}",
        )
    if len(chats) > 10:
        lines.append(f"... +{len(chats) - 10} chats")
    lines.extend(
        [
            "",
            "Подсказки:",
            "- `/configure <source_chat_id>` без опций покажет существующий config или создаст новый с дефолтами.",
            "- `/configure <source_chat_id> access=invite format=source_id media=off` сохранит указанные параметры.",
            "- Для полного списка используй `/chats`.",
        ],
    )
    return "\n".join(lines)


def _format_progress_hints(progress_hints: tuple[BotProgressHint, ...]) -> str:
    lines = ["Найденный прогресс:"]
    for hint in progress_hints:
        lines.append(
            f"- {hint.source_chat_id}: mapped={hint.mapped_total} "
            f"imported={hint.imported_count} last_source_message_id={hint.last_source_message_id or '-'}"
        )
    return "\n".join(lines)


def _format_telegram_status_result(result: TelegramSessionStatusResult) -> str:
    return (
        f"connected={'yes' if result.connected else 'no'}\n"
        f"source={result.source}\n"
        f"phone_number={result.phone_number or '-'}\n"
        f"telegram_user_id={result.telegram_user_id or '-'}\n"
        f"telegram_username={result.telegram_username or '-'}\n"
        f"telegram_display_name={result.telegram_display_name or '-'}\n"
        f"session_bound={'yes' if result.session_bound else 'no'}\n"
        f"updated_at={result.updated_at or '-'}\n"
        f"last_used_at={result.last_used_at or '-'}"
    )


def _format_telegram_disconnect_result(result: TelegramDisconnectResult) -> str:
    if result.disconnected:
        return "Telegram session disconnected."
    return "Привязанная Telegram session не найдена."


def _format_active_jobs(active_jobs: tuple[BotActiveJob, ...]) -> str:
    lines = ["Активные задачи:"]
    for job in active_jobs:
        started_at = job.started_at.astimezone(UTC).strftime("%Y-%m-%d %H:%M:%S UTC")
        lines.append(
            f"- {job.job_key} | operation={job.operation} | started_at={started_at} | chats={','.join(job.source_chat_ids)}"
        )
    return "\n".join(lines)


def _format_chat_checkpoints(checkpoints: tuple[BotChatCheckpointStatus, ...]) -> str:
    lines = ["Checkpoints:"]
    for checkpoint in checkpoints:
        lines.append(
            f"- {checkpoint.source_chat_id}: "
            f"backfill={checkpoint.backfill_last_source_message_id or '-'} "
            f"delta={checkpoint.delta_last_source_message_id or '-'}"
        )
    return "\n".join(lines)


def _help_text() -> str:
    return (
        "Команды:\n"
        "/connect [PHONE]\n"
        "/account\n"
        "/disconnect\n"
        "/chats [query=TEXT] [limit=N]\n"
        "/chat_users <source_chat_id>\n"
        "/add_users [source_chat_id]\n"
        "/import_archive\n"
        "/configure [source_chat_id] [access=invite|link] [format=quote|source_id|none] [media=on|off] [skip=on|off] [from=ISO] [to=ISO]\n"
        "/migrate <source_chat_id>\n"
        "/remigrate <source_chat_id>\n"
        "/migrate_all\n"
        "/status [source_chat_id]\n"
        "/stats\n"
        "/retry_failed [source_chat_id] [limit=N] [batch=N]\n"
        "/cancel [source_chat_id]\n\n"
        "Поведение по умолчанию:\n"
        "- /migrate_all автоматически создаст недостающие конфиги с access=link и стандартным форматом.\n"
        "- /configure без аргументов покажет доступные source_chat_id.\n"
        "- /configure <source_chat_id> без опций покажет текущую конфигурацию, а если ее еще нет — создаст с дефолтами.\n"
        "- /add_users откроет отдельный flow добавления пользователей в уже мигрированный target chat через Excel-матрицу.\n"
        "- /import_archive откроет отдельный flow: загрузи Telegram export (.json/.zip/.rar), бот создаст чат, сделает инициатора админом и перенесет только текстовые сообщения без участников и без вложений.\n"
        "- /migrate и /remigrate продолжают перенос с текущего безопасного смещения.\n"
        "- /cancel без аргументов останавливает wizard и запрашивает отмену всех ваших активных фоновых задач; /cancel <source_chat_id> отменяет только задачи по одному чату.\n"
        "- Если для чата нужны дополнительные шаги по участникам или доступу, бот сам запросит их в диалоге."
    )
