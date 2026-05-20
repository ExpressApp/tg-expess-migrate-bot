from __future__ import annotations

from enum import Enum, auto

from pybotx import Bot, IncomingMessage
from pybotx_fsm import FSMCollector

from extg_bot_ui.presentation.bot.menus import show_connected_menu
from extg_bot_ui.presentation.bot.screen import current_screen_payload, render_screen
from extg_telethon_service.application.telegram_session_service import (
    TelegramConnectionChallengeResult,
    TelegramPasswordChallengeResult,
    TelegramSessionService,
    TelegramSessionStatusResult,
)
from extg_shared.contracts.errors import (
    AmbiguousDeliveryError,
    ConfigurationError,
    FatalItemError,
    RecoverableItemError,
)

TELEGRAM_SESSION_WIZARD_TTL_SECONDS = 900
USER_VISIBLE_SESSION_ERRORS = (
    ConfigurationError,
    FatalItemError,
    RecoverableItemError,
    AmbiguousDeliveryError,
)


class TelegramSessionWizardState(Enum):
    INPUT_PHONE = auto()
    INPUT_CODE = auto()
    INPUT_PASSWORD = auto()


def build_telegram_session_wizard_collector(
    service: TelegramSessionService,
) -> FSMCollector:
    fsm = FSMCollector(TelegramSessionWizardState)

    @fsm.on(TelegramSessionWizardState.INPUT_PHONE)
    async def input_phone(message: IncomingMessage, bot: Bot) -> None:
        raw_choice = (message.body or "").strip()
        if _is_cancel(raw_choice.lower()):
            await _cancel(message, bot)
            return
        await begin_telegram_connection(
            message=message,
            bot=bot,
            service=service,
            phone_number=raw_choice,
        )

    @fsm.on(TelegramSessionWizardState.INPUT_CODE)
    async def input_code(message: IncomingMessage, bot: Bot) -> None:
        choice = _normalized_body(message)
        if _is_cancel(choice):
            await _cancel(message, bot)
            return
        if not choice:
            await _reply(message, bot, "Введите код из Telegram или отправьте `/cancel`.")
            return
        try:
            result = await service.complete_code(
                operator_huid=str(message.sender.huid),
                challenge_id=message.state.fsm_storage.challenge_id,
                code=choice,
            )
        except USER_VISIBLE_SESSION_ERRORS as error:
            await _reply(message, bot, f"Ошибка: {error}")
            return
        if isinstance(result, TelegramPasswordChallengeResult):
            await message.state.fsm.change_state(
                TelegramSessionWizardState.INPUT_PASSWORD,
                ttl_seconds=TELEGRAM_SESSION_WIZARD_TTL_SECONDS,
                challenge_id=result.challenge_id,
                **current_screen_payload(message),
            )
            await _reply(
                message,
                bot,
                "Для этого аккаунта включен Telegram 2FA. Отправьте пароль следующим сообщением или `/cancel`.",
            )
            return
        await message.state.fsm.drop_state()
        await show_connected_menu(
            bot,
            message=message,
            notice="Вы успешно авторизовались в Telegram.",
        )

    @fsm.on(TelegramSessionWizardState.INPUT_PASSWORD)
    async def input_password(message: IncomingMessage, bot: Bot) -> None:
        body = (message.body or "").strip()
        if _is_cancel(body.lower()):
            await _cancel(message, bot)
            return
        if not body:
            await _reply(message, bot, "Введите пароль Telegram 2FA или отправьте `/cancel`.")
            return
        try:
            result = await service.complete_password(
                operator_huid=str(message.sender.huid),
                challenge_id=message.state.fsm_storage.challenge_id,
                password=body,
            )
        except USER_VISIBLE_SESSION_ERRORS as error:
            await _reply(message, bot, f"Ошибка: {error}")
            return
        await message.state.fsm.drop_state()
        await show_connected_menu(
            bot,
            message=message,
            notice="Вы успешно авторизовались в Telegram.",
        )

    return fsm


async def begin_telegram_connection(
    *,
    message: IncomingMessage,
    bot: Bot,
    service: TelegramSessionService,
    phone_number: str | None,
) -> None:
    normalized_phone = _normalize_phone_number(phone_number)
    if not normalized_phone:
        await message.state.fsm.change_state(
            TelegramSessionWizardState.INPUT_PHONE,
            ttl_seconds=TELEGRAM_SESSION_WIZARD_TTL_SECONDS,
            **current_screen_payload(message),
        )
        await _reply(
            message,
            bot,
            "Отправьте номер телефона в формате Telegram, например `+79990001122`, или `/cancel`.",
        )
        return

    try:
        result = await service.start_connection(
            operator_huid=str(message.sender.huid),
            phone_number=normalized_phone,
        )
    except USER_VISIBLE_SESSION_ERRORS as error:
        await _reply(message, bot, f"Ошибка: {error}")
        return

    if isinstance(result, TelegramConnectionChallengeResult):
        await message.state.fsm.change_state(
            TelegramSessionWizardState.INPUT_CODE,
            ttl_seconds=TELEGRAM_SESSION_WIZARD_TTL_SECONDS,
            challenge_id=result.challenge_id,
            **current_screen_payload(message),
        )
        await _reply(
            message,
            bot,
            "Код отправлен в Telegram. Отправьте его следующим сообщением или `/cancel`.",
        )
        return

    await message.state.fsm.drop_state()
    await show_connected_menu(
        bot,
        message=message,
        notice="Вы успешно авторизовались в Telegram.",
    )


def _format_connected_status(result: TelegramSessionStatusResult) -> str:
    if not result.connected:
        return "Учетная запись Telegram не подключена."
    return (
        "Учетная запись Telegram подключена.\n"
        f"Источник: {result.source}\n"
        f"Номер телефона: {result.phone_number or '-'}\n"
        f"Telegram user id: {result.telegram_user_id or '-'}\n"
        f"Telegram username: {('@' + result.telegram_username) if result.telegram_username else '-'}\n"
        f"Имя в Telegram: {result.telegram_display_name or '-'}"
    )


async def _cancel(message: IncomingMessage, bot: Bot) -> None:
    await message.state.fsm.drop_state()
    await _reply(message, bot, "Подключение Telegram остановлено.")


async def _reply(message: IncomingMessage, bot: Bot, body: str) -> None:
    await render_screen(message=message, bot=bot, body=body)


def _normalized_body(message: IncomingMessage) -> str:
    return (message.body or "").strip().lower()


def _normalize_phone_number(value: str | None) -> str:
    raw = (value or "").strip()
    if not raw:
        return ""
    compact = (
        raw.replace(" ", "")
        .replace("-", "")
        .replace("(", "")
        .replace(")", "")
    )
    if compact.startswith("00"):
        compact = "+" + compact[2:]
    if compact.count("+") > 1 or ("+" in compact[1:]):
        return ""
    digits = compact[1:] if compact.startswith("+") else compact
    if not digits or not digits.isdigit():
        return ""
    return compact


def _is_cancel(value: str) -> bool:
    return value in {"cancel", "/cancel"}
