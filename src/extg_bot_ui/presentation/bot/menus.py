from __future__ import annotations

from pybotx import Bot, BubbleMarkup, IncomingMessage

from extg_bot_ui.presentation.bot.screen import render_screen


async def show_main_menu(
    bot: Bot,
    *,
    message: IncomingMessage | None = None,
    notice: str | None = None,
    phone_button_label: str = "Миграция через номер",
    json_button_label: str = "Миграция через JSON",
) -> None:
    lines = []
    if notice:
        lines.extend([notice, ""])
    lines.extend(
        [
            "Вас приветствует TGMigrate Bot!",
            "",
            "С моей помощью можно мигрировать чаты и каналы из Telegram.",
            "",
            "- Миграция через номер: авторизация по номеру телефона для миграции любых чатов и каналов.",
            "- Миграция через JSON: загрузка Telegram export archive без Telethon.",
            "- Конфигурация: просмотр и настройка конфигурации миграции.",
            "- Статус миграции: просмотр активных и завершенных миграций.",
            "- Добавление участников: добавление пользователей в уже мигрированный чат через файл.",
        ],
    )
    body = "\n".join(lines)
    bubbles = main_menu_bubbles(
        phone_button_label=phone_button_label,
        json_button_label=json_button_label,
    )
    if message is None:
        await bot.answer_message(body, wait_callback=False, bubbles=bubbles)
        return
    await render_screen(
        message=message,
        bot=bot,
        body=body,
        bubbles=bubbles,
    )


async def show_connected_menu(
    bot: Bot,
    *,
    message: IncomingMessage | None = None,
    notice: str | None = None,
) -> None:
    lines = []
    if notice:
        lines.extend([notice, ""])
    lines.append("Чем я могу вам помочь?")
    body = "\n".join(lines)
    bubbles = connected_menu_bubbles()
    if message is None:
        await bot.answer_message(body, wait_callback=False, bubbles=bubbles)
        return
    await render_screen(
        message=message,
        bot=bot,
        body=body,
        bubbles=bubbles,
    )


def main_menu_bubbles(
    *,
    phone_button_label: str = "Миграция через номер",
    json_button_label: str = "Миграция через JSON",
) -> BubbleMarkup:
    bubbles = BubbleMarkup()
    bubbles.add_button(phone_button_label, command="/main_phone")
    bubbles.add_button(json_button_label, command="/main_json")
    bubbles.add_button("Конфигурация", command="/main_configure")
    bubbles.add_button("Статус миграции", command="/main_status")
    bubbles.add_button("Добавление участников", command="/main_add_users")
    return bubbles


def connected_menu_bubbles() -> BubbleMarkup:
    bubbles = BubbleMarkup()
    bubbles.add_button("Мигрировать чаты", command="/main_migrate_chats")
    bubbles.add_button(
        "Выйти из учетной записи",
        command="/main_logout",
        new_row=False,
    )
    bubbles.add_button("Главное меню", command="/menu")
    return bubbles
