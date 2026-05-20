from __future__ import annotations

import logging
from typing import Any
from uuid import UUID

from pybotx import Bot, BubbleMarkup, IncomingMessage, KeyboardMarkup
from pybotx.missing import Undefined
from pybotx.models.attachments import IncomingFileAttachment, OutgoingAttachment

logger = logging.getLogger(__name__)


def current_screen_sync_id(message: IncomingMessage) -> UUID | None:
    if message.source_sync_id is not None:
        return message.source_sync_id
    fsm_storage = getattr(message.state, "fsm_storage", None)
    return getattr(fsm_storage, "screen_sync_id", None)


def current_screen_payload(message: IncomingMessage) -> dict[str, object]:
    return {"screen_sync_id": current_screen_sync_id(message)}


async def render_screen(
    *,
    message: IncomingMessage,
    bot: Bot,
    body: str,
    bubbles: BubbleMarkup | None = None,
    keyboard: KeyboardMarkup | None = None,
    file: IncomingFileAttachment | OutgoingAttachment | None = None,
    wait_callback: bool = False,
) -> UUID | None:
    sync_id = current_screen_sync_id(message)
    if sync_id is not None:
        try:
            await bot.edit_message(
                bot_id=message.bot.id,
                sync_id=sync_id,
                body=body,
                bubbles=bubbles if bubbles is not None else BubbleMarkup(),
                keyboard=keyboard if keyboard is not None else KeyboardMarkup(),
                file=file,
            )
            _store_screen_sync_id(message, sync_id)
            return sync_id
        except Exception:
            logger.warning(
                "screen edit failed, falling back to answer_message",
                exc_info=True,
                extra={
                    "chat_id": str(message.chat.id),
                    "sender_huid": str(message.sender.huid),
                    "sync_id": str(sync_id),
                },
            )

    answer_kwargs: dict[str, Any] = {"wait_callback": wait_callback}
    if bubbles is not None:
        answer_kwargs["bubbles"] = bubbles
    if keyboard is not None:
        answer_kwargs["keyboard"] = keyboard
    if file is not None:
        answer_kwargs["file"] = file
    new_sync_id = await bot.answer_message(body, **answer_kwargs)
    if isinstance(new_sync_id, UUID):
        _store_screen_sync_id(message, new_sync_id)
        return new_sync_id
    return None


def _store_screen_sync_id(message: IncomingMessage, sync_id: UUID) -> None:
    fsm_storage = getattr(message.state, "fsm_storage", None)
    if fsm_storage is not None:
        setattr(fsm_storage, "screen_sync_id", sync_id)
