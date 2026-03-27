from __future__ import annotations

import asyncio
from functools import partial
from types import SimpleNamespace
from typing import Any, Awaitable, Callable

import httpx
from pybotx import Bot

_FILE_FALLBACK_NOTE = (
    "\n\n[Вложение не удалось отправить из-за временной сетевой ошибки. "
    "Если файл нужен, повтори команду позже.]"
)

AnswerMessageCallable = Callable[..., Awaitable[Any]]


def install_resilient_answer_message(
    bot: Bot,
    *,
    logger: Any | None = None,
    max_attempts: int = 2,
    base_delay_seconds: float = 0.4,
) -> None:
    state = getattr(bot, "state", None)
    if state is None:
        state = SimpleNamespace()
        setattr(bot, "state", state)
    if getattr(state, "_extg_resilient_answer_message_installed", False):
        return
    if not hasattr(bot, "answer_message"):
        setattr(state, "_extg_resilient_answer_message_installed", True)
        return
    original_answer_message = bot.answer_message
    bot.answer_message = partial(  # type: ignore[method-assign]
        _resilient_answer_message,
        original=original_answer_message,
        logger=logger,
        max_attempts=max_attempts,
        base_delay_seconds=base_delay_seconds,
    )
    setattr(state, "_extg_resilient_answer_message_installed", True)


async def _resilient_answer_message(
    body: Any,
    *args: Any,
    original: AnswerMessageCallable,
    logger: Any | None,
    max_attempts: int,
    base_delay_seconds: float,
    **kwargs: Any,
) -> Any | None:
    try:
        return await _send_with_retry(
            original,
            body,
            args=args,
            kwargs=kwargs,
            logger=logger,
            max_attempts=max_attempts,
            base_delay_seconds=base_delay_seconds,
            stage="primary",
        )
    except httpx.RequestError as error:
        has_file = kwargs.get("file") is not None
        _log(
            logger,
            "warning",
            "bot answer_message exhausted retries",
            stage="primary",
            has_file=has_file,
            error_type=type(error).__name__,
            error=str(error),
        )
        if not has_file:
            return None

    fallback_kwargs = dict(kwargs)
    fallback_kwargs.pop("file", None)
    fallback_body = _with_file_fallback_note(body)

    try:
        return await _send_with_retry(
            original,
            fallback_body,
            args=args,
            kwargs=fallback_kwargs,
            logger=logger,
            max_attempts=max_attempts,
            base_delay_seconds=base_delay_seconds,
            stage="fallback_without_file",
        )
    except httpx.RequestError as error:
        _log(
            logger,
            "error",
            "bot answer_message dropped after transport failures",
            stage="fallback_without_file",
            error_type=type(error).__name__,
            error=str(error),
        )
        return None


async def _send_with_retry(
    send: AnswerMessageCallable,
    body: Any,
    *,
    args: tuple[Any, ...],
    kwargs: dict[str, Any],
    logger: Any | None,
    max_attempts: int,
    base_delay_seconds: float,
    stage: str,
) -> Any:
    attempt = 1
    while True:
        try:
            return await send(body, *args, **kwargs)
        except httpx.RequestError as error:
            _log(
                logger,
                "warning",
                "bot answer_message transport failed",
                stage=stage,
                attempt=attempt,
                max_attempts=max_attempts,
                has_file=kwargs.get("file") is not None,
                error_type=type(error).__name__,
                error=str(error),
            )
            if attempt >= max_attempts:
                raise
            await asyncio.sleep(base_delay_seconds * attempt)
            attempt += 1


def _with_file_fallback_note(body: Any) -> Any:
    if not isinstance(body, str):
        return body
    if _FILE_FALLBACK_NOTE in body:
        return body
    return f"{body}{_FILE_FALLBACK_NOTE}"


def _log(logger: Any | None, level: str, event: str, **kwargs: Any) -> None:
    if logger is None:
        return
    method = getattr(logger, level, None)
    if callable(method):
        method(event, **kwargs)
