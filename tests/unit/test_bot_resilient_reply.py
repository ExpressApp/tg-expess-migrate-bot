from types import SimpleNamespace

import httpx
import pytest

from extg_bot_ui.presentation.bot.resilient_reply import (
    install_resilient_answer_message,
)


class FakeBot:
    def __init__(self, side_effects):
        self._side_effects = list(side_effects)
        self.calls: list[tuple[object, dict[str, object]]] = []
        self.state = SimpleNamespace()

    async def answer_message(self, body, **kwargs):
        self.calls.append((body, dict(kwargs)))
        effect = self._side_effects.pop(0)
        if isinstance(effect, Exception):
            raise effect
        return effect


@pytest.mark.asyncio
async def test_resilient_answer_message_retries_transport_error():
    bot = FakeBot([httpx.ConnectTimeout("timeout"), "ok"])
    install_resilient_answer_message(
        bot,
        max_attempts=2,
        base_delay_seconds=0.0,
    )

    result = await bot.answer_message("hello", wait_callback=False)

    assert result == "ok"
    assert len(bot.calls) == 2
    assert bot.calls[0][0] == "hello"
    assert bot.calls[1][0] == "hello"


@pytest.mark.asyncio
async def test_resilient_answer_message_falls_back_to_text_only_when_file_send_keeps_timing_out():
    bot = FakeBot(
        [
            httpx.ConnectTimeout("timeout"),
            httpx.ConnectTimeout("timeout"),
            "fallback-ok",
        ],
    )
    install_resilient_answer_message(
        bot,
        max_attempts=2,
        base_delay_seconds=0.0,
    )

    result = await bot.answer_message(
        "body",
        wait_callback=False,
        file=object(),
    )

    assert result == "fallback-ok"
    assert len(bot.calls) == 3
    assert bot.calls[0][1]["file"] is not None
    assert bot.calls[1][1]["file"] is not None
    assert "file" not in bot.calls[2][1]
    assert "Вложение не удалось отправить" in str(bot.calls[2][0])


@pytest.mark.asyncio
async def test_resilient_answer_message_swallows_transport_error_after_all_attempts():
    bot = FakeBot(
        [
            httpx.ConnectTimeout("timeout"),
            httpx.ConnectTimeout("timeout"),
        ],
    )
    install_resilient_answer_message(
        bot,
        max_attempts=2,
        base_delay_seconds=0.0,
    )

    result = await bot.answer_message("hello", wait_callback=False)

    assert result is None
    assert len(bot.calls) == 2
