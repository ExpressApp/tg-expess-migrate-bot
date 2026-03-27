from extg_bot_ui.presentation.bot.telegram_session_wizard import _normalize_phone_number


def test_normalize_phone_number_strips_visual_formatting() -> None:
    assert _normalize_phone_number("+7 (999) 000-11-22") == "+79990001122"


def test_normalize_phone_number_rejects_non_numeric_input() -> None:
    assert _normalize_phone_number("not-a-phone") == ""
