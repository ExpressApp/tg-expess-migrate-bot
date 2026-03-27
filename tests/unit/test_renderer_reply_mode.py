from datetime import UTC, datetime

from extg_migration_runtime.application.renderer import MessageRenderer
from extg_shared.contracts.models import CanonicalMessage, ContentType, ReplyPreview


def build_message() -> CanonicalMessage:
    return CanonicalMessage(
        source_platform="telegram",
        source_chat_id="chat-1",
        source_message_id="2",
        source_thread_id=None,
        source_reply_to_message_id="1",
        sender_external_id="user-1",
        sender_display_name="Иван Петров",
        sender_username="ivan.petrov",
        sent_at_utc=datetime(2026, 3, 18, 10, 0, tzinfo=UTC),
        edited_at_utc=None,
        deleted_in_source=False,
        content_type=ContentType.TEXT,
        body_plain="Ответ",
        body_rendered="Ответ",
    )


def test_renderer_reply_mode_none_suppresses_reply_block():
    renderer = MessageRenderer(timezone_name="Europe/Moscow")
    rendered = renderer.render(
        build_message(),
        source_chat_title="Telegram Chat",
        reply_preview=ReplyPreview(
            sent_at_utc=datetime(2026, 3, 18, 9, 55, tzinfo=UTC),
            author_display_name="Петр",
            excerpt="Исходное сообщение",
        ),
        reply_mode="none",
    )

    assert "↪ Reply to" not in rendered.display_body
    assert rendered.display_body == "Ответ"


def test_renderer_reply_mode_source_id_uses_source_reference():
    renderer = MessageRenderer(timezone_name="Europe/Moscow")
    rendered = renderer.render(
        build_message(),
        source_chat_title="Telegram Chat",
        reply_preview=ReplyPreview(
            sent_at_utc=datetime(2026, 3, 18, 9, 55, tzinfo=UTC),
            author_display_name="Петр",
            excerpt="Исходное сообщение",
        ),
        reply_mode="source_id",
    )

    assert rendered.display_body.startswith("↪ Reply to source message #1")
