from datetime import UTC, datetime

from extg_migration_runtime.application.renderer import MessageRenderer
from extg_shared.contracts.models import CanonicalMessage, ContentType, ReplyPreview


def build_message(**overrides):
    payload = {
        "source_platform": "telegram",
        "source_chat_id": "chat-1",
        "source_message_id": "msg-2",
        "source_thread_id": None,
        "source_reply_to_message_id": "msg-1",
        "sender_external_id": "user-1",
        "sender_display_name": "Иван Петров",
        "sender_username": "ivan.petrov",
        "sent_at_utc": datetime(2026, 3, 17, 11, 32, 10, tzinfo=UTC),
        "edited_at_utc": datetime(2026, 3, 17, 11, 40, 3, tzinfo=UTC),
        "deleted_in_source": False,
        "content_type": ContentType.TEXT,
        "body_plain": "Текущий текст сообщения",
        "body_rendered": "Текущий текст сообщения",
        "attachments": [],
        "entities": [],
        "service_payload": None,
        "raw_snapshot": {},
        "resolved_target_huid": None,
        "identity_resolution_source": "display_only",
        "attachment_failures": ["file missing in source"],
    }
    payload.update(overrides)
    return CanonicalMessage(**payload)


def test_renderer_renders_header_reply_fallback_and_footer():
    renderer = MessageRenderer(timezone_name="Europe/Moscow")
    rendered = renderer.render(
        build_message(),
        source_chat_title="Project X",
        reply_preview=ReplyPreview(
            sent_at_utc=datetime(2026, 3, 17, 11, 31, 2, tzinfo=UTC),
            author_display_name="Петр Иванов",
            excerpt="Цитата исходного сообщения...",
        ),
    )

    assert (
        rendered.display_header
        == "[2026-03-17 14:32:10 UTC+03:00] Иван Петров (@ivan.petrov) | Source: Project X"
    )
    assert "↪ Reply to [2026-03-17 14:31:02 UTC+03:00] Петр Иванов:" in rendered.display_body
    assert '"Цитата исходного сообщения..."' in rendered.display_body
    assert "[attachment import failed: file missing in source]" in rendered.display_body
    assert rendered.footer == "[edited in source at 2026-03-17 14:40:03 UTC+03:00]"


def test_renderer_renders_skipped_attachment_notice():
    renderer = MessageRenderer(timezone_name="Europe/Moscow")
    rendered = renderer.render(
        build_message(
            attachment_failures=[
                "skipped: file exceeds eXpress BotX upload limit (120.0 MiB > 100.0 MiB)",
            ],
        ),
        source_chat_title="Project X",
    )

    assert (
        "[attachment skipped: file exceeds eXpress BotX upload limit "
        "(120.0 MiB > 100.0 MiB)]"
    ) in rendered.display_body
