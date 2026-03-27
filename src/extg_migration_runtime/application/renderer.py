from __future__ import annotations

from datetime import UTC
from uuid import UUID
from zoneinfo import ZoneInfo

from pybotx import MentionBuilder

from extg_shared.contracts.models import (
    CanonicalEntity,
    CanonicalMessage,
    RenderedAttachment,
    RenderedMessage,
    ReplyPreview,
)


class MessageRenderer:
    def __init__(self, timezone_name: str = "UTC") -> None:
        self._timezone = ZoneInfo(timezone_name)

    def render(
        self,
        message: CanonicalMessage,
        *,
        source_chat_title: str | None = None,
        reply_preview: ReplyPreview | None = None,
        reply_mode: str = "inline_quote",
    ) -> RenderedMessage:
        body = self._render_body_content(message)
        if reply_mode == "none":
            pass
        elif reply_mode == "source_id":
            if message.source_reply_to_message_id:
                body = (
                    f"↪ Reply to source message #{message.source_reply_to_message_id}\n\n"
                    f"{body}"
                )
        elif reply_preview:
            body = self._render_reply_block(reply_preview, body)
        elif message.source_reply_to_message_id:
            body = (
                f"↪ Reply to source message #{message.source_reply_to_message_id}\n\n"
                f"{body}"
            )

        if message.attachment_failures:
            failure_lines = [
                self._render_attachment_failure_line(reason)
                for reason in message.attachment_failures
            ]
            body = "\n".join([body, *failure_lines]).strip()

        footer = None
        if message.edited_at_utc:
            footer = (
                f"[edited in source at {self._format_timestamp(message.edited_at_utc)}]"
            )
        header = self._render_header(message, source_chat_title=source_chat_title)
        attachments = [
            RenderedAttachment(
                filename=attachment.filename,
                media_kind=attachment.media_kind,
            )
            for attachment in message.attachments
        ]
        return RenderedMessage(
            display_header=header,
            display_body=body,
            footer=footer,
            attachments=attachments,
        )

    def _render_header(
        self,
        message: CanonicalMessage,
        *,
        source_chat_title: str | None,
    ) -> str:
        author = self._render_author(message)
        if message.sender_username:
            author = f"{author} (@{message.sender_username})"
        timestamp = self._format_timestamp(message.sent_at_utc)
        if source_chat_title:
            return f"[{timestamp}] {author} | Source: {source_chat_title}"
        return f"[{timestamp}] {author}"

    def _render_body_content(self, message: CanonicalMessage) -> str:
        if not message.entities:
            return message.body_rendered or message.body_plain or ""
        source = message.body_plain or message.body_rendered or ""
        if not source:
            return source

        parts: list[str] = []
        cursor = 0
        for entity in sorted(message.entities, key=lambda item: item.offset):
            if entity.kind != "mention":
                continue
            if entity.offset < cursor:
                continue
            entity_end = entity.offset + entity.length
            if entity.offset < 0 or entity.length <= 0 or entity_end > len(source):
                continue
            parts.append(source[cursor:entity.offset])
            parts.append(self._render_entity(entity, source[entity.offset:entity_end]))
            cursor = entity_end
        parts.append(source[cursor:])
        return "".join(parts)

    def _render_author(self, message: CanonicalMessage) -> str:
        if not message.resolved_target_huid:
            return message.sender_display_name
        return self._build_user_mention(
            target_huid=message.resolved_target_huid,
            fallback=message.sender_display_name,
        )

    def _render_entity(
        self,
        entity: CanonicalEntity,
        fallback_text: str,
    ) -> str:
        if entity.target_huid is None:
            return fallback_text
        return self._build_user_mention(
            target_huid=entity.target_huid,
            fallback=entity.text or fallback_text,
        )

    def _build_user_mention(
        self,
        *,
        target_huid: str,
        fallback: str,
    ) -> str:
        try:
            return str(MentionBuilder.user(UUID(target_huid), fallback))
        except (ValueError, TypeError):
            return fallback

    def _render_reply_block(
        self,
        preview: ReplyPreview,
        body: str,
    ) -> str:
        if preview.sent_at_utc:
            header = (
                f"↪ Reply to [{self._format_timestamp(preview.sent_at_utc)}] "
                f"{preview.author_display_name}:"
            )
        else:
            header = f"↪ Reply to {preview.author_display_name}:"
        quote = preview.excerpt or "[source message unavailable]"
        return f'{header}\n"{quote}"\n\n{body}'.strip()

    def _format_timestamp(self, value) -> str:
        localized = value.astimezone(self._timezone)
        offset = localized.utcoffset() or UTC.utcoffset(localized)
        hours = int(offset.total_seconds() // 3600)
        minutes = int((abs(offset.total_seconds()) % 3600) // 60)
        sign = "+" if offset.total_seconds() >= 0 else "-"
        return (
            f"{localized.strftime('%Y-%m-%d %H:%M:%S')} "
            f"UTC{sign}{abs(hours):02d}:{minutes:02d}"
        )

    def _render_attachment_failure_line(self, reason: str) -> str:
        normalized = reason.strip()
        if normalized.lower().startswith("skipped:"):
            return f"[attachment skipped: {normalized[8:].strip()}]"
        return f"[attachment import failed: {reason}]"
