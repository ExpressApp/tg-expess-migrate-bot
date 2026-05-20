from __future__ import annotations

from datetime import UTC
from uuid import UUID
from zoneinfo import ZoneInfo

from pybotx import MentionBuilder

from extg_shared.contracts.output_template import render_output_template
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
        output_template: str | None = None,
    ) -> RenderedMessage:
        header = self._render_header(message, source_chat_title=source_chat_title)
        body = self._render_body_content(message)
        reply_block = self._render_reply_prefix(
            message=message,
            reply_preview=reply_preview,
            reply_mode=reply_mode,
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
        attachments = [
            RenderedAttachment(
                filename=attachment.filename,
                media_kind=attachment.media_kind,
            )
            for attachment in message.attachments
        ]
        if output_template and output_template.strip():
            rendered_text = render_output_template(
                template=output_template,
                values={
                    "author": message.sender_display_name,
                    "username": message.sender_username or "",
                    "timestamp": self._format_timestamp(message.sent_at_utc),
                    "source_chat_title": source_chat_title or "",
                    "source_chat_id": message.source_chat_id,
                    "source_message_id": message.source_message_id,
                    "reply_to_source_message_id": message.source_reply_to_message_id or "",
                    "reply_block": reply_block,
                    "body": body,
                    "header": header,
                    "footer": footer or "",
                },
            ).strip()
            return RenderedMessage(
                display_header="",
                display_body=rendered_text,
                footer=None,
                attachments=attachments,
            )
        rendered_body = body
        if reply_block:
            rendered_body = f"{reply_block}\n\n{body}".strip()
        return RenderedMessage(
            display_header=header,
            display_body=rendered_body,
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

    def _render_reply_prefix(
        self,
        *,
        message: CanonicalMessage,
        reply_preview: ReplyPreview | None,
        reply_mode: str,
    ) -> str:
        if reply_mode == "none":
            return ""
        if reply_mode == "source_id":
            if message.source_reply_to_message_id:
                return f"↪ Reply to source message #{message.source_reply_to_message_id}"
            return ""
        if reply_preview is None:
            if message.source_reply_to_message_id:
                return f"↪ Reply to source message #{message.source_reply_to_message_id}"
            return ""
        if reply_preview.sent_at_utc:
            header = (
                f"↪ Reply to [{self._format_timestamp(reply_preview.sent_at_utc)}] "
                f"{reply_preview.author_display_name}:"
            )
        else:
            header = f"↪ Reply to {reply_preview.author_display_name}:"
        quote = reply_preview.excerpt or "[source message unavailable]"
        return f'{header}\n"{quote}"'

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
