from __future__ import annotations

import re
from dataclasses import dataclass, replace

from extg_migration_runtime.application.identity import UsernameEmailIdentityDirectory
from extg_shared.contracts.models import (
    CanonicalEntity,
    CanonicalMessage,
    ContentType,
    TelegramSourceMessage,
)
from extg_shared.contracts.ports import IdentityResolver


@dataclass(frozen=True, slots=True)
class ResolvedDisplayIdentity:
    display_name: str
    username: str | None
    target_huid: str | None = None
    resolution_source: str = "display_only"


class DisplayOnlyIdentityResolver:
    async def resolve(self, message: CanonicalMessage) -> ResolvedDisplayIdentity:
        return ResolvedDisplayIdentity(
            display_name=message.sender_display_name,
            username=message.sender_username,
        )


class DirectoryBackedIdentityResolver:
    def __init__(self, identity_directory: UsernameEmailIdentityDirectory) -> None:
        self._identity_directory = identity_directory

    async def resolve(self, message: CanonicalMessage) -> ResolvedDisplayIdentity:
        lookup = await self._identity_directory.resolve_telegram_identity(
            telegram_user_id=message.sender_external_id,
            telegram_username=message.sender_username,
            telegram_display_name=message.sender_display_name,
        )
        return ResolvedDisplayIdentity(
            display_name=message.sender_display_name,
            username=lookup.telegram_username or message.sender_username,
            target_huid=lookup.target_huid,
            resolution_source=lookup.resolution_source,
        )


class TelegramMessageNormalizer:
    def __init__(
        self,
        identity_resolver: IdentityResolver,
        *,
        identity_directory: UsernameEmailIdentityDirectory | None = None,
    ) -> None:
        self._identity_resolver = identity_resolver
        self._identity_directory = identity_directory

    async def normalize(self, message: TelegramSourceMessage) -> CanonicalMessage:
        body_plain = self._normalize_body(message)
        canonical = CanonicalMessage(
            source_platform="telegram",
            source_chat_id=message.chat_id,
            source_message_id=message.message_id,
            source_thread_id=message.thread_id,
            source_reply_to_message_id=message.reply_to_message_id,
            sender_external_id=message.author.external_id,
            sender_display_name=message.author.display_name,
            sender_username=message.author.username,
            sent_at_utc=message.sent_at_utc,
            edited_at_utc=message.edited_at_utc,
            deleted_in_source=message.deleted_in_source,
            content_type=message.content_type,
            body_plain=body_plain,
            body_rendered=body_plain,
            attachments=list(message.attachments),
            entities=list(message.entities),
            service_payload=message.service_payload,
            raw_snapshot=dict(message.raw_payload),
            attachment_failures=list(message.attachment_failures),
        )
        resolved_identity = await self._identity_resolver.resolve(canonical)
        resolved_entities = await self._resolve_entities(canonical)
        return replace(
            canonical,
            sender_display_name=resolved_identity.display_name,
            sender_username=resolved_identity.username,
            resolved_target_huid=resolved_identity.target_huid,
            identity_resolution_source=resolved_identity.resolution_source,
            entities=resolved_entities,
        )

    async def _resolve_entities(
        self,
        message: CanonicalMessage,
    ) -> list[CanonicalEntity]:
        if self._identity_directory is None or not message.entities:
            return list(message.entities)
        resolved: list[CanonicalEntity] = []
        for entity in message.entities:
            if entity.kind != "mention":
                resolved.append(entity)
                continue
            lookup = await self._identity_directory.resolve_telegram_identity(
                telegram_user_id=entity.telegram_user_id,
                telegram_username=entity.telegram_username,
            )
            resolved.append(
                replace(
                    entity,
                    telegram_user_id=lookup.telegram_user_id or entity.telegram_user_id,
                    telegram_username=lookup.telegram_username or entity.telegram_username,
                    corporate_email=lookup.corporate_email,
                    target_huid=lookup.target_huid,
                    resolution_source=lookup.resolution_source,
                    reason=lookup.reason,
                ),
            )
        return resolved

    def _normalize_body(self, message: TelegramSourceMessage) -> str:
        if message.body:
            return message.body
        if message.content_type is ContentType.SERVICE and message.service_payload:
            return "[service event]"
        if message.content_type is ContentType.UNSUPPORTED:
            unsupported_kind = self._unsupported_content_kind(message.raw_payload)
            if unsupported_kind:
                return f"[unsupported: {unsupported_kind}]"
        return f"[{message.content_type.value}]"

    def _unsupported_content_kind(self, raw_payload: dict[str, object]) -> str | None:
        candidates: list[str] = []
        for nested_key in ("media", "action"):
            nested = raw_payload.get(nested_key)
            if isinstance(nested, dict):
                nested_type = nested.get("_")
                if isinstance(nested_type, str) and nested_type:
                    candidates.append(nested_type)
        for key in ("media_type", "type", "_"):
            value = raw_payload.get(key)
            if isinstance(value, str) and value:
                candidates.append(value)
        for candidate in candidates:
            normalized = self._normalize_telegram_type_name(candidate)
            if normalized and normalized not in {"message", "service"}:
                return normalized
        return None

    def _normalize_telegram_type_name(self, raw_type_name: str) -> str | None:
        normalized = raw_type_name.strip()
        if not normalized:
            return None
        normalized = re.sub(
            r"^(?:message(?:media|action)?|inputmedia|decryptedmessageaction)",
            "",
            normalized,
            flags=re.IGNORECASE,
        )
        normalized = normalized.strip("_ ")
        if not normalized:
            return None
        normalized = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", normalized)
        normalized = normalized.replace("-", "_").replace(" ", "_").lower()
        normalized = re.sub(r"_+", "_", normalized).strip("_")
        return normalized or None
