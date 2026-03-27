from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from extg_shared.contracts.errors import RecoverableItemError
from extg_shared.contracts.models import ExpressStagedFile, FilePayload, SentMessageRef


@dataclass(frozen=True, slots=True)
class SentEnvelope:
    target_chat_id: str
    target_sync_id: str
    body: str
    idempotency_key: str | None
    file_filename: str | None = None
    file_mime_type: str | None = None
    file_content: bytes | None = None
    file_id: str | None = None


@dataclass(frozen=True, slots=True)
class CreatedChat:
    target_chat_id: str
    title: str
    participant_huids: list[str]
    kind: str
    admin_huids: list[str]


class FakeExpressGateway:
    def __init__(
        self,
        *,
        send_failures_before_success: dict[str, int] | None = None,
        user_huid_by_email: dict[str, str] | None = None,
        user_huid_by_ad_login: dict[str, str] | None = None,
        user_huid_by_other_id: dict[str, str] | None = None,
        existing_huids: list[str] | None = None,
    ) -> None:
        self._chat_seq = 0
        self._message_seq = 0
        self._messages_by_chat: dict[str, list[SentEnvelope]] = {}
        self._messages_by_idempotency_key: dict[str, SentEnvelope] = {}
        self._send_failures_before_success = dict(send_failures_before_success or {})
        self._user_huid_by_email = {
            email.strip().lower(): huid
            for email, huid in (user_huid_by_email or {}).items()
        }
        self._user_huid_by_ad_login = {
            login.strip().lower(): huid
            for login, huid in (user_huid_by_ad_login or {}).items()
        }
        self._user_huid_by_other_id = {
            other_id.strip(): huid
            for other_id, huid in (user_huid_by_other_id or {}).items()
        }
        self._existing_huids = set(existing_huids or [])
        self._existing_huids.update(self._user_huid_by_email.values())
        self._existing_huids.update(self._user_huid_by_ad_login.values())
        self._existing_huids.update(self._user_huid_by_other_id.values())
        self._created_chats: dict[str, CreatedChat] = {}

    async def create_chat(
        self,
        title: str,
        participant_huids: list[str] | None = None,
        *,
        chat_type: str | None = None,
    ) -> str:
        self._chat_seq += 1
        target_chat_id = f"express-chat-{self._chat_seq}"
        self._messages_by_chat.setdefault(target_chat_id, [])
        kind = {
            "PERSONAL_CHAT": "personal",
            "GROUP_CHAT": "group",
            "CHANNEL": "channel",
            "THREAD": "thread",
        }.get((chat_type or "GROUP_CHAT").strip().upper(), "group")
        self._created_chats[target_chat_id] = CreatedChat(
            target_chat_id=target_chat_id,
            title=title,
            participant_huids=list(participant_huids or []),
            kind=kind,
            admin_huids=list(participant_huids or []),
        )
        return target_chat_id

    async def ensure_personal_chat(
        self,
        user_huid: str,
        *,
        title: str | None = None,
    ) -> str:
        target_chat_id = f"personal-chat-{user_huid}"
        self._messages_by_chat.setdefault(target_chat_id, [])
        self._created_chats[target_chat_id] = CreatedChat(
            target_chat_id=target_chat_id,
            title=title or target_chat_id,
            participant_huids=[user_huid],
            kind="personal",
            admin_huids=[user_huid],
        )
        return target_chat_id

    async def create_chat_link(
        self,
        target_chat_id: str,
    ) -> str | None:
        if target_chat_id not in self._created_chats:
            return None
        return f"https://express.example/chat/{target_chat_id}"

    async def search_user_by_email(self, email: str) -> str | None:
        return self._user_huid_by_email.get(email.strip().lower())

    async def search_user_by_huid(self, huid: str) -> str | None:
        normalized = huid.strip()
        if normalized in self._existing_huids:
            return normalized
        return None

    async def search_user_by_ad_login(
        self,
        ad_login: str,
        *,
        ad_domain: str | None = None,
    ) -> str | None:
        normalized_login = ad_login.strip().lower()
        if ad_domain:
            composite = f"{normalized_login}@{ad_domain.strip().lower()}"
            if composite in self._user_huid_by_ad_login:
                return self._user_huid_by_ad_login[composite]
        return self._user_huid_by_ad_login.get(normalized_login)

    async def search_user_by_other_id(self, other_id: str) -> str | None:
        return self._user_huid_by_other_id.get(other_id.strip())

    async def ensure_chat_members(
        self,
        target_chat_id: str,
        participant_huids: list[str],
    ) -> tuple[str, ...]:
        chat = self._created_chats.get(target_chat_id)
        if chat is None:
            raise RecoverableItemError("target chat was not found in fake express gateway")
        existing = list(chat.participant_huids)
        added: list[str] = []
        for huid in participant_huids:
            normalized = huid.strip()
            if not normalized or normalized in existing:
                continue
            existing.append(normalized)
            added.append(normalized)
            self._existing_huids.add(normalized)
        self._created_chats[target_chat_id] = CreatedChat(
            target_chat_id=chat.target_chat_id,
            title=chat.title,
            participant_huids=existing,
            kind=chat.kind,
            admin_huids=list(chat.admin_huids),
        )
        return tuple(added)

    async def promote_chat_admins(
        self,
        target_chat_id: str,
        participant_huids: list[str],
    ) -> tuple[str, ...]:
        chat = self._created_chats.get(target_chat_id)
        if chat is None:
            raise RecoverableItemError("target chat was not found in fake express gateway")
        admins = list(chat.admin_huids)
        promoted: list[str] = []
        for huid in participant_huids:
            normalized = huid.strip()
            if not normalized or normalized in admins:
                continue
            admins.append(normalized)
            promoted.append(normalized)
        self._created_chats[target_chat_id] = CreatedChat(
            target_chat_id=chat.target_chat_id,
            title=chat.title,
            participant_huids=list(chat.participant_huids),
            kind=chat.kind,
            admin_huids=admins,
        )
        return tuple(promoted)

    async def send_message(
        self,
        target_chat_id: str,
        body: str,
        *,
        idempotency_key: str | None = None,
        file: FilePayload | None = None,
        staged_file: ExpressStagedFile | None = None,
        fallback_file: FilePayload | None = None,
    ) -> SentMessageRef:
        if idempotency_key and idempotency_key in self._messages_by_idempotency_key:
            existing = self._messages_by_idempotency_key[idempotency_key]
            return SentMessageRef(
                target_chat_id=existing.target_chat_id,
                target_sync_id=existing.target_sync_id,
                deduplicated=True,
            )

        failure_budget_key = idempotency_key or body
        remaining_failures = self._send_failures_before_success.get(
            failure_budget_key,
            0,
        )
        if remaining_failures > 0:
            self._send_failures_before_success[failure_budget_key] = (
                remaining_failures - 1
            )
            raise RecoverableItemError("planned temporary eXpress failure")

        self._message_seq += 1
        envelope = SentEnvelope(
            target_chat_id=target_chat_id,
            target_sync_id=f"sync-{self._message_seq}",
            body=body,
            idempotency_key=idempotency_key,
            file_filename=(
                staged_file.filename
                if staged_file is not None
                else file.filename if file else None
            ),
            file_mime_type=(
                staged_file.mime_type
                if staged_file is not None
                else file.mime_type if file else None
            ),
            file_content=self._file_payload_content(file) if file else None,
            file_id=staged_file.file_id if staged_file is not None else None,
        )
        self._messages_by_chat.setdefault(target_chat_id, []).append(envelope)
        if idempotency_key:
            self._messages_by_idempotency_key[idempotency_key] = envelope
        return SentMessageRef(
            target_chat_id=target_chat_id,
            target_sync_id=envelope.target_sync_id,
        )

    def messages_for_chat(self, target_chat_id: str) -> list[SentEnvelope]:
        return list(self._messages_by_chat.get(target_chat_id, []))

    def created_chat(self, target_chat_id: str) -> CreatedChat | None:
        return self._created_chats.get(target_chat_id)

    def _file_payload_content(self, file: FilePayload) -> bytes:
        if file.content is not None:
            return file.content
        if file.local_path:
            return Path(file.local_path).read_bytes()
        return b""


class FakeExpressFileStore:
    def __init__(self) -> None:
        self._seq = 0
        self.uploads: list[tuple[str, FilePayload, ExpressStagedFile]] = []

    async def upload_file(
        self,
        target_chat_id: str,
        *,
        file: FilePayload,
    ) -> ExpressStagedFile:
        self._seq += 1
        staged = ExpressStagedFile(
            attachment_type=_attachment_type_for_media_kind(file.media_kind),
            file_id=f"file-{self._seq}",
            file_url=f"https://files.example/{target_chat_id}/{self._seq}",
            filename=file.filename,
            size_bytes=len(self._file_payload_content(file)),
            mime_type=file.mime_type or "application/octet-stream",
            file_hash=f"hash-{self._seq}",
            duration_seconds=file.duration_seconds,
        )
        self.uploads.append((target_chat_id, file, staged))
        return staged

    def _file_payload_content(self, file: FilePayload) -> bytes:
        if file.content is not None:
            return file.content
        if file.local_path:
            return Path(file.local_path).read_bytes()
        return b""


def _attachment_type_for_media_kind(media_kind: str | None) -> str:
    normalized = (media_kind or "document").lower()
    if normalized in {"photo", "sticker"}:
        return "image"
    if normalized == "video":
        return "video"
    if normalized == "voice":
        return "voice"
    return "document"
