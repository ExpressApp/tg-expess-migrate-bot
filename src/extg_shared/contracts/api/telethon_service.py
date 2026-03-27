from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, Field, TypeAdapter

from extg_telethon_service.application.telegram_session_service import (
    TelegramConnectionChallengeResult,
    TelegramDisconnectResult,
    TelegramPasswordChallengeResult,
    TelegramSessionStatusResult,
)
from extg_shared.contracts.manifest import MigrationManifest
from extg_shared.contracts.models import (
    CanonicalAttachment,
    HistoryBatch,
    HistoryCursor,
    SourceChannelAccessProfile,
    SourceDialog,
    SourceParticipant,
    SourceTopic,
)


class SessionStartRequest(BaseModel):
    operator_huid: str
    phone_number: str
    force_sms: bool = False


class SessionCompleteCodeRequest(BaseModel):
    operator_huid: str
    challenge_id: str
    code: str


class SessionCompletePasswordRequest(BaseModel):
    operator_huid: str
    challenge_id: str
    password: str


class SessionResolveBindingRequest(BaseModel):
    operator_huid: str
    mark_used: bool = True


class SessionResultEnvelope(BaseModel):
    kind: Literal["challenge", "password_challenge", "status"]
    payload: dict[str, Any]


class ListAvailableDialogsRequest(BaseModel):
    operator_huid: str
    limit: int = 100
    query: str | None = None
    source_backend: str = "telethon_user_session"


class ListDialogsRequest(BaseModel):
    operator_huid: str
    manifest: dict[str, Any]


class FetchHistoryRequest(BaseModel):
    operator_huid: str
    dialog_id: str
    cursor: dict[str, Any] | None = None
    limit: int = Field(ge=1)
    source_backend: str = "telethon_user_session"


class DownloadAttachmentRequest(BaseModel):
    operator_huid: str
    attachment: dict[str, Any]
    source_backend: str = "telethon_user_session"


class ListParticipantsRequest(BaseModel):
    operator_huid: str
    dialog_id: str
    source_backend: str = "telethon_user_session"


class ListTopicsRequest(BaseModel):
    operator_huid: str
    dialog_id: str
    source_backend: str = "telethon_user_session"


class ChannelAccessProfileRequest(BaseModel):
    operator_huid: str
    dialog_id: str
    source_backend: str = "telethon_user_session"


TELEGRAM_CONNECTION_CHALLENGE_ADAPTER = TypeAdapter(TelegramConnectionChallengeResult)
TELEGRAM_PASSWORD_CHALLENGE_ADAPTER = TypeAdapter(TelegramPasswordChallengeResult)
TELEGRAM_SESSION_STATUS_ADAPTER = TypeAdapter(TelegramSessionStatusResult)
TELEGRAM_DISCONNECT_ADAPTER = TypeAdapter(TelegramDisconnectResult)
SOURCE_DIALOG_LIST_ADAPTER = TypeAdapter(list[SourceDialog])
SOURCE_PARTICIPANT_LIST_ADAPTER = TypeAdapter(list[SourceParticipant])
SOURCE_TOPIC_LIST_ADAPTER = TypeAdapter(list[SourceTopic])
HISTORY_CURSOR_ADAPTER = TypeAdapter(HistoryCursor)
HISTORY_BATCH_ADAPTER = TypeAdapter(HistoryBatch)
CANONICAL_ATTACHMENT_ADAPTER = TypeAdapter(CanonicalAttachment)
SOURCE_CHANNEL_ACCESS_PROFILE_ADAPTER = TypeAdapter(SourceChannelAccessProfile)
MIGRATION_MANIFEST_ADAPTER = TypeAdapter(MigrationManifest)


__all__ = [
    "CANONICAL_ATTACHMENT_ADAPTER",
    "ChannelAccessProfileRequest",
    "DownloadAttachmentRequest",
    "FetchHistoryRequest",
    "HISTORY_BATCH_ADAPTER",
    "HISTORY_CURSOR_ADAPTER",
    "ListAvailableDialogsRequest",
    "ListDialogsRequest",
    "ListParticipantsRequest",
    "ListTopicsRequest",
    "MIGRATION_MANIFEST_ADAPTER",
    "SOURCE_CHANNEL_ACCESS_PROFILE_ADAPTER",
    "SOURCE_DIALOG_LIST_ADAPTER",
    "SOURCE_PARTICIPANT_LIST_ADAPTER",
    "SOURCE_TOPIC_LIST_ADAPTER",
    "SessionCompleteCodeRequest",
    "SessionCompletePasswordRequest",
    "SessionResolveBindingRequest",
    "SessionResultEnvelope",
    "SessionStartRequest",
    "TELEGRAM_CONNECTION_CHALLENGE_ADAPTER",
    "TELEGRAM_DISCONNECT_ADAPTER",
    "TELEGRAM_PASSWORD_CHALLENGE_ADAPTER",
    "TELEGRAM_SESSION_STATUS_ADAPTER",
]
