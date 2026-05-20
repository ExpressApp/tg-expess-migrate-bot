from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, Field, model_validator

from extg_shared.contracts.errors import ConfigurationError


class ManifestDefaults(BaseModel):
    migrate_media: bool = True
    media_kinds: tuple[str, ...] | None = None
    service_messages: bool = True
    propagate_edits: bool = False
    propagate_deletes: bool = False
    reply_mode: str = "inline_quote"
    output_template: str | None = None
    identity_policy: str = "display_only"
    access_strategy: str = "direct_add"


class ManifestDialog(BaseModel):
    source_chat_id: str
    source_chat_type: Literal["private", "group", "supergroup", "channel"]
    source_backend: Literal[
        "telethon_user_session",
        "telegram_bot_api_live",
        "telegram_export_archive",
    ] = "telethon_user_session"
    target_strategy: str
    target_title: str | None = None
    target_chat_id: str | None = None
    initiator_huid: str | None = None
    anchor_cts_host: str | None = None
    anchor_bot_id: str | None = None
    include_from: str | None = None
    include_to: str | None = None
    migrate_media: bool | None = None
    media_kinds: tuple[str, ...] | None = None
    service_messages: bool | None = None
    reply_mode: str | None = None
    output_template: str | None = None
    identity_policy: str | None = None
    access_strategy: str | None = None
    topic_strategy: str = "single_chat"
    skip_in_all: bool = False
    telegram_chat_id: str | None = None
    source_topic_id: str | None = None
    source_thread_id: str | None = None
    source_thread_title: str | None = None

    @model_validator(mode="after")
    def validate_target(self) -> "ManifestDialog":
        if self.target_strategy == "create" and not self.target_title:
            raise ValueError("target_title is required when target_strategy=create")
        if self.target_strategy == "bind" and not self.target_chat_id:
            raise ValueError("target_chat_id is required when target_strategy=bind")
        if self.source_thread_id is not None and not self.telegram_chat_id:
            raise ValueError(
                "telegram_chat_id is required when source_thread_id is set",
            )
        return self

    @property
    def physical_source_chat_id(self) -> str:
        return self.telegram_chat_id or self.source_chat_id

    def matches_source_message(
        self,
        *,
        source_message_id: str,
        source_thread_id: str | None,
    ) -> bool:
        if self.source_thread_id is None:
            return True
        return (
            source_thread_id == self.source_thread_id
            or source_message_id == self.source_thread_id
        )


class MigrationManifest(BaseModel):
    migration_id: str
    mode: str = "backfill_delta_cutover"
    defaults: ManifestDefaults = Field(default_factory=ManifestDefaults)
    dialogs: list[ManifestDialog] = Field(default_factory=list)

    @model_validator(mode="after")
    def ensure_unique_dialogs(self) -> "MigrationManifest":
        dialog_ids = [dialog.source_chat_id for dialog in self.dialogs]
        if len(dialog_ids) != len(set(dialog_ids)):
            raise ValueError("dialog source_chat_id values must be unique")
        return self

    def dialog_for(self, source_chat_id: str) -> ManifestDialog:
        for dialog in self.dialogs:
            if dialog.source_chat_id == source_chat_id:
                return dialog
        raise ConfigurationError(
            f"source_chat_id={source_chat_id!r} is absent in the manifest",
        )


__all__ = ["ManifestDefaults", "ManifestDialog", "MigrationManifest"]
