from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Iterable

from extg_shared.contracts.manifest import MigrationManifest
from extg_shared.contracts.models import (
    ContentType,
    SourceDialog,
    TelegramAuthor,
    TelegramSourceMessage,
)


def load_sample_manifest() -> MigrationManifest:
    return MigrationManifest.model_validate(
        {
            "migration_id": "tg_to_express_sample",
            "defaults": {
                "migrate_media": True,
                "reply_mode": "inline_quote",
                "identity_policy": "display_only",
            },
            "dialogs": [
                {
                    "source_chat_id": "123456789",
                    "source_chat_type": "supergroup",
                    "target_strategy": "create",
                    "target_title": "Telegram Project Chat",
                },
                {
                    "source_chat_id": "987654321",
                    "source_chat_type": "channel",
                    "target_strategy": "create",
                    "target_title": "Telegram Announcements",
                    "migrate_media": False,
                },
            ],
        },
    )


def sample_dialog(dialog_id: str = "123456789") -> SourceDialog:
    return SourceDialog(
        dialog_id=dialog_id,
        chat_type="supergroup",
        title="Project X / Imported from Telegram",
    )


def sample_messages(
    *,
    dialog_id: str = "123456789",
    count: int = 3,
    start_minute: int = 0,
) -> list[TelegramSourceMessage]:
    base_time = datetime(2026, 3, 17, 10, 0, tzinfo=UTC)
    return [
        TelegramSourceMessage(
            chat_id=dialog_id,
            message_id=str(idx + 1),
            sent_at_utc=base_time + timedelta(minutes=start_minute + idx),
            chat_title="Telegram Project Chat",
            author=TelegramAuthor(
                external_id=f"user-{idx + 1}",
                display_name=f"User {idx + 1}",
            ),
            body=f"Message {idx + 1}",
            content_type=ContentType.TEXT,
        )
        for idx in range(count)
    ]


def sample_dialogs_and_messages(
    *,
    dialog_ids: Iterable[str] = ("123456789",),
) -> tuple[list[SourceDialog], dict[str, list[TelegramSourceMessage]]]:
    dialogs = [sample_dialog(dialog_id=id_) for id_ in dialog_ids]
    messages = {
        dialog.dialog_id: sample_messages(dialog_id=dialog.dialog_id, count=3)
        for dialog in dialogs
    }
    return dialogs, messages
