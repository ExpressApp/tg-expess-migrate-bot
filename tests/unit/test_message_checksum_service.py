from __future__ import annotations

from datetime import UTC, datetime

import pytest

from extg_migration_runtime.application.message_checksum import MessageChecksumService
from extg_shared.contracts.errors import ConfigurationError
from extg_shared.contracts.models import CanonicalMessage, ContentType


def _build_message() -> CanonicalMessage:
    return CanonicalMessage(
        source_platform="telegram",
        source_chat_id="chat-1",
        source_message_id="100",
        source_thread_id=None,
        source_reply_to_message_id=None,
        sender_external_id="tg-user-1",
        sender_display_name="Ivan",
        sender_username="ivan",
        sent_at_utc=datetime(2026, 3, 19, 10, 0, tzinfo=UTC),
        edited_at_utc=None,
        deleted_in_source=False,
        content_type=ContentType.TEXT,
        body_plain="secret body",
        body_rendered="secret body",
    )


def test_message_checksum_service_uses_stable_hmac_for_same_secret():
    message = _build_message()

    first = MessageChecksumService(secret="secret-a").checksum(message)
    second = MessageChecksumService(secret="secret-a").checksum(message)

    assert first == second
    assert len(first) == 64


def test_message_checksum_service_changes_digest_when_secret_changes():
    message = _build_message()

    first = MessageChecksumService(secret="secret-a").checksum(message)
    second = MessageChecksumService(secret="secret-b").checksum(message)

    assert first != second


def test_message_checksum_service_rejects_empty_secret():
    with pytest.raises(ConfigurationError, match="message checksum secret must not be empty"):
        MessageChecksumService(secret="   ")
