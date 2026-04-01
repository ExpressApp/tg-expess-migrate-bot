from datetime import UTC, datetime

import pytest

from extg_migration_runtime.application.identity import UsernameEmailIdentityDirectory
from extg_migration_runtime.application.normalizer import (
    DisplayOnlyIdentityResolver,
    DirectoryBackedIdentityResolver,
    TelegramMessageNormalizer,
)
from extg_shared.contracts.models import (
    CanonicalEntity,
    ContentType,
    IdentityMappingRecord,
    TelegramAuthor,
    TelegramSourceMessage,
)
from extg_migration_runtime.infrastructure.express.fake_gateway import FakeExpressGateway
from extg_migration_runtime.infrastructure.persistence.in_memory import (
    InMemoryIdentityMappingRepository,
)


@pytest.mark.asyncio
async def test_normalizer_preserves_body_offsets_for_mentions():
    identity_repo = InMemoryIdentityMappingRepository()
    await identity_repo.save(
        IdentityMappingRecord(
            telegram_user_id="200",
            telegram_username="alice",
            telegram_display_name="Alice Smith",
            corporate_email="alice@example.com",
        ),
    )
    identity_directory = UsernameEmailIdentityDirectory(
        identity_mapping_repository=identity_repo,
        express_gateway=FakeExpressGateway(
            user_huid_by_email={"alice@example.com": "22222222-2222-2222-2222-222222222222"},
        ),
    )
    normalizer = TelegramMessageNormalizer(
        DirectoryBackedIdentityResolver(identity_directory),
        identity_directory=identity_directory,
    )

    message = TelegramSourceMessage(
        chat_id="chat-1",
        message_id="1",
        sent_at_utc=datetime(2026, 3, 19, 10, 0, tzinfo=UTC),
        author=TelegramAuthor(
            external_id="100",
            display_name="Peer User",
            username="peer.user",
        ),
        body="  Привет, @alice",
        content_type=ContentType.TEXT,
        entities=[
            CanonicalEntity(
                kind="mention",
                offset=10,
                length=6,
                text="@alice",
                telegram_user_id="200",
                telegram_username="alice",
            ),
        ],
    )

    normalized = await normalizer.normalize(message)

    assert normalized.body_plain == "  Привет, @alice"
    assert normalized.entities[0].target_huid == "22222222-2222-2222-2222-222222222222"
    assert normalized.entities[0].offset == 10


@pytest.mark.asyncio
async def test_normalizer_marks_unsupported_media_with_specific_kind():
    normalizer = TelegramMessageNormalizer(DisplayOnlyIdentityResolver())

    message = TelegramSourceMessage(
        chat_id="chat-1",
        message_id="2",
        sent_at_utc=datetime(2026, 3, 19, 10, 5, tzinfo=UTC),
        author=TelegramAuthor(
            external_id="100",
            display_name="Peer User",
            username="peer.user",
        ),
        body=None,
        content_type=ContentType.UNSUPPORTED,
        raw_payload={
            "_": "Message",
            "media": {"_": "MessageMediaContact"},
        },
    )

    normalized = await normalizer.normalize(message)

    assert normalized.body_plain == "[unsupported: contact]"


@pytest.mark.asyncio
async def test_normalizer_marks_unsupported_archive_media_with_specific_kind():
    normalizer = TelegramMessageNormalizer(DisplayOnlyIdentityResolver())

    message = TelegramSourceMessage(
        chat_id="chat-1",
        message_id="3",
        sent_at_utc=datetime(2026, 3, 19, 10, 10, tzinfo=UTC),
        author=TelegramAuthor(
            external_id="100",
            display_name="Peer User",
            username="peer.user",
        ),
        body=None,
        content_type=ContentType.UNSUPPORTED,
        raw_payload={
            "type": "message",
            "media_type": "video message",
        },
    )

    normalized = await normalizer.normalize(message)

    assert normalized.body_plain == "[unsupported: video_message]"
