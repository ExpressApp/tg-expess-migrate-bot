from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest

from extg_shared.contracts.models import ChatMigrationConfigRecord
from extg_migration_runtime.infrastructure.persistence.models import (
    MigrationChatConfigModel,
)
from extg_migration_runtime.infrastructure.persistence.repositories import (
    PostgresChatMigrationConfigRepository,
)
from extg_migration_runtime.infrastructure.persistence.in_memory import (
    InMemoryChatMigrationConfigRepository,
)


def _base_time() -> datetime:
    return datetime(2026, 3, 18, 12, 0, tzinfo=UTC)


def _build_record(
    *,
    source_chat_id: str = "chat-1",
    source_chat_title: str = "Source Chat 1",
    anchor_cts_host: str | None = None,
    anchor_bot_id: str | None = None,
    target_strategy: str = "existing_chat",
    target_title: str | None = "Target Chat 1",
    target_chat_id: str | None = "target-chat-1",
    migrate_media: bool = True,
    source_backend: str = "telethon_user_session",
    skip_in_all: bool = False,
    updated_by_huid: str = "huid-1",
    created_at: datetime | None = None,
    updated_at: datetime | None = None,
) -> ChatMigrationConfigRecord:
    created_at = created_at or _base_time()
    updated_at = updated_at or created_at
    return ChatMigrationConfigRecord(
        migration_id="migration-1",
        source_chat_id=source_chat_id,
        source_chat_type="supergroup",
        source_chat_title=source_chat_title,
        target_strategy=target_strategy,
        target_title=target_title,
        target_chat_id=target_chat_id,
        include_from=created_at - timedelta(days=7),
        include_to=created_at + timedelta(days=7),
        migrate_media=migrate_media,
        reply_mode="quote",
        identity_policy="directory",
        skip_in_all=skip_in_all,
        updated_by_huid=updated_by_huid,
        created_at=created_at,
        updated_at=updated_at,
        anchor_cts_host=anchor_cts_host,
        anchor_bot_id=anchor_bot_id,
        source_backend=source_backend,
    )


def _build_model(
    *,
    source_chat_id: str = "chat-1",
    source_chat_title: str = "Source Chat 1",
    anchor_cts_host: str | None = None,
    anchor_bot_id: str | None = None,
    target_strategy: str = "existing_chat",
    target_title: str | None = "Target Chat 1",
    target_chat_id: str | None = "target-chat-1",
    migrate_media: bool = True,
    source_backend: str = "telethon_user_session",
    skip_in_all: bool = False,
    updated_by_huid: str = "huid-1",
    created_at: datetime | None = None,
    updated_at: datetime | None = None,
) -> MigrationChatConfigModel:
    created_at = created_at or _base_time()
    updated_at = updated_at or created_at
    return MigrationChatConfigModel(
        migration_id="migration-1",
        source_chat_id=source_chat_id,
        source_chat_type="supergroup",
        source_chat_title=source_chat_title,
        anchor_cts_host=anchor_cts_host,
        anchor_bot_id=anchor_bot_id,
        source_backend=source_backend,
        target_strategy=target_strategy,
        target_title=target_title,
        target_chat_id=target_chat_id,
        include_from=created_at - timedelta(days=7),
        include_to=created_at + timedelta(days=7),
        migrate_media=migrate_media,
        reply_mode="quote",
        identity_policy="directory",
        skip_in_all=skip_in_all,
        updated_by_huid=updated_by_huid,
        created_at=created_at,
        updated_at=updated_at,
    )


class _Result:
    def __init__(self, value):
        self._value = value

    def scalar_one_or_none(self):
        return self._value

    def scalar_one(self):
        if self._value is None:
            raise AssertionError("expected scalar_one value, got None")
        return self._value

    def scalars(self):
        values = self._value if isinstance(self._value, list) else [self._value]
        return _Scalars(values)


class _Scalars:
    def __init__(self, values):
        self._values = values

    def all(self):
        return self._values


class _FakeSession:
    def __init__(self, execute_results):
        self._execute_results = list(execute_results)
        self.commit_calls = 0

    async def execute(self, _statement):
        return _Result(self._execute_results.pop(0))

    async def commit(self):
        self.commit_calls += 1


class _SessionFactory:
    def __init__(self, session: _FakeSession):
        self._session = session

    def __call__(self):
        return self

    async def __aenter__(self):
        return self._session

    async def __aexit__(self, exc_type, exc, tb):
        return False


@pytest.mark.asyncio
async def test_inmemory_chat_migration_config_repo_supports_upsert_list_and_delete():
    repository = InMemoryChatMigrationConfigRepository()
    created_at = _base_time()
    later = created_at + timedelta(hours=1)

    await repository.save(
        _build_record(
            source_chat_id="chat-b",
            source_chat_title="Chat B",
            created_at=created_at,
            updated_at=created_at,
        ),
    )
    await repository.save(
        _build_record(
            source_chat_id="chat-a",
            source_chat_title="Chat A",
            created_at=created_at,
            updated_at=created_at,
        ),
    )

    updated = await repository.save(
        _build_record(
            source_chat_id="chat-a",
            source_chat_title="Chat A renamed",
            target_strategy="create_chat",
            target_title=None,
            target_chat_id=None,
            migrate_media=False,
            updated_by_huid="huid-1",
            created_at=created_at + timedelta(days=1),
            updated_at=later,
        ),
    )
    listed = await repository.list_by_migration("migration-1")
    fetched = await repository.get("migration-1", "chat-a")
    deleted = await repository.delete("migration-1", "chat-a")
    deleted_again = await repository.delete("migration-1", "chat-a")

    assert updated.created_at == created_at
    assert updated.updated_at == later
    assert updated.target_strategy == "create_chat"
    assert updated.target_chat_id is None
    assert updated.source_backend == "telethon_user_session"
    assert fetched == updated
    assert [item.source_chat_id for item in listed] == ["chat-a", "chat-b"]
    assert deleted is True
    assert deleted_again is False


@pytest.mark.asyncio
async def test_inmemory_chat_migration_config_repo_does_not_override_foreign_owner():
    repository = InMemoryChatMigrationConfigRepository()
    created_at = _base_time()
    original = _build_record(
        source_chat_id="chat-a",
        source_chat_title="Chat A",
        updated_by_huid="huid-1",
        created_at=created_at,
        updated_at=created_at,
    )
    await repository.save(original)

    updated = await repository.save(
        _build_record(
            source_chat_id="chat-a",
            source_chat_title="Chat A hijacked",
            updated_by_huid="huid-2",
            created_at=created_at + timedelta(days=1),
            updated_at=created_at + timedelta(hours=1),
        ),
    )

    assert updated.updated_by_huid == "huid-1"
    assert updated.source_chat_title == "Chat A"


@pytest.mark.asyncio
async def test_postgres_chat_migration_config_repo_save_returns_saved_record():
    model = _build_model(
        source_backend="telegram_bot_api_live",
        anchor_cts_host="cts2.example",
        anchor_bot_id="bot-cts2",
    )
    session = _FakeSession(execute_results=[model])
    repository = PostgresChatMigrationConfigRepository(_SessionFactory(session))

    saved = await repository.save(
        _build_record(
            source_backend="telegram_bot_api_live",
            anchor_cts_host="cts2.example",
            anchor_bot_id="bot-cts2",
        ),
    )

    assert saved.source_chat_id == "chat-1"
    assert saved.target_chat_id == "target-chat-1"
    assert saved.updated_by_huid == "huid-1"
    assert saved.source_backend == "telegram_bot_api_live"
    assert saved.anchor_cts_host == "cts2.example"
    assert saved.anchor_bot_id == "bot-cts2"
    assert session.commit_calls == 1


@pytest.mark.asyncio
async def test_postgres_chat_migration_config_repo_lists_and_deletes_records():
    session = _FakeSession(
        execute_results=[
            [
                _build_model(source_chat_id="chat-a", source_chat_title="Chat A"),
                _build_model(source_chat_id="chat-b", source_chat_title="Chat B"),
            ],
            1,
            None,
        ],
    )
    repository = PostgresChatMigrationConfigRepository(_SessionFactory(session))

    listed = await repository.list_by_migration("migration-1")
    deleted = await repository.delete("migration-1", "chat-a")
    missing = await repository.delete("migration-1", "chat-missing")

    assert [item.source_chat_id for item in listed] == ["chat-a", "chat-b"]
    assert deleted is True
    assert missing is False
    assert session.commit_calls == 2
