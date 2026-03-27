from __future__ import annotations

from datetime import UTC, datetime
from types import SimpleNamespace

import pytest

from extg_shared.contracts.models import MessageImportStatus, MessageMappingRecord
from extg_shared.contracts.ports import MessageStatusSummary
from extg_migration_runtime.infrastructure.persistence.in_memory import (
    InMemoryMessageMappingRepository,
)
from extg_migration_runtime.infrastructure.persistence.repositories import (
    PostgresMessageMappingRepository,
)


def _build_record(
    *,
    source_chat_id: str,
    source_message_id: str,
    status: MessageImportStatus,
) -> MessageMappingRecord:
    return MessageMappingRecord(
        migration_id="migration-1",
        source_chat_id=source_chat_id,
        source_message_id=source_message_id,
        source_sent_at=datetime(2026, 3, 18, 10, 0, tzinfo=UTC),
        target_chat_id="target-chat",
        checksum="abc",
        import_status=status,
    )


@pytest.mark.asyncio
async def test_inmemory_summary_counts():
    repository = InMemoryMessageMappingRepository()
    repository._items[("migration-1", "chat-a", "msg-1")] = _build_record(
        source_chat_id="chat-a",
        source_message_id="msg-1",
        status=MessageImportStatus.IMPORTED,
    )
    repository._items[("migration-1", "chat-a", "msg-2")] = _build_record(
        source_chat_id="chat-a",
        source_message_id="msg-2",
        status=MessageImportStatus.FAILED,
    )
    repository._items[("migration-1", "chat-b", "msg-3")] = _build_record(
        source_chat_id="chat-b",
        source_message_id="msg-3",
        status=MessageImportStatus.IMPORTED,
    )

    summary = await repository.summarize_by_chat("migration-1")

    assert summary == [
        MessageStatusSummary(
            migration_id="migration-1",
            source_chat_id="chat-a",
            import_status=MessageImportStatus.FAILED,
            count=1,
        ),
        MessageStatusSummary(
            migration_id="migration-1",
            source_chat_id="chat-a",
            import_status=MessageImportStatus.IMPORTED,
            count=1,
        ),
        MessageStatusSummary(
            migration_id="migration-1",
            source_chat_id="chat-b",
            import_status=MessageImportStatus.IMPORTED,
            count=1,
        ),
    ]


class _FakeResult:
    def __init__(self, rows: list[SimpleNamespace]) -> None:
        self._rows = rows

    def all(self) -> list[SimpleNamespace]:
        return self._rows


class _FakeSession:
    def __init__(self, rows: list[SimpleNamespace]) -> None:
        self._rows = rows

    async def execute(self, _statement):
        return _FakeResult(self._rows)


class _SessionFactory:
    def __init__(self, session: _FakeSession) -> None:
        self._session = session

    def __call__(self) -> "_SessionFactory":
        return self

    async def __aenter__(self) -> _FakeSession:
        return self._session

    async def __aexit__(self, exc_type, exc, tb):
        return False


@pytest.mark.asyncio
async def test_postgres_summary_returns_expected_rows():
    rows = [
        SimpleNamespace(
            source_chat_id="chat-x",
            import_status=MessageImportStatus.FAILED.value,
            count=1,
        ),
        SimpleNamespace(
            source_chat_id="chat-x",
            import_status=MessageImportStatus.IMPORTED.value,
            count=2,
        ),
    ]
    session = _FakeSession(rows)
    repository = PostgresMessageMappingRepository(_SessionFactory(session))

    summary = await repository.summarize_by_chat("migration-1")

    assert summary == [
        MessageStatusSummary(
            migration_id="migration-1",
            source_chat_id="chat-x",
            import_status=MessageImportStatus.FAILED,
            count=1,
        ),
        MessageStatusSummary(
            migration_id="migration-1",
            source_chat_id="chat-x",
            import_status=MessageImportStatus.IMPORTED,
            count=2,
        ),
    ]
