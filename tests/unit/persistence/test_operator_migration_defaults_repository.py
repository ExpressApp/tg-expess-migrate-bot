from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest

from extg_shared.contracts.models import OperatorMigrationDefaultsRecord
from extg_migration_runtime.infrastructure.persistence.in_memory import (
    InMemoryOperatorMigrationDefaultsRepository,
)
from extg_migration_runtime.infrastructure.persistence.models import (
    MigrationOperatorDefaultsModel,
)
from extg_migration_runtime.infrastructure.persistence.repositories import (
    PostgresOperatorMigrationDefaultsRepository,
)


def _base_time() -> datetime:
    return datetime(2026, 4, 23, 18, 0, tzinfo=UTC)


def _build_record(
    *,
    operator_huid: str = "operator-1",
    created_at: datetime | None = None,
    updated_at: datetime | None = None,
) -> OperatorMigrationDefaultsRecord:
    created_at = created_at or _base_time()
    updated_at = updated_at or created_at
    return OperatorMigrationDefaultsRecord(
        migration_id="migration-1",
        operator_huid=operator_huid,
        include_from=created_at - timedelta(days=7),
        include_to=created_at + timedelta(days=7),
        migrate_media=True,
        media_kinds=("photo", "file"),
        service_messages=False,
        reply_mode="source_id",
        access_strategy="direct_add",
        topic_strategy="split_by_topic",
        created_at=created_at,
        updated_at=updated_at,
    )


def _build_model() -> MigrationOperatorDefaultsModel:
    record = _build_record()
    return MigrationOperatorDefaultsModel(
        migration_id=record.migration_id,
        operator_huid=record.operator_huid,
        include_from=record.include_from,
        include_to=record.include_to,
        migrate_media=record.migrate_media,
        media_kinds_json=list(record.media_kinds or ()),
        service_messages=record.service_messages,
        reply_mode=record.reply_mode,
        access_strategy=record.access_strategy,
        topic_strategy=record.topic_strategy,
        created_at=record.created_at,
        updated_at=record.updated_at,
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
async def test_inmemory_operator_defaults_repo_supports_upsert_and_get():
    repository = InMemoryOperatorMigrationDefaultsRepository()
    created_at = _base_time()
    later = created_at + timedelta(hours=1)

    saved = await repository.save(
        _build_record(
            created_at=created_at,
            updated_at=created_at,
        ),
    )
    updated = await repository.save(
        _build_record(
            created_at=created_at + timedelta(days=1),
            updated_at=later,
        ),
    )
    fetched = await repository.get("migration-1", "operator-1")

    assert saved.created_at == created_at
    assert updated.created_at == created_at
    assert updated.updated_at == later
    assert updated.media_kinds == ("photo", "file")
    assert fetched == updated


@pytest.mark.asyncio
async def test_postgres_operator_defaults_repo_save_and_get_return_domain_record():
    model = _build_model()
    session = _FakeSession(execute_results=[model, model])
    repository = PostgresOperatorMigrationDefaultsRepository(_SessionFactory(session))

    saved = await repository.save(_build_record())
    fetched = await repository.get("migration-1", "operator-1")

    assert saved.operator_huid == "operator-1"
    assert saved.media_kinds == ("photo", "file")
    assert saved.service_messages is False
    assert fetched == saved
    assert session.commit_calls == 1
