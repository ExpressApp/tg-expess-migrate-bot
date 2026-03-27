from __future__ import annotations

from datetime import UTC, datetime

import pytest

from extg_migration_runtime.infrastructure.persistence.in_memory import (
    InMemoryExpressUserCtsBindingRepository,
)
from extg_migration_runtime.infrastructure.persistence.models import (
    ExpressUserCtsBindingModel,
)
from extg_migration_runtime.infrastructure.persistence.repositories import (
    PostgresExpressUserCtsBindingRepository,
)
from extg_shared.contracts.models import ExpressUserCtsBindingRecord


def _base_time() -> datetime:
    return datetime(2026, 3, 26, 12, 0, tzinfo=UTC)


def _build_record(
    *,
    target_huid: str | None = "huid-1",
    corporate_email: str | None = "user@example.test",
    cts_host: str = "cts2.example.test",
) -> ExpressUserCtsBindingRecord:
    return ExpressUserCtsBindingRecord(
        target_huid=target_huid,
        corporate_email=corporate_email,
        cts_host=cts_host,
        created_at=_base_time(),
        updated_at=_base_time(),
        last_verified_at=_base_time(),
    )


def _build_model(
    *,
    target_huid: str | None = "huid-1",
    corporate_email: str | None = "user@example.test",
    cts_host: str = "cts2.example.test",
) -> ExpressUserCtsBindingModel:
    return ExpressUserCtsBindingModel(
        target_huid=target_huid,
        corporate_email=corporate_email,
        cts_host=cts_host,
        created_at=_base_time(),
        updated_at=_base_time(),
        last_verified_at=_base_time(),
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
        self.added_models = []
        self.deleted_models = []

    async def execute(self, _statement):
        if not self._execute_results:
            raise AssertionError("unexpected execute call")
        return _Result(self._execute_results.pop(0))

    async def commit(self):
        self.commit_calls += 1

    async def refresh(self, _model):
        return None

    def add(self, model):
        self.added_models.append(model)

    async def delete(self, model):
        self.deleted_models.append(model)


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
async def test_inmemory_express_user_cts_binding_repo_supports_cache_lookup_and_upsert():
    repository = InMemoryExpressUserCtsBindingRepository()

    await repository.save(_build_record())
    updated = await repository.save(
        _build_record(
            target_huid="huid-1",
            corporate_email="user@example.test",
            cts_host="cts3.example.test",
        ),
    )

    by_huid = await repository.get_by_target_huid("huid-1")
    by_email = await repository.get_by_email("user@example.test")

    assert updated.cts_host == "cts3.example.test"
    assert by_huid is not None
    assert by_email is not None
    assert by_huid.cts_host == "cts3.example.test"
    assert by_email.target_huid == "huid-1"


@pytest.mark.asyncio
async def test_postgres_express_user_cts_binding_repo_save_and_lookup():
    model = _build_model()
    session = _FakeSession(execute_results=[model, model])
    repository = PostgresExpressUserCtsBindingRepository(_SessionFactory(session))

    saved = await repository.save(_build_record())

    assert saved.target_huid == "huid-1"
    assert saved.corporate_email == "user@example.test"
    assert saved.cts_host == "cts2.example.test"
    assert session.commit_calls == 1

    lookup_session = _FakeSession(execute_results=[model, model])
    lookup_repository = PostgresExpressUserCtsBindingRepository(_SessionFactory(lookup_session))

    by_huid = await lookup_repository.get_by_target_huid("huid-1")
    by_email = await lookup_repository.get_by_email("user@example.test")

    assert by_huid is not None
    assert by_email is not None
    assert by_huid.cts_host == "cts2.example.test"
    assert by_email.target_huid == "huid-1"
