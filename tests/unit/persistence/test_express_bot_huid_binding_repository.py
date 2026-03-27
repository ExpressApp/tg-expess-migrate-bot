from __future__ import annotations

from datetime import UTC, datetime

import pytest

from extg_migration_runtime.infrastructure.persistence.in_memory import (
    InMemoryExpressBotHuidBindingRepository,
)
from extg_migration_runtime.infrastructure.persistence.models import (
    ExpressBotHuidBindingModel,
)
from extg_migration_runtime.infrastructure.persistence.repositories import (
    PostgresExpressBotHuidBindingRepository,
)
from extg_shared.contracts.models import ExpressBotHuidBindingRecord


def _base_time() -> datetime:
    return datetime(2026, 3, 27, 12, 0, tzinfo=UTC)


def _build_record(
    *,
    bot_id: str = "00000000-0000-0000-0000-000000000001",
    cts_host: str = "cts-helper.example.test",
    bot_huid: str = "helper-bot-huid",
) -> ExpressBotHuidBindingRecord:
    return ExpressBotHuidBindingRecord(
        bot_id=bot_id,
        cts_host=cts_host,
        bot_huid=bot_huid,
        created_at=_base_time(),
        updated_at=_base_time(),
        last_learned_at=_base_time(),
    )


def _build_model(
    *,
    bot_id: str = "00000000-0000-0000-0000-000000000001",
    cts_host: str = "cts-helper.example.test",
    bot_huid: str = "helper-bot-huid",
) -> ExpressBotHuidBindingModel:
    return ExpressBotHuidBindingModel(
        bot_id=bot_id,
        cts_host=cts_host,
        bot_huid=bot_huid,
        created_at=_base_time(),
        updated_at=_base_time(),
        last_learned_at=_base_time(),
    )


class _Result:
    def __init__(self, value):
        self._value = value

    def scalar_one_or_none(self):
        return self._value


class _FakeSession:
    def __init__(self, execute_results):
        self._execute_results = list(execute_results)
        self.commit_calls = 0
        self.added_models = []

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
async def test_inmemory_express_bot_huid_binding_repo_supports_lookup_and_upsert():
    repository = InMemoryExpressBotHuidBindingRepository()

    await repository.save(_build_record())
    updated = await repository.save(
        _build_record(
            cts_host="cts-helper-2.example.test",
            bot_huid="helper-bot-huid-v2",
        ),
    )

    fetched = await repository.get_by_bot_id("00000000-0000-0000-0000-000000000001")

    assert updated.bot_huid == "helper-bot-huid-v2"
    assert fetched is not None
    assert fetched.cts_host == "cts-helper-2.example.test"
    assert fetched.bot_huid == "helper-bot-huid-v2"


@pytest.mark.asyncio
async def test_postgres_express_bot_huid_binding_repo_save_and_lookup():
    model = _build_model()
    session = _FakeSession(execute_results=[None, model])
    repository = PostgresExpressBotHuidBindingRepository(_SessionFactory(session))

    saved = await repository.save(_build_record())

    assert saved.bot_id == "00000000-0000-0000-0000-000000000001"
    assert saved.cts_host == "cts-helper.example.test"
    assert saved.bot_huid == "helper-bot-huid"
    assert session.commit_calls == 1

    lookup_session = _FakeSession(execute_results=[model])
    lookup_repository = PostgresExpressBotHuidBindingRepository(_SessionFactory(lookup_session))

    fetched = await lookup_repository.get_by_bot_id("00000000-0000-0000-0000-000000000001")

    assert fetched is not None
    assert fetched.bot_huid == "helper-bot-huid"
