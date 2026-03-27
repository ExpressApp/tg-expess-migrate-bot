from __future__ import annotations

from datetime import UTC, datetime, timedelta
from enum import Enum, auto
from types import SimpleNamespace

import pytest

from pybotx_fsm.fsm import FSMStateData

from extg_bot_ui.infrastructure.persistence.fsm_state_repository import (
    PostgresFSMStateRepository,
)
from extg_bot_ui.infrastructure.persistence.models import BotFSMStateModel
from extg_bot_ui.infrastructure.persistence.in_memory_fsm_state import (
    InMemoryFSMStateRepository,
)


class SampleWizardState(Enum):
    SELECT = auto()
    CONFIRM = auto()


class _Result:
    def __init__(self, value):
        self._value = value

    def scalar_one_or_none(self):
        return self._value


class _FakeSession:
    def __init__(self, execute_results):
        self._execute_results = list(execute_results)
        self.commit_calls = 0

    async def execute(self, _statement):
        value = self._execute_results.pop(0)
        if isinstance(value, Exception):
            raise value
        return _Result(value)

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


class _MissingTableError(Exception):
    pass


@pytest.mark.asyncio
async def test_inmemory_fsm_state_repository_supports_set_get_delete_and_ttl():
    repository = InMemoryFSMStateRepository()
    value = FSMStateData(
        state=SampleWizardState.SELECT,
        storage=SimpleNamespace(source_chat_id="chat-1"),
    )

    await repository.set("key-1", value)
    loaded = await repository.get("key-1")
    await repository.delete("key-1")
    missing = await repository.get("key-1", default="missing")

    assert loaded.state is SampleWizardState.SELECT
    assert loaded.storage.source_chat_id == "chat-1"
    assert missing == "missing"

    await repository.set("key-expired", value, expire=0)
    expired = await repository.get("key-expired", default=None)
    assert expired is None


@pytest.mark.asyncio
async def test_postgres_fsm_state_repository_get_reconstructs_state_data():
    model = BotFSMStateModel(
        storage_key="key-1",
        state_module=__name__,
        state_class="SampleWizardState",
        state_name="CONFIRM",
        storage_payload={"reply_mode": "source_id"},
        expires_at=datetime.now(tz=UTC) + timedelta(minutes=5),
        created_at=datetime.now(tz=UTC),
        updated_at=datetime.now(tz=UTC),
    )
    session = _FakeSession(execute_results=[model])
    repository = PostgresFSMStateRepository(_SessionFactory(session))

    loaded = await repository.get("key-1")

    assert loaded.state is SampleWizardState.CONFIRM
    assert loaded.storage.reply_mode == "source_id"


@pytest.mark.asyncio
async def test_postgres_fsm_state_repository_drops_expired_records():
    model = BotFSMStateModel(
        storage_key="key-expired",
        state_module=__name__,
        state_class="SampleWizardState",
        state_name="SELECT",
        storage_payload={},
        expires_at=datetime.now(tz=UTC) - timedelta(seconds=1),
        created_at=datetime.now(tz=UTC),
        updated_at=datetime.now(tz=UTC),
    )
    session = _FakeSession(execute_results=[model, 1])
    repository = PostgresFSMStateRepository(_SessionFactory(session))

    loaded = await repository.get("key-expired", default="missing")

    assert loaded == "missing"
    assert session.commit_calls == 1


@pytest.mark.asyncio
async def test_postgres_fsm_state_repository_falls_back_to_memory_when_table_is_missing():
    session = _FakeSession(
        execute_results=[
            _MissingTableError('relation "bot_fsm_state" does not exist'),
        ],
    )
    repository = PostgresFSMStateRepository(_SessionFactory(session))
    value = FSMStateData(
        state=SampleWizardState.SELECT,
        storage=SimpleNamespace(source_chat_id="chat-2"),
    )

    missing = await repository.get("key-missing-table", default="default")
    await repository.set("key-memory", value, expire=60)
    loaded = await repository.get("key-memory")
    await repository.delete("key-memory")
    missing_after_delete = await repository.get("key-memory", default=None)

    assert missing == "default"
    assert loaded.state is SampleWizardState.SELECT
    assert loaded.storage.source_chat_id == "chat-2"
    assert missing_after_delete is None
