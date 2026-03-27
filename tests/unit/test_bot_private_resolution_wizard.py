from __future__ import annotations

from dataclasses import dataclass
from uuid import uuid4

import pytest

from extg_shared.contracts.errors import ConfigurationError
from extg_bot_ui.presentation.bot.wizard import _parse_private_peer_input


@dataclass
class _MentionUser:
    entity_id: object


@dataclass
class _Mentions:
    users: list[object]


@dataclass
class _Message:
    body: str | None
    mentions: object


def test_parse_private_peer_input_accepts_email() -> None:
    result = _parse_private_peer_input(
        _Message(body="peer.user@example.com", mentions=_Mentions(users=[])),
    )

    assert result.corporate_email == "peer.user@example.com"
    assert result.target_huid is None


def test_parse_private_peer_input_accepts_huid() -> None:
    huid = str(uuid4())

    result = _parse_private_peer_input(
        _Message(body=huid, mentions=_Mentions(users=[])),
    )

    assert result.target_huid == huid
    assert result.corporate_email is None


def test_parse_private_peer_input_prefers_mention() -> None:
    huid = uuid4()

    result = _parse_private_peer_input(
        _Message(
            body="ignored body",
            mentions=_Mentions(users=[_MentionUser(entity_id=huid)]),
        ),
    )

    assert result.target_huid == str(huid)
    assert result.corporate_email is None


def test_parse_private_peer_input_rejects_invalid_value() -> None:
    with pytest.raises(ConfigurationError):
        _parse_private_peer_input(
            _Message(body="not-a-valid-peer", mentions=_Mentions(users=[])),
        )
