from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import UTC, datetime, timedelta
from threading import Lock
from uuid import uuid4


@dataclass(frozen=True, slots=True)
class TelegramLoginChallenge:
    challenge_id: str
    operator_huid: str
    phone_number: str
    session_string: str
    phone_code_hash: str
    expires_at: datetime
    requires_password: bool = False


class InMemoryTelegramLoginChallengeStore:
    def __init__(self, *, ttl_seconds: int = 900) -> None:
        self._ttl_seconds = max(ttl_seconds, 60)
        self._items: dict[str, TelegramLoginChallenge] = {}
        self._lock = Lock()

    def create(
        self,
        *,
        operator_huid: str,
        phone_number: str,
        session_string: str,
        phone_code_hash: str,
    ) -> TelegramLoginChallenge:
        challenge = TelegramLoginChallenge(
            challenge_id=uuid4().hex,
            operator_huid=operator_huid,
            phone_number=phone_number,
            session_string=session_string,
            phone_code_hash=phone_code_hash,
            expires_at=self._now() + timedelta(seconds=self._ttl_seconds),
        )
        with self._lock:
            self._cleanup_locked()
            self._items[challenge.challenge_id] = challenge
        return challenge

    def get(
        self,
        challenge_id: str,
    ) -> TelegramLoginChallenge | None:
        with self._lock:
            self._cleanup_locked()
            return self._items.get(challenge_id)

    def mark_password_required(
        self,
        challenge_id: str,
    ) -> TelegramLoginChallenge | None:
        with self._lock:
            self._cleanup_locked()
            existing = self._items.get(challenge_id)
            if existing is None:
                return None
            updated = replace(existing, requires_password=True)
            self._items[challenge_id] = updated
            return updated

    def update_session_string(
        self,
        challenge_id: str,
        *,
        session_string: str,
    ) -> TelegramLoginChallenge | None:
        with self._lock:
            self._cleanup_locked()
            existing = self._items.get(challenge_id)
            if existing is None:
                return None
            updated = replace(existing, session_string=session_string)
            self._items[challenge_id] = updated
            return updated

    def delete(self, challenge_id: str) -> bool:
        with self._lock:
            return self._items.pop(challenge_id, None) is not None

    def _cleanup_locked(self) -> None:
        now = self._now()
        expired = [
            challenge_id
            for challenge_id, challenge in self._items.items()
            if challenge.expires_at <= now
        ]
        for challenge_id in expired:
            self._items.pop(challenge_id, None)

    def _now(self) -> datetime:
        return datetime.now(tz=UTC)
