from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, Callable, Protocol

from extg_shared.contracts.ports import AuditRepository, OperatorTelegramSessionRepository
from extg_telethon_service.application.telegram_login_challenge_store import (
    InMemoryTelegramLoginChallengeStore,
    TelegramLoginChallenge,
)
from extg_telethon_service.application.telegram_session_cipher import TelegramSessionCipher
from extg_shared.contracts.errors import (
    ConfigurationError,
    FatalItemError,
    TwoFactorPasswordRequiredError,
)
from extg_shared.contracts.models import AuditEvent, AuditSeverity, OperatorTelegramSessionRecord
from extg_telethon_service.infrastructure.telegram.ops import (
    TelegramLoginCodeRequestResult,
    TelegramSessionBootstrapResult,
    complete_telegram_login_code,
    complete_telegram_login_password,
    request_telegram_login_code,
)


class _TelegramSettingsLike(Protocol):
    api_id: int | None
    api_hash: str | None
    request_timeout_seconds: float
    connection_retries: int


class _BotSettingsLike(Protocol):
    migration_id: str | None
    operator_huids: list[str]


class TelegramSessionServiceSettings(Protocol):
    telegram: _TelegramSettingsLike


@dataclass(frozen=True, slots=True)
class TelegramConnectionChallengeResult:
    operator_huid: str
    phone_number: str
    challenge_id: str


@dataclass(frozen=True, slots=True)
class TelegramPasswordChallengeResult:
    operator_huid: str
    phone_number: str
    challenge_id: str


@dataclass(frozen=True, slots=True)
class TelegramSessionStatusResult:
    operator_huid: str
    connected: bool
    source: str
    phone_number: str | None = None
    telegram_user_id: str | None = None
    telegram_username: str | None = None
    telegram_display_name: str | None = None
    session_bound: bool = False
    updated_at: datetime | None = None
    last_used_at: datetime | None = None


@dataclass(frozen=True, slots=True)
class TelegramDisconnectResult:
    operator_huid: str
    disconnected: bool


class TelegramSessionService:
    def __init__(
        self,
        *,
        settings: TelegramSessionServiceSettings,
        operator_session_repository: OperatorTelegramSessionRepository,
        audit_repository: AuditRepository,
        session_cipher: TelegramSessionCipher,
        telegram_gateway: Any | None = None,
        challenge_store: InMemoryTelegramLoginChallengeStore | None = None,
        request_login_code: Callable[..., Any] = request_telegram_login_code,
        complete_login_code: Callable[..., Any] = complete_telegram_login_code,
        complete_login_password: Callable[..., Any] = complete_telegram_login_password,
    ) -> None:
        self._settings = settings
        self._operator_session_repository = operator_session_repository
        self._audit_repository = audit_repository
        self._session_cipher = session_cipher
        self._telegram_gateway = telegram_gateway
        self._challenge_store = challenge_store or InMemoryTelegramLoginChallengeStore()
        self._request_login_code = request_login_code
        self._complete_login_code = complete_login_code
        self._complete_login_password = complete_login_password

    async def start_connection(
        self,
        *,
        operator_huid: str,
        phone_number: str,
        force_sms: bool = False,
    ) -> TelegramConnectionChallengeResult | TelegramSessionStatusResult:
        self._authorize(operator_huid)
        normalized_phone = phone_number.strip()
        if not normalized_phone:
            raise ConfigurationError("telegram phone number is required")

        existing = await self._operator_session_repository.get_by_operator(operator_huid)
        if existing is not None and self._decrypt_session(existing) is not None:
            raise ConfigurationError(
                "telegram session is already connected for this user; use /disconnect before reconnecting",
            )
        if existing is not None:
            await self._operator_session_repository.delete(operator_huid)

        try:
            result = await self._request_login_code(
                api_id=self._settings.telegram.api_id,
                api_hash=self._settings.telegram.api_hash,
                phone_number=normalized_phone,
                session_string=None,
                request_timeout_seconds=self._settings.telegram.request_timeout_seconds,
                connection_retries=self._settings.telegram.connection_retries,
                force_sms=force_sms,
            )
        except TypeError as error:
            raise ConfigurationError(
                "telegram phone number is invalid; send it in format +79990001122",
            ) from error
        if not isinstance(result, TelegramLoginCodeRequestResult):
            raise TypeError("request_login_code must return TelegramLoginCodeRequestResult")
        if result.already_authorized:
            record = await self._persist_session_record(
                operator_huid=operator_huid,
                bootstrap_result=TelegramSessionBootstrapResult(
                    phone_number=result.phone_number,
                    session_string=result.session_string,
                    already_authorized=True,
                    user_id=result.user_id or "",
                    username=result.username,
                    display_name=result.display_name or (result.username or result.user_id or "telegram_user"),
                ),
            )
            await self._audit(
                "telegram_session_connected",
                operator_huid,
                payload={"source": "existing_authorized_session"},
            )
            return self._status_from_record(record)
        if not result.phone_code_hash:
            raise FatalItemError("telegram did not return phone_code_hash for login challenge")
        challenge = self._challenge_store.create(
            operator_huid=operator_huid,
            phone_number=result.phone_number,
            session_string=result.session_string,
            phone_code_hash=result.phone_code_hash,
        )
        await self._audit(
            "telegram_session_code_requested",
            operator_huid,
            payload={"phone_number": self._mask_phone_number(result.phone_number)},
        )
        return TelegramConnectionChallengeResult(
            operator_huid=operator_huid,
            phone_number=self._mask_phone_number(result.phone_number),
            challenge_id=challenge.challenge_id,
        )

    async def complete_code(
        self,
        *,
        operator_huid: str,
        challenge_id: str,
        code: str,
    ) -> TelegramPasswordChallengeResult | TelegramSessionStatusResult:
        self._authorize(operator_huid)
        challenge = self._get_challenge(operator_huid, challenge_id)
        try:
            result = await self._complete_login_code(
                api_id=self._settings.telegram.api_id,
                api_hash=self._settings.telegram.api_hash,
                phone_number=challenge.phone_number,
                session_string=challenge.session_string,
                phone_code_hash=challenge.phone_code_hash,
                code=code,
                request_timeout_seconds=self._settings.telegram.request_timeout_seconds,
                connection_retries=self._settings.telegram.connection_retries,
            )
        except TwoFactorPasswordRequiredError:
            self._challenge_store.mark_password_required(challenge_id)
            await self._audit(
                "telegram_session_password_required",
                operator_huid,
                payload={"phone_number": self._mask_phone_number(challenge.phone_number)},
            )
            return TelegramPasswordChallengeResult(
                operator_huid=operator_huid,
                phone_number=self._mask_phone_number(challenge.phone_number),
                challenge_id=challenge_id,
            )

        self._challenge_store.delete(challenge_id)
        record = await self._persist_session_record(
            operator_huid=operator_huid,
            bootstrap_result=result,
        )
        await self._audit(
            "telegram_session_connected",
            operator_huid,
            payload={"source": "code_confirmation"},
        )
        return self._status_from_record(record)

    async def complete_password(
        self,
        *,
        operator_huid: str,
        challenge_id: str,
        password: str,
    ) -> TelegramSessionStatusResult:
        self._authorize(operator_huid)
        challenge = self._get_challenge(operator_huid, challenge_id)
        if not challenge.requires_password:
            raise ConfigurationError("telegram password challenge is not active; request a new login code")
        result = await self._complete_login_password(
            api_id=self._settings.telegram.api_id,
            api_hash=self._settings.telegram.api_hash,
            phone_number=challenge.phone_number,
            session_string=challenge.session_string,
            password=password,
            request_timeout_seconds=self._settings.telegram.request_timeout_seconds,
            connection_retries=self._settings.telegram.connection_retries,
        )
        self._challenge_store.delete(challenge_id)
        record = await self._persist_session_record(
            operator_huid=operator_huid,
            bootstrap_result=result,
        )
        await self._audit(
            "telegram_session_connected",
            operator_huid,
            payload={"source": "password_confirmation"},
        )
        return self._status_from_record(record)

    async def status(
        self,
        *,
        operator_huid: str,
    ) -> TelegramSessionStatusResult:
        self._authorize(operator_huid)
        record = await self._operator_session_repository.get_by_operator(operator_huid)
        if record is None:
            return TelegramSessionStatusResult(
                operator_huid=operator_huid,
                connected=False,
                source="not_connected",
            )
        if self._decrypt_session(record) is None:
            return TelegramSessionStatusResult(
                operator_huid=operator_huid,
                connected=False,
                source="legacy_session_reconnect_required",
                phone_number=record.phone_number,
                telegram_user_id=record.telegram_user_id,
                telegram_username=record.telegram_username,
                telegram_display_name=record.telegram_display_name,
                session_bound=False,
                updated_at=record.updated_at,
                last_used_at=record.last_used_at,
            )
        return self._status_from_record(record)

    async def disconnect(
        self,
        *,
        operator_huid: str,
    ) -> TelegramDisconnectResult:
        self._authorize(operator_huid)
        record = await self._operator_session_repository.get_by_operator(operator_huid)
        if record is None:
            return TelegramDisconnectResult(operator_huid=operator_huid, disconnected=False)

        session_string = self._decrypt_session(record)
        if session_string is not None:
            await self._close_gateway_session(session_string)
        deleted = await self._operator_session_repository.delete(operator_huid)
        if deleted:
            await self._audit(
                "telegram_session_disconnected",
                operator_huid,
                payload={"scope": "operator_binding"},
            )
        return TelegramDisconnectResult(
            operator_huid=operator_huid,
            disconnected=deleted,
        )

    async def resolve_session_string(
        self,
        *,
        operator_huid: str,
        mark_used: bool = True,
    ) -> str:
        self._authorize(operator_huid)
        record = await self._operator_session_repository.get_by_operator(operator_huid)
        if record is None or not self._has_bound_session(record):
            raise ConfigurationError(
                "telegram session is not connected for this user; use /connect first",
            )
        session_string = self._decrypt_session(record)
        if session_string is None:
            await self._operator_session_repository.delete(operator_huid)
            raise ConfigurationError(
                "telegram session cannot be decrypted; reconnect via /connect",
            )
        if mark_used:
            now = self._now()
            await self._operator_session_repository.save(
                OperatorTelegramSessionRecord(
                    operator_huid=record.operator_huid,
                    phone_number=record.phone_number,
                    session_path=record.session_path,
                    telegram_user_id=record.telegram_user_id,
                    telegram_username=record.telegram_username,
                    telegram_display_name=record.telegram_display_name,
                    created_at=record.created_at,
                    updated_at=now,
                    last_used_at=now,
                ),
            )
        return session_string

    async def _persist_session_record(
        self,
        *,
        operator_huid: str,
        bootstrap_result: TelegramSessionBootstrapResult,
    ) -> OperatorTelegramSessionRecord:
        existing = await self._operator_session_repository.get_by_operator(operator_huid)
        now = self._now()
        return await self._operator_session_repository.save(
            OperatorTelegramSessionRecord(
                operator_huid=operator_huid,
                phone_number=self._mask_phone_number(bootstrap_result.phone_number),
                session_path=self._session_cipher.encrypt(bootstrap_result.session_string),
                telegram_user_id=bootstrap_result.user_id,
                telegram_username=bootstrap_result.username,
                telegram_display_name=bootstrap_result.display_name,
                created_at=existing.created_at if existing is not None else now,
                updated_at=now,
                last_used_at=now,
            ),
        )

    async def _close_gateway_session(self, session_string: str) -> None:
        close_session = getattr(self._telegram_gateway, "close_session", None)
        if close_session is None:
            return
        result = close_session(session_string)
        if hasattr(result, "__await__"):
            await result

    async def _audit(
        self,
        event_type: str,
        operator_huid: str,
        *,
        payload: dict[str, object],
    ) -> None:
        await self._audit_repository.add(
            AuditEvent(
                migration_id=self._migration_id_for_tracking(),
                event_type=event_type,
                severity=AuditSeverity.INFO,
                payload_json={
                    "operator_huid": operator_huid,
                    **payload,
                },
                created_at=self._now(),
            ),
        )

    def _authorize(self, operator_huid: str) -> None:
        bot_settings = self._bot_settings()
        allowed_huids = set(bot_settings.operator_huids) if bot_settings is not None else set()
        if not allowed_huids:
            return
        if operator_huid not in allowed_huids:
            raise FatalItemError(
                f"user huid={operator_huid} is not allowed to manage migrations",
            )

    def _get_challenge(
        self,
        operator_huid: str,
        challenge_id: str,
    ) -> TelegramLoginChallenge:
        challenge = self._challenge_store.get(challenge_id)
        if challenge is None or challenge.operator_huid != operator_huid:
            raise ConfigurationError("telegram login challenge expired; run /connect again")
        return challenge

    def _has_bound_session(self, record: OperatorTelegramSessionRecord) -> bool:
        return bool(record.session_path and ":" in record.session_path)

    def _decrypt_session(
        self,
        record: OperatorTelegramSessionRecord,
    ) -> str | None:
        if not self._has_bound_session(record):
            return None
        try:
            return self._session_cipher.decrypt(record.session_path)
        except FatalItemError:
            return None

    def _status_from_record(
        self,
        record: OperatorTelegramSessionRecord,
    ) -> TelegramSessionStatusResult:
        return TelegramSessionStatusResult(
            operator_huid=record.operator_huid,
            connected=True,
            source="operator_binding",
            phone_number=record.phone_number,
            telegram_user_id=record.telegram_user_id,
            telegram_username=record.telegram_username,
            telegram_display_name=record.telegram_display_name,
            session_bound=self._decrypt_session(record) is not None,
            updated_at=record.updated_at,
            last_used_at=record.last_used_at,
        )

    def _migration_id_for_tracking(self) -> str:
        bot_settings = self._bot_settings()
        if bot_settings is not None and bot_settings.migration_id:
            return bot_settings.migration_id
        return "bot_telegram_session"

    def _bot_settings(self) -> _BotSettingsLike | None:
        candidate = getattr(self._settings, "bot", None)
        if candidate is None:
            return None
        migration_id = getattr(candidate, "migration_id", None)
        operator_huids = getattr(candidate, "operator_huids", None)
        if migration_id is not None and not isinstance(migration_id, str):
            return None
        if not isinstance(operator_huids, list):
            return None
        return candidate

    def _mask_phone_number(self, phone_number: str | None) -> str:
        normalized = (phone_number or "").strip()
        if len(normalized) <= 4:
            return "*" * len(normalized)
        return f"{normalized[:2]}***{normalized[-2:]}"

    def _now(self) -> datetime:
        return datetime.now(tz=UTC)
