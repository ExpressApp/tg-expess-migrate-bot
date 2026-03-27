from __future__ import annotations

import asyncio
from contextlib import suppress
from dataclasses import dataclass
from typing import Any

from telethon import TelegramClient
from telethon.errors import (
    FloodWaitError,
    PasswordHashInvalidError,
    PhoneCodeExpiredError,
    PhoneCodeInvalidError,
    RPCError,
    SessionPasswordNeededError,
)
from telethon.sessions import StringSession

from extg_shared.contracts.errors import (
    ConfigurationError,
    FatalItemError,
    RecoverableItemError,
    TwoFactorPasswordRequiredError,
)


@dataclass(frozen=True, slots=True)
class TelegramSessionBootstrapResult:
    phone_number: str
    session_string: str
    already_authorized: bool
    user_id: str
    username: str | None
    display_name: str


@dataclass(frozen=True, slots=True)
class TelegramLoginCodeRequestResult:
    phone_number: str
    session_string: str
    code_sent: bool
    phone_code_hash: str | None = None
    already_authorized: bool = False
    user_id: str | None = None
    username: str | None = None
    display_name: str | None = None


async def request_telegram_login_code(
    *,
    api_id: int | None,
    api_hash: str | None,
    phone_number: str | None,
    session_string: str | None = None,
    request_timeout_seconds: float = 30.0,
    connection_retries: int = 5,
    force_sms: bool = False,
) -> TelegramLoginCodeRequestResult:
    if api_id is None or not api_hash:
        raise ConfigurationError(
            "telegram api_id/api_hash are required to initialize a Telethon session",
        )
    if not phone_number:
        raise ConfigurationError(
            "telegram phone_number is required to initialize a Telethon session",
        )
    client = _build_client(
        api_id=api_id,
        api_hash=api_hash,
        session_string=session_string,
        request_timeout_seconds=request_timeout_seconds,
        connection_retries=connection_retries,
    )
    skip_async_cleanup = False
    try:
        await client.connect()
        current_session_string = _save_string_session(client)
        if await client.is_user_authorized():
            me = await client.get_me()
            if me is None:
                raise FatalItemError("telegram session is authorized but current user is unavailable")
            bootstrap_result = _bootstrap_result(
                phone_number=phone_number,
                session_string=current_session_string,
                already_authorized=True,
                user=me,
            )
            return TelegramLoginCodeRequestResult(
                phone_number=bootstrap_result.phone_number,
                session_string=bootstrap_result.session_string,
                code_sent=False,
                already_authorized=True,
                user_id=bootstrap_result.user_id,
                username=bootstrap_result.username,
                display_name=bootstrap_result.display_name,
            )

        sent_code = await client.send_code_request(phone_number, force_sms=force_sms)
        return TelegramLoginCodeRequestResult(
            phone_number=phone_number,
            session_string=_save_string_session(client),
            code_sent=True,
            phone_code_hash=sent_code.phone_code_hash,
        )
    except (asyncio.CancelledError, GeneratorExit):
        skip_async_cleanup = True
        raise
    except FloodWaitError as error:
        raise RecoverableItemError(
            f"telegram flood wait during session bootstrap: {error.seconds}s",
        ) from error
    except (OSError, TimeoutError) as error:
        raise RecoverableItemError(
            f"telegram temporary network failure during session bootstrap: {error}",
        ) from error
    except RPCError as error:
        raise FatalItemError(f"telegram RPC error during session bootstrap: {error}") from error
    except TypeError as error:
        raise ConfigurationError(
            "telegram phone number is invalid; send it in format +79990001122",
        ) from error
    finally:
        if not skip_async_cleanup:
            with suppress(Exception):
                await client.disconnect()


async def complete_telegram_login_code(
    *,
    api_id: int | None,
    api_hash: str | None,
    phone_number: str | None,
    session_string: str | None,
    phone_code_hash: str | None,
    code: str | None,
    request_timeout_seconds: float = 30.0,
    connection_retries: int = 5,
) -> TelegramSessionBootstrapResult:
    if not phone_code_hash:
        raise ConfigurationError("telegram phone_code_hash is required to complete login")
    if not code:
        raise ConfigurationError("telegram login code is required to complete login")
    if not session_string:
        raise ConfigurationError("telegram session_string is required to complete login")

    client = _build_client(
        api_id=api_id,
        api_hash=api_hash,
        session_string=session_string,
        request_timeout_seconds=request_timeout_seconds,
        connection_retries=connection_retries,
    )
    skip_async_cleanup = False
    try:
        await client.connect()
        try:
            await client.sign_in(
                phone=phone_number,
                code=code.strip(),
                phone_code_hash=phone_code_hash,
            )
        except SessionPasswordNeededError as error:
            raise TwoFactorPasswordRequiredError(
                "telegram login requires a 2FA password",
            ) from error
        except PhoneCodeInvalidError as error:
            raise FatalItemError("invalid telegram login code") from error
        except PhoneCodeExpiredError as error:
            raise RecoverableItemError("telegram login code expired; request a new code") from error

        me = await client.get_me()
        if me is None:
            raise FatalItemError("telegram authorization completed but current user is unavailable")
        return _bootstrap_result(
            phone_number=phone_number or "",
            session_string=_save_string_session(client),
            already_authorized=False,
            user=me,
        )
    except (asyncio.CancelledError, GeneratorExit):
        skip_async_cleanup = True
        raise
    except FloodWaitError as error:
        raise RecoverableItemError(
            f"telegram flood wait during session bootstrap: {error.seconds}s",
        ) from error
    except (OSError, TimeoutError) as error:
        raise RecoverableItemError(
            f"telegram temporary network failure during session bootstrap: {error}",
        ) from error
    except RPCError as error:
        raise FatalItemError(f"telegram RPC error during session bootstrap: {error}") from error
    finally:
        if not skip_async_cleanup:
            with suppress(Exception):
                await client.disconnect()


async def complete_telegram_login_password(
    *,
    api_id: int | None,
    api_hash: str | None,
    phone_number: str | None,
    session_string: str | None,
    password: str | None,
    request_timeout_seconds: float = 30.0,
    connection_retries: int = 5,
) -> TelegramSessionBootstrapResult:
    if not password:
        raise ConfigurationError("telegram 2FA password is required to complete login")
    if not session_string:
        raise ConfigurationError("telegram session_string is required to complete login")

    client = _build_client(
        api_id=api_id,
        api_hash=api_hash,
        session_string=session_string,
        request_timeout_seconds=request_timeout_seconds,
        connection_retries=connection_retries,
    )
    skip_async_cleanup = False
    try:
        await client.connect()
        try:
            await client.sign_in(password=password)
        except PasswordHashInvalidError as error:
            raise FatalItemError("invalid telegram 2FA password") from error

        me = await client.get_me()
        if me is None:
            raise FatalItemError("telegram authorization completed but current user is unavailable")
        return _bootstrap_result(
            phone_number=phone_number or "",
            session_string=_save_string_session(client),
            already_authorized=False,
            user=me,
        )
    except (asyncio.CancelledError, GeneratorExit):
        skip_async_cleanup = True
        raise
    except FloodWaitError as error:
        raise RecoverableItemError(
            f"telegram flood wait during session bootstrap: {error.seconds}s",
        ) from error
    except (OSError, TimeoutError) as error:
        raise RecoverableItemError(
            f"telegram temporary network failure during session bootstrap: {error}",
        ) from error
    except RPCError as error:
        raise FatalItemError(f"telegram RPC error during session bootstrap: {error}") from error
    finally:
        if not skip_async_cleanup:
            with suppress(Exception):
                await client.disconnect()


def _bootstrap_result(
    *,
    phone_number: str,
    session_string: str,
    already_authorized: bool,
    user: Any,
) -> TelegramSessionBootstrapResult:
    username = getattr(user, "username", None)
    first_name = getattr(user, "first_name", None)
    last_name = getattr(user, "last_name", None)
    display_name = " ".join(part for part in [first_name, last_name] if part).strip()
    if not display_name:
        display_name = username or f"telegram_user_{user.id}"
    return TelegramSessionBootstrapResult(
        phone_number=phone_number,
        session_string=session_string,
        already_authorized=already_authorized,
        user_id=str(user.id),
        username=username,
        display_name=display_name,
    )


def _build_client(
    *,
    api_id: int | None,
    api_hash: str | None,
    session_string: str | None,
    request_timeout_seconds: float,
    connection_retries: int,
) -> TelegramClient:
    if api_id is None or not api_hash:
        raise ConfigurationError(
            "telegram api_id/api_hash are required to initialize a Telethon session",
        )
    return TelegramClient(
        session=StringSession(session_string or ""),
        api_id=api_id,
        api_hash=api_hash,
        timeout=request_timeout_seconds,
        request_retries=connection_retries,
        connection_retries=connection_retries,
    )


def _save_string_session(client: TelegramClient) -> str:
    session_string = StringSession.save(client.session)
    if not session_string:
        raise FatalItemError("telegram client did not produce a StringSession")
    return session_string
