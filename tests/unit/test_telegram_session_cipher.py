from __future__ import annotations

import pytest

from extg_telethon_service.application.telegram_session_cipher import TelegramSessionCipher
from extg_shared.contracts.errors import ConfigurationError, FatalItemError


def test_telegram_session_cipher_roundtrip():
    cipher = TelegramSessionCipher(master_key="master-key", key_version="v1")

    encrypted = cipher.encrypt("telethon-string-session")

    assert encrypted.startswith("v1:")
    assert cipher.decrypt(encrypted) == "telethon-string-session"


def test_telegram_session_cipher_rejects_empty_master_key():
    with pytest.raises(ConfigurationError, match="master key must not be empty"):
        TelegramSessionCipher(master_key="   ")


def test_telegram_session_cipher_rejects_unknown_key_version():
    encrypting = TelegramSessionCipher(master_key="master-key", key_version="v2")
    decrypting = TelegramSessionCipher(master_key="master-key", key_version="v1")

    encrypted = encrypting.encrypt("telethon-string-session")

    with pytest.raises(FatalItemError, match="unsupported telegram session key version"):
        decrypting.decrypt(encrypted)
