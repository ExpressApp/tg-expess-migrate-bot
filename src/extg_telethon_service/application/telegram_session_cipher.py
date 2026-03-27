from __future__ import annotations

import base64
import hashlib
from os import urandom

from cryptography.hazmat.primitives.ciphers.aead import AESGCM

from extg_shared.contracts.errors import ConfigurationError, FatalItemError


class TelegramSessionCipher:
    def __init__(
        self,
        *,
        master_key: str | bytes,
        key_version: str = "v1",
    ) -> None:
        normalized_key = self._normalize_master_key(master_key)
        normalized_version = key_version.strip()
        if not normalized_version:
            raise ConfigurationError("telegram session key version must not be empty")
        self._aesgcm = AESGCM(normalized_key)
        self._key_version = normalized_version

    @property
    def key_version(self) -> str:
        return self._key_version

    def encrypt(self, session_string: str) -> str:
        normalized_session = session_string.strip()
        if not normalized_session:
            raise ConfigurationError("telegram session string must not be empty")
        nonce = urandom(12)
        ciphertext = self._aesgcm.encrypt(
            nonce,
            normalized_session.encode("utf-8"),
            self._key_version.encode("utf-8"),
        )
        encoded = base64.urlsafe_b64encode(nonce + ciphertext).decode("ascii")
        return f"{self._key_version}:{encoded}"

    def decrypt(self, encrypted_session: str) -> str:
        key_version, encoded = self._split_payload(encrypted_session)
        if key_version != self._key_version:
            raise FatalItemError(
                f"unsupported telegram session key version: {key_version}",
            )
        try:
            raw = base64.urlsafe_b64decode(encoded.encode("ascii"))
        except Exception as error:
            raise FatalItemError("telegram session payload is not valid base64") from error
        if len(raw) < 13:
            raise FatalItemError("telegram session payload is too short")
        nonce = raw[:12]
        ciphertext = raw[12:]
        try:
            plaintext = self._aesgcm.decrypt(
                nonce,
                ciphertext,
                key_version.encode("utf-8"),
            )
        except Exception as error:
            raise FatalItemError("telegram session payload decryption failed") from error
        session_string = plaintext.decode("utf-8").strip()
        if not session_string:
            raise FatalItemError("telegram session payload decrypted to an empty value")
        return session_string

    def _normalize_master_key(self, master_key: str | bytes) -> bytes:
        if isinstance(master_key, bytes):
            normalized = master_key.strip()
        else:
            normalized = master_key.strip().encode("utf-8")
        if not normalized:
            raise ConfigurationError("telegram session master key must not be empty")
        return hashlib.sha256(normalized).digest()

    def _split_payload(self, encrypted_session: str) -> tuple[str, str]:
        normalized = encrypted_session.strip()
        if not normalized or ":" not in normalized:
            raise FatalItemError("telegram session payload format is invalid")
        return normalized.split(":", 1)
