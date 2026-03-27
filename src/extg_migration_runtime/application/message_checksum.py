from __future__ import annotations

import hashlib
import hmac

from extg_shared.contracts.errors import ConfigurationError
from extg_shared.contracts.models import CanonicalMessage


class MessageChecksumService:
    def __init__(self, *, secret: str | bytes) -> None:
        if isinstance(secret, str):
            normalized_secret = secret.strip().encode("utf-8")
        else:
            normalized_secret = bytes(secret)
        if not normalized_secret:
            raise ConfigurationError("message checksum secret must not be empty")
        self._secret = normalized_secret

    def checksum(self, message: CanonicalMessage) -> str:
        payload = "|".join(
            [
                message.idempotency_key,
                message.sent_at_utc.isoformat(),
                message.body_rendered or "",
                str(len(message.attachments)),
            ],
        ).encode("utf-8")
        return hmac.new(
            self._secret,
            payload,
            hashlib.sha256,
        ).hexdigest()
