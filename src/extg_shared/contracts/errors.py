from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field


class MigrationError(Exception):
    """Base error for the migration domain."""


class ConfigurationError(MigrationError):
    """Raised when the manifest or runtime configuration is invalid."""


class RecoverableItemError(MigrationError):
    """Raised when an item can be retried safely."""


class FatalItemError(MigrationError):
    """Raised when the current item cannot be imported as-is."""


class AmbiguousDeliveryError(MigrationError):
    """Raised when the item may already be visible in the target system."""


class TwoFactorPasswordRequiredError(MigrationError):
    """Raised when Telegram login requires a 2FA password step."""


class ServiceErrorEnvelope(BaseModel):
    code: str
    message: str
    retryable: bool = False
    details: dict[str, Any] = Field(default_factory=dict)


def build_service_error_envelope(
    *,
    code: str,
    message: str,
    retryable: bool,
    details: dict[str, Any] | None = None,
) -> ServiceErrorEnvelope:
    return ServiceErrorEnvelope(
        code=code,
        message=message,
        retryable=retryable,
        details=details or {},
    )


def parse_service_error_envelope(payload: object) -> ServiceErrorEnvelope | None:
    if not isinstance(payload, dict):
        return None
    try:
        return ServiceErrorEnvelope.model_validate(payload)
    except Exception:
        return None


__all__ = [
    "AmbiguousDeliveryError",
    "ConfigurationError",
    "FatalItemError",
    "MigrationError",
    "RecoverableItemError",
    "ServiceErrorEnvelope",
    "TwoFactorPasswordRequiredError",
    "build_service_error_envelope",
    "parse_service_error_envelope",
]
