"""Cross-service API and event contracts."""

from extg_shared.contracts.errors import (
    AmbiguousDeliveryError,
    ConfigurationError,
    FatalItemError,
    MigrationError,
    RecoverableItemError,
    ServiceErrorEnvelope,
    TwoFactorPasswordRequiredError,
    build_service_error_envelope,
    parse_service_error_envelope,
)
from extg_shared.contracts.manifest import *  # noqa: F403
from extg_shared.contracts.models import *  # noqa: F403
from extg_shared.contracts.ports import *  # noqa: F403
from extg_shared.contracts.events import (
    IntegrationEnvelope,
    integration_envelope_from_json_bytes,
    integration_envelope_from_outbox_event,
    integration_envelope_to_json_bytes,
    integration_partition_key_for_outbox_event,
)

__all__ = [
    "AmbiguousDeliveryError",
    "ConfigurationError",
    "FatalItemError",
    "IntegrationEnvelope",
    "MigrationError",
    "RecoverableItemError",
    "ServiceErrorEnvelope",
    "TwoFactorPasswordRequiredError",
    "build_service_error_envelope",
    "integration_envelope_from_json_bytes",
    "integration_envelope_from_outbox_event",
    "integration_envelope_to_json_bytes",
    "integration_partition_key_for_outbox_event",
    "parse_service_error_envelope",
]
