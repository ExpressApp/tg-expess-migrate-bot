from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from enum import Enum
import json
from typing import Any, Protocol


class SupportsIntegrationOutboxEvent(Protocol):
    event_id: str
    aggregate_type: str
    aggregate_id: str
    event_type: str
    payload_json: dict[str, Any]
    headers_json: dict[str, Any]
    created_at: datetime


@dataclass(frozen=True, slots=True)
class IntegrationEnvelope:
    message_id: str
    message_type: str
    schema_version: int
    occurred_at: datetime
    aggregate_type: str
    aggregate_id: str
    payload_json: dict[str, Any]
    headers_json: dict[str, Any]
    migration_id: str | None = None
    logical_chat_key: str | None = None


def integration_envelope_from_outbox_event(
    event: SupportsIntegrationOutboxEvent,
) -> IntegrationEnvelope:
    return IntegrationEnvelope(
        message_id=event.event_id,
        message_type=event.event_type,
        schema_version=_schema_version(event.headers_json),
        occurred_at=event.created_at,
        aggregate_type=event.aggregate_type,
        aggregate_id=event.aggregate_id,
        payload_json=_jsonable(event.payload_json),
        headers_json=_jsonable(event.headers_json),
        migration_id=_string_or_none(event.payload_json.get("migration_id")),
        logical_chat_key=_logical_chat_key(event),
    )


def integration_envelope_to_json_bytes(
    envelope: IntegrationEnvelope,
) -> bytes:
    return json.dumps(
        {
            "message_id": envelope.message_id,
            "message_type": envelope.message_type,
            "schema_version": envelope.schema_version,
            "occurred_at": envelope.occurred_at.astimezone(UTC).isoformat(),
            "aggregate_type": envelope.aggregate_type,
            "aggregate_id": envelope.aggregate_id,
            "migration_id": envelope.migration_id,
            "logical_chat_key": envelope.logical_chat_key,
            "payload": _jsonable(envelope.payload_json),
            "headers": _jsonable(envelope.headers_json),
        },
        ensure_ascii=True,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")


def integration_envelope_from_json_bytes(
    raw_value: bytes | str,
) -> IntegrationEnvelope:
    decoded = raw_value.decode("utf-8") if isinstance(raw_value, bytes) else raw_value
    payload = json.loads(decoded)
    if not isinstance(payload, dict):
        raise ValueError("integration envelope payload must be a JSON object")

    occurred_at_raw = payload.get("occurred_at")
    if not isinstance(occurred_at_raw, str):
        raise ValueError("integration envelope occurred_at must be ISO datetime string")

    occurred_at = datetime.fromisoformat(occurred_at_raw)
    if occurred_at.tzinfo is None:
        occurred_at = occurred_at.replace(tzinfo=UTC)

    message_id = payload.get("message_id")
    message_type = payload.get("message_type")
    aggregate_type = payload.get("aggregate_type")
    aggregate_id = payload.get("aggregate_id")
    schema_version = payload.get("schema_version")
    if not isinstance(message_id, str) or not message_id:
        raise ValueError("integration envelope message_id must be non-empty string")
    if not isinstance(message_type, str) or not message_type:
        raise ValueError("integration envelope message_type must be non-empty string")
    if not isinstance(aggregate_type, str) or not aggregate_type:
        raise ValueError("integration envelope aggregate_type must be non-empty string")
    if not isinstance(aggregate_id, str) or not aggregate_id:
        raise ValueError("integration envelope aggregate_id must be non-empty string")
    if not isinstance(schema_version, int):
        raise ValueError("integration envelope schema_version must be int")

    headers = payload.get("headers") or {}
    body = payload.get("payload") or {}
    if not isinstance(headers, dict):
        raise ValueError("integration envelope headers must be object")
    if not isinstance(body, dict):
        raise ValueError("integration envelope payload must be object")

    return IntegrationEnvelope(
        message_id=message_id,
        message_type=message_type,
        schema_version=schema_version,
        occurred_at=occurred_at,
        aggregate_type=aggregate_type,
        aggregate_id=aggregate_id,
        payload_json=body,
        headers_json=headers,
        migration_id=_string_or_none(payload.get("migration_id")),
        logical_chat_key=_string_or_none(payload.get("logical_chat_key")),
    )


def integration_partition_key_for_outbox_event(
    event: SupportsIntegrationOutboxEvent,
) -> str | None:
    migration_id = _string_or_none(event.payload_json.get("migration_id"))
    logical_chat_key = _logical_chat_key(event)
    if migration_id is None or logical_chat_key is None:
        return None
    return f"{migration_id}:{logical_chat_key}"


def _logical_chat_key(
    event: SupportsIntegrationOutboxEvent,
) -> str | None:
    explicit = _string_or_none(event.headers_json.get("logical_chat_key"))
    if explicit is not None:
        return explicit
    source_chat_ids = event.payload_json.get("source_chat_ids")
    if isinstance(source_chat_ids, list):
        normalized = [item for item in source_chat_ids if isinstance(item, str) and item]
        if len(normalized) == 1:
            return normalized[0]
        if normalized:
            return "__all__"
    return None


def _schema_version(headers_json: dict[str, Any]) -> int:
    value = headers_json.get("schema_version")
    return value if isinstance(value, int) else 1


def _string_or_none(value: object) -> str | None:
    return value if isinstance(value, str) and value else None


def _jsonable(value: Any) -> Any:
    if isinstance(value, datetime):
        return value.astimezone(UTC).isoformat()
    if isinstance(value, Enum):
        return value.value
    if isinstance(value, dict):
        return {str(key): _jsonable(inner) for key, inner in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [_jsonable(item) for item in value]
    return value


__all__ = [
    "IntegrationEnvelope",
    "SupportsIntegrationOutboxEvent",
    "integration_envelope_from_json_bytes",
    "integration_envelope_from_outbox_event",
    "integration_envelope_to_json_bytes",
    "integration_partition_key_for_outbox_event",
]
