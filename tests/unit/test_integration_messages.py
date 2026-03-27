from datetime import UTC, datetime

from extg_shared.contracts.events import (
    integration_envelope_from_json_bytes,
    integration_envelope_from_outbox_event,
    integration_envelope_to_json_bytes,
    integration_partition_key_for_outbox_event,
)
from extg_shared.contracts.models import IntegrationOutboxEventRecord


def test_integration_envelope_round_trip_preserves_core_fields() -> None:
    event = IntegrationOutboxEventRecord(
        event_id="event-1",
        aggregate_type="migration_job",
        aggregate_id="job-1",
        event_type="migration.job.requested",
        payload_json={
            "job_key": "job-1",
            "migration_id": "migration-1",
            "source_chat_ids": ["chat-1"],
        },
        headers_json={"schema_version": 1},
        created_at=datetime(2026, 3, 24, 12, 0, tzinfo=UTC),
    )

    envelope = integration_envelope_from_outbox_event(event)
    restored = integration_envelope_from_json_bytes(
        integration_envelope_to_json_bytes(envelope),
    )

    assert restored.message_id == "event-1"
    assert restored.message_type == "migration.job.requested"
    assert restored.aggregate_id == "job-1"
    assert restored.migration_id == "migration-1"
    assert restored.logical_chat_key == "chat-1"


def test_integration_partition_key_uses_migration_id_and_logical_chat_key() -> None:
    event = IntegrationOutboxEventRecord(
        event_id="event-2",
        aggregate_type="migration_job",
        aggregate_id="job-2",
        event_type="migration.job.requested",
        payload_json={
            "job_key": "job-2",
            "migration_id": "migration-1",
            "source_chat_ids": ["chat-1"],
        },
        headers_json={"schema_version": 1},
        created_at=datetime(2026, 3, 24, 12, 0, tzinfo=UTC),
    )

    assert integration_partition_key_for_outbox_event(event) == "migration-1:chat-1"
