from extg_migration_runtime.infrastructure.persistence.repositories import (
    PostgresInventorySnapshotRepository,
)


def test_postgres_inventory_repository_exposes_list_by_migration():
    assert hasattr(PostgresInventorySnapshotRepository, "list_by_migration")
