from extg_migration_runtime.infrastructure.persistence.repositories import (
    PostgresChatMappingRepository,
)


def test_postgres_chat_mapping_repository_exposes_list_by_migration():
    assert hasattr(PostgresChatMappingRepository, "list_by_migration")
