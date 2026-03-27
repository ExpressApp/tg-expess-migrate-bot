from __future__ import annotations

from pydantic import Field, model_validator

from extg_shared.contracts.errors import ConfigurationError
from extg_shared.config.common import (
    BackfillSettings,
    BotSettings,
    CommonServiceSettings,
    ExpressSettings,
    KafkaSettings,
    PostgresSettings,
    RetrySettings,
    SecuritySettings,
    TelegramSettings,
    TelethonServiceSettings,
    WorkerSettings,
)


class MigrationRuntimeSettings(CommonServiceSettings):
    runtime_role: str = "migration_runtime"
    retry: RetrySettings = Field(default_factory=RetrySettings)
    backfill: BackfillSettings = Field(default_factory=BackfillSettings)
    telegram: TelegramSettings = Field(default_factory=TelegramSettings)
    telethon_service: TelethonServiceSettings = Field(default_factory=TelethonServiceSettings)
    express: ExpressSettings = Field(default_factory=ExpressSettings)
    postgres: PostgresSettings = Field(default_factory=PostgresSettings)
    security: SecuritySettings = Field(default_factory=SecuritySettings)
    worker: WorkerSettings = Field(default_factory=WorkerSettings)
    kafka: KafkaSettings = Field(default_factory=KafkaSettings)
    bot: BotSettings = Field(default_factory=BotSettings)

    @model_validator(mode="after")
    def validate_runtime_requirements(self) -> MigrationRuntimeSettings:
        self.resolve_persistence_backend()
        if not self.bot.migration_id:
            raise ConfigurationError(
                "EXTG_BOT__MIGRATION_ID is required for migration-runtime",
            )
        self.validate_security_requirements()
        return self


def load_settings() -> MigrationRuntimeSettings:
    return MigrationRuntimeSettings()


__all__ = ["MigrationRuntimeSettings", "load_settings"]
