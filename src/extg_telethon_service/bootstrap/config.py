from __future__ import annotations

from pydantic import Field, model_validator

from extg_shared.config.common import (
    BotSettings,
    CommonServiceSettings,
    PostgresSettings,
    SecuritySettings,
    TelegramSettings,
    TelethonServiceSettings,
)


class TelethonServiceAppSettings(CommonServiceSettings):
    runtime_role: str = "telethon_service"
    telegram: TelegramSettings = Field(default_factory=TelegramSettings)
    telethon_service: TelethonServiceSettings = Field(default_factory=TelethonServiceSettings)
    postgres: PostgresSettings = Field(default_factory=PostgresSettings)
    security: SecuritySettings = Field(default_factory=SecuritySettings)
    bot: BotSettings = Field(default_factory=BotSettings)

    @model_validator(mode="after")
    def validate_runtime_requirements(self) -> TelethonServiceAppSettings:
        self.resolve_persistence_backend()
        self.validate_security_requirements()
        return self


def load_settings() -> TelethonServiceAppSettings:
    return TelethonServiceAppSettings()


__all__ = ["TelethonServiceAppSettings", "load_settings"]
