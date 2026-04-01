from __future__ import annotations

from dataclasses import dataclass
from typing import Annotated

from pydantic import BaseModel, Field, field_validator, model_validator
from pydantic_settings import BaseSettings, NoDecode, SettingsConfigDict

from extg_shared.contracts.errors import ConfigurationError
from extg_shared.utils.express_routing import normalize_express_cts_host


IN_MEMORY_ENVIRONMENTS = frozenset({"local", "test"})


class RetrySettings(BaseModel):
    max_attempts: int = 3
    base_delay_seconds: float = 0.2
    max_delay_seconds: float = 3.0
    jitter_seconds: float = 0.2


class BackfillSettings(BaseModel):
    batch_size: int = 300


class TelegramSettings(BaseModel):
    api_id: int | None = None
    api_hash: str | None = None
    request_timeout_seconds: float = 30.0
    connection_retries: int = 5
    default_attachment_dir: str | None = None
    archive_import_dir: str | None = "/tmp/extg-archive-imports"
    delta_queue_size: int = 1000
    attachment_temp_file_cleanup_enabled: bool = True
    attachment_temp_file_ttl_seconds: float = 3600.0
    attachment_temp_file_cleanup_interval_seconds: float = 600.0
    archive_import_cleanup_enabled: bool = True
    archive_import_ttl_seconds: float = 86400.0
    archive_import_cleanup_interval_seconds: float = 600.0


class TelethonServiceSettings(BaseModel):
    base_url: str | None = None
    host: str = "127.0.0.1"
    port: int = 8092
    request_timeout_seconds: float = 60.0
    attachment_request_timeout_seconds: float | None = 300.0
    internal_token: str | None = None

    @field_validator("base_url")
    @classmethod
    def normalize_base_url(cls, value: str | None) -> str | None:
        if value is None:
            return None
        normalized = value.strip().rstrip("/")
        return normalized or None


class ExpressBotAccountSettings(BaseModel):
    role: str = "technical"
    bot_id: str
    bot_huid: str | None = None
    cts_url: str
    secret_key: str
    visible: bool | None = None

    @field_validator("role")
    @classmethod
    def validate_role(cls, value: str) -> str:
        normalized = value.strip().lower()
        allowed = {"primary", "technical"}
        if normalized not in allowed:
            raise ConfigurationError(
                "EXTG_EXPRESS__ACCOUNTS role must be one of: primary, technical",
            )
        return normalized

    @field_validator("cts_url")
    @classmethod
    def normalize_cts_url(cls, value: str) -> str:
        normalized = value.strip().rstrip("/")
        if not normalized:
            raise ConfigurationError("EXTG_EXPRESS__ACCOUNTS cts_url must not be empty")
        return normalized

    @property
    def cts_host(self) -> str:
        host = normalize_express_cts_host(self.cts_url)
        if not host:
            raise ConfigurationError(
                f"Invalid EXTG_EXPRESS__ACCOUNTS cts_url: {self.cts_url}",
            )
        return host

    @property
    def is_primary(self) -> bool:
        return self.role == "primary"

    @property
    def effective_visible(self) -> bool:
        if self.visible is not None:
            return self.visible
        return self.is_primary

    @property
    def configured_bot_huid(self) -> str | None:
        normalized = (self.bot_huid or "").strip()
        return normalized or None

    @property
    def effective_bot_member_huid(self) -> str:
        return self.configured_bot_huid or self.bot_id.strip()


@dataclass(frozen=True)
class ExpressBotAccountRegistry:
    accounts: tuple[ExpressBotAccountSettings, ...]

    def __post_init__(self) -> None:
        if not self.accounts:
            raise ConfigurationError("At least one eXpress bot account must be configured")
        primary_accounts = [account for account in self.accounts if account.is_primary]
        if len(primary_accounts) != 1:
            raise ConfigurationError(
                "Exactly one primary eXpress bot account must be configured",
            )
        seen_hosts: set[str] = set()
        for account in self.accounts:
            if account.cts_host in seen_hosts:
                raise ConfigurationError(
                    f"Duplicate eXpress bot account CTS host configured: {account.cts_host}",
                )
            seen_hosts.add(account.cts_host)

    @property
    def primary_account(self) -> ExpressBotAccountSettings:
        for account in self.accounts:
            if account.is_primary:
                return account
        raise ConfigurationError("Primary eXpress bot account is not configured")

    def get_by_cts_host(self, cts_host: str | None) -> ExpressBotAccountSettings | None:
        if not cts_host:
            return None
        normalized = cts_host.strip().lower()
        for account in self.accounts:
            if account.cts_host == normalized:
                return account
        return None


class ExpressSettings(BaseModel):
    bot_id: str | None = None
    cts_url: str | None = None
    secret_key: str | None = None
    accounts: list[ExpressBotAccountSettings] = Field(default_factory=list)
    chat_type: str = "GROUP_CHAT"
    default_participant_huids: Annotated[list[str], NoDecode] = Field(default_factory=list)
    request_timeout_seconds: float = 20.0
    attachment_request_timeout_seconds: float | None = 300.0
    local_idempotency_cache_enabled: bool = True

    @field_validator("accounts", mode="before")
    @classmethod
    def parse_accounts(cls, value):
        if value is None or value == "":
            return []
        return value

    @field_validator("default_participant_huids", mode="before")
    @classmethod
    def parse_default_participant_huids(cls, value):
        if value is None or value == "":
            return []
        if isinstance(value, list):
            return value
        if isinstance(value, str):
            return [item.strip() for item in value.split(",") if item.strip()]
        return value

    @field_validator("cts_url")
    @classmethod
    def normalize_legacy_cts_url(cls, value: str | None) -> str | None:
        if value is None:
            return None
        normalized = value.strip().rstrip("/")
        return normalized or None

    @model_validator(mode="after")
    def validate_account_configuration(self) -> ExpressSettings:
        if self.accounts:
            ExpressBotAccountRegistry(tuple(self.accounts))
            return self
        if any(value is not None and str(value).strip() for value in (self.bot_id, self.cts_url, self.secret_key)):
            if not self.bot_id:
                raise ConfigurationError("EXTG_EXPRESS__BOT_ID is required when legacy express config is used")
            if not self.cts_url:
                raise ConfigurationError("EXTG_EXPRESS__CTS_URL is required when legacy express config is used")
            if not self.secret_key:
                raise ConfigurationError("EXTG_EXPRESS__SECRET_KEY is required when legacy express config is used")
        return self

    def configured_accounts(self) -> tuple[ExpressBotAccountSettings, ...]:
        if self.accounts:
            return tuple(self.accounts)
        if self.bot_id and self.cts_url and self.secret_key:
            return (
                ExpressBotAccountSettings(
                    role="primary",
                    bot_id=self.bot_id,
                    cts_url=self.cts_url,
                    secret_key=self.secret_key,
                    visible=True,
                ),
            )
        return ()

    def build_account_registry(self) -> ExpressBotAccountRegistry:
        return ExpressBotAccountRegistry(self.configured_accounts())

    def require_primary_account(self) -> ExpressBotAccountSettings:
        return self.build_account_registry().primary_account


class PostgresSettings(BaseModel):
    dsn: str | None = None
    echo: bool = False
    pool_size: int = 10
    max_overflow: int = 20
    persist_message_bodies: bool = False
    payload_storage_mode: str = "redacted"

    @field_validator("payload_storage_mode")
    @classmethod
    def validate_payload_storage_mode(cls, value: str) -> str:
        normalized = value.strip().lower()
        allowed = {"full", "redacted", "none"}
        if normalized not in allowed:
            raise ConfigurationError(
                "EXTG_POSTGRES__PAYLOAD_STORAGE_MODE must be one of: full, redacted, none",
            )
        return normalized


class SecuritySettings(BaseModel):
    message_hmac_secret: str | None = None
    telegram_session_master_key: str | None = None
    telegram_session_key_version: str = "v1"


class WorkerSettings(BaseModel):
    enabled: bool = True
    concurrency: int = 2
    poll_interval_seconds: float = 1.0
    lease_duration_seconds: float = 30.0
    heartbeat_interval_seconds: float = 10.0
    backpressure_max_queued_jobs: int = 1000
    backpressure_max_running_jobs: int = 100
    runtime_metrics_enabled: bool = True
    runtime_metrics_poll_interval_seconds: float = 30.0
    outbox_publisher_enabled: bool = True
    outbox_publisher_batch_size: int = 100
    outbox_publisher_poll_interval_seconds: float = 1.0
    outbox_publisher_lease_duration_seconds: float = 30.0
    attachment_max_upload_size_bytes: int = 100 * 1024 * 1024
    attachment_transfer_concurrency: int = 2
    attachment_spool_max_memory_bytes: int = 1024 * 1024


class KafkaSettings(BaseModel):
    enabled: bool = False
    bootstrap_servers: Annotated[list[str], NoDecode] = Field(default_factory=list)
    command_topic: str = "extg.migration.command.v1"
    result_topic: str = "extg.migration.result.v1"
    dlq_topic: str = "extg.migration.dlq.v1"
    consumer_group: str = "extg-worker"
    producer_client_id: str = "extg-producer"
    consumer_client_id: str = "extg-consumer"
    consumer_name: str = "migration-command-consumer"
    poll_timeout_seconds: float = 1.0
    producer_flush_timeout_seconds: float = 10.0

    @field_validator("bootstrap_servers", mode="before")
    @classmethod
    def parse_bootstrap_servers(cls, value):
        if value is None or value == "":
            return []
        if isinstance(value, list):
            return [str(item).strip() for item in value if str(item).strip()]
        if isinstance(value, str):
            return [item.strip() for item in value.split(",") if item.strip()]
        return value

    @model_validator(mode="after")
    def validate_enabled_state(self) -> KafkaSettings:
        if self.enabled and not self.bootstrap_servers:
            raise ConfigurationError(
                "EXTG_KAFKA__BOOTSTRAP_SERVERS is required when EXTG_KAFKA__ENABLED=true",
            )
        return self


class BotSettings(BaseModel):
    migration_id: str | None = None
    host: str = "127.0.0.1"
    port: int = 8080
    events_path: str = "/command"
    status_path: str = "/status"
    callbacks_path: str = "/notification/callback"
    operator_huids: Annotated[list[str], NoDecode] = Field(default_factory=list)
    verify_requests: bool | None = None
    fsm_storage_backend: str = "in_memory"

    @field_validator("operator_huids", mode="before")
    @classmethod
    def parse_operator_huids(cls, value):
        if value is None or value == "":
            return []
        if isinstance(value, list):
            return value
        if isinstance(value, str):
            return [item.strip() for item in value.split(",") if item.strip()]
        return value

    @field_validator("fsm_storage_backend")
    @classmethod
    def validate_fsm_storage_backend(cls, value: str) -> str:
        normalized = value.strip().lower()
        allowed = {"in_memory", "postgres"}
        if normalized not in allowed:
            raise ConfigurationError(
                "EXTG_BOT__FSM_STORAGE_BACKEND must be one of: in_memory, postgres",
            )
        return normalized


class CommonServiceSettings(BaseSettings):
    model_config = SettingsConfigDict(
        env_prefix="EXTG_",
        env_nested_delimiter="__",
        extra="ignore",
    )

    runtime_role: str = "service"
    environment: str = "local"
    timezone_name: str = "Europe/Moscow"
    postgres: PostgresSettings = Field(default_factory=PostgresSettings)
    security: SecuritySettings = Field(default_factory=SecuritySettings)

    @property
    def normalized_environment(self) -> str:
        return self.environment.strip().lower()

    @property
    def normalized_runtime_role(self) -> str:
        return self.runtime_role.strip().lower()

    def resolve_persistence_backend(self) -> str:
        if self.postgres.dsn:
            return "postgres"
        if self.normalized_environment in IN_MEMORY_ENVIRONMENTS:
            return "in_memory"
        raise ConfigurationError(
            "EXTG_POSTGRES__DSN is required when EXTG_ENVIRONMENT is not local/test",
        )

    def validate_security_requirements(self) -> None:
        if (
            self.normalized_environment not in IN_MEMORY_ENVIRONMENTS
            and not (self.security.message_hmac_secret or "").strip()
        ):
            raise ConfigurationError(
                "EXTG_SECURITY__MESSAGE_HMAC_SECRET is required when EXTG_ENVIRONMENT is not local/test",
            )
        if (
            self.normalized_environment not in IN_MEMORY_ENVIRONMENTS
            and not (self.security.telegram_session_master_key or "").strip()
        ):
            raise ConfigurationError(
                "EXTG_SECURITY__TELEGRAM_SESSION_MASTER_KEY is required when EXTG_ENVIRONMENT is not local/test",
            )


__all__ = [
    "BackfillSettings",
    "BotSettings",
    "CommonServiceSettings",
    "ExpressBotAccountRegistry",
    "ExpressBotAccountSettings",
    "ExpressSettings",
    "IN_MEMORY_ENVIRONMENTS",
    "KafkaSettings",
    "PostgresSettings",
    "RetrySettings",
    "SecuritySettings",
    "TelegramSettings",
    "TelethonServiceSettings",
    "WorkerSettings",
]
