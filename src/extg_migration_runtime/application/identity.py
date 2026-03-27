from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from urllib.parse import urlparse

from extg_migration_runtime.application.cts_resolution import (
    CtsResolutionResult,
    CtsResolutionService,
)
from extg_shared.contracts.models import IdentityMappingRecord
from extg_shared.contracts.ports import ExpressGateway, IdentityMappingRepository
from extg_shared.contracts.errors import ConfigurationError


@dataclass(frozen=True, slots=True)
class IdentityLookupResult:
    telegram_user_id: str | None
    telegram_username: str | None
    corporate_email: str | None
    target_huid: str | None
    resolution_source: str
    cts_host: str | None = None
    telegram_display_name: str | None = None
    reason: str | None = None


class UsernameEmailIdentityDirectory:
    def __init__(
        self,
        *,
        identity_mapping_repository: IdentityMappingRepository,
        express_gateway: ExpressGateway,
        express_host: str | None = None,
        cts_resolution_service: CtsResolutionService | None = None,
    ) -> None:
        self._identity_mapping_repository = identity_mapping_repository
        self._express_gateway = express_gateway
        self._express_host = self.normalize_express_host(express_host)
        self._cts_resolution_service = cts_resolution_service

    async def resolve_username(
        self,
        telegram_username: str | None,
    ) -> IdentityLookupResult:
        return await self.resolve_telegram_identity(
            telegram_user_id=None,
            telegram_username=telegram_username,
        )

    async def resolve_telegram_identity(
        self,
        *,
        telegram_user_id: str | None,
        telegram_username: str | None,
        telegram_display_name: str | None = None,
    ) -> IdentityLookupResult:
        normalized_user_id = self.normalize_user_id(telegram_user_id)
        normalized_username = self.normalize_username(telegram_username)
        mapping = await self._find_mapping(
            telegram_user_id=normalized_user_id,
            telegram_username=normalized_username,
        )
        if mapping is None:
            return self._missing_mapping_result(
                telegram_user_id=normalized_user_id,
                telegram_username=normalized_username,
                telegram_display_name=telegram_display_name,
            )
        hydrated = self._apply_identity_hints(
            mapping,
            telegram_user_id=normalized_user_id,
            telegram_username=normalized_username,
            telegram_display_name=telegram_display_name,
        )
        if hydrated != mapping:
            hydrated = await self._identity_mapping_repository.save(hydrated)
        return await self._resolve_mapping(hydrated)

    async def upsert_mapping(
        self,
        *,
        telegram_user_id: str | None,
        telegram_username: str | None,
        telegram_display_name: str | None,
        corporate_email: str,
    ) -> IdentityLookupResult:
        normalized_user_id = self.normalize_user_id(telegram_user_id)
        normalized_username = self.normalize_username(telegram_username)
        normalized_email = self.normalize_email(corporate_email)
        if normalized_user_id is None and normalized_username is None:
            raise ConfigurationError(
                "telegram_user_id or telegram_username is required",
            )
        existing = await self._find_mapping(
            telegram_user_id=normalized_user_id,
            telegram_username=normalized_username,
        )
        now = self._now()
        record = IdentityMappingRecord(
            express_host=self._express_host or (existing.express_host if existing else None),
            telegram_user_id=normalized_user_id or (existing.telegram_user_id if existing else None),
            telegram_username=normalized_username or (existing.telegram_username if existing else None),
            telegram_display_name=telegram_display_name
            or (existing.telegram_display_name if existing else None),
            corporate_email=normalized_email,
            target_huid=existing.target_huid if existing is not None else None,
            created_at=existing.created_at if existing is not None else now,
            updated_at=now,
            last_resolved_at=existing.last_resolved_at if existing is not None else None,
        )
        saved = await self._identity_mapping_repository.save(record)
        return await self._resolve_mapping(saved)

    async def upsert_direct_mapping(
        self,
        *,
        telegram_user_id: str | None,
        telegram_username: str | None,
        telegram_display_name: str | None,
        target_huid: str,
        corporate_email: str | None = None,
    ) -> IdentityLookupResult:
        normalized_user_id = self.normalize_user_id(telegram_user_id)
        normalized_username = self.normalize_username(telegram_username)
        normalized_target_huid = target_huid.strip()
        normalized_email = (
            self.normalize_email(corporate_email)
            if corporate_email is not None and corporate_email.strip()
            else None
        )
        if normalized_user_id is None and normalized_username is None:
            raise ConfigurationError(
                "telegram_user_id or telegram_username is required",
            )
        if not normalized_target_huid:
            raise ConfigurationError("target_huid is required")
        resolved_cts = await self._resolve_target_huid_cts(normalized_target_huid)
        validated_huid = resolved_cts.target_huid
        if validated_huid is None:
            raise ConfigurationError("target_huid was not found in eXpress")
        existing = await self._find_mapping(
            telegram_user_id=normalized_user_id,
            telegram_username=normalized_username,
        )
        now = self._now()
        record = IdentityMappingRecord(
            express_host=self._express_host or (existing.express_host if existing else None),
            telegram_user_id=normalized_user_id or (existing.telegram_user_id if existing else None),
            telegram_username=normalized_username or (existing.telegram_username if existing else None),
            telegram_display_name=telegram_display_name
            or (existing.telegram_display_name if existing else None),
            corporate_email=normalized_email
            or (existing.corporate_email if existing is not None else None),
            target_huid=validated_huid,
            created_at=existing.created_at if existing is not None else now,
            updated_at=now,
            last_resolved_at=now,
        )
        saved = await self._identity_mapping_repository.save(record)
        result = await self._resolve_mapping(saved)
        return IdentityLookupResult(
            telegram_user_id=result.telegram_user_id,
            telegram_username=result.telegram_username,
            telegram_display_name=result.telegram_display_name,
            corporate_email=result.corporate_email,
            target_huid=result.target_huid,
            cts_host=resolved_cts.cts_host or result.cts_host,
            resolution_source=result.resolution_source,
            reason=result.reason,
        )

    async def resolve_corporate_selector(
        self,
        *,
        corporate_email: str | None = None,
        target_huid: str | None = None,
        ad_login: str | None = None,
        other_id: str | None = None,
        default_ad_domain: str | None = None,
    ) -> IdentityLookupResult:
        selectors_count = sum(
            1
            for value in (corporate_email, target_huid, ad_login, other_id)
            if value is not None and str(value).strip()
        )
        if selectors_count != 1:
            raise ConfigurationError(
                "exactly one of corporate_email, target_huid, ad_login or other_id is required",
            )
        if corporate_email is not None and corporate_email.strip():
            normalized_email = self.normalize_email(corporate_email)
            resolved_cts = await self._resolve_email_cts(normalized_email)
            return IdentityLookupResult(
                telegram_user_id=None,
                telegram_username=None,
                corporate_email=normalized_email,
                target_huid=resolved_cts.target_huid,
                cts_host=resolved_cts.cts_host,
                resolution_source="manual_selector_email",
                reason=None if resolved_cts.target_huid else "email_not_found_in_express",
            )
        if target_huid is not None and target_huid.strip():
            normalized_huid = target_huid.strip()
            resolved_cts = await self._resolve_target_huid_cts(normalized_huid)
            return IdentityLookupResult(
                telegram_user_id=None,
                telegram_username=None,
                corporate_email=None,
                target_huid=resolved_cts.target_huid,
                cts_host=resolved_cts.cts_host,
                resolution_source="manual_selector_huid",
                reason=None if resolved_cts.target_huid else "huid_not_found_in_express",
            )
        if ad_login is not None and ad_login.strip():
            normalized_login, normalized_domain = self._normalize_ad_login_input(
                ad_login,
                default_ad_domain=default_ad_domain,
            )
            resolved_cts = await self._resolve_ad_login_cts(
                normalized_login,
                ad_domain=normalized_domain,
            )
            return IdentityLookupResult(
                telegram_user_id=None,
                telegram_username=None,
                corporate_email=None,
                target_huid=resolved_cts.target_huid,
                cts_host=resolved_cts.cts_host,
                resolution_source="manual_selector_ad_login",
                reason=None if resolved_cts.target_huid else "ad_login_not_found_in_express",
            )
        normalized_other_id = (other_id or "").strip()
        resolved_cts = await self._resolve_other_id_cts(normalized_other_id)
        return IdentityLookupResult(
            telegram_user_id=None,
            telegram_username=None,
            corporate_email=None,
            target_huid=resolved_cts.target_huid,
            cts_host=resolved_cts.cts_host,
            resolution_source="manual_selector_other_id",
            reason=None if resolved_cts.target_huid else "other_id_not_found_in_express",
        )

    def normalize_username(self, telegram_username: str | None) -> str | None:
        if telegram_username is None:
            return None
        normalized = telegram_username.strip().lstrip("@").lower()
        return normalized or None

    def normalize_user_id(self, telegram_user_id: str | None) -> str | None:
        if telegram_user_id is None:
            return None
        normalized = telegram_user_id.strip()
        return normalized or None

    def normalize_email(self, corporate_email: str) -> str:
        normalized = corporate_email.strip().lower()
        if not normalized or "@" not in normalized:
            raise ConfigurationError("corporate email must be a valid email address")
        return normalized

    def normalize_express_host(self, express_host: str | None) -> str | None:
        if express_host is None:
            return None
        normalized = express_host.strip().lower()
        if not normalized:
            return None
        parsed = urlparse(normalized if "://" in normalized else f"https://{normalized}")
        return (parsed.hostname or parsed.netloc or normalized).lower() or None

    async def _find_mapping(
        self,
        *,
        telegram_user_id: str | None,
        telegram_username: str | None,
    ) -> IdentityMappingRecord | None:
        by_user_id = None
        by_username = None
        if telegram_user_id is not None:
            by_user_id = await self._identity_mapping_repository.get_by_user_id(
                telegram_user_id,
                express_host=self._express_host,
            )
        if telegram_username is not None:
            by_username = await self._identity_mapping_repository.get_by_username(
                telegram_username,
                express_host=self._express_host,
            )
        if by_user_id is None:
            return by_username
        if by_username is None or by_username == by_user_id:
            return by_user_id
        return self._merge_records(by_user_id, by_username)

    async def _resolve_mapping(
        self,
        mapping: IdentityMappingRecord,
    ) -> IdentityLookupResult:
        if mapping.target_huid:
            cached_cts = await self._get_cached_target_huid_cts(mapping.target_huid)
            return IdentityLookupResult(
                telegram_user_id=mapping.telegram_user_id,
                telegram_username=mapping.telegram_username,
                telegram_display_name=mapping.telegram_display_name,
                corporate_email=mapping.corporate_email,
                target_huid=mapping.target_huid,
                cts_host=cached_cts.cts_host if cached_cts is not None else None,
                resolution_source="identity_map_cache",
            )
        if mapping.corporate_email is None:
            return IdentityLookupResult(
                telegram_user_id=mapping.telegram_user_id,
                telegram_username=mapping.telegram_username,
                telegram_display_name=mapping.telegram_display_name,
                corporate_email=None,
                target_huid=None,
                resolution_source="display_only",
                reason="identity_not_mapped",
            )

        resolved_cts = await self._resolve_email_cts(mapping.corporate_email)
        target_huid = resolved_cts.target_huid
        if target_huid is None:
            return IdentityLookupResult(
                telegram_user_id=mapping.telegram_user_id,
                telegram_username=mapping.telegram_username,
                telegram_display_name=mapping.telegram_display_name,
                corporate_email=mapping.corporate_email,
                target_huid=None,
                cts_host=resolved_cts.cts_host,
                resolution_source="identity_map_email",
                reason="email_not_found_in_express",
            )

        saved = await self._identity_mapping_repository.save(
            IdentityMappingRecord(
                express_host=mapping.express_host,
                telegram_user_id=mapping.telegram_user_id,
                telegram_username=mapping.telegram_username,
                telegram_display_name=mapping.telegram_display_name,
                corporate_email=mapping.corporate_email,
                target_huid=target_huid,
                created_at=mapping.created_at,
                updated_at=self._now(),
                last_resolved_at=self._now(),
            ),
        )
        return IdentityLookupResult(
            telegram_user_id=saved.telegram_user_id,
            telegram_username=saved.telegram_username,
            telegram_display_name=saved.telegram_display_name,
            corporate_email=saved.corporate_email,
            target_huid=target_huid,
            cts_host=resolved_cts.cts_host,
            resolution_source="identity_map_email",
        )

    async def _get_cached_target_huid_cts(
        self,
        target_huid: str | None,
    ):
        if self._cts_resolution_service is None or target_huid is None:
            return None
        return await self._cts_resolution_service.get_cached_by_target_huid(target_huid)

    async def _resolve_target_huid_cts(self, target_huid: str):
        if self._cts_resolution_service is None:
            validated_huid = await self._express_gateway.search_user_by_huid(target_huid)
            return CtsResolutionResult(
                target_huid=validated_huid,
                cts_host=None,
                resolution_source="express_gateway_direct",
            )
        return await self._cts_resolution_service.resolve_target_huid(target_huid)

    async def _resolve_email_cts(self, corporate_email: str):
        if self._cts_resolution_service is None:
            target_huid = await self._express_gateway.search_user_by_email(corporate_email)
            return CtsResolutionResult(
                target_huid=target_huid,
                corporate_email=corporate_email,
                cts_host=None,
                resolution_source="express_gateway_direct",
            )
        return await self._cts_resolution_service.resolve_email(corporate_email)

    async def _resolve_ad_login_cts(
        self,
        ad_login: str,
        *,
        ad_domain: str | None,
    ):
        if self._cts_resolution_service is None:
            target_huid = await self._express_gateway.search_user_by_ad_login(
                ad_login,
                ad_domain=ad_domain,
            )
            return CtsResolutionResult(
                target_huid=target_huid,
                cts_host=None,
                resolution_source="express_gateway_direct",
            )
        return await self._cts_resolution_service.resolve_ad_login(
            ad_login,
            ad_domain=ad_domain,
        )

    async def _resolve_other_id_cts(self, other_id: str):
        if self._cts_resolution_service is None:
            target_huid = await self._express_gateway.search_user_by_other_id(other_id)
            return CtsResolutionResult(
                target_huid=target_huid,
                cts_host=None,
                resolution_source="express_gateway_direct",
            )
        return await self._cts_resolution_service.resolve_other_id(other_id)

    def _apply_identity_hints(
        self,
        mapping: IdentityMappingRecord,
        *,
        telegram_user_id: str | None,
        telegram_username: str | None,
        telegram_display_name: str | None,
    ) -> IdentityMappingRecord:
        return IdentityMappingRecord(
            express_host=self._express_host or mapping.express_host,
            telegram_user_id=telegram_user_id or mapping.telegram_user_id,
            telegram_username=telegram_username or mapping.telegram_username,
            telegram_display_name=telegram_display_name or mapping.telegram_display_name,
            corporate_email=mapping.corporate_email,
            target_huid=mapping.target_huid,
            created_at=mapping.created_at,
            updated_at=self._now(),
            last_resolved_at=mapping.last_resolved_at,
        )

    def _merge_records(
        self,
        primary: IdentityMappingRecord,
        secondary: IdentityMappingRecord,
    ) -> IdentityMappingRecord:
        if (
            primary.corporate_email
            and secondary.corporate_email
            and primary.corporate_email != secondary.corporate_email
        ):
            raise ConfigurationError(
                "conflicting identity mappings found for the same Telegram identity; "
                "resolve duplicate corporate emails in identity_mapping",
            )
        return IdentityMappingRecord(
            express_host=primary.express_host or secondary.express_host,
            telegram_user_id=primary.telegram_user_id or secondary.telegram_user_id,
            telegram_username=primary.telegram_username or secondary.telegram_username,
            telegram_display_name=(
                primary.telegram_display_name or secondary.telegram_display_name
            ),
            corporate_email=primary.corporate_email or secondary.corporate_email,
            target_huid=primary.target_huid or secondary.target_huid,
            created_at=self._earliest(primary.created_at, secondary.created_at),
            updated_at=self._latest(primary.updated_at, secondary.updated_at),
            last_resolved_at=self._latest(
                primary.last_resolved_at,
                secondary.last_resolved_at,
            ),
        )

    def _missing_mapping_result(
        self,
        *,
        telegram_user_id: str | None,
        telegram_username: str | None,
        telegram_display_name: str | None,
    ) -> IdentityLookupResult:
        if telegram_user_id is None and telegram_username is None:
            reason = "missing_identity"
        elif telegram_user_id is not None and telegram_username is not None:
            reason = "identity_not_mapped"
        elif telegram_user_id is not None:
            reason = "user_id_not_mapped"
        else:
            reason = "username_not_mapped"
        return IdentityLookupResult(
            telegram_user_id=telegram_user_id,
            telegram_username=telegram_username,
            telegram_display_name=telegram_display_name,
            corporate_email=None,
            target_huid=None,
            resolution_source="display_only",
            reason=reason,
        )

    def _normalize_ad_login_input(
        self,
        ad_login: str,
        *,
        default_ad_domain: str | None,
    ) -> tuple[str, str]:
        normalized = ad_login.strip()
        if not normalized:
            raise ConfigurationError("ad_login must not be empty")
        if "\\" in normalized:
            domain, login = normalized.split("\\", 1)
            if domain.strip() and login.strip():
                return login.strip(), domain.strip()
        if "@" in normalized:
            login, domain = normalized.split("@", 1)
            if login.strip() and domain.strip():
                return login.strip(), domain.strip()
        normalized_default_domain = (default_ad_domain or "").strip()
        if not normalized_default_domain:
            raise ConfigurationError(
                "ad_login without domain requires operator ad_domain; use login@domain or DOMAIN\\login",
            )
        return normalized, normalized_default_domain

    def _now(self) -> datetime:
        return datetime.now(tz=UTC)

    def _earliest(
        self,
        left: datetime | None,
        right: datetime | None,
    ) -> datetime | None:
        if left is None:
            return right
        if right is None:
            return left
        return left if left <= right else right

    def _latest(
        self,
        left: datetime | None,
        right: datetime | None,
    ) -> datetime | None:
        if left is None:
            return right
        if right is None:
            return left
        return left if left >= right else right
