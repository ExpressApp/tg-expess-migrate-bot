from __future__ import annotations

from contextlib import AbstractContextManager, nullcontext
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Protocol

from extg_shared.contracts.models import ExpressUserCtsBindingRecord
from extg_shared.contracts.ports import ExpressGateway, ExpressUserCtsBindingRepository
from extg_shared.utils import get_current_express_cts_host, normalize_express_cts_host


class _RoutingAwareExpressGateway(Protocol):
    registered_cts_hosts: tuple[str, ...]

    def use_cts_host(self, cts_host: str | None) -> AbstractContextManager[None]: ...

    async def search_user_by_email(self, email: str) -> str | None: ...

    async def search_user_by_huid(self, huid: str) -> str | None: ...

    async def search_user_by_ad_login(
        self,
        ad_login: str,
        *,
        ad_domain: str | None = None,
    ) -> str | None: ...

    async def search_user_by_other_id(self, other_id: str) -> str | None: ...


@dataclass(frozen=True, slots=True)
class CtsResolutionResult:
    cts_host: str | None
    resolution_source: str
    target_huid: str | None = None
    corporate_email: str | None = None


class CtsResolutionService:
    def __init__(
        self,
        *,
        express_gateway: ExpressGateway,
        binding_repository: ExpressUserCtsBindingRepository,
    ) -> None:
        self._express_gateway = express_gateway
        self._binding_repository = binding_repository

    async def get_cached_by_target_huid(
        self,
        target_huid: str,
    ) -> CtsResolutionResult | None:
        normalized_huid = self._normalize_huid(target_huid)
        if normalized_huid is None:
            return None
        binding = await self._binding_repository.get_by_target_huid(normalized_huid)
        return self._binding_to_result(binding, resolution_source="cts_binding_cache")

    async def get_cached_by_email(
        self,
        corporate_email: str,
    ) -> CtsResolutionResult | None:
        normalized_email = self._normalize_email(corporate_email)
        if normalized_email is None:
            return None
        binding = await self._binding_repository.get_by_email(normalized_email)
        return self._binding_to_result(binding, resolution_source="cts_binding_cache")

    async def resolve_target_huid(
        self,
        target_huid: str,
    ) -> CtsResolutionResult:
        normalized_huid = self._normalize_huid(target_huid)
        if normalized_huid is None:
            return CtsResolutionResult(
                cts_host=None,
                resolution_source="cts_binding_missing",
            )
        cached = await self._binding_repository.get_by_target_huid(normalized_huid)
        if cached is not None:
            return self._binding_to_result(cached, resolution_source="cts_binding_cache")
        for cts_host in self._candidate_cts_hosts():
            with self._use_cts_host(cts_host):
                found_huid = await self._express_gateway.search_user_by_huid(normalized_huid)
            if found_huid is None:
                continue
            resolved_host = cts_host or self._current_or_default_cts_host()
            if not resolved_host:
                return CtsResolutionResult(
                    target_huid=found_huid,
                    cts_host=None,
                    resolution_source="cts_binding_probe",
                )
            saved = await self._binding_repository.save(
                ExpressUserCtsBindingRecord(
                    target_huid=found_huid,
                    cts_host=resolved_host,
                    created_at=self._now(),
                    updated_at=self._now(),
                    last_verified_at=self._now(),
                ),
            )
            return self._binding_to_result(saved, resolution_source="cts_binding_probe")
        return CtsResolutionResult(
            target_huid=None,
            cts_host=None,
            resolution_source="cts_binding_probe_miss",
        )

    async def resolve_email(
        self,
        corporate_email: str,
    ) -> CtsResolutionResult:
        normalized_email = self._normalize_email(corporate_email)
        if normalized_email is None:
            return CtsResolutionResult(
                cts_host=None,
                resolution_source="cts_binding_missing",
            )
        cached = await self._binding_repository.get_by_email(normalized_email)
        if cached is not None:
            return self._binding_to_result(cached, resolution_source="cts_binding_cache")
        for cts_host in self._candidate_cts_hosts():
            with self._use_cts_host(cts_host):
                found_huid = await self._express_gateway.search_user_by_email(normalized_email)
            if found_huid is None:
                continue
            resolved_host = cts_host or self._current_or_default_cts_host()
            if not resolved_host:
                return CtsResolutionResult(
                    target_huid=found_huid,
                    corporate_email=normalized_email,
                    cts_host=None,
                    resolution_source="cts_binding_probe",
                )
            saved = await self._binding_repository.save(
                ExpressUserCtsBindingRecord(
                    target_huid=found_huid,
                    corporate_email=normalized_email,
                    cts_host=resolved_host,
                    created_at=self._now(),
                    updated_at=self._now(),
                    last_verified_at=self._now(),
                ),
            )
            return self._binding_to_result(saved, resolution_source="cts_binding_probe")
        return CtsResolutionResult(
            corporate_email=normalized_email,
            cts_host=None,
            resolution_source="cts_binding_probe_miss",
        )

    async def resolve_ad_login(
        self,
        ad_login: str,
        *,
        ad_domain: str | None = None,
    ) -> CtsResolutionResult:
        normalized_login = self._normalize_login(ad_login)
        normalized_domain = self._normalize_ad_domain(ad_domain)
        if normalized_login is None:
            return CtsResolutionResult(
                cts_host=None,
                resolution_source="cts_binding_missing",
            )
        for cts_host in self._candidate_cts_hosts():
            with self._use_cts_host(cts_host):
                found_huid = await self._express_gateway.search_user_by_ad_login(
                    normalized_login,
                    ad_domain=normalized_domain,
                )
            if found_huid is None:
                continue
            resolved_host = cts_host or self._current_or_default_cts_host()
            if not resolved_host:
                return CtsResolutionResult(
                    target_huid=found_huid,
                    cts_host=None,
                    resolution_source="cts_binding_probe",
                )
            saved = await self._binding_repository.save(
                ExpressUserCtsBindingRecord(
                    target_huid=found_huid,
                    cts_host=resolved_host,
                    created_at=self._now(),
                    updated_at=self._now(),
                    last_verified_at=self._now(),
                ),
            )
            return self._binding_to_result(saved, resolution_source="cts_binding_probe")
        return CtsResolutionResult(
            cts_host=None,
            resolution_source="cts_binding_probe_miss",
        )

    async def resolve_other_id(self, other_id: str) -> CtsResolutionResult:
        normalized_other_id = self._normalize_other_id(other_id)
        if normalized_other_id is None:
            return CtsResolutionResult(
                cts_host=None,
                resolution_source="cts_binding_missing",
            )
        for cts_host in self._candidate_cts_hosts():
            with self._use_cts_host(cts_host):
                found_huid = await self._express_gateway.search_user_by_other_id(
                    normalized_other_id,
                )
            if found_huid is None:
                continue
            resolved_host = cts_host or self._current_or_default_cts_host()
            if not resolved_host:
                return CtsResolutionResult(
                    target_huid=found_huid,
                    cts_host=None,
                    resolution_source="cts_binding_probe",
                )
            saved = await self._binding_repository.save(
                ExpressUserCtsBindingRecord(
                    target_huid=found_huid,
                    cts_host=resolved_host,
                    created_at=self._now(),
                    updated_at=self._now(),
                    last_verified_at=self._now(),
                ),
            )
            return self._binding_to_result(saved, resolution_source="cts_binding_probe")
        return CtsResolutionResult(
            cts_host=None,
            resolution_source="cts_binding_probe_miss",
        )

    def _candidate_cts_hosts(self) -> tuple[str | None, ...]:
        ordered: list[str | None] = []
        seen: set[str | None] = set()
        current_host = normalize_express_cts_host(get_current_express_cts_host())
        if current_host is not None and current_host not in seen:
            ordered.append(current_host)
            seen.add(current_host)
        routing_gateway = self._routing_gateway()
        if routing_gateway is not None:
            for host in routing_gateway.registered_cts_hosts:
                normalized = normalize_express_cts_host(host)
                if normalized in seen:
                    continue
                ordered.append(normalized)
                seen.add(normalized)
        if not ordered:
            return (None,)
        return tuple(ordered)

    def _use_cts_host(self, cts_host: str | None) -> AbstractContextManager[None]:
        routing_gateway = self._routing_gateway()
        if routing_gateway is None:
            return nullcontext()
        return routing_gateway.use_cts_host(cts_host)

    def _routing_gateway(self) -> _RoutingAwareExpressGateway | None:
        gateway = self._express_gateway
        if hasattr(gateway, "registered_cts_hosts") and hasattr(gateway, "use_cts_host"):
            return gateway  # type: ignore[return-value]
        return None

    def _binding_to_result(
        self,
        binding: ExpressUserCtsBindingRecord | None,
        *,
        resolution_source: str,
    ) -> CtsResolutionResult | None:
        if binding is None:
            return None
        return CtsResolutionResult(
            target_huid=binding.target_huid,
            corporate_email=binding.corporate_email,
            cts_host=normalize_express_cts_host(binding.cts_host),
            resolution_source=resolution_source,
        )

    def _current_or_default_cts_host(self) -> str:
        current = normalize_express_cts_host(get_current_express_cts_host())
        if current is not None:
            return current
        routing_gateway = self._routing_gateway()
        if routing_gateway is not None and routing_gateway.registered_cts_hosts:
            return normalize_express_cts_host(routing_gateway.registered_cts_hosts[0]) or ""
        return ""

    def _normalize_huid(self, target_huid: str | None) -> str | None:
        if target_huid is None:
            return None
        normalized = target_huid.strip()
        return normalized or None

    def _normalize_email(self, corporate_email: str | None) -> str | None:
        if corporate_email is None:
            return None
        normalized = corporate_email.strip().lower()
        return normalized or None

    def _normalize_login(self, ad_login: str | None) -> str | None:
        if ad_login is None:
            return None
        normalized = ad_login.strip()
        return normalized or None

    def _normalize_ad_domain(self, ad_domain: str | None) -> str | None:
        if ad_domain is None:
            return None
        normalized = ad_domain.strip()
        return normalized or None

    def _normalize_other_id(self, other_id: str | None) -> str | None:
        if other_id is None:
            return None
        normalized = other_id.strip()
        return normalized or None

    def _now(self) -> datetime:
        return datetime.now(tz=UTC)
