from __future__ import annotations

import pytest

from extg_migration_runtime.application.cts_resolution import CtsResolutionService
from extg_migration_runtime.infrastructure.express.router import ExpressGatewayRouter
from extg_migration_runtime.infrastructure.persistence.in_memory import (
    InMemoryExpressUserCtsBindingRepository,
)
from extg_shared.config.common import (
    ExpressBotAccountRegistry,
    ExpressBotAccountSettings,
)
from extg_shared.contracts.models import ExpressUserCtsBindingRecord


class _SearchGateway:
    def __init__(
        self,
        host: str,
        *,
        huid_by_email: dict[str, str] | None = None,
        existing_huids: set[str] | None = None,
    ) -> None:
        self.host = host
        self.calls: list[tuple[str, str]] = []
        self._huid_by_email = {
            email.strip().lower(): huid
            for email, huid in (huid_by_email or {}).items()
        }
        self._existing_huids = set(existing_huids or set())
        self._existing_huids.update(self._huid_by_email.values())

    async def search_user_by_email(self, email: str) -> str | None:
        self.calls.append(("email", email.strip().lower()))
        return self._huid_by_email.get(email.strip().lower())

    async def search_user_by_huid(self, huid: str) -> str | None:
        normalized = huid.strip()
        self.calls.append(("huid", normalized))
        if normalized in self._existing_huids:
            return normalized
        return None


def _registry() -> ExpressBotAccountRegistry:
    return ExpressBotAccountRegistry(
        (
            ExpressBotAccountSettings(
                role="primary",
                bot_id="00000000-0000-0000-0000-000000000001",
                cts_url="https://cts-main.example.test",
                secret_key="primary-secret",
            ),
            ExpressBotAccountSettings(
                role="technical",
                bot_id="00000000-0000-0000-0000-000000000002",
                cts_url="https://cts-helper.example.test",
                secret_key="helper-secret",
                visible=False,
            ),
        ),
    )


@pytest.mark.asyncio
async def test_cts_resolution_service_probes_accounts_and_caches_email_binding() -> None:
    gateways: dict[str, _SearchGateway] = {}
    router = ExpressGatewayRouter(
        account_registry=_registry(),
        gateway_factory=lambda account: gateways.setdefault(
            account.cts_host,
            _SearchGateway(
                account.cts_host,
                huid_by_email=(
                    {"user@example.test": "huid-user"}
                    if account.cts_host == "cts-helper.example.test"
                    else {}
                ),
            ),
        ),
    )
    repository = InMemoryExpressUserCtsBindingRepository()
    service = CtsResolutionService(
        express_gateway=router,
        binding_repository=repository,
    )

    result = await service.resolve_email("user@example.test")

    assert result.target_huid == "huid-user"
    assert result.cts_host == "cts-helper.example.test"
    assert result.resolution_source == "cts_binding_probe"
    cached = await repository.get_by_email("user@example.test")
    assert cached is not None
    assert cached.target_huid == "huid-user"
    assert cached.cts_host == "cts-helper.example.test"
    assert gateways["cts-main.example.test"].calls == [("email", "user@example.test")]
    assert gateways["cts-helper.example.test"].calls == [("email", "user@example.test")]


@pytest.mark.asyncio
async def test_cts_resolution_service_uses_cached_binding_before_probe() -> None:
    gateways: dict[str, _SearchGateway] = {}
    router = ExpressGatewayRouter(
        account_registry=_registry(),
        gateway_factory=lambda account: gateways.setdefault(
            account.cts_host,
            _SearchGateway(account.cts_host, existing_huids={"huid-user"}),
        ),
    )
    repository = InMemoryExpressUserCtsBindingRepository()
    await repository.save(
        ExpressUserCtsBindingRecord(
            target_huid="huid-user",
            corporate_email="user@example.test",
            cts_host="cts-helper.example.test",
        ),
    )
    service = CtsResolutionService(
        express_gateway=router,
        binding_repository=repository,
    )

    result = await service.resolve_target_huid("huid-user")

    assert result.target_huid == "huid-user"
    assert result.cts_host == "cts-helper.example.test"
    assert result.resolution_source == "cts_binding_cache"
    assert gateways["cts-main.example.test"].calls == []
    assert gateways["cts-helper.example.test"].calls == []
