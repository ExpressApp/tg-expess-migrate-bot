import pytest

from extg_migration_runtime.application.cts_resolution import CtsResolutionService
from extg_migration_runtime.application.identity import UsernameEmailIdentityDirectory
from extg_migration_runtime.infrastructure.express.router import ExpressGatewayRouter
from extg_migration_runtime.infrastructure.persistence.in_memory import (
    InMemoryExpressUserCtsBindingRepository,
    InMemoryIdentityMappingRepository,
)
from extg_shared.config.common import ExpressBotAccountRegistry, ExpressBotAccountSettings


class _FakeExpressGateway:
    def __init__(
        self,
        huid_by_email: dict[str, str],
        *,
        huid_by_ad_login: dict[str, str] | None = None,
        huid_by_other_id: dict[str, str] | None = None,
        existing_huids: set[str] | None = None,
    ) -> None:
        self._huid_by_email = {email.strip().lower(): huid for email, huid in huid_by_email.items()}
        self._huid_by_ad_login = {
            login.strip().lower(): huid
            for login, huid in (huid_by_ad_login or {}).items()
        }
        self._huid_by_other_id = {
            other_id.strip(): huid
            for other_id, huid in (huid_by_other_id or {}).items()
        }
        self._existing_huids = set(existing_huids or set())
        self._existing_huids.update(self._huid_by_email.values())
        self._existing_huids.update(self._huid_by_ad_login.values())
        self._existing_huids.update(self._huid_by_other_id.values())

    async def search_user_by_email(self, email: str) -> str | None:
        return self._huid_by_email.get(email.strip().lower())

    async def search_user_by_huid(self, huid: str) -> str | None:
        normalized = huid.strip()
        if normalized in self._existing_huids:
            return normalized
        return None

    async def search_user_by_ad_login(
        self,
        ad_login: str,
        *,
        ad_domain: str | None = None,
    ) -> str | None:
        normalized = ad_login.strip().lower()
        if ad_domain:
            composite = f"{normalized}@{ad_domain.strip().lower()}"
            if composite in self._huid_by_ad_login:
                return self._huid_by_ad_login[composite]
        return self._huid_by_ad_login.get(normalized)

    async def search_user_by_other_id(self, other_id: str) -> str | None:
        return self._huid_by_other_id.get(other_id.strip())


class _RoutingGateway(_FakeExpressGateway):
    pass


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
async def test_identity_directory_scopes_cached_huid_by_express_host() -> None:
    repository = InMemoryIdentityMappingRepository()
    same_email = "user@example.com"

    directory_a = UsernameEmailIdentityDirectory(
        identity_mapping_repository=repository,
        express_gateway=_FakeExpressGateway({same_email: "huid-host-a"}),
        express_host="https://cts11dev.ccsteam.ru/",
    )
    directory_b = UsernameEmailIdentityDirectory(
        identity_mapping_repository=repository,
        express_gateway=_FakeExpressGateway({same_email: "huid-host-b"}),
        express_host="https://cts12dev.ccsteam.ru/",
    )

    result_a = await directory_a.upsert_mapping(
        telegram_user_id="100",
        telegram_username="@same.user",
        telegram_display_name="Same User",
        corporate_email=same_email,
    )
    result_b = await directory_b.upsert_mapping(
        telegram_user_id="100",
        telegram_username="@same.user",
        telegram_display_name="Same User",
        corporate_email=same_email,
    )

    cached_a = await repository.get_by_user_id("100", express_host="cts11dev.ccsteam.ru")
    cached_b = await repository.get_by_user_id("100", express_host="cts12dev.ccsteam.ru")

    assert result_a.target_huid == "huid-host-a"
    assert result_b.target_huid == "huid-host-b"
    assert cached_a is not None
    assert cached_b is not None
    assert cached_a.express_host == "cts11dev.ccsteam.ru"
    assert cached_b.express_host == "cts12dev.ccsteam.ru"
    assert cached_a.target_huid == "huid-host-a"
    assert cached_b.target_huid == "huid-host-b"


@pytest.mark.asyncio
async def test_identity_directory_supports_direct_huid_mapping_without_email() -> None:
    repository = InMemoryIdentityMappingRepository()
    directory = UsernameEmailIdentityDirectory(
        identity_mapping_repository=repository,
        express_gateway=_FakeExpressGateway({}, existing_huids={"huid-direct"}),
        express_host="https://cts11dev.ccsteam.ru/",
    )

    result = await directory.upsert_direct_mapping(
        telegram_user_id="100",
        telegram_username="@same.user",
        telegram_display_name="Same User",
        target_huid="huid-direct",
    )

    cached = await repository.get_by_user_id("100", express_host="cts11dev.ccsteam.ru")

    assert result.target_huid == "huid-direct"
    assert result.corporate_email is None
    assert cached is not None
    assert cached.target_huid == "huid-direct"
    assert cached.corporate_email is None


@pytest.mark.asyncio
async def test_identity_directory_returns_cts_host_when_email_resolved_via_binding_service() -> None:
    repository = InMemoryIdentityMappingRepository()
    binding_repository = InMemoryExpressUserCtsBindingRepository()
    gateways: dict[str, _FakeExpressGateway] = {}
    router = ExpressGatewayRouter(
        account_registry=_registry(),
        gateway_factory=lambda account: gateways.setdefault(
            account.cts_host,
            _FakeExpressGateway(
                {"user@example.test": "huid-helper"}
                if account.cts_host == "cts-helper.example.test"
                else {},
            ),
        ),
    )
    cts_resolution_service = CtsResolutionService(
        express_gateway=router,
        binding_repository=binding_repository,
    )
    directory = UsernameEmailIdentityDirectory(
        identity_mapping_repository=repository,
        express_gateway=router,
        cts_resolution_service=cts_resolution_service,
    )

    result = await directory.upsert_mapping(
        telegram_user_id="100",
        telegram_username="@same.user",
        telegram_display_name="Same User",
        corporate_email="user@example.test",
    )

    assert result.target_huid == "huid-helper"
    assert result.cts_host == "cts-helper.example.test"
    cached_binding = await binding_repository.get_by_target_huid("huid-helper")
    assert cached_binding is not None
    assert cached_binding.cts_host == "cts-helper.example.test"


@pytest.mark.asyncio
async def test_identity_directory_resolves_ad_login_selector() -> None:
    repository = InMemoryIdentityMappingRepository()
    directory = UsernameEmailIdentityDirectory(
        identity_mapping_repository=repository,
        express_gateway=_FakeExpressGateway(
            {},
            huid_by_ad_login={"alice@corp.example": "alice-huid"},
        ),
        express_host="https://cts11dev.ccsteam.ru/",
    )

    result = await directory.resolve_corporate_selector(
        ad_login="alice",
        default_ad_domain="corp.example",
    )

    assert result.target_huid == "alice-huid"
    assert result.resolution_source == "manual_selector_ad_login"


@pytest.mark.asyncio
async def test_identity_directory_resolves_other_id_selector() -> None:
    repository = InMemoryIdentityMappingRepository()
    directory = UsernameEmailIdentityDirectory(
        identity_mapping_repository=repository,
        express_gateway=_FakeExpressGateway(
            {},
            huid_by_other_id={"hr-123": "alice-huid"},
        ),
        express_host="https://cts11dev.ccsteam.ru/",
    )

    result = await directory.resolve_corporate_selector(other_id="hr-123")

    assert result.target_huid == "alice-huid"
    assert result.resolution_source == "manual_selector_other_id"
