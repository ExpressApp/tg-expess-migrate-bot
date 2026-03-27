from __future__ import annotations

from collections.abc import Callable
from contextlib import AbstractContextManager
from typing import Protocol

from extg_shared.config.common import (
    ExpressBotAccountRegistry,
    ExpressBotAccountSettings,
)
from extg_shared.contracts.errors import FatalItemError
from extg_shared.contracts.models import ExpressStagedFile, FilePayload, SentMessageRef
from extg_shared.utils.express_routing import (
    get_current_express_cts_host,
    normalize_express_cts_host,
    use_express_cts_host,
)
from extg_shared.utils.runtime import close_if_possible
from extg_migration_runtime.infrastructure.express.pybotx_file_store import (
    PybotxExpressFileStore,
)
from extg_migration_runtime.infrastructure.express.pybotx_gateway import (
    PybotxExpressGateway,
)


class _ExpressGatewayLike(Protocol):
    async def create_chat(
        self,
        title: str,
        participant_huids: list[str] | None = None,
        *,
        chat_type: str | None = None,
    ) -> str: ...

    async def ensure_chat_members(
        self,
        target_chat_id: str,
        participant_huids: list[str],
    ) -> tuple[str, ...]: ...

    async def promote_chat_admins(
        self,
        target_chat_id: str,
        participant_huids: list[str],
    ) -> tuple[str, ...]: ...

    async def ensure_personal_chat(
        self,
        user_huid: str,
        *,
        title: str | None = None,
    ) -> str: ...

    async def create_chat_link(self, target_chat_id: str) -> str | None: ...

    async def search_user_by_email(self, email: str) -> str | None: ...

    async def search_user_by_huid(self, huid: str) -> str | None: ...

    async def search_user_by_ad_login(
        self,
        ad_login: str,
        *,
        ad_domain: str | None = None,
    ) -> str | None: ...

    async def search_user_by_other_id(self, other_id: str) -> str | None: ...

    async def send_message(
        self,
        target_chat_id: str,
        body: str,
        *,
        idempotency_key: str | None = None,
        file: FilePayload | None = None,
        staged_file: ExpressStagedFile | None = None,
        fallback_file: FilePayload | None = None,
    ) -> SentMessageRef: ...


class _ExpressFileStoreLike(Protocol):
    async def upload_file(
        self,
        target_chat_id: str,
        *,
        file: FilePayload,
    ) -> ExpressStagedFile: ...


class ExpressGatewayRouter:
    def __init__(
        self,
        *,
        account_registry: ExpressBotAccountRegistry,
        chat_type: str = "GROUP_CHAT",
        default_participant_huids: list[str] | None = None,
        request_timeout_seconds: float = 20.0,
        local_idempotency_cache_enabled: bool = True,
        gateway_factory: Callable[[ExpressBotAccountSettings], _ExpressGatewayLike] | None = None,
    ) -> None:
        self._account_registry = account_registry
        self._gateways: dict[str, _ExpressGatewayLike] = {}
        if gateway_factory is None:
            gateway_factory = lambda account: PybotxExpressGateway(
                bot_id=account.bot_id,
                cts_url=account.cts_url,
                secret_key=account.secret_key,
                chat_type=chat_type,
                default_participant_huids=default_participant_huids,
                request_timeout_seconds=request_timeout_seconds,
                local_idempotency_cache_enabled=local_idempotency_cache_enabled,
            )
        for account in account_registry.accounts:
            self._gateways[account.cts_host] = gateway_factory(account)

    @property
    def registered_cts_hosts(self) -> tuple[str, ...]:
        return tuple(self._gateways.keys())

    @property
    def primary_cts_host(self) -> str:
        return self._account_registry.primary_account.cts_host

    def bot_huid_for_cts_host(self, cts_host: str | None) -> str | None:
        normalized = normalize_express_cts_host(cts_host)
        account = (
            self._account_registry.primary_account
            if normalized is None
            else self._account_registry.get_by_cts_host(normalized)
        )
        if account is None:
            return None
        return account.effective_bot_member_huid

    def bot_id_for_cts_host(self, cts_host: str | None) -> str | None:
        normalized = normalize_express_cts_host(cts_host)
        account = (
            self._account_registry.primary_account
            if normalized is None
            else self._account_registry.get_by_cts_host(normalized)
        )
        if account is None:
            return None
        return account.bot_id

    def use_cts_host(self, cts_host: str | None) -> AbstractContextManager[None]:
        return use_express_cts_host(cts_host)

    async def create_chat(
        self,
        title: str,
        participant_huids: list[str] | None = None,
        *,
        chat_type: str | None = None,
    ) -> str:
        return await self._current_gateway().create_chat(
            title,
            participant_huids,
            chat_type=chat_type,
        )

    async def ensure_chat_members(
        self,
        target_chat_id: str,
        participant_huids: list[str],
    ) -> tuple[str, ...]:
        return await self._current_gateway().ensure_chat_members(
            target_chat_id,
            participant_huids,
        )

    async def promote_chat_admins(
        self,
        target_chat_id: str,
        participant_huids: list[str],
    ) -> tuple[str, ...]:
        return await self._current_gateway().promote_chat_admins(
            target_chat_id,
            participant_huids,
        )

    async def ensure_personal_chat(
        self,
        user_huid: str,
        *,
        title: str | None = None,
    ) -> str:
        return await self._current_gateway().ensure_personal_chat(
            user_huid,
            title=title,
        )

    async def create_chat_link(self, target_chat_id: str) -> str | None:
        return await self._current_gateway().create_chat_link(target_chat_id)

    async def search_user_by_email(self, email: str) -> str | None:
        return await self._current_gateway().search_user_by_email(email)

    async def search_user_by_huid(self, huid: str) -> str | None:
        return await self._current_gateway().search_user_by_huid(huid)

    async def search_user_by_ad_login(
        self,
        ad_login: str,
        *,
        ad_domain: str | None = None,
    ) -> str | None:
        return await self._current_gateway().search_user_by_ad_login(
            ad_login,
            ad_domain=ad_domain,
        )

    async def search_user_by_other_id(self, other_id: str) -> str | None:
        return await self._current_gateway().search_user_by_other_id(other_id)

    async def send_message(
        self,
        target_chat_id: str,
        body: str,
        *,
        idempotency_key: str | None = None,
        file: FilePayload | None = None,
        staged_file: ExpressStagedFile | None = None,
        fallback_file: FilePayload | None = None,
    ) -> SentMessageRef:
        return await self._current_gateway().send_message(
            target_chat_id,
            body,
            idempotency_key=idempotency_key,
            file=file,
            staged_file=staged_file,
            fallback_file=fallback_file,
        )

    async def close(self) -> None:
        for gateway in self._gateways.values():
            await close_if_possible(gateway)

    def _current_gateway(self) -> _ExpressGatewayLike:
        host = get_current_express_cts_host()
        if host is None:
            return self._gateways[self.primary_cts_host]
        normalized = normalize_express_cts_host(host)
        gateway = self._gateways.get(normalized or "")
        if gateway is None:
            raise FatalItemError(
                f"No eXpress bot account configured for CTS host: {host}",
            )
        return gateway


class ExpressFileStoreRouter:
    def __init__(
        self,
        *,
        account_registry: ExpressBotAccountRegistry,
        request_timeout_seconds: float = 20.0,
        max_upload_size_bytes: int = 100 * 1024 * 1024,
        spool_max_memory_bytes: int = 1024 * 1024,
        file_store_factory: Callable[[ExpressBotAccountSettings], _ExpressFileStoreLike] | None = None,
    ) -> None:
        self._account_registry = account_registry
        self._file_stores: dict[str, _ExpressFileStoreLike] = {}
        if file_store_factory is None:
            file_store_factory = lambda account: PybotxExpressFileStore(
                bot_id=account.bot_id,
                cts_url=account.cts_url,
                secret_key=account.secret_key,
                request_timeout_seconds=request_timeout_seconds,
                max_upload_size_bytes=max_upload_size_bytes,
                spool_max_memory_bytes=spool_max_memory_bytes,
            )
        for account in account_registry.accounts:
            self._file_stores[account.cts_host] = file_store_factory(account)

    @property
    def registered_cts_hosts(self) -> tuple[str, ...]:
        return tuple(self._file_stores.keys())

    @property
    def primary_cts_host(self) -> str:
        return self._account_registry.primary_account.cts_host

    def bot_huid_for_cts_host(self, cts_host: str | None) -> str | None:
        normalized = normalize_express_cts_host(cts_host)
        account = (
            self._account_registry.primary_account
            if normalized is None
            else self._account_registry.get_by_cts_host(normalized)
        )
        if account is None:
            return None
        return account.effective_bot_member_huid

    def bot_id_for_cts_host(self, cts_host: str | None) -> str | None:
        normalized = normalize_express_cts_host(cts_host)
        account = (
            self._account_registry.primary_account
            if normalized is None
            else self._account_registry.get_by_cts_host(normalized)
        )
        if account is None:
            return None
        return account.bot_id

    def use_cts_host(self, cts_host: str | None) -> AbstractContextManager[None]:
        return use_express_cts_host(cts_host)

    async def upload_file(
        self,
        target_chat_id: str,
        *,
        file: FilePayload,
    ) -> ExpressStagedFile:
        return await self._current_file_store().upload_file(
            target_chat_id,
            file=file,
        )

    async def close(self) -> None:
        for file_store in self._file_stores.values():
            await close_if_possible(file_store)

    def _current_file_store(self) -> _ExpressFileStoreLike:
        host = get_current_express_cts_host()
        if host is None:
            return self._file_stores[self.primary_cts_host]
        normalized = normalize_express_cts_host(host)
        file_store = self._file_stores.get(normalized or "")
        if file_store is None:
            raise FatalItemError(
                f"No eXpress file store configured for CTS host: {host}",
            )
        return file_store


__all__ = ["ExpressFileStoreRouter", "ExpressGatewayRouter"]
