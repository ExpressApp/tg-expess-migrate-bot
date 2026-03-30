from __future__ import annotations

import asyncio
from pathlib import Path
from urllib.parse import urlparse
from uuid import UUID

import httpx
from pybotx import Bot, ChatTypes
from pybotx.auth import build_botx_jwt_v2
from pybotx.constants import MAX_NOTIFICATION_BODY_LENGTH
from pybotx.client.exceptions.base import BaseClientError
from pybotx.client.exceptions.callbacks import CallbackNotReceivedError
from pybotx.client.exceptions.chats import (
    ChatCreationError,
    ChatCreationProhibitedError,
    ChatLinkCreationError,
    ChatLinkCreationProhibitedError,
    InvalidUsersListError,
)
from pybotx.client.exceptions.common import (
    ChatNotFoundError,
    InvalidBotAccountError,
    PermissionDeniedError,
    RateLimitReachedError,
)
from pybotx.client.exceptions.users import UserNotFoundError
from pybotx.models.bot_account import BotAccountWithSecret
from pybotx.models.enums import ChatLinkTypes
from pybotx.models.attachments import (
    AttachmentDocument,
    AttachmentImage,
    AttachmentTypes,
    AttachmentVideo,
    AttachmentVoice,
    OutgoingAttachment,
)
from pybotx.client.notifications_api.direct_notification import (
    BotXAPIDirectNotificationRequestPayload,
    BotXAPIDirectNotificationSyncResponsePayload,
    _raise_direct_notification_sync_error,
)
from pybotx.missing import Undefined

from extg_shared.contracts.errors import (
    AmbiguousDeliveryError,
    ConfigurationError,
    FatalItemError,
    RecoverableItemError,
)
from extg_shared.contracts.models import ExpressStagedFile, FilePayload, SentMessageRef


class _StagedFileUnsupportedError(FatalItemError):
    pass


class PybotxExpressGateway:
    """Real eXpress adapter based on pybotx.

    Current BotX API does not expose an explicit idempotency key for
    `send_message`. This adapter keeps an in-process cache by idempotency key
    to reduce duplicates during retries in a single process.
    """

    _SUPPORTED_CHAT_TYPES = "PERSONAL_CHAT|GROUP_CHAT|CHANNEL|THREAD"

    def __init__(
        self,
        *,
        bot_id: str | None,
        cts_url: str | None,
        secret_key: str | None,
        chat_type: str = "GROUP_CHAT",
        default_participant_huids: list[str] | None = None,
        request_timeout_seconds: float = 20.0,
        local_idempotency_cache_enabled: bool = True,
        bot: Bot | None = None,
        httpx_client: httpx.AsyncClient | None = None,
    ) -> None:
        if bot is None:
            self._bot_id = self._parse_uuid(
                bot_id,
                setting_name="express.bot_id",
            )
            if not cts_url:
                raise ConfigurationError("express.cts_url is required for pybotx")
            if not secret_key:
                raise ConfigurationError("express.secret_key is required for pybotx")
            self._httpx_client = httpx_client or httpx.AsyncClient(
                timeout=httpx.Timeout(request_timeout_seconds),
            )
            self._bot = Bot(
                collectors=[],
                bot_accounts=[
                    BotAccountWithSecret(
                        id=self._bot_id,
                        cts_url=cts_url,
                        secret_key=secret_key,
                    ),
                ],
                httpx_client=self._httpx_client,
            )
            self._cts_url = cts_url
            self._secret_key = secret_key
        else:
            if not bot_id:
                raise ConfigurationError(
                    "express.bot_id is required even when custom Bot is injected",
                )
            self._bot_id = self._parse_uuid(
                bot_id,
                setting_name="express.bot_id",
            )
            self._bot = bot
            self._httpx_client = httpx_client
            self._cts_url = cts_url
            self._secret_key = secret_key

        self._default_chat_type = self._parse_chat_type(
            chat_type,
            setting_name="express.chat_type",
        )

        self._default_participants = [
            self._parse_uuid(value, setting_name="express.default_participant_huids")
            for value in (default_participant_huids or [])
        ]
        self._local_idempotency_cache_enabled = local_idempotency_cache_enabled
        self._idempotency_cache: dict[str, SentMessageRef] = {}
        self._idempotency_lock = asyncio.Lock()

    async def create_chat(
        self,
        title: str,
        participant_huids: list[str] | None = None,
        *,
        chat_type: str | None = None,
    ) -> str:
        participants = self._coalesce_participants(participant_huids)
        if not participants:
            raise FatalItemError(
                "pybotx create_chat requires at least one participant HUID; "
                "provide participant_huids or pre-create target chats",
            )
        resolved_chat_type = (
            self._parse_chat_type(
                chat_type,
                setting_name="express.create_chat.chat_type",
            )
            if chat_type is not None
            else self._default_chat_type
        )
        try:
            chat_id = await self._bot.create_chat(
                bot_id=self._bot_id,
                name=title,
                chat_type=resolved_chat_type,
                huids=participants,
            )
        except (RateLimitReachedError, CallbackNotReceivedError) as error:
            raise RecoverableItemError(f"temporary eXpress create_chat failure: {error}") from error
        except (httpx.ConnectError, httpx.ConnectTimeout) as error:
            raise RecoverableItemError(
                f"pre-send eXpress network failure during create_chat: {error}",
            ) from error
        except (
            PermissionDeniedError,
            InvalidBotAccountError,
            InvalidUsersListError,
            ChatCreationError,
            ChatCreationProhibitedError,
            BaseClientError,
        ) as error:
            raise FatalItemError(f"eXpress create_chat failed: {error}") from error
        except (httpx.TimeoutException, httpx.NetworkError, OSError) as error:
            raise AmbiguousDeliveryError(
                f"create_chat result is ambiguous after transport failure: {error}",
            ) from error
        return str(chat_id)

    async def ensure_personal_chat(
        self,
        user_huid: str,
        *,
        title: str | None = None,
    ) -> str:
        target_user_huid = self._parse_uuid(user_huid, setting_name="user_huid")
        try:
            chat_info = await self._bot.ensure_personal_chat(
                bot_id=self._bot_id,
                user_huid=target_user_huid,
                name=title,
            )
        except (RateLimitReachedError, CallbackNotReceivedError) as error:
            raise RecoverableItemError(
                f"temporary eXpress ensure_personal_chat failure: {error}",
            ) from error
        except (httpx.ConnectError, httpx.ConnectTimeout) as error:
            raise RecoverableItemError(
                f"pre-send eXpress network failure during ensure_personal_chat: {error}",
            ) from error
        except (
            PermissionDeniedError,
            InvalidBotAccountError,
            BaseClientError,
        ) as error:
            raise FatalItemError(
                f"eXpress ensure_personal_chat failed: {error}",
            ) from error
        except (httpx.TimeoutException, httpx.NetworkError, OSError) as error:
            raise AmbiguousDeliveryError(
                f"ensure_personal_chat result is ambiguous after transport failure: {error}",
            ) from error
        return str(chat_info.chat_id)

    async def create_chat_link(
        self,
        target_chat_id: str,
    ) -> str | None:
        chat_uuid = self._parse_uuid(target_chat_id, setting_name="target_chat_id")
        try:
            chat_link = await self._bot.create_chat_link(
                bot_id=self._bot_id,
                chat_id=chat_uuid,
                link_type=ChatLinkTypes.CORPORATE,
            )
        except (
            ChatNotFoundError,
            PermissionDeniedError,
            InvalidBotAccountError,
            ChatLinkCreationError,
            ChatLinkCreationProhibitedError,
            BaseClientError,
            httpx.HTTPError,
            OSError,
        ):
            return None
        return str(chat_link.url)

    async def search_user_by_email(self, email: str) -> str | None:
        try:
            user = await self._bot.search_user_by_email(
                bot_id=self._bot_id,
                email=email,
            )
        except UserNotFoundError:
            return None
        except (RateLimitReachedError, CallbackNotReceivedError) as error:
            raise RecoverableItemError(
                f"temporary eXpress search_user_by_email failure: {error}",
            ) from error
        except (httpx.ConnectError, httpx.ConnectTimeout) as error:
            raise RecoverableItemError(
                f"pre-send eXpress network failure during search_user_by_email: {error}",
            ) from error
        except (
            PermissionDeniedError,
            InvalidBotAccountError,
            BaseClientError,
        ) as error:
            raise FatalItemError(
                f"eXpress search_user_by_email failed: {error}",
            ) from error
        except (httpx.TimeoutException, httpx.NetworkError, OSError) as error:
            raise AmbiguousDeliveryError(
                f"search_user_by_email result is ambiguous after transport failure: {error}",
            ) from error
        return str(user.huid)

    async def search_user_by_huid(self, huid: str) -> str | None:
        normalized_huid = self._parse_uuid(huid, setting_name="user_huid")
        try:
            user = await self._bot.search_user_by_huid(
                bot_id=self._bot_id,
                huid=normalized_huid,
            )
        except UserNotFoundError:
            return None
        except (RateLimitReachedError, CallbackNotReceivedError) as error:
            raise RecoverableItemError(
                f"temporary eXpress search_user_by_huid failure: {error}",
            ) from error
        except (httpx.ConnectError, httpx.ConnectTimeout) as error:
            raise RecoverableItemError(
                f"pre-send eXpress network failure during search_user_by_huid: {error}",
            ) from error
        except (
            PermissionDeniedError,
            InvalidBotAccountError,
            BaseClientError,
        ) as error:
            raise FatalItemError(
                f"eXpress search_user_by_huid failed: {error}",
            ) from error
        except (httpx.TimeoutException, httpx.NetworkError, OSError) as error:
            raise AmbiguousDeliveryError(
                f"search_user_by_huid result is ambiguous after transport failure: {error}",
            ) from error
        return str(user.huid)

    async def search_user_by_ad_login(
        self,
        ad_login: str,
        *,
        ad_domain: str | None = None,
    ) -> str | None:
        normalized_login = ad_login.strip()
        normalized_domain = (ad_domain or "").strip()
        if not normalized_login:
            return None
        if not normalized_domain:
            raise ConfigurationError("ad_domain is required for search_user_by_ad_login")
        try:
            user = await self._bot.search_user_by_ad(
                bot_id=self._bot_id,
                ad_login=normalized_login,
                ad_domain=normalized_domain,
            )
        except UserNotFoundError:
            return None
        except (RateLimitReachedError, CallbackNotReceivedError) as error:
            raise RecoverableItemError(
                f"temporary eXpress search_user_by_ad_login failure: {error}",
            ) from error
        except (httpx.ConnectError, httpx.ConnectTimeout) as error:
            raise RecoverableItemError(
                f"pre-send eXpress network failure during search_user_by_ad_login: {error}",
            ) from error
        except (
            PermissionDeniedError,
            InvalidBotAccountError,
            BaseClientError,
        ) as error:
            raise FatalItemError(
                f"eXpress search_user_by_ad_login failed: {error}",
            ) from error
        except (httpx.TimeoutException, httpx.NetworkError, OSError) as error:
            raise AmbiguousDeliveryError(
                f"search_user_by_ad_login result is ambiguous after transport failure: {error}",
            ) from error
        return str(user.huid)

    async def search_user_by_other_id(self, other_id: str) -> str | None:
        normalized_other_id = other_id.strip()
        if not normalized_other_id:
            return None
        try:
            user = await self._bot.search_user_by_other_id(
                bot_id=self._bot_id,
                other_id=normalized_other_id,
            )
        except UserNotFoundError:
            return None
        except (RateLimitReachedError, CallbackNotReceivedError) as error:
            raise RecoverableItemError(
                f"temporary eXpress search_user_by_other_id failure: {error}",
            ) from error
        except (httpx.ConnectError, httpx.ConnectTimeout) as error:
            raise RecoverableItemError(
                f"pre-send eXpress network failure during search_user_by_other_id: {error}",
            ) from error
        except (
            PermissionDeniedError,
            InvalidBotAccountError,
            BaseClientError,
        ) as error:
            raise FatalItemError(
                f"eXpress search_user_by_other_id failed: {error}",
            ) from error
        except (httpx.TimeoutException, httpx.NetworkError, OSError) as error:
            raise AmbiguousDeliveryError(
                f"search_user_by_other_id result is ambiguous after transport failure: {error}",
            ) from error
        return str(user.huid)

    async def ensure_chat_members(
        self,
        target_chat_id: str,
        participant_huids: list[str],
    ) -> tuple[str, ...]:
        desired = self._coalesce_participants(participant_huids)
        if not desired:
            return ()
        chat_uuid = self._parse_uuid(target_chat_id, setting_name="target_chat_id")
        try:
            chat_info = await self._bot.chat_info(
                bot_id=self._bot_id,
                chat_id=chat_uuid,
            )
            existing = {str(member.huid) for member in chat_info.members}
            missing = [huid for huid in desired if str(huid) not in existing]
            if not missing:
                return ()
            await self._bot.add_users_to_chat(
                bot_id=self._bot_id,
                chat_id=chat_uuid,
                huids=missing,
            )
            refreshed_chat_info = await self._bot.chat_info(
                bot_id=self._bot_id,
                chat_id=chat_uuid,
            )
            refreshed_members = {
                str(member.huid)
                for member in refreshed_chat_info.members
            }
            missing_after_add = [
                huid
                for huid in missing
                if str(huid) not in refreshed_members
            ]
            if missing_after_add:
                raise FatalItemError(
                    "eXpress ensure_chat_members verification failed: "
                    "members not visible after add_users_to_chat; "
                    f"missing_huids={missing_after_add}",
                )
        except (RateLimitReachedError, CallbackNotReceivedError) as error:
            raise RecoverableItemError(
                f"temporary eXpress ensure_chat_members failure: {error}",
            ) from error
        except (httpx.ConnectError, httpx.ConnectTimeout) as error:
            raise RecoverableItemError(
                f"pre-send eXpress network failure during ensure_chat_members: {error}",
            ) from error
        except (
            ChatNotFoundError,
            PermissionDeniedError,
            InvalidBotAccountError,
            InvalidUsersListError,
            BaseClientError,
        ) as error:
            raise FatalItemError(
                f"eXpress ensure_chat_members failed: {error}",
            ) from error
        except (httpx.TimeoutException, httpx.NetworkError, OSError) as error:
            raise AmbiguousDeliveryError(
                f"ensure_chat_members result is ambiguous after transport failure: {error}",
            ) from error
        return tuple(str(huid) for huid in missing)

    async def promote_chat_admins(
        self,
        target_chat_id: str,
        participant_huids: list[str],
    ) -> tuple[str, ...]:
        desired = self._coalesce_participants(participant_huids)
        if not desired:
            return ()
        chat_uuid = self._parse_uuid(target_chat_id, setting_name="target_chat_id")
        try:
            chat_info = await self._bot.chat_info(
                bot_id=self._bot_id,
                chat_id=chat_uuid,
            )
            existing_admins = {
                str(member.huid)
                for member in chat_info.members
                if member.is_admin
            }
            missing_admins = [
                huid
                for huid in desired
                if str(huid) not in existing_admins
            ]
            if not missing_admins:
                return ()
            await self._bot.promote_to_chat_admins(
                bot_id=self._bot_id,
                chat_id=chat_uuid,
                huids=missing_admins,
            )
        except InvalidUsersListError as error:
            try:
                refreshed_chat_info = await self._bot.chat_info(
                    bot_id=self._bot_id,
                    chat_id=chat_uuid,
                )
            except (
                ChatNotFoundError,
                PermissionDeniedError,
                InvalidBotAccountError,
                BaseClientError,
            ) as refresh_error:
                raise FatalItemError(
                    f"eXpress promote_chat_admins failed: {error}",
                ) from refresh_error
            refreshed_admins = {
                str(member.huid)
                for member in refreshed_chat_info.members
                if member.is_admin
            }
            if all(str(huid) in refreshed_admins for huid in desired):
                return ()
            raise FatalItemError(
                f"eXpress promote_chat_admins failed: {error}",
            ) from error
        except (RateLimitReachedError, CallbackNotReceivedError) as error:
            raise RecoverableItemError(
                f"temporary eXpress promote_chat_admins failure: {error}",
            ) from error
        except (httpx.ConnectError, httpx.ConnectTimeout) as error:
            raise RecoverableItemError(
                f"pre-send eXpress network failure during promote_chat_admins: {error}",
            ) from error
        except (
            ChatNotFoundError,
            PermissionDeniedError,
            InvalidBotAccountError,
            BaseClientError,
        ) as error:
            raise FatalItemError(
                f"eXpress promote_chat_admins failed: {error}",
            ) from error
        except (httpx.TimeoutException, httpx.NetworkError, OSError) as error:
            raise AmbiguousDeliveryError(
                f"promote_chat_admins result is ambiguous after transport failure: {error}",
            ) from error
        return tuple(str(huid) for huid in missing_admins)

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
        if staged_file is not None:
            return await self.send_message_with_staged_file(
                target_chat_id,
                body,
                staged_file=staged_file,
                idempotency_key=idempotency_key,
                fallback_file=fallback_file,
            )
        if file is not None:
            return await self.send_message_with_file(
                target_chat_id,
                body,
                file=file,
                idempotency_key=idempotency_key,
            )
        chunks = self._split_notification_body(body)
        return await self._send_plain_chunks(
            target_chat_id=target_chat_id,
            chunks=chunks,
            idempotency_key=idempotency_key,
        )

    async def send_message_with_staged_file(
        self,
        target_chat_id: str,
        body: str,
        *,
        staged_file: ExpressStagedFile,
        idempotency_key: str | None = None,
        fallback_file: FilePayload | None = None,
    ) -> SentMessageRef:
        chunks = self._split_notification_body(body)
        first_chunk = chunks[0]
        remaining_chunks = chunks[1:]
        first_chunk_key = self._chunk_idempotency_key(
            idempotency_key,
            index=0,
            total=len(chunks),
        )
        if self._local_idempotency_cache_enabled and first_chunk_key:
            cached = await self._get_cached_message(first_chunk_key)
            if cached is not None:
                result = SentMessageRef(
                    target_chat_id=cached.target_chat_id,
                    target_sync_id=cached.target_sync_id,
                    deduplicated=True,
                )
            else:
                try:
                    result = await self._send_message_with_staged_file_raw(
                        target_chat_id,
                        first_chunk,
                        staged_file=staged_file,
                        idempotency_key=first_chunk_key,
                    )
                except _StagedFileUnsupportedError:
                    if fallback_file is None:
                        raise
                    return await self.send_message_with_file(
                        target_chat_id,
                        body,
                        file=fallback_file,
                        idempotency_key=idempotency_key,
                    )
                await self._store_cached_message(first_chunk_key, result)
        else:
            try:
                result = await self._send_message_with_staged_file_raw(
                    target_chat_id,
                    first_chunk,
                    staged_file=staged_file,
                    idempotency_key=first_chunk_key,
                )
            except _StagedFileUnsupportedError:
                if fallback_file is None:
                    raise
                return await self.send_message_with_file(
                    target_chat_id,
                    body,
                    file=fallback_file,
                    idempotency_key=idempotency_key,
                )

        for index, chunk in enumerate(remaining_chunks, start=1):
            await self._send_plain_message_raw(
                target_chat_id=target_chat_id,
                body=chunk,
                idempotency_key=self._chunk_idempotency_key(
                    idempotency_key,
                    index=index,
                    total=len(chunks),
                ),
            )
        return result

    async def send_message_with_file(
        self,
        target_chat_id: str,
        body: str,
        *,
        file: FilePayload | None = None,
        file_path: str | None = None,
        file_content: bytes | None = None,
        filename: str | None = None,
        idempotency_key: str | None = None,
    ) -> SentMessageRef:
        """Send a message with an attachment via typed pybotx attachment models."""
        payload = file
        if payload is None:
            if file_content is None:
                if not file_path:
                    raise ConfigurationError(
                        "either file, file_content or file_path must be provided",
                    )
                source_path = Path(file_path)
                if not source_path.exists():
                    raise FatalItemError("attachment temp file is missing")
                file_content = source_path.read_bytes()
                if filename is None:
                    filename = source_path.name
            payload = FilePayload(
                content=file_content,
                filename=filename or "attachment.bin",
            )
        elif file_content is not None or file_path is not None or filename is not None:
            raise ConfigurationError(
                "use either file or file_content/file_path arguments, not both",
            )

        chunks = self._split_notification_body(body)
        first_result = await self._send_file_message_raw(
            target_chat_id=target_chat_id,
            body=chunks[0],
            file=payload,
            idempotency_key=self._chunk_idempotency_key(
                idempotency_key,
                index=0,
                total=len(chunks),
            ),
        )
        for index, chunk in enumerate(chunks[1:], start=1):
            await self._send_plain_message_raw(
                target_chat_id=target_chat_id,
                body=chunk,
                idempotency_key=self._chunk_idempotency_key(
                    idempotency_key,
                    index=index,
                    total=len(chunks),
                ),
            )
        return first_result

    async def _send_message_with_staged_file_raw(
        self,
        target_chat_id: str,
        body: str,
        *,
        staged_file: ExpressStagedFile,
        idempotency_key: str | None,
    ) -> SentMessageRef:
        if self._httpx_client is None:
            raise FatalItemError("pybotx HTTP client is not available for staged file delivery")
        if not self._cts_url:
            raise FatalItemError("express.cts_url is required for staged file delivery")
        if not self._secret_key:
            raise FatalItemError("express.secret_key is required for staged file delivery")

        chat_uuid = self._parse_uuid(target_chat_id, setting_name="target_chat_id")
        payload = BotXAPIDirectNotificationRequestPayload.from_domain(
            chat_id=chat_uuid,
            body=body,
            metadata=Undefined,
            bubbles=Undefined,
            keyboard=Undefined,
            file=Undefined,
            recipients=Undefined,
            silent_response=Undefined,
            markup_auto_adjust=Undefined,
            stealth_mode=Undefined,
            send_push=Undefined,
            ignore_mute=Undefined,
        ).jsonable_dict()
        payload["file"] = self._staged_file_notification_payload(staged_file)

        try:
            response = await self._httpx_client.request(
                "POST",
                self._build_botx_url("/api/v4/botx/notifications/direct/sync"),
                json=payload,
                headers=self._authorization_headers(),
            )
        except (httpx.ConnectError, httpx.ConnectTimeout) as error:
            raise RecoverableItemError(
                f"pre-send eXpress network failure during send_message_with_staged_file: {error}",
            ) from error
        except (httpx.TimeoutException, httpx.NetworkError, OSError) as error:
            raise AmbiguousDeliveryError(
                f"send_message_with_staged_file result is ambiguous after transport failure: {error}",
            ) from error

        if response.status_code == 400:
            detail = self._response_detail(response)
            if "malformed_request" in detail or "invalid" in detail.lower():
                raise _StagedFileUnsupportedError(
                    f"staged file payload is not accepted by eXpress: {detail}",
                )
            raise FatalItemError(f"eXpress send_message_with_staged_file failed: {detail}")
        if response.status_code == 429 or response.status_code >= 500:
            raise RecoverableItemError(
                f"temporary eXpress send_message_with_staged_file failure: {self._response_detail(response)}",
            )
        if response.status_code >= 400:
            detail = self._response_detail(response)
            if response.status_code == 401:
                raise FatalItemError(f"eXpress send_message_with_staged_file failed: {detail}")
            raise FatalItemError(f"eXpress send_message_with_staged_file failed: {detail}")

        api_model = BotXAPIDirectNotificationSyncResponsePayload.model_validate(response.json())
        if api_model.status == "error":
            try:
                _raise_direct_notification_sync_error(response, api_model.reason)
            except BaseClientError as error:
                raise FatalItemError(
                    f"eXpress send_message_with_staged_file failed: {error}",
                ) from error

        assert api_model.result is not None
        return SentMessageRef(
            target_chat_id=target_chat_id,
            target_sync_id=str(api_model.result.sync_id),
            deduplicated=False,
        )

    async def _send_plain_chunks(
        self,
        *,
        target_chat_id: str,
        chunks: list[str],
        idempotency_key: str | None,
    ) -> SentMessageRef:
        first_result: SentMessageRef | None = None
        for index, chunk in enumerate(chunks):
            result = await self._send_plain_message_raw(
                target_chat_id=target_chat_id,
                body=chunk,
                idempotency_key=self._chunk_idempotency_key(
                    idempotency_key,
                    index=index,
                    total=len(chunks),
                ),
            )
            if first_result is None:
                first_result = result
        if first_result is None:
            raise FatalItemError("notification body chunking produced no chunks")
        return first_result

    async def _send_plain_message_raw(
        self,
        *,
        target_chat_id: str,
        body: str,
        idempotency_key: str | None,
    ) -> SentMessageRef:
        if self._local_idempotency_cache_enabled and idempotency_key:
            cached = await self._get_cached_message(idempotency_key)
            if cached is not None:
                return SentMessageRef(
                    target_chat_id=cached.target_chat_id,
                    target_sync_id=cached.target_sync_id,
                    deduplicated=True,
                )

        chat_uuid = self._parse_uuid(target_chat_id, setting_name="target_chat_id")
        try:
            sync_id = await self._bot.send_message(
                bot_id=self._bot_id,
                chat_id=chat_uuid,
                body=body,
                wait_callback=False,
            )
        except (RateLimitReachedError, CallbackNotReceivedError) as error:
            raise RecoverableItemError(f"temporary eXpress send_message failure: {error}") from error
        except (httpx.ConnectError, httpx.ConnectTimeout) as error:
            raise RecoverableItemError(
                f"pre-send eXpress network failure during send_message: {error}",
            ) from error
        except (
            ChatNotFoundError,
            PermissionDeniedError,
            InvalidBotAccountError,
            BaseClientError,
        ) as error:
            raise FatalItemError(f"eXpress send_message failed: {error}") from error
        except (httpx.TimeoutException, httpx.NetworkError, OSError) as error:
            raise AmbiguousDeliveryError(
                f"send_message result is ambiguous after transport failure: {error}",
            ) from error

        result = SentMessageRef(
            target_chat_id=target_chat_id,
            target_sync_id=str(sync_id),
            deduplicated=False,
        )
        if self._local_idempotency_cache_enabled and idempotency_key:
            await self._store_cached_message(idempotency_key, result)
        return result

    async def _send_file_message_raw(
        self,
        *,
        target_chat_id: str,
        body: str,
        file: FilePayload,
        idempotency_key: str | None,
    ) -> SentMessageRef:
        if self._local_idempotency_cache_enabled and idempotency_key:
            cached = await self._get_cached_message(idempotency_key)
            if cached is not None:
                return SentMessageRef(
                    target_chat_id=cached.target_chat_id,
                    target_sync_id=cached.target_sync_id,
                    deduplicated=True,
                )

        chat_uuid = self._parse_uuid(target_chat_id, setting_name="target_chat_id")
        outgoing = self._build_outgoing_attachment(file)
        try:
            sync_id = await self._bot.send_message(
                bot_id=self._bot_id,
                chat_id=chat_uuid,
                body=body,
                file=outgoing,
                wait_callback=False,
            )
        except (RateLimitReachedError, CallbackNotReceivedError) as error:
            raise RecoverableItemError(
                f"temporary eXpress send_message_with_file failure: {error}",
            ) from error
        except (httpx.ConnectError, httpx.ConnectTimeout) as error:
            raise RecoverableItemError(
                f"pre-send eXpress network failure during send_message_with_file: {error}",
            ) from error
        except (
            ChatNotFoundError,
            PermissionDeniedError,
            InvalidBotAccountError,
            BaseClientError,
        ) as error:
            raise FatalItemError(
                f"eXpress send_message_with_file failed: {error}",
            ) from error
        except (httpx.TimeoutException, httpx.NetworkError, OSError) as error:
            raise AmbiguousDeliveryError(
                f"send_message_with_file result is ambiguous after transport failure: {error}",
            ) from error

        result = SentMessageRef(
            target_chat_id=target_chat_id,
            target_sync_id=str(sync_id),
            deduplicated=False,
        )
        if self._local_idempotency_cache_enabled and idempotency_key:
            await self._store_cached_message(idempotency_key, result)
        return result

    def _build_outgoing_attachment(
        self,
        file: FilePayload,
    ) -> AttachmentImage | AttachmentVideo | AttachmentDocument | AttachmentVoice | OutgoingAttachment:
        attachment_name = file.filename or "attachment.bin"
        content, attachment_name = self._resolve_file_payload(file)
        size = len(content)
        media_kind = (file.media_kind or "").lower()
        duration = max(file.duration_seconds or 0, 0)

        if media_kind in {"photo", "sticker"}:
            return AttachmentImage(
                type=AttachmentTypes.IMAGE,
                filename=attachment_name,
                size=size,
                is_async_file=False,
                content=content,
            )
        if media_kind == "video":
            return AttachmentVideo(
                type=AttachmentTypes.VIDEO,
                filename=attachment_name,
                size=size,
                is_async_file=False,
                content=content,
                duration=duration,
            )
        if media_kind == "voice":
            return AttachmentVoice(
                type=AttachmentTypes.VOICE,
                filename=attachment_name,
                size=size,
                is_async_file=False,
                content=content,
                duration=duration,
            )
        if media_kind in {"document", "audio", "file"}:
            return AttachmentDocument(
                type=AttachmentTypes.DOCUMENT,
                filename=attachment_name,
                size=size,
                is_async_file=False,
                content=content,
            )
        return OutgoingAttachment(content=content, filename=attachment_name)

    def _build_botx_url(self, path: str) -> str:
        return "/".join(part.strip("/") for part in (self._cts_url or "", path))

    def _authorization_headers(self) -> dict[str, str]:
        bot_host = urlparse(self._cts_url or "").netloc
        if not bot_host:
            raise FatalItemError("express.cts_url host is required for staged file delivery")
        return {
            "Authorization": "Bearer "
            + build_botx_jwt_v2(
                bot_id=self._bot_id,
                bot_host=bot_host,
                secret_key=self._secret_key or "",
            ),
        }

    def _staged_file_notification_payload(
        self,
        staged_file: ExpressStagedFile,
    ) -> dict[str, object]:
        payload: dict[str, object] = {
            "type": staged_file.attachment_type,
            "file": staged_file.file_url,
            "file_id": staged_file.file_id,
            "file_name": staged_file.filename,
            "file_size": staged_file.size_bytes,
            "file_mime_type": staged_file.mime_type,
            "file_hash": staged_file.file_hash,
        }
        if staged_file.duration_seconds is not None:
            payload["duration"] = staged_file.duration_seconds
        if staged_file.preview_url is not None:
            payload["file_preview"] = staged_file.preview_url
        if staged_file.preview_height is not None:
            payload["file_preview_height"] = staged_file.preview_height
        if staged_file.preview_width is not None:
            payload["file_preview_width"] = staged_file.preview_width
        if staged_file.encryption_algo is not None:
            payload["file_encryption_algo"] = staged_file.encryption_algo
        if staged_file.chunk_size is not None:
            payload["chunk_size"] = staged_file.chunk_size
        if staged_file.caption is not None:
            payload["caption"] = staged_file.caption
        return payload

    def _response_detail(self, response: httpx.Response) -> str:
        try:
            payload = response.json()
        except ValueError:
            return response.text
        if not isinstance(payload, dict):
            return str(payload)
        detail = payload.get("reason") or payload.get("detail") or payload.get("error_data") or payload
        return str(detail)

    async def close(self) -> None:
        if self._httpx_client is not None:
            await self._httpx_client.aclose()

    async def _get_cached_message(self, key: str) -> SentMessageRef | None:
        async with self._idempotency_lock:
            return self._idempotency_cache.get(key)

    async def _store_cached_message(self, key: str, value: SentMessageRef) -> None:
        async with self._idempotency_lock:
            self._idempotency_cache[key] = value

    def _chunk_idempotency_key(
        self,
        base_key: str | None,
        *,
        index: int,
        total: int,
    ) -> str | None:
        if not base_key:
            return None
        if total <= 1:
            return base_key
        return f"{base_key}:chunk:{index + 1}"

    def _split_notification_body(self, body: str) -> list[str]:
        if len(body) <= MAX_NOTIFICATION_BODY_LENGTH:
            return [body]
        chunks: list[str] = []
        remaining = body
        while len(remaining) > MAX_NOTIFICATION_BODY_LENGTH:
            split_at = self._preferred_chunk_boundary(remaining, MAX_NOTIFICATION_BODY_LENGTH)
            current = remaining[:split_at].rstrip()
            if not current:
                split_at = MAX_NOTIFICATION_BODY_LENGTH
                current = remaining[:split_at]
            chunks.append(current)
            remaining = remaining[split_at:]
            if remaining[:1].isspace():
                remaining = remaining.lstrip()
        if remaining or not chunks:
            chunks.append(remaining)
        return chunks

    def _preferred_chunk_boundary(self, text: str, limit: int) -> int:
        window = text[: limit + 1]
        minimum_soft_boundary = max(limit // 2, 1)
        for separator in ("\n\n", "\n", " "):
            position = window.rfind(separator)
            if position >= minimum_soft_boundary:
                return position + len(separator)
        return limit

    def _coalesce_participants(self, participant_huids: list[str] | None) -> list[UUID]:
        raw_values = participant_huids if participant_huids is not None else []
        if not raw_values:
            return list(self._default_participants)
        return [
            self._parse_uuid(value, setting_name="participant_huids")
            for value in raw_values
        ]

    def _parse_chat_type(self, value: str, *, setting_name: str) -> ChatTypes:
        try:
            return ChatTypes[value.strip().upper()]
        except KeyError as error:
            raise ConfigurationError(
                f"{setting_name} must be one of {self._SUPPORTED_CHAT_TYPES}, got {value!r}",
            ) from error

    def _parse_uuid(self, value: str | None, *, setting_name: str) -> UUID:
        if not value:
            raise ConfigurationError(f"{setting_name} is required")
        try:
            return UUID(value)
        except ValueError as error:
            raise ConfigurationError(
                f"{setting_name} must be UUID, got {value!r}",
            ) from error

    def _resolve_file_payload(self, file: object) -> tuple[bytes, str]:
        if isinstance(file, OutgoingAttachment):
            return file.content, file.filename
        if isinstance(file, FilePayload):
            if file.content is not None:
                return file.content, file.filename
            temp_local_path = file.local_path
            filename = file.filename
        else:
            temp_local_path = getattr(file, "temp_local_path", None)
            filename = getattr(file, "filename", None)
        if temp_local_path:
            source_path = Path(temp_local_path)
            if not source_path.exists():
                raise FatalItemError("attachment temp file is missing")
            return source_path.read_bytes(), filename or source_path.name
        raise ConfigurationError(
            "unsupported file payload for send_message; "
            "expected OutgoingAttachment or object with temp_local_path/local_path",
        )
