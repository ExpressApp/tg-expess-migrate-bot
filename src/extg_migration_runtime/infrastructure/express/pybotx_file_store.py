from __future__ import annotations

from aiofiles import open as aiofiles_open
from aiofiles.tempfile import SpooledTemporaryFile
import httpx
from pybotx import Bot
from pybotx.client.exceptions.base import BaseClientError
from pybotx.client.exceptions.callbacks import CallbackNotReceivedError
from pybotx.client.exceptions.common import InvalidBotAccountError, RateLimitReachedError
from pybotx.models.async_files import Document, Image, Video, Voice
from pybotx.models.bot_account import BotAccountWithSecret
from uuid import UUID
from pathlib import Path

from extg_shared.contracts.errors import (
    AmbiguousDeliveryError,
    ConfigurationError,
    FatalItemError,
    RecoverableItemError,
)
from extg_shared.contracts.models import ExpressStagedFile, FilePayload


class PybotxExpressFileStore:
    def __init__(
        self,
        *,
        bot_id: str | None,
        cts_url: str | None,
        secret_key: str | None,
        request_timeout_seconds: float = 20.0,
        max_upload_size_bytes: int = 100 * 1024 * 1024,
        spool_max_memory_bytes: int = 1024 * 1024,
        bot: Bot | None = None,
        httpx_client: httpx.AsyncClient | None = None,
    ) -> None:
        self._bot_id = self._parse_uuid(bot_id, setting_name="express.bot_id")
        self._max_upload_size_bytes = max_upload_size_bytes
        self._spool_max_memory_bytes = spool_max_memory_bytes
        if bot is None:
            if not cts_url:
                raise ConfigurationError("express.cts_url is required for pybotx file store")
            if not secret_key:
                raise ConfigurationError("express.secret_key is required for pybotx file store")
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
        else:
            self._bot = bot
            self._httpx_client = httpx_client

    async def upload_file(
        self,
        target_chat_id: str,
        *,
        file: FilePayload,
    ) -> ExpressStagedFile:
        chat_uuid = self._parse_uuid(target_chat_id, setting_name="target_chat_id")
        file_size_bytes = self._file_size_bytes(file)
        if file_size_bytes > self._max_upload_size_bytes:
            raise FatalItemError(
                "eXpress upload_file size limit exceeded: "
                f"{file_size_bytes} > {self._max_upload_size_bytes}",
            )
        try:
            if file.local_path:
                async with aiofiles_open(file.local_path, "rb") as buffer:
                    uploaded = await self._bot.upload_file(
                        bot_id=self._bot_id,
                        chat_id=chat_uuid,
                        async_buffer=buffer,
                        filename=file.filename,
                        duration=file.duration_seconds,
                    )
            else:
                if file.content is None:
                    raise FatalItemError("upload_file requires content or local_path")
                async with SpooledTemporaryFile(
                    max_size=max(self._spool_max_memory_bytes, 1024),
                ) as buffer:
                    await buffer.write(file.content)
                    await buffer.seek(0)
                    uploaded = await self._bot.upload_file(
                        bot_id=self._bot_id,
                        chat_id=chat_uuid,
                        async_buffer=buffer,
                        filename=file.filename,
                        duration=file.duration_seconds,
                    )
        except (RateLimitReachedError, CallbackNotReceivedError) as error:
            raise RecoverableItemError(f"temporary eXpress upload_file failure: {error}") from error
        except (httpx.ConnectError, httpx.ConnectTimeout) as error:
            raise RecoverableItemError(
                f"pre-send eXpress network failure during upload_file: {error}",
            ) from error
        except InvalidBotAccountError as error:
            raise FatalItemError(f"eXpress upload_file failed: {error}") from error
        except BaseClientError as error:
            message = str(error)
            if self._looks_recoverable_client_error(message):
                raise RecoverableItemError(f"temporary eXpress upload_file failure: {error}") from error
            raise FatalItemError(f"eXpress upload_file failed: {error}") from error
        except (httpx.TimeoutException, httpx.NetworkError, OSError) as error:
            raise AmbiguousDeliveryError(
                f"upload_file result is ambiguous after transport failure: {error}",
            ) from error
        return self._to_staged_file(uploaded)

    async def close(self) -> None:
        if self._httpx_client is not None:
            await self._httpx_client.aclose()

    def _to_staged_file(
        self,
        uploaded: Image | Video | Document | Voice,
    ) -> ExpressStagedFile:
        duration = getattr(uploaded, "duration", None)
        return ExpressStagedFile(
            attachment_type=uploaded.type.value,
            file_id=str(uploaded._file_id),
            file_url=uploaded.file_url,
            filename=uploaded.filename,
            size_bytes=uploaded.size,
            mime_type=uploaded.file_mimetype,
            file_hash=uploaded.file_hash,
            duration_seconds=duration,
            preview_url=uploaded.file_preview,
            preview_height=uploaded.file_preview_height,
            preview_width=uploaded.file_preview_width,
            encryption_algo=uploaded.file_encryption_algo,
            chunk_size=uploaded.chunk_size,
            caption=uploaded.caption,
        )

    def _looks_recoverable_client_error(self, message: str) -> bool:
        normalized = message.lower()
        return "429" in normalized or "rate limit" in normalized or "timeout" in normalized

    def _file_size_bytes(self, file: FilePayload) -> int:
        if file.content is not None:
            return len(file.content)
        if file.local_path:
            source_path = Path(file.local_path)
            if not source_path.exists():
                raise FatalItemError("attachment temp file is missing")
            return source_path.stat().st_size
        raise FatalItemError("upload_file requires content or local_path")

    def _parse_uuid(
        self,
        value: str | None,
        *,
        setting_name: str,
    ) -> UUID:
        if not value:
            raise ConfigurationError(f"{setting_name} is required for pybotx")
        return UUID(str(value))
