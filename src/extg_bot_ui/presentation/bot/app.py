from __future__ import annotations

from contextlib import asynccontextmanager
from uuid import UUID

import httpx
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, Response
from pybotx import Bot
from pybotx.auth import BotXAuthVersion
from pybotx.bot.api.responses.command_accepted import build_command_accepted_response
from pybotx.bot.api.responses.unverified_request import (
    build_unverified_request_response,
)
from pybotx.bot.exceptions import (
    UnknownBotAccountError,
    RequestHeadersNotProvidedError,
    UnverifiedRequestError,
)
from pybotx.models.bot_account import BotAccountWithSecret
from pybotx_fsm import FSMMiddleware

from extg_bot_ui.presentation.bot.handlers import build_handler_collector
from extg_bot_ui.presentation.bot.resilient_reply import (
    install_resilient_answer_message,
)
from extg_bot_ui.presentation.bot.telegram_session_wizard import (
    build_telegram_session_wizard_collector,
)
from extg_bot_ui.presentation.bot.wizard import (
    WIZARD_STATE_REPO_KEY,
    build_wizard_collector,
)
from extg_bot_ui.bootstrap.container import BotUiContainer, create_container as create_bot_container
from extg_shared.contracts.errors import ConfigurationError
from extg_shared.contracts.models import ExpressBotHuidBindingRecord
from extg_shared.utils.express_routing import use_express_cts_host
from extg_shared.utils.runtime import close_runtime_resources


def create_bot_app(
    container: BotUiContainer | None = None,
    *,
    bot: Bot | None = None,
    runtime_httpx_client: httpx.AsyncClient | None = None,
) -> FastAPI:
    container = container or create_bot_container()
    settings = container.settings()
    logger = container.logger()
    express_bot_huid_binding_repository = container.express_bot_huid_binding_repository()
    archive_import_temp_file_janitor = container.archive_import_temp_file_janitor()
    configured_bot_ids = frozenset(
        account.bot_id.strip().lower()
        for account in settings.express.configured_accounts()
    )
    if not settings.bot.migration_id:
        raise ConfigurationError(
            "EXTG_BOT__MIGRATION_ID is required for extg-bot",
        )
    verify_requests = (
        settings.bot.verify_requests
        if settings.bot.verify_requests is not None
        else settings.normalized_environment not in {"local", "test"}
    )
    owns_runtime_bot = bot is None
    httpx_client = runtime_httpx_client
    if bot is None:
        handler_collector = build_handler_collector(
            container.bot_control_service(),
            telegram_session_service=container.telegram_session_service(),
            default_batch_size=settings.backfill.batch_size,
        )
        wizard_collector = build_wizard_collector(
            container.bot_control_service(),
            default_batch_size=settings.backfill.batch_size,
        )
        telegram_session_wizard_collector = build_telegram_session_wizard_collector(
            container.telegram_session_service(),
        )
        httpx_client = httpx_client or httpx.AsyncClient(
            timeout=httpx.Timeout(settings.express.request_timeout_seconds),
        )
        bot = Bot(
            collectors=[handler_collector],
            bot_accounts=list(_build_bot_accounts(settings)),
            auth_version=BotXAuthVersion.V2,
            httpx_client=httpx_client,
            middlewares=[
                FSMMiddleware(
                    [wizard_collector, telegram_session_wizard_collector],
                    state_repo_key=WIZARD_STATE_REPO_KEY,
                ),
            ],
        )
        setattr(bot.state, WIZARD_STATE_REPO_KEY, container.fsm_state_repository())
    install_resilient_answer_message(
        bot,
        logger=container.logger(),
    )

    @asynccontextmanager
    async def lifespan(_: FastAPI):
        await archive_import_temp_file_janitor.start()
        if owns_runtime_bot:
            await bot.startup(fetch_tokens=False)
        try:
            yield
        finally:
            await archive_import_temp_file_janitor.stop()
            if owns_runtime_bot:
                await bot.shutdown()
            if httpx_client is not None:
                await httpx_client.aclose()
            await close_runtime_resources(container)

    app = FastAPI(title="extg-bot", lifespan=lifespan)

    async def handle_events(request: Request):
        payload = await request.json()
        incoming_bot_id = _extract_bot_id(payload)
        if _is_unconfigured_bot_id(incoming_bot_id, configured_bot_ids):
            logger.warning(
                "ignored_event_from_unconfigured_bot",
                incoming_bot_id=incoming_bot_id,
                request_path=request.url.path,
            )
            return JSONResponse(build_command_accepted_response())
        await _learn_bot_huid_binding_if_possible(
            payload,
            configured_bot_ids=configured_bot_ids,
            repository=express_bot_huid_binding_repository,
            logger=logger,
        )
        try:
            with use_express_cts_host(_extract_bot_cts_host(payload)):
                bot.async_execute_raw_bot_command(
                    payload,
                    verify_request=verify_requests,
                    request_headers=request.headers,
                )
        except UnknownBotAccountError:
            logger.warning(
                "ignored_event_from_unconfigured_bot",
                incoming_bot_id=incoming_bot_id,
                request_path=request.url.path,
            )
            return JSONResponse(build_command_accepted_response())
        except (RequestHeadersNotProvidedError, UnverifiedRequestError):
            return JSONResponse(
                build_unverified_request_response("unverified request"),
                status_code=401,
            )
        return JSONResponse(build_command_accepted_response())

    async def get_status(request: Request):
        try:
            payload = await bot.raw_get_status(
                dict(request.query_params),
                verify_request=verify_requests,
                request_headers=request.headers,
            )
        except (RequestHeadersNotProvidedError, UnverifiedRequestError):
            return JSONResponse(
                build_unverified_request_response("unverified request"),
                status_code=401,
            )
        return JSONResponse(payload)

    async def handle_callbacks(request: Request):
        payload = await request.json()
        incoming_bot_id = _extract_bot_id(payload)
        if _is_unconfigured_bot_id(incoming_bot_id, configured_bot_ids):
            logger.warning(
                "ignored_callback_from_unconfigured_bot",
                incoming_bot_id=incoming_bot_id,
                request_path=request.url.path,
            )
            return Response(status_code=204)
        try:
            await bot.set_raw_botx_method_result(
                payload,
                verify_request=verify_requests,
                request_headers=request.headers,
            )
        except UnknownBotAccountError:
            logger.warning(
                "ignored_callback_from_unconfigured_bot",
                incoming_bot_id=incoming_bot_id,
                request_path=request.url.path,
            )
            return Response(status_code=204)
        except (RequestHeadersNotProvidedError, UnverifiedRequestError):
            return JSONResponse(
                build_unverified_request_response("unverified request"),
                status_code=401,
            )
        return Response(status_code=204)

    @app.get("/health")
    async def health() -> JSONResponse:
        return JSONResponse({"status": "ok"})

    for path in _unique_paths(settings.bot.events_path, "/command", "/api/bot/events"):
        app.add_api_route(path, handle_events, methods=["POST"])
    for path in _unique_paths(settings.bot.status_path, "/status", "/api/bot/status"):
        app.add_api_route(path, get_status, methods=["GET"])
    for path in _unique_paths(
        settings.bot.callbacks_path,
        "/notification/callback",
        "/api/bot/callbacks",
    ):
        app.add_api_route(path, handle_callbacks, methods=["POST"])

    return app


def main() -> None:
    import uvicorn

    container = create_bot_container()
    app = create_bot_app(container)
    settings = container.settings()
    uvicorn.run(
        app,
        host=settings.bot.host,
        port=settings.bot.port,
    )


def _build_bot_accounts(settings) -> tuple[BotAccountWithSecret, ...]:
    configured_accounts = settings.express.configured_accounts()
    if not configured_accounts:
        raise ConfigurationError(
            "At least one eXpress bot account must be configured for extg-bot",
        )
    return tuple(
        BotAccountWithSecret(
            id=UUID(account.bot_id),
            cts_url=account.cts_url,
            secret_key=account.secret_key,
        )
        for account in configured_accounts
    )


def _extract_bot_cts_host(payload: dict) -> str | None:
    bot_payload = payload.get("bot")
    if not isinstance(bot_payload, dict):
        bot_payload = payload.get("from")
        if not isinstance(bot_payload, dict):
            return None
    host = bot_payload.get("host")
    if isinstance(host, str):
        normalized = host.strip().lower()
        return normalized or None
    return None


def _extract_bot_id(payload: dict) -> str | None:
    value = payload.get("bot_id")
    if value is None:
        bot_payload = payload.get("bot")
        if isinstance(bot_payload, dict):
            value = bot_payload.get("id")
    if value is None:
        return None
    normalized = str(value).strip().lower()
    return normalized or None


def _is_unconfigured_bot_id(
    bot_id: str | None,
    configured_bot_ids: frozenset[str],
) -> bool:
    return bot_id is not None and bot_id not in configured_bot_ids


async def _learn_bot_huid_binding_if_possible(
    payload: dict,
    *,
    configured_bot_ids: frozenset[str],
    repository,
    logger,
) -> None:
    if not _is_bot_huid_learning_candidate(payload):
        return
    bot_id = _extract_bot_id(payload)
    if bot_id is None or bot_id not in configured_bot_ids:
        return
    bot_huid = _extract_sender_user_huid(payload)
    cts_host = _extract_bot_cts_host(payload)
    if bot_huid is None or cts_host is None:
        return
    try:
        await repository.save(
            ExpressBotHuidBindingRecord(
                bot_id=bot_id,
                cts_host=cts_host,
                bot_huid=bot_huid,
            ),
        )
    except Exception:
        logger.exception(
            "failed_to_persist_express_bot_huid_binding",
            bot_id=bot_id,
            cts_host=cts_host,
        )


def _is_bot_huid_learning_candidate(payload: dict) -> bool:
    command_payload = payload.get("command")
    if not isinstance(command_payload, dict):
        return False
    if command_payload.get("command_type") != "system":
        return False
    body = command_payload.get("body")
    return body in {
        "system:event_edit",
        "system:internal_bot_notification",
    }


def _extract_sender_user_huid(payload: dict) -> str | None:
    sender_payload = payload.get("from")
    if not isinstance(sender_payload, dict):
        return None
    user_huid = sender_payload.get("user_huid")
    if user_huid is None:
        return None
    normalized = str(user_huid).strip().lower()
    return normalized or None


def _unique_paths(*paths: str) -> tuple[str, ...]:
    unique: list[str] = []
    for path in paths:
        if path not in unique:
            unique.append(path)
    return tuple(unique)
