from __future__ import annotations

from typing import Any


async def close_runtime_resources(
    container: Any,
    *,
    provider_names: tuple[str, ...] = (
        "telegram_gateway",
        "telegram_session_service",
        "express_gateway",
        "express_file_store",
        "integration_event_publisher",
    ),
    close_database: bool = True,
) -> None:
    for provider_name in provider_names:
        provider = getattr(container, provider_name, None)
        if provider is None:
            continue
        if getattr(provider, "last_overriding", None) is not None:
            continue
        instance = safe_provider_call(provider)
        await close_if_possible(instance)
    if close_database and hasattr(container, "persistence_backend") and container.persistence_backend() == "postgres":
        await close_if_possible(safe_provider_call(container.database))


async def close_if_possible(instance: object | None) -> None:
    if instance is None:
        return
    close = getattr(instance, "close", None)
    if close is None:
        return
    result = close()
    if hasattr(result, "__await__"):
        await result


def safe_provider_call(provider) -> Any | None:
    try:
        return provider()
    except Exception:
        return None


__all__ = ["close_if_possible", "close_runtime_resources", "safe_provider_call"]
