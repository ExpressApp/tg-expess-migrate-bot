from __future__ import annotations

from contextlib import contextmanager
from contextvars import ContextVar, Token
from typing import Iterator
from urllib.parse import urlparse


_current_express_cts_host: ContextVar[str | None] = ContextVar(
    "current_express_cts_host",
    default=None,
)


def normalize_express_cts_host(value: str | None) -> str | None:
    if value is None:
        return None
    raw = value.strip()
    if not raw:
        return None
    parsed = urlparse(raw if "://" in raw else f"https://{raw}")
    host = (parsed.netloc or parsed.path).strip().lower()
    return host or None


def get_current_express_cts_host() -> str | None:
    return _current_express_cts_host.get()


@contextmanager
def use_express_cts_host(cts_host: str | None) -> Iterator[None]:
    token: Token[str | None] = _current_express_cts_host.set(
        normalize_express_cts_host(cts_host),
    )
    try:
        yield
    finally:
        _current_express_cts_host.reset(token)


__all__ = [
    "get_current_express_cts_host",
    "normalize_express_cts_host",
    "use_express_cts_host",
]
