from __future__ import annotations

import re
from collections.abc import Mapping

from extg_shared.contracts.errors import ConfigurationError

OUTPUT_TEMPLATE_PLACEHOLDERS = frozenset(
    {
        "author",
        "username",
        "timestamp",
        "source_chat_title",
        "source_chat_id",
        "source_message_id",
        "reply_to_source_message_id",
        "reply_block",
        "body",
        "header",
        "footer",
    },
)
OUTPUT_TEMPLATE_PATTERN = re.compile(r"{{\s*([a-zA-Z_][a-zA-Z0-9_]*)\s*}}")


def normalize_output_template(value: str | None) -> str | None:
    if value is None:
        return None
    normalized = value.strip()
    return normalized or None


def validate_output_template(value: str | None) -> str | None:
    normalized = normalize_output_template(value)
    if normalized is None:
        return None
    unknown_placeholders = sorted(
        {
            placeholder
            for placeholder in OUTPUT_TEMPLATE_PATTERN.findall(normalized)
            if placeholder not in OUTPUT_TEMPLATE_PLACEHOLDERS
        },
    )
    if unknown_placeholders:
        placeholders = ", ".join(f"`{{{{{placeholder}}}}}`" for placeholder in unknown_placeholders)
        raise ConfigurationError(
            "неизвестные переменные шаблона: "
            + placeholders
            + ". Доступны: "
            + ", ".join(
                f"`{{{{{placeholder}}}}}`"
                for placeholder in sorted(OUTPUT_TEMPLATE_PLACEHOLDERS)
            ),
        )
    return normalized


def render_output_template(
    *,
    template: str,
    values: Mapping[str, str],
) -> str:
    def replace(match: re.Match[str]) -> str:
        placeholder = match.group(1)
        return values.get(placeholder, match.group(0))

    return OUTPUT_TEMPLATE_PATTERN.sub(replace, template)
