from __future__ import annotations

import shlex
from dataclasses import dataclass

from extg_bot_ui.application.bot_control import MigrationRunOptions


class CommandArgumentError(ValueError):
    """Raised when a bot command contains unsupported arguments."""


@dataclass(frozen=True, slots=True)
class ParsedArguments:
    positionals: tuple[str, ...]
    options: dict[str, str]


@dataclass(frozen=True, slots=True)
class IdentityMappingArguments:
    telegram_user_id: str | None
    telegram_username: str | None
    corporate_email: str
    telegram_display_name: str | None = None


def parse_arguments(argument: str) -> ParsedArguments:
    try:
        tokens = shlex.split(argument)
    except ValueError as error:
        raise CommandArgumentError(str(error)) from error

    positionals: list[str] = []
    options: dict[str, str] = {}
    for token in tokens:
        if "=" not in token:
            positionals.append(token)
            continue
        key, value = token.split("=", 1)
        key = key.strip().lower().replace("-", "_")
        value = value.strip()
        if not key:
            raise CommandArgumentError(f"invalid option token: {token!r}")
        options[key] = value
    return ParsedArguments(positionals=tuple(positionals), options=options)


def parse_migrate_all_options(argument: str) -> MigrationRunOptions:
    parsed = parse_arguments(argument)
    if parsed.positionals:
        raise CommandArgumentError("command does not accept positional arguments")
    return _to_run_options(parsed)


def parse_migrate_chat_options(argument: str) -> tuple[str, MigrationRunOptions]:
    parsed = parse_arguments(argument)
    source_chat_id = parsed.options.pop("chat", None)
    if source_chat_id is None:
        if not parsed.positionals:
            raise CommandArgumentError("source chat id is required")
        source_chat_id = parsed.positionals[0]
        if len(parsed.positionals) > 1:
            raise CommandArgumentError("too many positional arguments")
        parsed = ParsedArguments(positionals=(), options=dict(parsed.options))
    elif parsed.positionals:
        raise CommandArgumentError("use either positional source_chat_id or chat=<id>")
    return source_chat_id, _to_run_options(parsed)


def parse_optional_migrate_chat_options(argument: str) -> tuple[str | None, MigrationRunOptions]:
    parsed = parse_arguments(argument)
    source_chat_id = parsed.options.pop("chat", None)
    if source_chat_id is None:
        if parsed.positionals:
            source_chat_id = parsed.positionals[0]
            if len(parsed.positionals) > 1:
                raise CommandArgumentError("too many positional arguments")
            parsed = ParsedArguments(positionals=(), options=dict(parsed.options))
        else:
            if parsed.options:
                raise CommandArgumentError("source chat id is required when options are provided")
            return None, MigrationRunOptions()
    elif parsed.positionals:
        raise CommandArgumentError("use either positional source_chat_id or chat=<id>")
    return source_chat_id, _to_run_options(parsed)


def parse_optional_source_chat_id(argument: str) -> str | None:
    parsed = parse_arguments(argument)
    if "chat" in parsed.options:
        source_chat_id = parsed.options.pop("chat")
        if parsed.positionals or parsed.options:
            raise CommandArgumentError("unsupported extra arguments")
        return source_chat_id
    if len(parsed.positionals) > 1:
        raise CommandArgumentError("too many positional arguments")
    if parsed.options:
        raise CommandArgumentError("unsupported extra arguments")
    return parsed.positionals[0] if parsed.positionals else None


def parse_required_source_chat_id(argument: str) -> str:
    source_chat_id = parse_optional_source_chat_id(argument)
    if source_chat_id is None:
        raise CommandArgumentError("source chat id is required")
    return source_chat_id


def parse_map_identity_arguments(argument: str) -> IdentityMappingArguments:
    parsed = parse_arguments(argument)
    username = parsed.options.pop("username", None)
    telegram_user_id = (
        parsed.options.pop("tg_id", None)
        or parsed.options.pop("telegram_user_id", None)
        or parsed.options.pop("user_id", None)
    )
    corporate_email = parsed.options.pop("email", None)
    telegram_display_name = (
        parsed.options.pop("display_name", None)
        or parsed.options.pop("name", None)
    )
    positionals = list(parsed.positionals)
    if telegram_user_id is None and username is None and positionals:
        username = positionals.pop(0)
    if corporate_email is None and positionals:
        corporate_email = positionals.pop(0)
    if positionals:
        raise CommandArgumentError("too many positional arguments")
    if parsed.options:
        unknown = ", ".join(sorted(parsed.options))
        raise CommandArgumentError(f"unsupported options: {unknown}")
    if (not username and not telegram_user_id) or not corporate_email:
        raise CommandArgumentError(
            "telegram user id or username and corporate email are required",
        )
    return IdentityMappingArguments(
        telegram_user_id=telegram_user_id,
        telegram_username=username,
        corporate_email=corporate_email,
        telegram_display_name=telegram_display_name,
    )


def parse_list_chats_options(argument: str) -> tuple[int, str | None]:
    parsed = parse_arguments(argument)
    if parsed.positionals:
        raise CommandArgumentError("command does not accept positional arguments")
    limit = _parse_positive_int(parsed.options.pop("limit", None), 20, "limit")
    query = parsed.options.pop("query", None)
    if parsed.options:
        unknown = ", ".join(sorted(parsed.options))
        raise CommandArgumentError(f"unsupported options: {unknown}")
    return limit, query


def parse_list_chat_users_options(argument: str) -> str:
    parsed = parse_arguments(argument)
    if "chat" in parsed.options:
        source_chat_id = parsed.options.pop("chat")
        if parsed.positionals:
            raise CommandArgumentError("use either positional source_chat_id or chat=<id>")
    else:
        if not parsed.positionals:
            raise CommandArgumentError("source chat id is required")
        source_chat_id = parsed.positionals[0]
        if len(parsed.positionals) > 1:
            raise CommandArgumentError("too many positional arguments")
    if parsed.options:
        unknown = ", ".join(sorted(parsed.options))
        raise CommandArgumentError(f"unsupported options: {unknown}")
    return source_chat_id


def parse_replay_failed_options(
    argument: str,
    *,
    default_limit: int,
    default_batch_size: int,
) -> tuple[str | None, int, int]:
    parsed = parse_arguments(argument)
    source_chat_id = parsed.options.pop("chat", None)
    if source_chat_id is None and parsed.positionals:
        source_chat_id = parsed.positionals[0]
        if len(parsed.positionals) > 1:
            raise CommandArgumentError("too many positional arguments")
    elif parsed.positionals:
        raise CommandArgumentError("use either positional source_chat_id or chat=<id>")

    limit = _parse_positive_int(parsed.options.pop("limit", None), default_limit, "limit")
    batch_size = _parse_positive_int(
        parsed.options.pop("batch", None),
        default_batch_size,
        "batch",
    )
    if parsed.options:
        unknown = ", ".join(sorted(parsed.options))
        raise CommandArgumentError(f"unsupported options: {unknown}")
    return source_chat_id, limit, batch_size


def _to_run_options(parsed: ParsedArguments) -> MigrationRunOptions:
    if parsed.positionals:
        raise CommandArgumentError("unsupported positional arguments")
    options = dict(parsed.options)
    include_from = options.pop("from", None)
    include_to = options.pop("to", None)
    migrate_media = _parse_optional_bool(options.pop("media", None), "media")
    service_messages = _parse_optional_bool(
        options.pop("service", None) or options.pop("service_messages", None),
        "service",
    )
    reply_mode = _parse_reply_mode(
        options.pop("format", None) or options.pop("reply", None),
    )
    target_strategy = _parse_target_strategy(options.pop("target", None))
    target_title = options.pop("target_title", None)
    target_chat_id = options.pop("target_chat_id", None)
    identity_policy = options.pop("identity", None)
    access_strategy = _parse_access_strategy(
        options.pop("access", None)
        or options.pop("users", None)
        or options.pop("members", None)
        or options.pop("channel_access", None),
    )
    topic_strategy = _parse_topic_strategy(options.pop("topics", None))
    skip_in_all = _parse_optional_bool(
        options.pop("skip", None) or options.pop("skip_in_all", None),
        "skip",
    )
    progress_policy = options.pop("progress", "resume").strip().lower()
    if progress_policy not in {"resume", "ask"}:
        raise CommandArgumentError("progress must be one of: resume, ask")
    batch_size = _parse_optional_positive_int(options.pop("batch", None), "batch")
    if options:
        unknown = ", ".join(sorted(options))
        raise CommandArgumentError(f"unsupported options: {unknown}")
    return MigrationRunOptions(
        include_from=include_from,
        include_to=include_to,
        migrate_media=migrate_media,
        service_messages=service_messages,
        reply_mode=reply_mode,
        target_strategy=target_strategy,
        target_title=target_title,
        target_chat_id=target_chat_id,
        identity_policy=identity_policy,
        access_strategy=access_strategy,
        topic_strategy=topic_strategy,
        skip_in_all=skip_in_all,
        progress_policy=progress_policy,
        batch_size=batch_size,
    )


def _parse_optional_bool(value: str | None, option_name: str) -> bool | None:
    if value is None:
        return None
    normalized = value.strip().lower()
    if normalized in {"on", "true", "1", "yes"}:
        return True
    if normalized in {"off", "false", "0", "no"}:
        return False
    raise CommandArgumentError(f"{option_name} must be one of: on, off")


def _parse_reply_mode(value: str | None) -> str | None:
    if value is None:
        return None
    normalized = value.strip().lower()
    mapping = {
        "quote": "inline_quote",
        "inline_quote": "inline_quote",
        "source_id": "source_id",
        "none": "none",
    }
    resolved = mapping.get(normalized)
    if resolved is None:
        raise CommandArgumentError("format must be one of: quote, source_id, none")
    return resolved


def _parse_access_strategy(value: str | None) -> str | None:
    if value is None:
        return None
    normalized = value.strip().lower()
    mapping = {
        "none": "none",
        "skip": "none",
        "off": "none",
        "invite": "direct_add",
        "direct_add": "direct_add",
        "add": "direct_add",
        "direct": "direct_add",
        "invite_link": "invite_link",
        "link": "invite_link",
    }
    resolved = mapping.get(normalized)
    if resolved is None:
        raise CommandArgumentError("access must be one of: none, invite, link")
    return resolved


def _parse_target_strategy(value: str | None) -> str | None:
    if value is None:
        return None
    normalized = value.strip().lower()
    if normalized not in {"create", "bind", "manual_review_required"}:
        raise CommandArgumentError(
            "target must be one of: create, bind, manual_review_required",
        )
    return normalized


def _parse_topic_strategy(value: str | None) -> str | None:
    if value is None:
        return None
    normalized = value.strip().lower()
    mapping = {
        "single_chat": "single_chat",
        "one_chat": "single_chat",
        "single": "single_chat",
        "split_by_topic": "split_by_topic",
        "split": "split_by_topic",
    }
    resolved = mapping.get(normalized)
    if resolved is None:
        raise CommandArgumentError(
            "topics must be one of: single_chat, split_by_topic",
        )
    return resolved


def _parse_positive_int(value: str | None, default: int, option_name: str) -> int:
    if value is None:
        return default
    return _parse_int(value, option_name)


def _parse_optional_positive_int(value: str | None, option_name: str) -> int | None:
    if value is None:
        return None
    return _parse_int(value, option_name)


def _parse_int(value: str, option_name: str) -> int:
    try:
        parsed = int(value)
    except ValueError as error:
        raise CommandArgumentError(f"{option_name} must be a positive integer") from error
    if parsed <= 0:
        raise CommandArgumentError(f"{option_name} must be a positive integer")
    return parsed
