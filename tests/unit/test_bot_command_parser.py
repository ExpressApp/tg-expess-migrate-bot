import pytest

from extg_bot_ui.presentation.bot.command_parser import (
    CommandArgumentError,
    parse_list_chats_options,
    parse_map_identity_arguments,
    parse_migrate_all_options,
    parse_migrate_chat_options,
    parse_replay_failed_options,
)


def test_parse_migrate_chat_options_supports_overrides():
    source_chat_id, options = parse_migrate_chat_options(
        "chat-1 from=2026-03-01T00:00:00Z to=2026-03-05 media=off format=source_id "
        "target=bind target_chat_id=express-1 access=link "
        "topics=single_chat skip=on progress=ask batch=25",
    )

    assert source_chat_id == "chat-1"
    assert options.include_from == "2026-03-01T00:00:00Z"
    assert options.include_to == "2026-03-05"
    assert options.migrate_media is False
    assert options.reply_mode == "source_id"
    assert options.source_backend is None
    assert options.target_strategy == "bind"
    assert options.target_chat_id == "express-1"
    assert options.access_strategy == "invite_link"
    assert options.topic_strategy == "single_chat"
    assert options.skip_in_all is True
    assert options.progress_policy == "ask"
    assert options.batch_size == 25


def test_parse_migrate_chat_options_rejects_removed_source_backend_override():
    with pytest.raises(CommandArgumentError):
        parse_migrate_chat_options("chat-1 source=telethon")


def test_parse_migrate_chat_options_rejects_unknown_topic_strategy():
    with pytest.raises(CommandArgumentError):
        parse_migrate_chat_options("chat-1 topics=invalid")


def test_parse_migrate_chat_options_rejects_unknown_access_strategy():
    with pytest.raises(CommandArgumentError):
        parse_migrate_chat_options("chat-1 access=invalid")


def test_parse_migrate_all_options_rejects_unknown_reply_mode():
    with pytest.raises(CommandArgumentError):
        parse_migrate_all_options("format=invalid")


def test_parse_replay_failed_options_uses_defaults():
    source_chat_id, limit, batch_size = parse_replay_failed_options(
        "chat=chat-1",
        default_limit=100,
        default_batch_size=50,
    )

    assert source_chat_id == "chat-1"
    assert limit == 100
    assert batch_size == 50


def test_parse_list_chats_options_uses_defaults():
    limit, query = parse_list_chats_options("query=python")

    assert limit == 20
    assert query == "python"


def test_parse_map_identity_arguments_supports_positionals():
    mapping = parse_map_identity_arguments("@peer.user peer.user@example.com")

    assert mapping.telegram_username == "@peer.user"
    assert mapping.telegram_user_id is None
    assert mapping.corporate_email == "peer.user@example.com"


def test_parse_map_identity_arguments_supports_tg_id_and_display_name():
    mapping = parse_map_identity_arguments(
        "tg_id=123456 username=@peer.user email=peer.user@example.com display_name='Peer User'",
    )

    assert mapping.telegram_user_id == "123456"
    assert mapping.telegram_username == "@peer.user"
    assert mapping.telegram_display_name == "Peer User"
    assert mapping.corporate_email == "peer.user@example.com"
