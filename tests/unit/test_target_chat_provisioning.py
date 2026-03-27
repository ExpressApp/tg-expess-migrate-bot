from datetime import UTC, datetime

import pytest

from extg_migration_runtime.application.identity import UsernameEmailIdentityDirectory
from extg_migration_runtime.application.cts_resolution import CtsResolutionService
from extg_migration_runtime.application.target_chat_provisioning import (
    TargetChatProvisioningService,
)
from extg_migration_runtime.infrastructure.express.router import ExpressGatewayRouter
from extg_shared.contracts.errors import FatalItemError, RecoverableItemError
from extg_shared.contracts.manifest import ManifestDialog
from extg_shared.contracts.models import (
    ExpressBotHuidBindingRecord,
    ExpressUserCtsBindingRecord,
    IdentityMappingRecord,
    SourceChannelAccessProfile,
    SourceDialog,
    SourceParticipant,
)
from extg_shared.config.common import ExpressBotAccountRegistry, ExpressBotAccountSettings
from extg_migration_runtime.infrastructure.express.fake_gateway import FakeExpressGateway
from extg_migration_runtime.infrastructure.persistence.in_memory import (
    InMemoryChatMappingRepository,
    InMemoryExpressBotHuidBindingRepository,
    InMemoryExpressUserCtsBindingRepository,
    InMemoryIdentityMappingRepository,
)
from extg_shared.utils.retry import AsyncRetryPolicy
from extg_telethon_service.infrastructure.persistence.in_memory import (
    InMemoryAuditRepository,
)
from extg_telethon_service.infrastructure.telegram.fake_gateway import (
    FakeTelegramGateway,
)


class FailingParticipantTelegramGateway:
    def __init__(
        self,
        *,
        is_admin: bool = True,
        can_list_participants: bool = True,
    ) -> None:
        self._is_admin = is_admin
        self._can_list_participants = can_list_participants

    async def list_participants(
        self,
        dialog_id: str,
        *,
        source_backend: str = "telethon_user_session",
    ):
        raise FatalItemError(
            f"telegram failed to list participants for {dialog_id}: Chat admin privileges are required",
        )

    async def get_channel_access_profile(
        self,
        dialog_id: str,
        *,
        source_backend: str = "telethon_user_session",
    ) -> SourceChannelAccessProfile:
        return SourceChannelAccessProfile(
            dialog_id=dialog_id,
            is_admin=self._is_admin,
            can_list_participants=self._can_list_participants,
        )


class FatalEnsureMembersExpressGateway(FakeExpressGateway):
    async def ensure_chat_members(
        self,
        target_chat_id: str,
        participant_huids: list[str],
    ) -> tuple[str, ...]:
        raise FatalItemError("direct add is not allowed for channel delivery")


class RecoverableEnsureMembersExpressGateway(FakeExpressGateway):
    async def ensure_chat_members(
        self,
        target_chat_id: str,
        participant_huids: list[str],
    ) -> tuple[str, ...]:
        raise RecoverableItemError("temporary eXpress failure during direct add")


class _SharedCrossCtsGateway:
    def __init__(
        self,
        *,
        host: str,
        bot_member_huid: str,
        shared_state: dict[str, object],
        user_huid_by_email: dict[str, str] | None = None,
        fatal_member_sync_hosts: set[str] | None = None,
    ) -> None:
        self.host = host
        self.bot_member_huid = bot_member_huid
        self._shared_state = shared_state
        self._user_huid_by_email = {
            email.strip().lower(): huid
            for email, huid in (user_huid_by_email or {}).items()
        }
        self._fatal_member_sync_hosts = set(fatal_member_sync_hosts or set())
        self.member_sync_calls: list[tuple[str, tuple[str, ...]]] = []
        self.admin_promotion_calls: list[tuple[str, tuple[str, ...]]] = []
        self.personal_chat_calls: list[str] = []
        self.sent_messages: list[tuple[str, str, str]] = []

    async def create_chat(
        self,
        title: str,
        participant_huids: list[str] | None = None,
        *,
        chat_type: str | None = None,
    ) -> str:
        chat_seq = int(self._shared_state.get("chat_seq", 0)) + 1
        self._shared_state["chat_seq"] = chat_seq
        target_chat_id = f"cross-chat-{chat_seq}"
        self._shared_state.setdefault("created_by", {})[target_chat_id] = self.host
        self._shared_state.setdefault("titles", {})[target_chat_id] = title
        self._shared_state.setdefault("kinds", {})[target_chat_id] = chat_type or "GROUP_CHAT"
        self._shared_state.setdefault("participants", {})[target_chat_id] = list(
            participant_huids or []
        )
        self._shared_state.setdefault("admins", {})[target_chat_id] = [self.bot_member_huid]
        return target_chat_id

    async def ensure_chat_members(
        self,
        target_chat_id: str,
        participant_huids: list[str],
    ) -> tuple[str, ...]:
        self.member_sync_calls.append((target_chat_id, tuple(participant_huids)))
        admins = self._shared_state.setdefault("admins", {}).setdefault(target_chat_id, [])
        if self.bot_member_huid not in admins:
            raise FatalItemError(f"sender is not chat admin for host={self.host}")
        if self.host in self._fatal_member_sync_hosts:
            raise FatalItemError(f"direct add is not allowed for host={self.host}")
        participants = self._shared_state.setdefault("participants", {}).setdefault(
            target_chat_id,
            [],
        )
        added: list[str] = []
        for huid in participant_huids:
            if huid not in participants:
                participants.append(huid)
                added.append(huid)
        return tuple(added)

    async def promote_chat_admins(
        self,
        target_chat_id: str,
        participant_huids: list[str],
    ) -> tuple[str, ...]:
        self.admin_promotion_calls.append((target_chat_id, tuple(participant_huids)))
        admins = self._shared_state.setdefault("admins", {}).setdefault(target_chat_id, [])
        promoted: list[str] = []
        for huid in participant_huids:
            if huid not in admins:
                admins.append(huid)
                promoted.append(huid)
        return tuple(promoted)

    async def ensure_personal_chat(
        self,
        user_huid: str,
        *,
        title: str | None = None,
    ) -> str:
        self.personal_chat_calls.append(user_huid)
        target_chat_id = f"{self.host}:personal:{user_huid}"
        self._shared_state.setdefault("participants", {})[target_chat_id] = [user_huid]
        return target_chat_id

    async def create_chat_link(self, target_chat_id: str) -> str | None:
        if target_chat_id not in self._shared_state.setdefault("participants", {}):
            return None
        return f"https://express.example/chat/{target_chat_id}"

    async def search_user_by_email(self, email: str) -> str | None:
        return self._user_huid_by_email.get(email.strip().lower())

    async def search_user_by_huid(self, huid: str) -> str | None:
        return huid

    async def send_message(
        self,
        target_chat_id: str,
        body: str,
        *,
        idempotency_key: str | None = None,
        file=None,
        staged_file=None,
        fallback_file=None,
    ):
        self.sent_messages.append((target_chat_id, body, idempotency_key or ""))
        return type(
            "_SentRef",
            (),
            {"target_chat_id": target_chat_id, "target_sync_id": f"{self.host}:sync"},
        )()


def _multi_cts_registry(*, helper_bot_huid: str | None = "helper-bot-huid") -> ExpressBotAccountRegistry:
    return ExpressBotAccountRegistry(
        (
            ExpressBotAccountSettings(
                role="primary",
                bot_id="00000000-0000-0000-0000-000000000001",
                bot_huid="main-bot-huid",
                cts_url="https://cts-main.example.test",
                secret_key="primary-secret",
            ),
            ExpressBotAccountSettings(
                role="technical",
                bot_id="00000000-0000-0000-0000-000000000002",
                bot_huid=helper_bot_huid,
                cts_url="https://cts-helper.example.test",
                secret_key="helper-secret",
                visible=False,
            ),
        ),
    )


@pytest.mark.asyncio
async def test_validate_dialog_preconditions_allows_private_chat_without_identity_mapping():
    telegram_gateway = FakeTelegramGateway(
        dialogs=[SourceDialog(dialog_id="dm-1", chat_type="private", title="Direct Chat")],
        messages_by_dialog={},
        participants_by_dialog={
            "dm-1": [
                SourceParticipant(
                    external_id="self-1",
                    username="self.user",
                    display_name="Self User",
                    is_self=True,
                ),
                SourceParticipant(
                    external_id="peer-1",
                    username="peer.user",
                    display_name="Peer User",
                ),
            ],
        },
    )
    express_gateway = FakeExpressGateway()
    service = TargetChatProvisioningService(
        telegram_gateway=telegram_gateway,
        express_gateway=express_gateway,
        chat_mapping_repository=InMemoryChatMappingRepository(),
        identity_directory=UsernameEmailIdentityDirectory(
            identity_mapping_repository=InMemoryIdentityMappingRepository(),
            express_gateway=express_gateway,
        ),
        audit_repository=InMemoryAuditRepository(),
        retry_policy=AsyncRetryPolicy(
            max_attempts=1,
            base_delay_seconds=0,
            max_delay_seconds=0,
            jitter_seconds=0,
        ),
    )

    await service.validate_dialog_preconditions(
        migration_id="migration-bot",
        dialog=ManifestDialog(
            source_chat_id="dm-1",
            source_chat_type="private",
            target_strategy="create",
            target_title="Imported DM",
            initiator_huid="initiator-huid",
        ),
    )


@pytest.mark.asyncio
async def test_validate_dialog_preconditions_allows_private_chat_with_existing_mapping():
    identity_repo = InMemoryIdentityMappingRepository()
    await identity_repo.save(
        IdentityMappingRecord(
            telegram_user_id="peer-1",
            telegram_username="peer.user",
            telegram_display_name="Peer User",
            corporate_email="peer.user@example.com",
            target_huid=None,
            created_at=datetime(2026, 3, 18, 10, 0, tzinfo=UTC),
            updated_at=datetime(2026, 3, 18, 10, 0, tzinfo=UTC),
            last_resolved_at=None,
        ),
    )
    telegram_gateway = FakeTelegramGateway(
        dialogs=[SourceDialog(dialog_id="dm-1", chat_type="private", title="Direct Chat")],
        messages_by_dialog={},
        participants_by_dialog={
            "dm-1": [
                SourceParticipant(
                    external_id="self-1",
                    username="self.user",
                    display_name="Self User",
                    is_self=True,
                ),
                SourceParticipant(
                    external_id="peer-1",
                    username="peer.user",
                    display_name="Peer User",
                ),
            ],
        },
    )
    express_gateway = FakeExpressGateway(
        user_huid_by_email={"peer.user@example.com": "huid-peer-user"},
    )
    service = TargetChatProvisioningService(
        telegram_gateway=telegram_gateway,
        express_gateway=express_gateway,
        chat_mapping_repository=InMemoryChatMappingRepository(),
        identity_directory=UsernameEmailIdentityDirectory(
            identity_mapping_repository=identity_repo,
            express_gateway=express_gateway,
        ),
        audit_repository=InMemoryAuditRepository(),
        retry_policy=AsyncRetryPolicy(
            max_attempts=1,
            base_delay_seconds=0,
            max_delay_seconds=0,
            jitter_seconds=0,
        ),
    )

    await service.validate_dialog_preconditions(
        migration_id="migration-bot",
        dialog=ManifestDialog(
            source_chat_id="dm-1",
            source_chat_type="private",
            target_strategy="create",
            target_title="Imported DM",
            initiator_huid="initiator-huid",
        ),
    )


@pytest.mark.asyncio
async def test_ensure_target_chat_creates_private_chat_with_initiator_and_resolved_peer():
    identity_repo = InMemoryIdentityMappingRepository()
    await identity_repo.save(
        IdentityMappingRecord(
            telegram_user_id="peer-1",
            telegram_username="peer.user",
            telegram_display_name="Peer User",
            corporate_email="peer.user@example.com",
        ),
    )
    telegram_gateway = FakeTelegramGateway(
        dialogs=[SourceDialog(dialog_id="dm-1", chat_type="private", title="Direct Chat")],
        messages_by_dialog={},
        participants_by_dialog={
            "dm-1": [
                SourceParticipant(
                    external_id="self-1",
                    username="self.user",
                    display_name="Self User",
                    is_self=True,
                ),
                SourceParticipant(
                    external_id="peer-1",
                    username="peer.user",
                    display_name="Peer User",
                ),
            ],
        },
    )
    express_gateway = FakeExpressGateway(
        user_huid_by_email={"peer.user@example.com": "peer-huid"},
    )
    audit_repository = InMemoryAuditRepository()
    service = TargetChatProvisioningService(
        telegram_gateway=telegram_gateway,
        express_gateway=express_gateway,
        chat_mapping_repository=InMemoryChatMappingRepository(),
        identity_directory=UsernameEmailIdentityDirectory(
            identity_mapping_repository=identity_repo,
            express_gateway=express_gateway,
        ),
        audit_repository=audit_repository,
        retry_policy=AsyncRetryPolicy(
            max_attempts=1,
            base_delay_seconds=0,
            max_delay_seconds=0,
            jitter_seconds=0,
        ),
    )

    mapping = await service.ensure_target_chat(
        migration_id="migration-bot",
        dialog=ManifestDialog(
            source_chat_id="dm-1",
            source_chat_type="private",
            target_strategy="create",
            target_title="Imported DM",
            initiator_huid="initiator-huid",
        ),
    )

    created_chat = express_gateway.created_chat(mapping.target_chat_id)
    audit_events = await audit_repository.list_all()
    assert created_chat is not None
    assert created_chat.kind == "group"
    assert created_chat.participant_huids == ["initiator-huid", "peer-huid"]
    assert "initiator-huid" in created_chat.admin_huids
    assert any(
        event.event_type == "target_chat_initiator_admin_synced"
        for event in audit_events
    )


@pytest.mark.asyncio
async def test_ensure_target_chat_creates_private_chat_with_initiator_only_when_peer_unresolved():
    telegram_gateway = FakeTelegramGateway(
        dialogs=[SourceDialog(dialog_id="dm-1", chat_type="private", title="Direct Chat")],
        messages_by_dialog={},
        participants_by_dialog={
            "dm-1": [
                SourceParticipant(
                    external_id="self-1",
                    username="self.user",
                    display_name="Self User",
                    is_self=True,
                ),
                SourceParticipant(
                    external_id="peer-1",
                    username="peer.user",
                    display_name="Peer User",
                ),
            ],
        },
    )
    express_gateway = FakeExpressGateway()
    audit_repository = InMemoryAuditRepository()
    service = TargetChatProvisioningService(
        telegram_gateway=telegram_gateway,
        express_gateway=express_gateway,
        chat_mapping_repository=InMemoryChatMappingRepository(),
        identity_directory=UsernameEmailIdentityDirectory(
            identity_mapping_repository=InMemoryIdentityMappingRepository(),
            express_gateway=express_gateway,
        ),
        audit_repository=audit_repository,
        retry_policy=AsyncRetryPolicy(
            max_attempts=1,
            base_delay_seconds=0,
            max_delay_seconds=0,
            jitter_seconds=0,
        ),
    )

    mapping = await service.ensure_target_chat(
        migration_id="migration-bot",
        dialog=ManifestDialog(
            source_chat_id="dm-1",
            source_chat_type="private",
            target_strategy="create",
            target_title="Imported DM",
            initiator_huid="initiator-huid",
        ),
    )

    created_chat = express_gateway.created_chat(mapping.target_chat_id)
    audit_events = await audit_repository.list_all()
    assert created_chat is not None
    assert created_chat.kind == "group"
    assert created_chat.participant_huids == ["initiator-huid"]
    assert "initiator-huid" in created_chat.admin_huids
    assert any(
        event.event_type == "target_chat_initiator_admin_synced"
        for event in audit_events
    )


@pytest.mark.asyncio
async def test_ensure_target_chat_private_chat_degrades_when_participant_listing_fails():
    express_gateway = FakeExpressGateway()
    audit_repository = InMemoryAuditRepository()
    service = TargetChatProvisioningService(
        telegram_gateway=FailingParticipantTelegramGateway(),
        express_gateway=express_gateway,
        chat_mapping_repository=InMemoryChatMappingRepository(),
        identity_directory=UsernameEmailIdentityDirectory(
            identity_mapping_repository=InMemoryIdentityMappingRepository(),
            express_gateway=express_gateway,
        ),
        audit_repository=audit_repository,
        retry_policy=AsyncRetryPolicy(
            max_attempts=1,
            base_delay_seconds=0,
            max_delay_seconds=0,
            jitter_seconds=0,
        ),
    )

    mapping = await service.ensure_target_chat(
        migration_id="migration-bot",
        dialog=ManifestDialog(
            source_chat_id="dm-1",
            source_chat_type="private",
            target_strategy="create",
            target_title="Imported DM",
            initiator_huid="initiator-huid",
        ),
    )

    created_chat = express_gateway.created_chat(mapping.target_chat_id)
    audit_events = await audit_repository.list_all()
    assert created_chat is not None
    assert created_chat.kind == "group"
    assert created_chat.participant_huids == ["initiator-huid"]
    assert "initiator-huid" in created_chat.admin_huids
    assert any(event.event_type == "private_chat_peer_resolution_degraded" for event in audit_events)
    assert any(
        event.event_type == "target_chat_initiator_admin_synced"
        for event in audit_events
    )


@pytest.mark.asyncio
async def test_group_preconditions_degrade_when_member_list_is_unavailable():
    express_gateway = FakeExpressGateway()
    audit_repository = InMemoryAuditRepository()
    service = TargetChatProvisioningService(
        telegram_gateway=FailingParticipantTelegramGateway(),
        express_gateway=express_gateway,
        chat_mapping_repository=InMemoryChatMappingRepository(),
        identity_directory=UsernameEmailIdentityDirectory(
            identity_mapping_repository=InMemoryIdentityMappingRepository(),
            express_gateway=express_gateway,
        ),
        audit_repository=audit_repository,
        retry_policy=AsyncRetryPolicy(
            max_attempts=1,
            base_delay_seconds=0,
            max_delay_seconds=0,
            jitter_seconds=0,
        ),
    )
    dialog = ManifestDialog(
        source_chat_id="-1002210509299",
        source_chat_type="supergroup",
        target_strategy="create",
        target_title="Imported Group",
        initiator_huid="initiator-huid",
    )

    await service.validate_dialog_preconditions(
        migration_id="migration-bot",
        dialog=dialog,
    )
    mapping = await service.ensure_target_chat(
        migration_id="migration-bot",
        dialog=dialog,
    )

    created_chat = express_gateway.created_chat(mapping.target_chat_id)
    audit_events = await audit_repository.list_all()
    assert created_chat is not None
    assert created_chat.kind == "group"
    assert created_chat.participant_huids == ["initiator-huid"]
    assert "initiator-huid" in created_chat.admin_huids
    assert any(event.event_type == "participant_listing_degraded" for event in audit_events)
    assert any(
        event.event_type == "target_chat_initiator_admin_synced"
        for event in audit_events
    )


@pytest.mark.asyncio
async def test_channel_chat_skips_participant_listing_and_creates_notification_chat():
    express_gateway = FakeExpressGateway()
    audit_repository = InMemoryAuditRepository()
    service = TargetChatProvisioningService(
        telegram_gateway=FailingParticipantTelegramGateway(
            is_admin=True,
            can_list_participants=False,
        ),
        express_gateway=express_gateway,
        chat_mapping_repository=InMemoryChatMappingRepository(),
        identity_directory=UsernameEmailIdentityDirectory(
            identity_mapping_repository=InMemoryIdentityMappingRepository(),
            express_gateway=express_gateway,
        ),
        audit_repository=audit_repository,
        retry_policy=AsyncRetryPolicy(
            max_attempts=1,
            base_delay_seconds=0,
            max_delay_seconds=0,
            jitter_seconds=0,
        ),
    )
    dialog = ManifestDialog(
        source_chat_id="-1002210509299",
        source_chat_type="channel",
        target_strategy="create",
        target_title="Imported Channel",
        initiator_huid="initiator-huid",
    )

    await service.validate_dialog_preconditions(
        migration_id="migration-bot",
        dialog=dialog,
    )
    mapping = await service.ensure_target_chat(
        migration_id="migration-bot",
        dialog=dialog,
    )

    created_chat = express_gateway.created_chat(mapping.target_chat_id)
    audit_events = await audit_repository.list_all()
    assert created_chat is not None
    assert created_chat.kind == "channel"
    assert created_chat.participant_huids == ["initiator-huid"]
    assert "initiator-huid" in created_chat.admin_huids
    assert any(event.event_type == "channel_participant_preview_degraded" for event in audit_events)
    assert any(event.event_type == "channel_access_link_prepared" for event in audit_events)
    assert any(
        event.event_type == "channel_initiator_admin_synced"
        for event in audit_events
    )


@pytest.mark.asyncio
async def test_validate_channel_preconditions_degrades_non_admin_operator():
    audit_repository = InMemoryAuditRepository()
    service = TargetChatProvisioningService(
        telegram_gateway=FailingParticipantTelegramGateway(
            is_admin=False,
            can_list_participants=False,
        ),
        express_gateway=FakeExpressGateway(),
        chat_mapping_repository=InMemoryChatMappingRepository(),
        identity_directory=UsernameEmailIdentityDirectory(
            identity_mapping_repository=InMemoryIdentityMappingRepository(),
            express_gateway=FakeExpressGateway(),
        ),
        audit_repository=audit_repository,
        retry_policy=AsyncRetryPolicy(
            max_attempts=1,
            base_delay_seconds=0,
            max_delay_seconds=0,
            jitter_seconds=0,
        ),
    )

    await service.validate_dialog_preconditions(
        migration_id="migration-bot",
        dialog=ManifestDialog(
            source_chat_id="-100999",
            source_chat_type="channel",
            target_strategy="create",
            target_title="Imported Channel",
            initiator_huid="initiator-huid",
        ),
    )
    audit_events = await audit_repository.list_all()
    assert any(event.event_type == "channel_admin_preflight_degraded" for event in audit_events)


@pytest.mark.asyncio
async def test_channel_chat_dispatches_invite_links_when_invite_strategy_selected():
    identity_repo = InMemoryIdentityMappingRepository()
    await identity_repo.save(
        IdentityMappingRecord(
            telegram_user_id="user-1",
            telegram_username="alice",
            telegram_display_name="Alice",
            corporate_email="alice@example.com",
        ),
    )
    telegram_gateway = FakeTelegramGateway(
        dialogs=[SourceDialog(dialog_id="-100321", chat_type="channel", title="Announcements")],
        messages_by_dialog={},
        participants_by_dialog={
            "-100321": [
                SourceParticipant(
                    external_id="user-1",
                    username="alice",
                    display_name="Alice",
                ),
            ],
        },
        channel_access_profiles_by_dialog={
            "-100321": SourceChannelAccessProfile(
                dialog_id="-100321",
                is_admin=True,
                can_list_participants=True,
            ),
        },
    )
    express_gateway = FakeExpressGateway(
        user_huid_by_email={"alice@example.com": "alice-huid"},
    )
    service = TargetChatProvisioningService(
        telegram_gateway=telegram_gateway,
        express_gateway=express_gateway,
        chat_mapping_repository=InMemoryChatMappingRepository(),
        identity_directory=UsernameEmailIdentityDirectory(
            identity_mapping_repository=identity_repo,
            express_gateway=express_gateway,
        ),
        audit_repository=InMemoryAuditRepository(),
        retry_policy=AsyncRetryPolicy(
            max_attempts=1,
            base_delay_seconds=0,
            max_delay_seconds=0,
            jitter_seconds=0,
        ),
    )

    mapping = await service.ensure_target_chat(
        migration_id="migration-bot",
        dialog=ManifestDialog(
            source_chat_id="-100321",
            source_chat_type="channel",
            target_strategy="create",
            target_title="Imported Channel",
            initiator_huid="initiator-huid",
            access_strategy="invite_link",
        ),
    )

    created_chat = express_gateway.created_chat(mapping.target_chat_id)
    personal_chat = express_gateway.created_chat("personal-chat-alice-huid")
    personal_messages = express_gateway.messages_for_chat("personal-chat-alice-huid")
    assert created_chat is not None
    assert created_chat.kind == "channel"
    assert created_chat.participant_huids == ["initiator-huid"]
    assert personal_chat is not None
    assert len(personal_messages) == 1
    assert "Imported Channel" in personal_messages[0].body
    assert "https://express.example/chat/" in personal_messages[0].body


@pytest.mark.asyncio
async def test_channel_chat_falls_back_to_invite_links_when_direct_add_is_fatal():
    identity_repo = InMemoryIdentityMappingRepository()
    await identity_repo.save(
        IdentityMappingRecord(
            telegram_user_id="user-1",
            telegram_username="alice",
            telegram_display_name="Alice",
            corporate_email="alice@example.com",
        ),
    )
    telegram_gateway = FakeTelegramGateway(
        dialogs=[SourceDialog(dialog_id="-100654", chat_type="channel", title="Announcements")],
        messages_by_dialog={},
        participants_by_dialog={
            "-100654": [
                SourceParticipant(
                    external_id="user-1",
                    username="alice",
                    display_name="Alice",
                ),
            ],
        },
        channel_access_profiles_by_dialog={
            "-100654": SourceChannelAccessProfile(
                dialog_id="-100654",
                is_admin=True,
                can_list_participants=True,
            ),
        },
    )
    express_gateway = FatalEnsureMembersExpressGateway(
        user_huid_by_email={"alice@example.com": "alice-huid"},
    )
    service = TargetChatProvisioningService(
        telegram_gateway=telegram_gateway,
        express_gateway=express_gateway,
        chat_mapping_repository=InMemoryChatMappingRepository(),
        identity_directory=UsernameEmailIdentityDirectory(
            identity_mapping_repository=identity_repo,
            express_gateway=express_gateway,
        ),
        audit_repository=InMemoryAuditRepository(),
        retry_policy=AsyncRetryPolicy(
            max_attempts=1,
            base_delay_seconds=0,
            max_delay_seconds=0,
            jitter_seconds=0,
        ),
    )

    mapping = await service.ensure_target_chat(
        migration_id="migration-bot",
        dialog=ManifestDialog(
            source_chat_id="-100654",
            source_chat_type="channel",
            target_strategy="create",
            target_title="Imported Channel",
            initiator_huid="initiator-huid",
            access_strategy="direct_add",
        ),
    )

    created_chat = express_gateway.created_chat(mapping.target_chat_id)
    personal_messages = express_gateway.messages_for_chat("personal-chat-alice-huid")
    assert created_chat is not None
    assert created_chat.kind == "channel"
    assert created_chat.participant_huids == ["initiator-huid"]
    assert len(personal_messages) == 1
    assert "https://express.example/chat/" in personal_messages[0].body


@pytest.mark.asyncio
async def test_channel_chat_propagates_recoverable_direct_add_failures():
    identity_repo = InMemoryIdentityMappingRepository()
    await identity_repo.save(
        IdentityMappingRecord(
            telegram_user_id="user-1",
            telegram_username="alice",
            telegram_display_name="Alice",
            corporate_email="alice@example.com",
        ),
    )
    telegram_gateway = FakeTelegramGateway(
        dialogs=[SourceDialog(dialog_id="-100777", chat_type="channel", title="Announcements")],
        messages_by_dialog={},
        participants_by_dialog={
            "-100777": [
                SourceParticipant(
                    external_id="user-1",
                    username="alice",
                    display_name="Alice",
                ),
            ],
        },
        channel_access_profiles_by_dialog={
            "-100777": SourceChannelAccessProfile(
                dialog_id="-100777",
                is_admin=True,
                can_list_participants=True,
            ),
        },
    )
    express_gateway = RecoverableEnsureMembersExpressGateway(
        user_huid_by_email={"alice@example.com": "alice-huid"},
    )
    service = TargetChatProvisioningService(
        telegram_gateway=telegram_gateway,
        express_gateway=express_gateway,
        chat_mapping_repository=InMemoryChatMappingRepository(),
        identity_directory=UsernameEmailIdentityDirectory(
            identity_mapping_repository=identity_repo,
            express_gateway=express_gateway,
        ),
        audit_repository=InMemoryAuditRepository(),
        retry_policy=AsyncRetryPolicy(
            max_attempts=1,
            base_delay_seconds=0,
            max_delay_seconds=0,
            jitter_seconds=0,
        ),
    )

    with pytest.raises(RecoverableItemError, match="temporary eXpress failure"):
        await service.ensure_target_chat(
            migration_id="migration-bot",
            dialog=ManifestDialog(
                source_chat_id="-100777",
                source_chat_type="channel",
                target_strategy="create",
                target_title="Imported Channel",
                initiator_huid="initiator-huid",
                access_strategy="direct_add",
            ),
        )


@pytest.mark.asyncio
async def test_group_chat_routes_foreign_participant_direct_add_via_helper_cts():
    identity_repo = InMemoryIdentityMappingRepository()
    await identity_repo.save(
        IdentityMappingRecord(
            telegram_user_id="user-1",
            telegram_username="alice",
            telegram_display_name="Alice",
            corporate_email="alice@example.com",
            target_huid="alice-huid",
        ),
    )
    binding_repo = InMemoryExpressUserCtsBindingRepository()
    await binding_repo.save(
        ExpressUserCtsBindingRecord(
            target_huid="alice-huid",
            corporate_email="alice@example.com",
            cts_host="cts-helper.example.test",
        ),
    )
    shared_state: dict[str, object] = {}
    gateways: dict[str, _SharedCrossCtsGateway] = {}
    express_gateway = ExpressGatewayRouter(
        account_registry=_multi_cts_registry(),
        gateway_factory=lambda account: gateways.setdefault(
            account.cts_host,
            _SharedCrossCtsGateway(
                host=account.cts_host,
                bot_member_huid=account.effective_bot_member_huid,
                shared_state=shared_state,
            ),
        ),
    )
    telegram_gateway = FakeTelegramGateway(
        dialogs=[SourceDialog(dialog_id="-100888", chat_type="group", title="Team Chat")],
        messages_by_dialog={},
        participants_by_dialog={
            "-100888": [
                SourceParticipant(
                    external_id="user-1",
                    username="alice",
                    display_name="Alice",
                ),
            ],
        },
    )
    service = TargetChatProvisioningService(
        telegram_gateway=telegram_gateway,
        express_gateway=express_gateway,
        chat_mapping_repository=InMemoryChatMappingRepository(),
        identity_directory=UsernameEmailIdentityDirectory(
            identity_mapping_repository=identity_repo,
            express_gateway=express_gateway,
            cts_resolution_service=CtsResolutionService(
                express_gateway=express_gateway,
                binding_repository=binding_repo,
            ),
        ),
        audit_repository=InMemoryAuditRepository(),
        retry_policy=AsyncRetryPolicy(
            max_attempts=1,
            base_delay_seconds=0,
            max_delay_seconds=0,
            jitter_seconds=0,
        ),
    )

    mapping = await service.ensure_target_chat(
        migration_id="migration-bot",
        dialog=ManifestDialog(
            source_chat_id="-100888",
            source_chat_type="group",
            target_strategy="create",
            target_title="Imported Group",
            initiator_huid="initiator-huid",
            anchor_cts_host="cts-main.example.test",
            access_strategy="direct_add",
        ),
    )

    participants = shared_state["participants"][mapping.target_chat_id]
    assert participants == ["initiator-huid", "helper-bot-huid", "alice-huid"]
    assert shared_state["created_by"][mapping.target_chat_id] == "cts-main.example.test"
    assert gateways["cts-main.example.test"].member_sync_calls == [
        (mapping.target_chat_id, ("helper-bot-huid",)),
    ]
    assert gateways["cts-main.example.test"].admin_promotion_calls == [
        (mapping.target_chat_id, ("initiator-huid",)),
        (mapping.target_chat_id, ("helper-bot-huid",)),
    ]
    assert gateways["cts-helper.example.test"].member_sync_calls == [
        (mapping.target_chat_id, ("alice-huid",)),
    ]


@pytest.mark.asyncio
async def test_group_chat_uses_learned_helper_bot_huid_when_not_configured_in_env():
    identity_repo = InMemoryIdentityMappingRepository()
    await identity_repo.save(
        IdentityMappingRecord(
            telegram_user_id="user-1",
            telegram_username="alice",
            telegram_display_name="Alice",
            corporate_email="alice@example.com",
            target_huid="alice-huid",
        ),
    )
    binding_repo = InMemoryExpressUserCtsBindingRepository()
    await binding_repo.save(
        ExpressUserCtsBindingRecord(
            target_huid="alice-huid",
            corporate_email="alice@example.com",
            cts_host="cts-helper.example.test",
        ),
    )
    helper_bot_binding_repo = InMemoryExpressBotHuidBindingRepository()
    await helper_bot_binding_repo.save(
        ExpressBotHuidBindingRecord(
            bot_id="00000000-0000-0000-0000-000000000002",
            cts_host="cts-helper.example.test",
            bot_huid="learned-helper-bot-huid",
        ),
    )
    shared_state: dict[str, object] = {}
    gateways: dict[str, _SharedCrossCtsGateway] = {}
    express_gateway = ExpressGatewayRouter(
        account_registry=_multi_cts_registry(helper_bot_huid=None),
        gateway_factory=lambda account: gateways.setdefault(
            account.cts_host,
            _SharedCrossCtsGateway(
                host=account.cts_host,
                bot_member_huid=(
                    "learned-helper-bot-huid"
                    if account.cts_host == "cts-helper.example.test"
                    else account.effective_bot_member_huid
                ),
                shared_state=shared_state,
            ),
        ),
    )
    telegram_gateway = FakeTelegramGateway(
        dialogs=[SourceDialog(dialog_id="-100889", chat_type="group", title="Team Chat")],
        messages_by_dialog={},
        participants_by_dialog={
            "-100889": [
                SourceParticipant(
                    external_id="user-1",
                    username="alice",
                    display_name="Alice",
                ),
            ],
        },
    )
    service = TargetChatProvisioningService(
        telegram_gateway=telegram_gateway,
        express_gateway=express_gateway,
        chat_mapping_repository=InMemoryChatMappingRepository(),
        identity_directory=UsernameEmailIdentityDirectory(
            identity_mapping_repository=identity_repo,
            express_gateway=express_gateway,
            cts_resolution_service=CtsResolutionService(
                express_gateway=express_gateway,
                binding_repository=binding_repo,
            ),
        ),
        audit_repository=InMemoryAuditRepository(),
        retry_policy=AsyncRetryPolicy(
            max_attempts=1,
            base_delay_seconds=0,
            max_delay_seconds=0,
            jitter_seconds=0,
        ),
        express_bot_huid_binding_repository=helper_bot_binding_repo,
    )

    mapping = await service.ensure_target_chat(
        migration_id="migration-bot",
        dialog=ManifestDialog(
            source_chat_id="-100889",
            source_chat_type="group",
            target_strategy="create",
            target_title="Imported Group",
            initiator_huid="initiator-huid",
            anchor_cts_host="cts-main.example.test",
            access_strategy="direct_add",
        ),
    )

    participants = shared_state["participants"][mapping.target_chat_id]
    assert participants == ["initiator-huid", "learned-helper-bot-huid", "alice-huid"]
    assert gateways["cts-main.example.test"].member_sync_calls == [
        (mapping.target_chat_id, ("learned-helper-bot-huid",)),
    ]
    assert gateways["cts-main.example.test"].admin_promotion_calls == [
        (mapping.target_chat_id, ("initiator-huid",)),
        (mapping.target_chat_id, ("learned-helper-bot-huid",)),
    ]


@pytest.mark.asyncio
async def test_group_chat_falls_back_to_helper_bot_id_when_bot_huid_is_not_configured() -> None:
    identity_repo = InMemoryIdentityMappingRepository()
    await identity_repo.save(
        IdentityMappingRecord(
            telegram_user_id="user-1",
            telegram_username="alice",
            telegram_display_name="Alice",
            corporate_email="alice@example.com",
            target_huid="alice-huid",
        ),
    )
    binding_repo = InMemoryExpressUserCtsBindingRepository()
    await binding_repo.save(
        ExpressUserCtsBindingRecord(
            target_huid="alice-huid",
            corporate_email="alice@example.com",
            cts_host="cts-helper.example.test",
        ),
    )
    shared_state: dict[str, object] = {}
    gateways: dict[str, _SharedCrossCtsGateway] = {}
    express_gateway = ExpressGatewayRouter(
        account_registry=_multi_cts_registry(helper_bot_huid=None),
        gateway_factory=lambda account: gateways.setdefault(
            account.cts_host,
            _SharedCrossCtsGateway(
                host=account.cts_host,
                bot_member_huid=account.effective_bot_member_huid,
                shared_state=shared_state,
            ),
        ),
    )
    telegram_gateway = FakeTelegramGateway(
        dialogs=[SourceDialog(dialog_id="-100890", chat_type="group", title="Team Chat")],
        messages_by_dialog={},
        participants_by_dialog={
            "-100890": [
                SourceParticipant(
                    external_id="user-1",
                    username="alice",
                    display_name="Alice",
                ),
            ],
        },
    )
    service = TargetChatProvisioningService(
        telegram_gateway=telegram_gateway,
        express_gateway=express_gateway,
        chat_mapping_repository=InMemoryChatMappingRepository(),
        identity_directory=UsernameEmailIdentityDirectory(
            identity_mapping_repository=identity_repo,
            express_gateway=express_gateway,
            cts_resolution_service=CtsResolutionService(
                express_gateway=express_gateway,
                binding_repository=binding_repo,
            ),
        ),
        audit_repository=InMemoryAuditRepository(),
        retry_policy=AsyncRetryPolicy(
            max_attempts=1,
            base_delay_seconds=0,
            max_delay_seconds=0,
            jitter_seconds=0,
        ),
        express_bot_huid_binding_repository=InMemoryExpressBotHuidBindingRepository(),
    )

    mapping = await service.ensure_target_chat(
        migration_id="migration-bot",
        dialog=ManifestDialog(
            source_chat_id="-100890",
            source_chat_type="group",
            target_strategy="create",
            target_title="Imported Group",
            initiator_huid="initiator-huid",
            anchor_cts_host="cts-main.example.test",
            access_strategy="direct_add",
        ),
    )

    participants = shared_state["participants"][mapping.target_chat_id]
    assert participants == [
        "initiator-huid",
        "00000000-0000-0000-0000-000000000002",
        "alice-huid",
    ]
    assert gateways["cts-main.example.test"].member_sync_calls == [
        (
            mapping.target_chat_id,
            ("00000000-0000-0000-0000-000000000002",),
        ),
    ]
    assert gateways["cts-main.example.test"].admin_promotion_calls == [
        (
            mapping.target_chat_id,
            ("initiator-huid",),
        ),
        (
            mapping.target_chat_id,
            ("00000000-0000-0000-0000-000000000002",),
        ),
    ]
    assert gateways["cts-helper.example.test"].member_sync_calls == [
        (mapping.target_chat_id, ("alice-huid",)),
    ]


@pytest.mark.asyncio
async def test_channel_chat_sends_invite_via_helper_cts_when_foreign_direct_add_is_fatal():
    identity_repo = InMemoryIdentityMappingRepository()
    await identity_repo.save(
        IdentityMappingRecord(
            telegram_user_id="user-1",
            telegram_username="alice",
            telegram_display_name="Alice",
            corporate_email="alice@example.com",
            target_huid="alice-huid",
        ),
    )
    binding_repo = InMemoryExpressUserCtsBindingRepository()
    await binding_repo.save(
        ExpressUserCtsBindingRecord(
            target_huid="alice-huid",
            corporate_email="alice@example.com",
            cts_host="cts-helper.example.test",
        ),
    )
    shared_state: dict[str, object] = {}
    gateways: dict[str, _SharedCrossCtsGateway] = {}
    express_gateway = ExpressGatewayRouter(
        account_registry=_multi_cts_registry(),
        gateway_factory=lambda account: gateways.setdefault(
            account.cts_host,
            _SharedCrossCtsGateway(
                host=account.cts_host,
                bot_member_huid=account.effective_bot_member_huid,
                shared_state=shared_state,
                fatal_member_sync_hosts={"cts-helper.example.test"},
            ),
        ),
    )
    telegram_gateway = FakeTelegramGateway(
        dialogs=[SourceDialog(dialog_id="-100999", chat_type="channel", title="Announcements")],
        messages_by_dialog={},
        participants_by_dialog={
            "-100999": [
                SourceParticipant(
                    external_id="user-1",
                    username="alice",
                    display_name="Alice",
                ),
            ],
        },
        channel_access_profiles_by_dialog={
            "-100999": SourceChannelAccessProfile(
                dialog_id="-100999",
                is_admin=True,
                can_list_participants=True,
            ),
        },
    )
    service = TargetChatProvisioningService(
        telegram_gateway=telegram_gateway,
        express_gateway=express_gateway,
        chat_mapping_repository=InMemoryChatMappingRepository(),
        identity_directory=UsernameEmailIdentityDirectory(
            identity_mapping_repository=identity_repo,
            express_gateway=express_gateway,
            cts_resolution_service=CtsResolutionService(
                express_gateway=express_gateway,
                binding_repository=binding_repo,
            ),
        ),
        audit_repository=InMemoryAuditRepository(),
        retry_policy=AsyncRetryPolicy(
            max_attempts=1,
            base_delay_seconds=0,
            max_delay_seconds=0,
            jitter_seconds=0,
        ),
    )

    mapping = await service.ensure_target_chat(
        migration_id="migration-bot",
        dialog=ManifestDialog(
            source_chat_id="-100999",
            source_chat_type="channel",
            target_strategy="create",
            target_title="Imported Channel",
            initiator_huid="initiator-huid",
            anchor_cts_host="cts-main.example.test",
            access_strategy="direct_add",
        ),
    )

    participants = shared_state["participants"][mapping.target_chat_id]
    assert participants == ["initiator-huid", "helper-bot-huid"]
    assert gateways["cts-main.example.test"].member_sync_calls == [
        (mapping.target_chat_id, ("helper-bot-huid",)),
    ]
    assert gateways["cts-main.example.test"].admin_promotion_calls == [
        (mapping.target_chat_id, ("initiator-huid",)),
        (mapping.target_chat_id, ("helper-bot-huid",)),
    ]
    assert gateways["cts-helper.example.test"].member_sync_calls == [
        (mapping.target_chat_id, ("alice-huid",)),
    ]
    assert gateways["cts-helper.example.test"].personal_chat_calls == ["alice-huid"]
    assert len(gateways["cts-helper.example.test"].sent_messages) == 1
    _, body, _ = gateways["cts-helper.example.test"].sent_messages[0]
    assert "Imported Channel" in body
    assert "https://express.example/chat/" in body
