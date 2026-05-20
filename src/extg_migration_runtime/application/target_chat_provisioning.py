from __future__ import annotations

from contextlib import AbstractContextManager, nullcontext
from dataclasses import dataclass, replace
from datetime import UTC, datetime

from extg_migration_runtime.application.identity import UsernameEmailIdentityDirectory
from extg_shared.contracts.ports import (
    AuditRepository,
    ChatMappingRepository,
    ExpressBotHuidBindingRepository,
    ExpressGateway,
    TelegramGateway,
)
from extg_shared.utils.retry import AsyncRetryPolicy
from extg_shared.contracts.errors import (
    AmbiguousDeliveryError,
    FatalItemError,
    RecoverableItemError,
)
from extg_shared.contracts.manifest import ManifestDialog
from extg_shared.contracts.models import (
    AuditEvent,
    AuditSeverity,
    ChatMappingRecord,
    SourceChannelAccessProfile,
)
from extg_shared.utils import (
    get_current_express_cts_host,
    normalize_express_cts_host,
    use_express_cts_host,
)


@dataclass(frozen=True, slots=True)
class ParticipantResolutionPreview:
    resolved_huids: list[str]
    unresolved_usernames: list[str]
    unresolved_display_names: list[str]
    non_self_participants_count: int


@dataclass(frozen=True, slots=True)
class ParticipantMatrixRow:
    telegram_user_id: str | None
    telegram_username: str | None
    telegram_display_name: str
    is_self: bool
    corporate_email: str | None
    target_huid: str | None
    resolution_source: str
    reason: str | None = None


@dataclass(frozen=True, slots=True)
class ResolvedParticipantTarget:
    target_huid: str
    cts_host: str | None = None


@dataclass(frozen=True, slots=True)
class ParticipantAccessFailure:
    target_huid: str
    cts_host: str | None = None
    reason: str = "direct_add_failed"
    error: str | None = None


@dataclass(frozen=True, slots=True)
class ParticipantAccessApplyResult:
    added_huids: tuple[str, ...] = ()
    invited_huids: tuple[str, ...] = ()
    failed_targets: tuple[ParticipantAccessFailure, ...] = ()

    @property
    def changed(self) -> bool:
        return bool(self.added_huids or self.invited_huids)


@dataclass(frozen=True, slots=True)
class _ProvisionedTargetChat:
    target_chat_id: str
    member_success_count: int | None = None
    member_total_count: int | None = None


class TargetChatProvisioningService:
    def __init__(
        self,
        *,
        telegram_gateway: TelegramGateway,
        express_gateway: ExpressGateway,
        chat_mapping_repository: ChatMappingRepository,
        identity_directory: UsernameEmailIdentityDirectory,
        audit_repository: AuditRepository,
        retry_policy: AsyncRetryPolicy,
        express_bot_huid_binding_repository: ExpressBotHuidBindingRepository | None = None,
        logger: object | None = None,
    ) -> None:
        self._telegram_gateway = telegram_gateway
        self._express_gateway = express_gateway
        self._chat_mapping_repository = chat_mapping_repository
        self._express_bot_huid_binding_repository = express_bot_huid_binding_repository
        self._identity_directory = identity_directory
        self._audit_repository = audit_repository
        self._retry_policy = retry_policy
        self._logger = logger

    async def get_existing_target_chat(
        self,
        *,
        migration_id: str,
        source_chat_id: str,
    ) -> ChatMappingRecord | None:
        return await self._chat_mapping_repository.get(migration_id, source_chat_id)

    async def get_channel_access_profile(
        self,
        *,
        source_chat_id: str,
        source_backend: str,
    ) -> SourceChannelAccessProfile:
        return await self._telegram_gateway.get_channel_access_profile(
            source_chat_id,
            source_backend=source_backend,
        )

    async def add_resolved_members_to_existing_chat(
        self,
        *,
        migration_id: str,
        source_chat_id: str,
        source_chat_type: str,
        source_backend: str,
        target_chat_id: str,
        target_chat_title: str,
        participant_targets: list[ResolvedParticipantTarget],
        initiator_huid: str | None,
        access_strategy: str,
        anchor_cts_host: str | None,
    ) -> ParticipantAccessApplyResult:
        chat_kind = "channel" if source_chat_type == "channel" else "group"
        return await self._apply_member_access_strategy(
            chat_kind=chat_kind,
            migration_id=migration_id,
            source_chat_id=source_chat_id,
            source_backend=source_backend,
            target_chat_id=target_chat_id,
            target_chat_title=target_chat_title,
            participant_targets=participant_targets,
            initiator_huid=initiator_huid,
            access_strategy=access_strategy,
            anchor_cts_host=anchor_cts_host,
        )

    async def ensure_target_chat(
        self,
        *,
        migration_id: str,
        dialog: ManifestDialog,
    ) -> ChatMappingRecord:
        existing = await self._chat_mapping_repository.get(
            migration_id,
            dialog.source_chat_id,
        )
        if existing:
            if dialog.source_chat_type == "channel" and dialog.target_strategy == "create":
                existing = await self._sync_channel_chat_access(
                    migration_id=migration_id,
                    dialog=dialog,
                    existing=existing,
                )
            if dialog.source_chat_type in {"group", "supergroup"} and dialog.target_strategy == "create":
                existing = await self._sync_group_chat_members(
                    migration_id=migration_id,
                    dialog=dialog,
                    existing=existing,
                )
            return existing
        if dialog.target_strategy == "manual_review_required":
            raise FatalItemError(
                f"source_chat_id={dialog.source_chat_id} requires manual review before migration",
            )
        if dialog.target_strategy == "bind":
            if not dialog.target_chat_id:
                raise FatalItemError(
                    f"source_chat_id={dialog.source_chat_id} requires target_chat_id for bind strategy",
                )
            target_chat_id = dialog.target_chat_id
            member_success_count = None
            member_total_count = None
            status = "bound"
        elif dialog.target_strategy == "create":
            target_chat_title = dialog.target_title or dialog.source_chat_id
            if dialog.source_chat_type == "private":
                provisioned = await self._provision_private_chat(
                    target_chat_title=target_chat_title,
                    migration_id=migration_id,
                    source_chat_id=dialog.source_chat_id,
                    source_backend=dialog.source_backend,
                    initiator_huid=dialog.initiator_huid,
                    access_strategy=dialog.access_strategy or "direct_add",
                    anchor_cts_host=dialog.anchor_cts_host,
                )
            elif dialog.source_chat_type == "channel":
                provisioned = await self._provision_channel_chat(
                    target_chat_title=target_chat_title,
                    migration_id=migration_id,
                    source_chat_id=dialog.source_chat_id,
                    source_backend=dialog.source_backend,
                    initiator_huid=dialog.initiator_huid,
                    access_strategy=dialog.access_strategy or "direct_add",
                    anchor_cts_host=dialog.anchor_cts_host,
                )
            else:
                provisioned = await self._provision_group_chat(
                    target_chat_title=target_chat_title,
                    migration_id=migration_id,
                    source_chat_id=dialog.source_chat_id,
                    participant_source_chat_id=dialog.physical_source_chat_id,
                    source_chat_type=dialog.source_chat_type,
                    source_backend=dialog.source_backend,
                    initiator_huid=dialog.initiator_huid,
                    access_strategy=dialog.access_strategy or "direct_add",
                    anchor_cts_host=dialog.anchor_cts_host,
                )
            target_chat_id = provisioned.target_chat_id
            member_success_count = provisioned.member_success_count
            member_total_count = provisioned.member_total_count
            status = "active"
        else:
            raise FatalItemError(
                f"unsupported target_strategy={dialog.target_strategy!r} for source_chat_id={dialog.source_chat_id}",
            )
        record = ChatMappingRecord(
            migration_id=migration_id,
            source_chat_id=dialog.source_chat_id,
            source_chat_type=dialog.source_chat_type,
            source_chat_title=(
                dialog.source_thread_title
                or dialog.target_title
                or dialog.source_chat_id
            ),
            target_chat_id=target_chat_id,
            target_chat_title=(
                dialog.target_title
                or dialog.source_thread_title
                or dialog.source_chat_id
            ),
            status=status,
            created_at=self._now(),
            updated_at=self._now(),
            anchor_cts_host=dialog.anchor_cts_host,
            anchor_bot_id=dialog.anchor_bot_id,
            member_success_count=member_success_count,
            member_total_count=member_total_count,
        )
        await self._chat_mapping_repository.save(record)
        return record

    async def validate_dialog_preconditions(
        self,
        *,
        migration_id: str,
        dialog: ManifestDialog,
    ) -> None:
        existing = await self._chat_mapping_repository.get(
            migration_id,
            dialog.source_chat_id,
        )
        if existing is not None:
            return
        if dialog.target_strategy == "manual_review_required":
            raise FatalItemError(
                f"source_chat_id={dialog.source_chat_id} requires manual review before migration",
            )
        if dialog.target_strategy == "bind":
            if not dialog.target_chat_id:
                raise FatalItemError(
                    f"source_chat_id={dialog.source_chat_id} requires target_chat_id for bind strategy",
                )
            if dialog.source_chat_type == "channel":
                await self._validate_channel_preconditions(
                    migration_id=migration_id,
                    dialog=dialog,
                )
            return
        if dialog.target_strategy != "create":
            raise FatalItemError(
                f"unsupported target_strategy={dialog.target_strategy!r} for source_chat_id={dialog.source_chat_id}",
            )
        if dialog.source_chat_type == "channel":
            await self._validate_channel_preconditions(
                migration_id=migration_id,
                dialog=dialog,
            )
            return
        if dialog.source_chat_type == "private":
            if not (dialog.initiator_huid or "").strip():
                raise FatalItemError(
                    "private chat migration requires initiator_huid from the bot request context; "
                    f"source_chat_id={dialog.source_chat_id}",
                )
            return
        if dialog.source_chat_type != "private":
            try:
                await self._preview_participant_resolution(
                    source_chat_id=dialog.source_chat_id,
                    participant_source_chat_id=dialog.physical_source_chat_id,
                    source_chat_type=dialog.source_chat_type,
                    source_backend=dialog.source_backend,
                    include_self=False,
                )
            except FatalItemError as error:
                await self._audit_repository.add(
                    AuditEvent(
                        migration_id=migration_id,
                        source_chat_id=dialog.source_chat_id,
                        event_type="target_chat_participant_preview_degraded",
                        severity=AuditSeverity.WARNING,
                        payload_json={
                            "source_chat_type": dialog.source_chat_type,
                            "error": str(error),
                        },
                        created_at=self._now(),
                    ),
                )
            return

    async def list_participant_matrix(
        self,
        *,
        source_chat_id: str,
        participant_source_chat_id: str | None = None,
        source_backend: str = "telethon_user_session",
    ) -> list[ParticipantMatrixRow]:
        participants = await self._telegram_gateway.list_participants(
            participant_source_chat_id or source_chat_id,
            source_backend=source_backend,
        )
        rows: list[ParticipantMatrixRow] = []
        for participant in participants:
            lookup = await self._identity_directory.resolve_telegram_identity(
                telegram_user_id=participant.external_id,
                telegram_username=participant.username,
                telegram_display_name=participant.display_name,
            )
            rows.append(
                ParticipantMatrixRow(
                    telegram_user_id=lookup.telegram_user_id or participant.external_id,
                    telegram_username=lookup.telegram_username or (
                        self._identity_directory.normalize_username(participant.username)
                        if participant.username
                        else None
                    ),
                    telegram_display_name=participant.display_name,
                    is_self=participant.is_self,
                    corporate_email=lookup.corporate_email,
                    target_huid=lookup.target_huid,
                    resolution_source=lookup.resolution_source,
                    reason=lookup.reason,
                ),
            )
        rows.sort(
            key=lambda item: (
                item.is_self,
                item.telegram_display_name.casefold(),
                item.telegram_user_id or "",
            ),
        )
        return rows

    async def _validate_channel_preconditions(
        self,
        *,
        migration_id: str,
        dialog: ManifestDialog,
    ) -> None:
        self._require_channel_initiator_huid(
            source_chat_id=dialog.source_chat_id,
            initiator_huid=dialog.initiator_huid,
        )
        profile = await self.get_channel_access_profile(
            source_chat_id=dialog.source_chat_id,
            source_backend=dialog.source_backend,
        )
        if profile.is_admin:
            if not profile.can_list_participants:
                await self._audit_repository.add(
                    AuditEvent(
                        migration_id=migration_id,
                        source_chat_id=dialog.source_chat_id,
                        event_type="channel_participant_preview_degraded",
                        severity=AuditSeverity.WARNING,
                        payload_json={
                            "source_chat_type": dialog.source_chat_type,
                            "source_backend": dialog.source_backend,
                            "reason": "participant_listing_not_available",
                        },
                        created_at=self._now(),
                    ),
                )
                return
            try:
                await self._preview_participant_resolution(
                    source_chat_id=dialog.source_chat_id,
                    participant_source_chat_id=dialog.physical_source_chat_id,
                    source_chat_type=dialog.source_chat_type,
                    source_backend=dialog.source_backend,
                    include_self=False,
                )
            except FatalItemError as error:
                await self._audit_repository.add(
                    AuditEvent(
                        migration_id=migration_id,
                        source_chat_id=dialog.source_chat_id,
                        event_type="channel_participant_preview_degraded",
                        severity=AuditSeverity.WARNING,
                        payload_json={
                            "source_chat_type": dialog.source_chat_type,
                            "source_backend": dialog.source_backend,
                            "reason": "participant_preview_failed",
                            "error": str(error),
                        },
                        created_at=self._now(),
                    ),
                )
            return
        await self._audit_repository.add(
            AuditEvent(
                migration_id=migration_id,
                source_chat_id=dialog.source_chat_id,
                event_type="channel_admin_preflight_degraded",
                severity=AuditSeverity.WARNING,
                payload_json={
                    "source_chat_type": dialog.source_chat_type,
                    "source_backend": dialog.source_backend,
                    "can_list_participants": profile.can_list_participants,
                    "reason": "connected_session_is_not_channel_admin",
                },
                created_at=self._now(),
            ),
        )
        return

    async def _provision_private_chat(
        self,
        *,
        target_chat_title: str,
        migration_id: str,
        source_chat_id: str,
        source_backend: str,
        initiator_huid: str | None,
        access_strategy: str,
        anchor_cts_host: str | None,
    ) -> _ProvisionedTargetChat:
        normalized_initiator_huid = (initiator_huid or "").strip()
        if not normalized_initiator_huid:
            raise FatalItemError(
                "private chat migration requires initiator_huid from the bot request context; "
                f"source_chat_id={source_chat_id}",
            )
        participant_targets = []
        resolution_mode = "partial"
        try:
            participant_targets = await self._resolve_participant_targets(
                migration_id=migration_id,
                source_chat_id=source_chat_id,
                participant_source_chat_id=source_chat_id,
                source_chat_type="private",
                source_backend=source_backend,
                include_self=False,
            )
        except FatalItemError as error:
            await self._audit_repository.add(
                AuditEvent(
                    migration_id=migration_id,
                    event_type="private_chat_peer_resolution_degraded",
                    source_chat_id=source_chat_id,
                    severity=AuditSeverity.WARNING,
                    payload_json={"error": str(error)},
                    created_at=self._now(),
                ),
            )
        if participant_targets:
            resolution_mode = "full"
        seed_participants = self._seed_participant_huids(
            participant_targets=participant_targets,
            initiator_huid=normalized_initiator_huid,
            access_strategy=access_strategy,
            anchor_cts_host=anchor_cts_host,
        )
        await self._audit_repository.add(
            AuditEvent(
                migration_id=migration_id,
                event_type="private_chat_participant_set_prepared",
                source_chat_id=source_chat_id,
                severity=AuditSeverity.INFO,
                payload_json={
                    "mode": resolution_mode,
                    "participant_huids": seed_participants,
                    "resolved_targets": [
                        {
                            "target_huid": target.target_huid,
                            "cts_host": target.cts_host,
                        }
                        for target in participant_targets
                    ],
                },
                created_at=self._now(),
            ),
        )
        target_chat_id = await self._retry_policy.run(
            lambda: self._express_gateway.create_chat(
                target_chat_title,
                participant_huids=seed_participants,
                chat_type="GROUP_CHAT",
            ),
        )
        await self._ensure_initiator_chat_admin(
            chat_kind="private",
            migration_id=migration_id,
            source_chat_id=source_chat_id,
            target_chat_id=target_chat_id,
            initiator_huid=initiator_huid,
            anchor_cts_host=anchor_cts_host,
        )
        access_result = await self._apply_member_access_strategy(
            chat_kind="private",
            migration_id=migration_id,
            source_chat_id=source_chat_id,
            source_backend=source_backend,
            target_chat_id=target_chat_id,
            target_chat_title=target_chat_title,
            participant_targets=participant_targets,
            initiator_huid=initiator_huid,
            access_strategy=access_strategy,
            anchor_cts_host=anchor_cts_host,
            skip_huids=set(seed_participants),
        )
        member_success_count, member_total_count = self._member_sync_counts(
            participant_targets=participant_targets,
            access_strategy=access_strategy,
            access_result=access_result,
        )
        return _ProvisionedTargetChat(
            target_chat_id=target_chat_id,
            member_success_count=member_success_count,
            member_total_count=member_total_count,
        )

    async def _provision_group_chat(
        self,
        *,
        target_chat_title: str,
        migration_id: str,
        source_chat_id: str,
        participant_source_chat_id: str | None,
        source_chat_type: str,
        source_backend: str,
        initiator_huid: str | None,
        access_strategy: str,
        anchor_cts_host: str | None,
    ) -> _ProvisionedTargetChat:
        participant_targets = await self._prepare_group_participant_targets(
            migration_id=migration_id,
            source_chat_id=source_chat_id,
            participant_source_chat_id=participant_source_chat_id,
            source_chat_type=source_chat_type,
            source_backend=source_backend,
            initiator_huid=initiator_huid,
        )
        seed_participants = self._seed_participant_huids(
            participant_targets=participant_targets,
            initiator_huid=initiator_huid,
            access_strategy=access_strategy,
            anchor_cts_host=anchor_cts_host,
        )
        target_chat_id = await self._retry_policy.run(
            lambda: self._express_gateway.create_chat(
                target_chat_title,
                participant_huids=seed_participants or None,
                chat_type="GROUP_CHAT",
            ),
        )
        await self._ensure_initiator_chat_admin(
            chat_kind="group",
            migration_id=migration_id,
            source_chat_id=source_chat_id,
            target_chat_id=target_chat_id,
            initiator_huid=initiator_huid,
            anchor_cts_host=anchor_cts_host,
        )
        access_result = await self._apply_member_access_strategy(
            chat_kind="group",
            migration_id=migration_id,
            source_chat_id=source_chat_id,
            source_backend=source_backend,
            target_chat_id=target_chat_id,
            target_chat_title=target_chat_title,
            participant_targets=participant_targets,
            initiator_huid=initiator_huid,
            access_strategy=access_strategy,
            anchor_cts_host=anchor_cts_host,
            skip_huids=set(seed_participants),
        )
        member_success_count, member_total_count = self._member_sync_counts(
            participant_targets=participant_targets,
            access_strategy=access_strategy,
            access_result=access_result,
        )
        return _ProvisionedTargetChat(
            target_chat_id=target_chat_id,
            member_success_count=member_success_count,
            member_total_count=member_total_count,
        )

    async def _sync_group_chat_members(
        self,
        *,
        migration_id: str,
        dialog: ManifestDialog,
        existing: ChatMappingRecord,
    ) -> ChatMappingRecord:
        participant_targets = await self._prepare_group_participant_targets(
            migration_id=migration_id,
            source_chat_id=dialog.source_chat_id,
            participant_source_chat_id=dialog.physical_source_chat_id,
            source_chat_type=dialog.source_chat_type,
            source_backend=dialog.source_backend,
            initiator_huid=dialog.initiator_huid,
        )
        access_result = await self._apply_member_access_strategy(
            chat_kind="group",
            migration_id=migration_id,
            source_chat_id=dialog.source_chat_id,
            source_backend=dialog.source_backend,
            target_chat_id=existing.target_chat_id,
            target_chat_title=existing.target_chat_title,
            participant_targets=participant_targets,
            initiator_huid=dialog.initiator_huid,
            access_strategy=dialog.access_strategy or "direct_add",
            anchor_cts_host=existing.anchor_cts_host or dialog.anchor_cts_host,
        )
        member_success_count, member_total_count = self._member_sync_counts(
            participant_targets=participant_targets,
            access_strategy=dialog.access_strategy or "direct_add",
            access_result=access_result,
        )
        if (
            not access_result.changed
            and existing.member_success_count == member_success_count
            and existing.member_total_count == member_total_count
        ):
            return existing
        updated = replace(
            existing,
            updated_at=self._now(),
            member_success_count=member_success_count,
            member_total_count=member_total_count,
        )
        await self._chat_mapping_repository.save(updated)
        return updated

    async def _sync_channel_chat_access(
        self,
        *,
        migration_id: str,
        dialog: ManifestDialog,
        existing: ChatMappingRecord,
    ) -> ChatMappingRecord:
        access_strategy = (dialog.access_strategy or "direct_add").strip().lower()
        participant_targets = await self._prepare_channel_participant_targets(
            migration_id=migration_id,
            source_chat_id=dialog.source_chat_id,
            source_backend=dialog.source_backend,
            initiator_huid=dialog.initiator_huid,
        )
        access_result = await self._apply_channel_access_strategy(
            migration_id=migration_id,
            source_chat_id=dialog.source_chat_id,
            source_backend=dialog.source_backend,
            target_chat_id=existing.target_chat_id,
            target_chat_title=existing.target_chat_title,
            participant_targets=participant_targets,
            initiator_huid=dialog.initiator_huid,
            access_strategy=access_strategy,
            anchor_cts_host=existing.anchor_cts_host or dialog.anchor_cts_host,
        )
        member_success_count, member_total_count = self._member_sync_counts(
            participant_targets=participant_targets,
            access_strategy=access_strategy,
            access_result=access_result,
        )
        if (
            not access_result.changed
            and existing.member_success_count == member_success_count
            and existing.member_total_count == member_total_count
        ):
            return existing
        updated = replace(
            existing,
            updated_at=self._now(),
            member_success_count=member_success_count,
            member_total_count=member_total_count,
        )
        await self._chat_mapping_repository.save(updated)
        return updated

    async def _prepare_group_participant_targets(
        self,
        *,
        migration_id: str,
        source_chat_id: str,
        participant_source_chat_id: str | None,
        source_chat_type: str,
        source_backend: str,
        initiator_huid: str | None,
    ) -> list[ResolvedParticipantTarget]:
        normalized_initiator_huid = (initiator_huid or "").strip()
        participant_targets = await self._resolve_participant_targets(
            migration_id=migration_id,
            source_chat_id=source_chat_id,
            participant_source_chat_id=participant_source_chat_id,
            source_chat_type=source_chat_type,
            source_backend=source_backend,
            include_self=False,
            degrade_on_listing_error=True,
        )
        await self._audit_repository.add(
            AuditEvent(
                migration_id=migration_id,
                source_chat_id=source_chat_id,
                event_type="group_chat_participant_set_prepared",
                severity=AuditSeverity.INFO,
                payload_json={
                    "initiator_huid": normalized_initiator_huid or None,
                    "participant_huids": [
                        target.target_huid for target in participant_targets
                    ],
                    "participant_targets": [
                        {
                            "target_huid": target.target_huid,
                            "cts_host": target.cts_host,
                        }
                        for target in participant_targets
                    ],
                    "resolved_count": len(participant_targets),
                },
                created_at=self._now(),
            ),
        )
        return participant_targets

    async def _prepare_channel_participant_targets(
        self,
        *,
        migration_id: str,
        source_chat_id: str,
        source_backend: str,
        initiator_huid: str | None,
    ) -> list[ResolvedParticipantTarget]:
        normalized_initiator_huid = (initiator_huid or "").strip()
        participant_targets = await self._resolve_participant_targets(
            migration_id=migration_id,
            source_chat_id=source_chat_id,
            participant_source_chat_id=source_chat_id,
            source_chat_type="channel",
            source_backend=source_backend,
            include_self=False,
            degrade_on_listing_error=True,
        )
        await self._audit_repository.add(
            AuditEvent(
                migration_id=migration_id,
                source_chat_id=source_chat_id,
                event_type="channel_participant_set_prepared",
                severity=AuditSeverity.INFO,
                payload_json={
                    "initiator_huid": normalized_initiator_huid or None,
                    "participant_huids": [
                        target.target_huid for target in participant_targets
                    ],
                    "participant_targets": [
                        {
                            "target_huid": target.target_huid,
                            "cts_host": target.cts_host,
                        }
                        for target in participant_targets
                    ],
                    "resolved_count": len(participant_targets),
                },
                created_at=self._now(),
            ),
        )
        return participant_targets

    async def _provision_channel_chat(
        self,
        *,
        target_chat_title: str,
        migration_id: str,
        source_chat_id: str,
        source_backend: str,
        initiator_huid: str | None,
        access_strategy: str,
        anchor_cts_host: str | None,
    ) -> _ProvisionedTargetChat:
        normalized_initiator_huid = self._require_channel_initiator_huid(
            source_chat_id=source_chat_id,
            initiator_huid=initiator_huid,
        )
        participant_targets = await self._prepare_channel_participant_targets(
            migration_id=migration_id,
            source_chat_id=source_chat_id,
            source_backend=source_backend,
            initiator_huid=initiator_huid,
        )
        seed_participants = [normalized_initiator_huid]
        target_chat_id = await self._retry_policy.run(
            lambda: self._express_gateway.create_chat(
                target_chat_title,
                participant_huids=seed_participants,
                chat_type="CHANNEL",
            ),
        )
        await self._audit_repository.add(
            AuditEvent(
                migration_id=migration_id,
                source_chat_id=source_chat_id,
                event_type="channel_target_chat_created",
                severity=AuditSeverity.INFO,
                payload_json={
                    "target_chat_id": target_chat_id,
                    "seed_participants": seed_participants,
                    "access_strategy": access_strategy,
                },
                created_at=self._now(),
            ),
        )
        await self._ensure_initiator_chat_admin(
            chat_kind="channel",
            migration_id=migration_id,
            source_chat_id=source_chat_id,
            target_chat_id=target_chat_id,
            initiator_huid=initiator_huid,
            anchor_cts_host=anchor_cts_host,
        )
        access_result = await self._apply_channel_access_strategy(
            migration_id=migration_id,
            source_chat_id=source_chat_id,
            source_backend=source_backend,
            target_chat_id=target_chat_id,
            target_chat_title=target_chat_title,
            participant_targets=participant_targets,
            initiator_huid=initiator_huid,
            access_strategy=access_strategy,
            anchor_cts_host=anchor_cts_host,
            skip_huids=set(seed_participants),
        )
        member_success_count, member_total_count = self._member_sync_counts(
            participant_targets=participant_targets,
            access_strategy=access_strategy,
            access_result=access_result,
        )
        return _ProvisionedTargetChat(
            target_chat_id=target_chat_id,
            member_success_count=member_success_count,
            member_total_count=member_total_count,
        )

    async def _apply_channel_access_strategy(
        self,
        *,
        migration_id: str,
        source_chat_id: str,
        source_backend: str,
        target_chat_id: str,
        target_chat_title: str,
        participant_targets: list[ResolvedParticipantTarget],
        initiator_huid: str | None,
        access_strategy: str,
        anchor_cts_host: str | None,
        skip_huids: set[str] | None = None,
    ) -> ParticipantAccessApplyResult:
        return await self._apply_member_access_strategy(
            chat_kind="channel",
            migration_id=migration_id,
            source_chat_id=source_chat_id,
            source_backend=source_backend,
            target_chat_id=target_chat_id,
            target_chat_title=target_chat_title,
            participant_targets=participant_targets,
            initiator_huid=initiator_huid,
            access_strategy=access_strategy,
            anchor_cts_host=anchor_cts_host,
            skip_huids=skip_huids,
        )

    def _member_sync_counts(
        self,
        *,
        participant_targets: list[ResolvedParticipantTarget],
        access_strategy: str,
        access_result: ParticipantAccessApplyResult,
    ) -> tuple[int | None, int | None]:
        normalized_strategy = (access_strategy or "direct_add").strip().lower()
        if normalized_strategy == "none" or not participant_targets:
            return None, None
        total_count = len(participant_targets)
        success_count = max(total_count - len(access_result.failed_targets), 0)
        return success_count, total_count

    async def _resolve_participant_targets(
        self,
        *,
        migration_id: str,
        source_chat_id: str,
        participant_source_chat_id: str | None,
        source_chat_type: str,
        source_backend: str,
        include_self: bool = False,
        degrade_on_listing_error: bool = False,
    ) -> list[ResolvedParticipantTarget]:
        try:
            participants = await self._telegram_gateway.list_participants(
                participant_source_chat_id or source_chat_id,
                source_backend=source_backend,
            )
        except FatalItemError as error:
            if degrade_on_listing_error:
                await self._audit_repository.add(
                    AuditEvent(
                        migration_id=migration_id,
                        source_chat_id=source_chat_id,
                        event_type="participant_listing_degraded",
                        severity=AuditSeverity.WARNING,
                        payload_json={
                            "source_chat_type": source_chat_type,
                            "error": str(error),
                        },
                        created_at=self._now(),
                    ),
                )
                return []
            raise FatalItemError(
                self._participant_listing_error(
                    source_chat_id=source_chat_id,
                    source_chat_type=source_chat_type,
                    error=error,
                ),
            ) from error
        resolved_targets: list[ResolvedParticipantTarget] = []
        seen: set[str] = set()
        for participant in participants:
            if participant.is_self and not include_self:
                continue
            lookup = await self._identity_directory.resolve_telegram_identity(
                telegram_user_id=participant.external_id,
                telegram_username=participant.username,
                telegram_display_name=participant.display_name,
            )
            if lookup.target_huid is None:
                await self._audit_repository.add(
                    AuditEvent(
                        migration_id=migration_id,
                        event_type="participant_skipped_unresolved_identity",
                        source_chat_id=source_chat_id,
                        severity=AuditSeverity.WARNING,
                        payload_json={
                            "participant_external_id": participant.external_id,
                            "participant_username": participant.username,
                            "participant_display_name": participant.display_name,
                            "reason": lookup.reason,
                            "corporate_email": lookup.corporate_email,
                            "is_self": participant.is_self,
                        },
                        created_at=self._now(),
                    ),
                )
                continue
            if lookup.target_huid in seen:
                continue
            seen.add(lookup.target_huid)
            resolved_targets.append(
                ResolvedParticipantTarget(
                    target_huid=lookup.target_huid,
                    cts_host=normalize_express_cts_host(lookup.cts_host),
                ),
            )

        await self._audit_repository.add(
            AuditEvent(
                migration_id=migration_id,
                event_type="target_chat_participants_resolved",
                source_chat_id=source_chat_id,
                severity=AuditSeverity.INFO,
                payload_json={
                    "resolved_participants_count": len(resolved_targets),
                    "resolved_participant_huids": [
                        target.target_huid for target in resolved_targets
                    ],
                    "resolved_participant_targets": [
                        {
                            "target_huid": target.target_huid,
                            "cts_host": target.cts_host,
                        }
                        for target in resolved_targets
                    ],
                },
                created_at=self._now(),
            ),
        )
        return resolved_targets

    async def _resolve_participant_huids(
        self,
        *,
        migration_id: str,
        source_chat_id: str,
        participant_source_chat_id: str | None,
        source_chat_type: str,
        source_backend: str,
        include_self: bool = False,
        degrade_on_listing_error: bool = False,
    ) -> list[str]:
        resolved_targets = await self._resolve_participant_targets(
            migration_id=migration_id,
            source_chat_id=source_chat_id,
            participant_source_chat_id=participant_source_chat_id,
            source_chat_type=source_chat_type,
            source_backend=source_backend,
            include_self=include_self,
            degrade_on_listing_error=degrade_on_listing_error,
        )
        return [target.target_huid for target in resolved_targets]

    async def _preview_participant_resolution(
        self,
        *,
        source_chat_id: str,
        participant_source_chat_id: str | None,
        source_chat_type: str,
        source_backend: str,
        include_self: bool,
    ) -> ParticipantResolutionPreview:
        try:
            participants = await self._telegram_gateway.list_participants(
                participant_source_chat_id or source_chat_id,
                source_backend=source_backend,
            )
        except FatalItemError as error:
            raise FatalItemError(
                self._participant_listing_error(
                    source_chat_id=source_chat_id,
                    source_chat_type=source_chat_type,
                    error=error,
                ),
            ) from error
        resolved_huids: list[str] = []
        unresolved_usernames: list[str] = []
        unresolved_display_names: list[str] = []
        seen: set[str] = set()
        non_self_participants_count = 0

        for participant in participants:
            if participant.is_self and not include_self:
                continue
            non_self_participants_count += 1
            lookup = await self._identity_directory.resolve_telegram_identity(
                telegram_user_id=participant.external_id,
                telegram_username=participant.username,
                telegram_display_name=participant.display_name,
            )
            if lookup.target_huid is None:
                if participant.username:
                    unresolved_usernames.append(
                        self._identity_directory.normalize_username(participant.username)
                        or participant.username,
                    )
                else:
                    unresolved_display_names.append(participant.display_name)
                continue
            if lookup.target_huid in seen:
                continue
            seen.add(lookup.target_huid)
            resolved_huids.append(lookup.target_huid)

        return ParticipantResolutionPreview(
            resolved_huids=resolved_huids,
            unresolved_usernames=unresolved_usernames,
            unresolved_display_names=unresolved_display_names,
            non_self_participants_count=non_self_participants_count,
        )

    def _private_chat_preflight_error(
        self,
        *,
        source_chat_id: str,
        preview: ParticipantResolutionPreview,
    ) -> str:
        details: list[str] = []
        if preview.unresolved_usernames:
            usernames = ", ".join(f"@{username}" for username in preview.unresolved_usernames[:5])
            details.append(f"unresolved_usernames={usernames}")
        if preview.unresolved_display_names:
            display_names = ", ".join(preview.unresolved_display_names[:5])
            details.append(f"participants_without_username={display_names}")
        if preview.non_self_participants_count == 0:
            details.append("no non-self participants were discovered in Telegram")

        fix_hint = (
            "Inspect members via /chat_users "
            f"{source_chat_id} and add identity mapping via "
            "/map_identity tg_id=<telegram_user_id> email=<corporate_email> "
            f"or run /migrate {source_chat_id} target=bind target_chat_id=<express_chat_id>"
        )
        details_suffix = f"; {'; '.join(details)}" if details else ""
        return (
            "private chat migration requires at least one resolved target participant; "
            f"source_chat_id={source_chat_id}{details_suffix}; {fix_hint}"
        )

    def _participant_listing_error(
        self,
        *,
        source_chat_id: str,
        source_chat_type: str,
        error: Exception,
    ) -> str:
        return (
            f"cannot enumerate Telegram participants for source_chat_id={source_chat_id} "
            f"(type={source_chat_type}): {error}; automatic participant migration requires "
            "access to the Telegram member list. Re-run with an account that can list participants "
            f"or use /migrate {source_chat_id} target=bind target_chat_id=<express_chat_id>"
        )

    async def _apply_member_access_strategy(
        self,
        *,
        chat_kind: str,
        migration_id: str,
        source_chat_id: str,
        source_backend: str,
        target_chat_id: str,
        target_chat_title: str,
        participant_targets: list[ResolvedParticipantTarget],
        initiator_huid: str | None,
        access_strategy: str,
        anchor_cts_host: str | None,
        skip_huids: set[str] | None = None,
    ) -> ParticipantAccessApplyResult:
        normalized_access_strategy = (access_strategy or "direct_add").strip().lower()
        normalized_initiator_huid = (initiator_huid or "").strip() or None
        filtered_targets = self._deduplicate_participant_targets(
            participant_targets,
            initiator_huid=normalized_initiator_huid,
            skip_huids=skip_huids or set(),
        )
        effective_anchor_cts_host = self._resolve_anchor_cts_host(anchor_cts_host)
        if normalized_access_strategy == "none":
            return ParticipantAccessApplyResult()
        if normalized_access_strategy == "invite_link":
            return await self._dispatch_access_invites(
                chat_kind=chat_kind,
                migration_id=migration_id,
                source_chat_id=source_chat_id,
                source_backend=source_backend,
                target_chat_id=target_chat_id,
                target_chat_title=target_chat_title,
                recipient_targets=filtered_targets,
                access_strategy=normalized_access_strategy,
                reason="invite_link_selected",
                anchor_cts_host=effective_anchor_cts_host,
            )
        if not filtered_targets:
            return await self._dispatch_access_invites(
                chat_kind=chat_kind,
                migration_id=migration_id,
                source_chat_id=source_chat_id,
                source_backend=source_backend,
                target_chat_id=target_chat_id,
                target_chat_title=target_chat_title,
                recipient_targets=filtered_targets,
                access_strategy=normalized_access_strategy,
                reason="no_resolved_participants_for_direct_add",
                anchor_cts_host=effective_anchor_cts_host,
            )

        added_huids: list[str] = []
        added_by_cts_host: dict[str, list[str]] = {}
        fallback_targets: list[ResolvedParticipantTarget] = []
        failed_targets: list[ParticipantAccessFailure] = []
        for route_cts_host, grouped_targets in self._partition_targets_by_cts_host(
            filtered_targets,
            anchor_cts_host=effective_anchor_cts_host,
        ).items():
            if not self._is_cts_host_configured(route_cts_host):
                fallback_targets.extend(grouped_targets)
                failed_targets.extend(
                    self._participant_access_failures(
                        grouped_targets,
                        reason="invite_route_missing",
                    ),
                )
                await self._audit_repository.add(
                    AuditEvent(
                        migration_id=migration_id,
                        source_chat_id=source_chat_id,
                        event_type=self._invite_route_missing_event_type(chat_kind),
                        severity=AuditSeverity.WARNING,
                        payload_json={
                            "target_chat_id": target_chat_id,
                            "requested_cts_host": route_cts_host,
                            "recipient_huids": [
                                target.target_huid for target in grouped_targets
                            ],
                        },
                        created_at=self._now(),
                    ),
                )
                continue
            participant_huids = [target.target_huid for target in grouped_targets]
            try:
                await self._ensure_helper_bot_route_membership(
                    chat_kind=chat_kind,
                    migration_id=migration_id,
                    source_chat_id=source_chat_id,
                    target_chat_id=target_chat_id,
                    route_cts_host=route_cts_host,
                    anchor_cts_host=effective_anchor_cts_host,
                )
            except FatalItemError as error:
                fallback_targets.extend(grouped_targets)
                reason = "helper_bot_route_membership_failed"
                failed_targets.extend(
                    self._participant_access_failures(
                        grouped_targets,
                        reason=reason,
                        error=str(error),
                    ),
                )
                self._log(
                    "warning",
                    self._members_sync_degraded_event_type(chat_kind),
                    migration_id=migration_id,
                    source_chat_id=source_chat_id,
                    target_chat_id=target_chat_id,
                    route_cts_host=route_cts_host,
                    participant_huids=participant_huids,
                    reason=reason,
                    error_type=type(error).__name__,
                    error=str(error),
                )
                await self._audit_repository.add(
                    AuditEvent(
                        migration_id=migration_id,
                        source_chat_id=source_chat_id,
                        event_type=self._members_sync_degraded_event_type(chat_kind),
                        severity=AuditSeverity.WARNING,
                        payload_json={
                            "target_chat_id": target_chat_id,
                            "route_cts_host": route_cts_host,
                            "participant_huids": participant_huids,
                            "reason": reason,
                            "error": str(error),
                        },
                        created_at=self._now(),
                    ),
                )
                continue
            except (RecoverableItemError, AmbiguousDeliveryError):
                raise

            try:
                newly_added, effective_sync_cts_host = await self._ensure_chat_members_with_cross_cts_retry(
                    chat_kind=chat_kind,
                    migration_id=migration_id,
                    source_chat_id=source_chat_id,
                    target_chat_id=target_chat_id,
                    participant_huids=participant_huids,
                    route_cts_host=route_cts_host,
                    anchor_cts_host=effective_anchor_cts_host,
                )
            except FatalItemError as error:
                if len(grouped_targets) > 1:
                    self._log(
                        "warning",
                        self._members_sync_degraded_event_type(chat_kind),
                        migration_id=migration_id,
                        source_chat_id=source_chat_id,
                        target_chat_id=target_chat_id,
                        route_cts_host=route_cts_host,
                        participant_huids=participant_huids,
                        reason="direct_add_batch_failed_retrying_individually",
                        error_type=type(error).__name__,
                        error=str(error),
                    )
                    await self._audit_repository.add(
                        AuditEvent(
                            migration_id=migration_id,
                            source_chat_id=source_chat_id,
                            event_type=self._members_sync_degraded_event_type(chat_kind),
                            severity=AuditSeverity.WARNING,
                            payload_json={
                                "target_chat_id": target_chat_id,
                                "route_cts_host": route_cts_host,
                                "participant_huids": participant_huids,
                                "reason": "direct_add_batch_failed_retrying_individually",
                                "error": str(error),
                            },
                            created_at=self._now(),
                        ),
                    )
                    (
                        individually_added_huids,
                        individually_added_by_cts_host,
                        individually_failed_targets,
                        individually_failed_participants,
                    ) = await self._ensure_chat_members_with_best_effort(
                        chat_kind=chat_kind,
                        migration_id=migration_id,
                        source_chat_id=source_chat_id,
                        target_chat_id=target_chat_id,
                        grouped_targets=grouped_targets,
                        route_cts_host=route_cts_host,
                        anchor_cts_host=effective_anchor_cts_host,
                    )
                    added_huids.extend(individually_added_huids)
                    for cts_host, huids in individually_added_by_cts_host.items():
                        added_by_cts_host.setdefault(cts_host, []).extend(huids)
                    fallback_targets.extend(individually_failed_targets)
                    failed_targets.extend(individually_failed_participants)
                    continue
                fallback_targets.extend(grouped_targets)
                reason = "direct_add_failed"
                failed_targets.extend(
                    self._participant_access_failures(
                        grouped_targets,
                        reason=reason,
                        error=str(error),
                    ),
                )
                self._log(
                    "warning",
                    self._members_sync_degraded_event_type(chat_kind),
                    migration_id=migration_id,
                    source_chat_id=source_chat_id,
                    target_chat_id=target_chat_id,
                    route_cts_host=route_cts_host,
                    participant_huids=participant_huids,
                    reason=reason,
                    error_type=type(error).__name__,
                    error=str(error),
                )
                await self._audit_repository.add(
                    AuditEvent(
                        migration_id=migration_id,
                        source_chat_id=source_chat_id,
                        event_type=self._members_sync_degraded_event_type(chat_kind),
                        severity=AuditSeverity.WARNING,
                        payload_json={
                            "target_chat_id": target_chat_id,
                            "route_cts_host": route_cts_host,
                            "participant_huids": participant_huids,
                            "reason": reason,
                            "error": str(error),
                        },
                        created_at=self._now(),
                    ),
                )
                continue
            except (RecoverableItemError, AmbiguousDeliveryError):
                raise

            if newly_added:
                normalized_route = (
                    effective_sync_cts_host or effective_anchor_cts_host or "default"
                )
                added_huids.extend(newly_added)
                added_by_cts_host.setdefault(normalized_route, []).extend(newly_added)

        invite_result = ParticipantAccessApplyResult()
        if fallback_targets:
            invite_result = await self._dispatch_access_invites(
                chat_kind=chat_kind,
                migration_id=migration_id,
                source_chat_id=source_chat_id,
                source_backend=source_backend,
                target_chat_id=target_chat_id,
                target_chat_title=target_chat_title,
                recipient_targets=fallback_targets,
                access_strategy="invite_link",
                reason="direct_add_failed",
                anchor_cts_host=effective_anchor_cts_host,
            )

        await self._audit_repository.add(
            AuditEvent(
                migration_id=migration_id,
                source_chat_id=source_chat_id,
                event_type=self._members_synced_event_type(chat_kind),
                severity=AuditSeverity.INFO if added_huids else AuditSeverity.WARNING,
                payload_json={
                    "target_chat_id": target_chat_id,
                    "requested_participant_count": len(filtered_targets),
                    "added_huids": added_huids,
                    "added_by_cts_host": added_by_cts_host,
                    "fallback_invite_count": len(fallback_targets),
                    "failed_target_huids": [
                        item.target_huid for item in failed_targets
                    ],
                    "failed_target_count": len(failed_targets),
                },
                created_at=self._now(),
            ),
        )
        return ParticipantAccessApplyResult(
            added_huids=tuple(added_huids),
            invited_huids=invite_result.invited_huids,
            failed_targets=tuple(failed_targets),
        )

    async def _ensure_chat_members_with_cross_cts_retry(
        self,
        *,
        chat_kind: str,
        migration_id: str,
        source_chat_id: str,
        target_chat_id: str,
        participant_huids: list[str],
        route_cts_host: str | None,
        anchor_cts_host: str | None,
    ) -> tuple[tuple[str, ...], str | None]:
        normalized_route_cts_host = normalize_express_cts_host(route_cts_host)
        normalized_anchor_cts_host = self._resolve_anchor_cts_host(anchor_cts_host)
        try:
            newly_added = await self._ensure_chat_members_via_cts_host(
                cts_host=normalized_route_cts_host,
                target_chat_id=target_chat_id,
                participant_huids=participant_huids,
            )
            return newly_added, normalized_route_cts_host
        except FatalItemError as route_error:
            if not self._should_retry_member_sync_via_anchor(
                error=route_error,
                route_cts_host=normalized_route_cts_host,
                anchor_cts_host=normalized_anchor_cts_host,
            ):
                raise
            try:
                newly_added = await self._ensure_chat_members_via_cts_host(
                    cts_host=normalized_anchor_cts_host,
                    target_chat_id=target_chat_id,
                    participant_huids=participant_huids,
                )
            except FatalItemError as anchor_error:
                raise FatalItemError(
                    "eXpress ensure_chat_members failed after cross-CTS reroute: "
                    f"route_cts_host={normalized_route_cts_host} route_error={route_error}; "
                    f"anchor_cts_host={normalized_anchor_cts_host} anchor_error={anchor_error}",
                ) from anchor_error
            self._log(
                "info",
                self._members_sync_rerouted_event_type(chat_kind),
                migration_id=migration_id,
                source_chat_id=source_chat_id,
                target_chat_id=target_chat_id,
                route_cts_host=normalized_route_cts_host,
                fallback_cts_host=normalized_anchor_cts_host,
                participant_huids=participant_huids,
                reason="route_chat_not_found",
                route_error_type=type(route_error).__name__,
                route_error=str(route_error),
            )
            await self._audit_repository.add(
                AuditEvent(
                    migration_id=migration_id,
                    source_chat_id=source_chat_id,
                    event_type=self._members_sync_rerouted_event_type(chat_kind),
                    severity=AuditSeverity.INFO,
                    payload_json={
                        "target_chat_id": target_chat_id,
                        "route_cts_host": normalized_route_cts_host,
                        "fallback_cts_host": normalized_anchor_cts_host,
                        "participant_huids": participant_huids,
                        "reason": "route_chat_not_found",
                        "route_error": str(route_error),
                    },
                    created_at=self._now(),
                ),
            )
            return newly_added, normalized_anchor_cts_host

    async def _ensure_chat_members_with_best_effort(
        self,
        *,
        chat_kind: str,
        migration_id: str,
        source_chat_id: str,
        target_chat_id: str,
        grouped_targets: list[ResolvedParticipantTarget],
        route_cts_host: str | None,
        anchor_cts_host: str | None,
    ) -> tuple[
        tuple[str, ...],
        dict[str, list[str]],
        tuple[ResolvedParticipantTarget, ...],
        tuple[ParticipantAccessFailure, ...],
    ]:
        added_huids: list[str] = []
        added_by_cts_host: dict[str, list[str]] = {}
        failed_targets: list[ResolvedParticipantTarget] = []
        failures: list[ParticipantAccessFailure] = []
        for target in grouped_targets:
            try:
                newly_added, effective_sync_cts_host = await self._ensure_chat_members_with_cross_cts_retry(
                    chat_kind=chat_kind,
                    migration_id=migration_id,
                    source_chat_id=source_chat_id,
                    target_chat_id=target_chat_id,
                    participant_huids=[target.target_huid],
                    route_cts_host=route_cts_host,
                    anchor_cts_host=anchor_cts_host,
                )
            except FatalItemError as error:
                failed_targets.append(target)
                failures.append(
                    ParticipantAccessFailure(
                        target_huid=target.target_huid,
                        cts_host=normalize_express_cts_host(target.cts_host),
                        reason="direct_add_failed",
                        error=str(error),
                    ),
                )
                continue
            normalized_route = (
                effective_sync_cts_host
                or self._resolve_anchor_cts_host(anchor_cts_host)
                or "default"
            )
            if newly_added:
                added_huids.extend(newly_added)
                added_by_cts_host.setdefault(normalized_route, []).extend(newly_added)
        return (
            tuple(added_huids),
            added_by_cts_host,
            tuple(failed_targets),
            tuple(failures),
        )

    def _participant_access_failures(
        self,
        targets: list[ResolvedParticipantTarget],
        *,
        reason: str,
        error: str | None = None,
    ) -> list[ParticipantAccessFailure]:
        return [
            ParticipantAccessFailure(
                target_huid=target.target_huid,
                cts_host=normalize_express_cts_host(target.cts_host),
                reason=reason,
                error=error,
            )
            for target in targets
        ]

    async def _ensure_chat_members_via_cts_host(
        self,
        *,
        cts_host: str | None,
        target_chat_id: str,
        participant_huids: list[str],
    ) -> tuple[str, ...]:
        with self._use_cts_host(cts_host):
            return await self._retry_policy.run(
                lambda participant_huids=participant_huids: self._express_gateway.ensure_chat_members(
                    target_chat_id,
                    participant_huids,
                ),
            )

    def _should_retry_member_sync_via_anchor(
        self,
        *,
        error: FatalItemError,
        route_cts_host: str | None,
        anchor_cts_host: str | None,
    ) -> bool:
        normalized_route_cts_host = normalize_express_cts_host(route_cts_host)
        normalized_anchor_cts_host = normalize_express_cts_host(anchor_cts_host)
        if (
            normalized_route_cts_host is None
            or normalized_anchor_cts_host is None
            or normalized_route_cts_host == normalized_anchor_cts_host
        ):
            return False
        return self._is_chat_not_found_error(error)

    def _is_chat_not_found_error(self, error: BaseException) -> bool:
        current: BaseException | None = error
        while current is not None:
            if type(current).__name__ == "ChatNotFoundError":
                return True
            normalized_message = str(current).strip().lower()
            if (
                "chat_not_found" in normalized_message
                or "chat with specified id not found" in normalized_message
            ):
                return True
            next_error = current.__cause__
            if next_error is current:
                break
            current = next_error
        return False

    async def _ensure_initiator_chat_admin(
        self,
        *,
        chat_kind: str,
        migration_id: str,
        source_chat_id: str,
        target_chat_id: str,
        initiator_huid: str | None,
        anchor_cts_host: str | None,
    ) -> None:
        normalized_initiator_huid = (initiator_huid or "").strip()
        if not normalized_initiator_huid:
            return
        effective_anchor_cts_host = self._resolve_anchor_cts_host(anchor_cts_host)
        try:
            with self._use_cts_host(effective_anchor_cts_host):
                promoted_admin_huids = await self._retry_policy.run(
                    lambda initiator_huid=normalized_initiator_huid: self._express_gateway.promote_chat_admins(
                        target_chat_id,
                        [initiator_huid],
                    ),
                )
        except FatalItemError as error:
            await self._audit_repository.add(
                AuditEvent(
                    migration_id=migration_id,
                    source_chat_id=source_chat_id,
                    event_type=self._initiator_admin_degraded_event_type(chat_kind),
                    severity=AuditSeverity.WARNING,
                    payload_json={
                        "target_chat_id": target_chat_id,
                        "initiator_huid": normalized_initiator_huid,
                        "anchor_cts_host": effective_anchor_cts_host,
                        "error": str(error),
                    },
                    created_at=self._now(),
                ),
            )
            raise
        except (RecoverableItemError, AmbiguousDeliveryError):
            raise
        await self._audit_repository.add(
            AuditEvent(
                migration_id=migration_id,
                source_chat_id=source_chat_id,
                event_type=self._initiator_admin_synced_event_type(chat_kind),
                severity=AuditSeverity.INFO,
                payload_json={
                    "target_chat_id": target_chat_id,
                    "initiator_huid": normalized_initiator_huid,
                    "anchor_cts_host": effective_anchor_cts_host,
                    "promoted_admin_huids": list(promoted_admin_huids),
                },
                created_at=self._now(),
            ),
        )

    async def _ensure_helper_bot_route_membership(
        self,
        *,
        chat_kind: str,
        migration_id: str,
        source_chat_id: str,
        target_chat_id: str,
        route_cts_host: str | None,
        anchor_cts_host: str | None,
    ) -> None:
        normalized_route_cts_host = normalize_express_cts_host(route_cts_host)
        normalized_anchor_cts_host = self._resolve_anchor_cts_host(anchor_cts_host)
        if (
            normalized_route_cts_host is None
            or normalized_anchor_cts_host is None
            or normalized_route_cts_host == normalized_anchor_cts_host
        ):
            return
        helper_bot_huid = await self._resolved_helper_bot_huid_for_cts_host(
            normalized_route_cts_host,
        )
        if not helper_bot_huid:
            await self._audit_repository.add(
                AuditEvent(
                    migration_id=migration_id,
                    source_chat_id=source_chat_id,
                    event_type=self._helper_bot_attach_skipped_event_type(chat_kind),
                    severity=AuditSeverity.INFO,
                    payload_json={
                        "target_chat_id": target_chat_id,
                        "helper_cts_host": normalized_route_cts_host,
                        "reason": "helper_bot_huid_unavailable",
                    },
                    created_at=self._now(),
                ),
            )
            return
        try:
            with self._use_cts_host(normalized_anchor_cts_host):
                attached_huids = await self._retry_policy.run(
                    lambda helper_bot_huid=helper_bot_huid: self._express_gateway.ensure_chat_members(
                        target_chat_id,
                        [helper_bot_huid],
                    ),
                )
        except FatalItemError as error:
            await self._audit_repository.add(
                AuditEvent(
                    migration_id=migration_id,
                    source_chat_id=source_chat_id,
                    event_type=self._helper_bot_attach_degraded_event_type(chat_kind),
                    severity=AuditSeverity.WARNING,
                    payload_json={
                        "target_chat_id": target_chat_id,
                        "helper_cts_host": normalized_route_cts_host,
                        "helper_bot_huid": helper_bot_huid,
                        "anchor_cts_host": normalized_anchor_cts_host,
                        "error": str(error),
                    },
                    created_at=self._now(),
                ),
            )
            raise FatalItemError(
                "helper bot route membership attach failed: "
                f"helper_cts_host={normalized_route_cts_host} "
                f"anchor_cts_host={normalized_anchor_cts_host} "
                f"helper_bot_huid={helper_bot_huid}; error={error}",
            ) from error
        except (RecoverableItemError, AmbiguousDeliveryError):
            raise
        try:
            with self._use_cts_host(normalized_anchor_cts_host):
                promoted_admin_huids = await self._retry_policy.run(
                    lambda helper_bot_huid=helper_bot_huid: self._express_gateway.promote_chat_admins(
                        target_chat_id,
                        [helper_bot_huid],
                    ),
                )
        except FatalItemError as error:
            await self._audit_repository.add(
                AuditEvent(
                    migration_id=migration_id,
                    source_chat_id=source_chat_id,
                    event_type=self._helper_bot_attach_degraded_event_type(chat_kind),
                    severity=AuditSeverity.WARNING,
                    payload_json={
                        "target_chat_id": target_chat_id,
                        "helper_cts_host": normalized_route_cts_host,
                        "helper_bot_huid": helper_bot_huid,
                        "anchor_cts_host": normalized_anchor_cts_host,
                        "stage": "promote_helper_bot_admin",
                        "error": str(error),
                    },
                    created_at=self._now(),
                ),
            )
            raise FatalItemError(
                "helper bot route membership admin promotion failed: "
                f"helper_cts_host={normalized_route_cts_host} "
                f"anchor_cts_host={normalized_anchor_cts_host} "
                f"helper_bot_huid={helper_bot_huid}; error={error}",
            ) from error
        except (RecoverableItemError, AmbiguousDeliveryError):
            raise
        try:
            await self._ensure_helper_bot_route_admin_ready(
                target_chat_id=target_chat_id,
                route_cts_host=normalized_route_cts_host,
                helper_bot_huid=helper_bot_huid,
            )
        except FatalItemError as error:
            await self._audit_repository.add(
                AuditEvent(
                    migration_id=migration_id,
                    source_chat_id=source_chat_id,
                    event_type=self._helper_bot_attach_degraded_event_type(chat_kind),
                    severity=AuditSeverity.WARNING,
                    payload_json={
                        "target_chat_id": target_chat_id,
                        "helper_cts_host": normalized_route_cts_host,
                        "helper_bot_huid": helper_bot_huid,
                        "anchor_cts_host": normalized_anchor_cts_host,
                        "stage": "route_admin_visibility",
                        "error": str(error),
                    },
                    created_at=self._now(),
                ),
            )
            raise FatalItemError(
                "helper bot route membership route-admin visibility failed: "
                f"helper_cts_host={normalized_route_cts_host} "
                f"anchor_cts_host={normalized_anchor_cts_host} "
                f"helper_bot_huid={helper_bot_huid}; error={error}",
            ) from error
        except (RecoverableItemError, AmbiguousDeliveryError) as error:
            raise FatalItemError(
                "helper bot route membership route-admin visibility did not converge: "
                f"helper_cts_host={normalized_route_cts_host} "
                f"anchor_cts_host={normalized_anchor_cts_host} "
                f"helper_bot_huid={helper_bot_huid}; error={error}",
            ) from error
        await self._audit_repository.add(
            AuditEvent(
                migration_id=migration_id,
                source_chat_id=source_chat_id,
                event_type=self._helper_bot_attached_event_type(chat_kind),
                severity=AuditSeverity.INFO,
                payload_json={
                    "target_chat_id": target_chat_id,
                    "helper_cts_host": normalized_route_cts_host,
                    "helper_bot_huid": helper_bot_huid,
                    "anchor_cts_host": normalized_anchor_cts_host,
                    "attached_huids": list(attached_huids),
                    "promoted_admin_huids": list(promoted_admin_huids),
                    "route_admin_confirmed_cts_host": normalized_route_cts_host,
                },
                created_at=self._now(),
            ),
        )

    async def _ensure_helper_bot_route_admin_ready(
        self,
        *,
        target_chat_id: str,
        route_cts_host: str,
        helper_bot_huid: str,
    ) -> None:
        async def _assert_route_admin_visible() -> None:
            try:
                with self._use_cts_host(route_cts_host):
                    admin_huids = await self._express_gateway.list_chat_admin_huids(
                        target_chat_id,
                    )
            except FatalItemError as error:
                if self._is_route_admin_visibility_pending_error(error):
                    raise RecoverableItemError(
                        "helper bot route admin visibility is not ready yet: "
                        f"route_cts_host={route_cts_host} "
                        f"target_chat_id={target_chat_id} "
                        f"helper_bot_huid={helper_bot_huid} "
                        f"error={error}",
                    ) from error
                raise
            if helper_bot_huid not in admin_huids:
                raise RecoverableItemError(
                    "helper bot route admin visibility is not ready yet: "
                    f"route_cts_host={route_cts_host} "
                    f"target_chat_id={target_chat_id} "
                    f"helper_bot_huid={helper_bot_huid} "
                    f"visible_admin_huids={list(admin_huids)}",
                )

        await self._retry_policy.run(_assert_route_admin_visible)

    def _is_route_admin_visibility_pending_error(self, error: BaseException) -> bool:
        normalized_message = str(error).strip().lower()
        return any(
            marker in normalized_message
            for marker in (
                "chat_not_found",
                "chat with specified id not found",
                "no_permission_for_operation",
                "permission denied",
                "sender is not chat admin",
                "not chat admin",
            )
        )

    async def _emit_access_link(
        self,
        *,
        chat_kind: str,
        migration_id: str,
        source_chat_id: str,
        target_chat_id: str,
        access_strategy: str,
        reason: str,
        anchor_cts_host: str | None,
        error: str | None = None,
    ) -> str | None:
        with self._use_cts_host(anchor_cts_host):
            target_chat_link = await self._retry_policy.run(
                lambda: self._express_gateway.create_chat_link(target_chat_id),
            )
        await self._audit_repository.add(
            AuditEvent(
                migration_id=migration_id,
                source_chat_id=source_chat_id,
                event_type=self._access_link_prepared_event_type(chat_kind),
                severity=AuditSeverity.INFO if target_chat_link else AuditSeverity.WARNING,
                payload_json={
                    "target_chat_id": target_chat_id,
                    "target_chat_link": target_chat_link,
                    "access_strategy": access_strategy,
                    "reason": reason,
                    "anchor_cts_host": anchor_cts_host,
                    "error": error,
                },
                created_at=self._now(),
            ),
        )
        return target_chat_link

    async def _dispatch_access_invites(
        self,
        *,
        chat_kind: str,
        migration_id: str,
        source_chat_id: str,
        source_backend: str,
        target_chat_id: str,
        target_chat_title: str,
        recipient_targets: list[ResolvedParticipantTarget],
        access_strategy: str,
        reason: str,
        anchor_cts_host: str | None,
        error: str | None = None,
    ) -> ParticipantAccessApplyResult:
        target_chat_link = await self._emit_access_link(
            chat_kind=chat_kind,
            migration_id=migration_id,
            source_chat_id=source_chat_id,
            target_chat_id=target_chat_id,
            access_strategy=access_strategy,
            reason=reason,
            anchor_cts_host=anchor_cts_host,
            error=error,
        )
        if not target_chat_link:
            return ParticipantAccessApplyResult()
        delivered_huids: list[str] = []
        delivered_by_cts_host: dict[str, list[str]] = {}
        for target in recipient_targets:
            route_cts_host = self._target_route_cts_host(
                participant_cts_host=target.cts_host,
                anchor_cts_host=anchor_cts_host,
            )
            if not self._is_cts_host_configured(route_cts_host):
                await self._audit_repository.add(
                    AuditEvent(
                        migration_id=migration_id,
                        source_chat_id=source_chat_id,
                        event_type=self._invite_route_missing_event_type(chat_kind),
                        severity=AuditSeverity.WARNING,
                        payload_json={
                            "target_chat_id": target_chat_id,
                            "requested_cts_host": route_cts_host,
                            "recipient_huid": target.target_huid,
                        },
                        created_at=self._now(),
                    ),
                )
                continue
            try:
                with self._use_cts_host(route_cts_host):
                    personal_chat_id = await self._retry_policy.run(
                        lambda huid=target.target_huid: self._express_gateway.ensure_personal_chat(huid),
                    )
                    await self._retry_policy.run(
                        lambda chat_id=personal_chat_id, huid=target.target_huid: self._express_gateway.send_message(
                            chat_id,
                            self._access_invite_message(
                                chat_kind=chat_kind,
                                target_chat_title=target_chat_title,
                                target_chat_link=target_chat_link,
                            ),
                            idempotency_key=(
                                f"{chat_kind}-invite:{source_backend}:{source_chat_id}:"
                                f"{target_chat_id}:{route_cts_host or 'default'}:{huid}"
                            ),
                        ),
                    )
            except (FatalItemError, RecoverableItemError, AmbiguousDeliveryError) as invite_error:
                await self._audit_repository.add(
                    AuditEvent(
                        migration_id=migration_id,
                        source_chat_id=source_chat_id,
                        event_type=self._invite_dispatch_failed_event_type(chat_kind),
                        severity=AuditSeverity.WARNING,
                        payload_json={
                            "target_chat_id": target_chat_id,
                            "recipient_huid": target.target_huid,
                            "recipient_cts_host": target.cts_host,
                            "route_cts_host": route_cts_host,
                            "error": str(invite_error),
                        },
                        created_at=self._now(),
                    ),
                )
                continue
            delivered_huids.append(target.target_huid)
            delivered_by_cts_host.setdefault(
                route_cts_host or anchor_cts_host or "default",
                [],
            ).append(target.target_huid)
        await self._audit_repository.add(
            AuditEvent(
                migration_id=migration_id,
                source_chat_id=source_chat_id,
                event_type=self._invite_dispatched_event_type(chat_kind),
                severity=AuditSeverity.INFO if delivered_huids else AuditSeverity.WARNING,
                payload_json={
                    "target_chat_id": target_chat_id,
                    "recipient_huids": delivered_huids,
                    "recipient_count": len(recipient_targets),
                    "delivered_count": len(delivered_huids),
                    "delivered_by_cts_host": delivered_by_cts_host,
                    "reason": reason,
                },
                created_at=self._now(),
            ),
        )
        return ParticipantAccessApplyResult(invited_huids=tuple(delivered_huids))

    def _access_invite_message(
        self,
        *,
        chat_kind: str,
        target_chat_title: str,
        target_chat_link: str,
    ) -> str:
        if chat_kind == "channel":
            return (
                f"В eXpress подготовлен канал: {target_chat_title}\n"
                f"Ссылка для входа: {target_chat_link}"
            )
        return (
            f"В eXpress подготовлен чат: {target_chat_title}\n"
            f"Ссылка для входа: {target_chat_link}"
        )

    def _deduplicate_participant_targets(
        self,
        participant_targets: list[ResolvedParticipantTarget],
        *,
        initiator_huid: str | None,
        skip_huids: set[str],
    ) -> list[ResolvedParticipantTarget]:
        unique: list[ResolvedParticipantTarget] = []
        seen: set[str] = set(skip_huids)
        if initiator_huid:
            seen.add(initiator_huid)
        for target in participant_targets:
            normalized_huid = target.target_huid.strip()
            if not normalized_huid or normalized_huid in seen:
                continue
            seen.add(normalized_huid)
            unique.append(
                ResolvedParticipantTarget(
                    target_huid=normalized_huid,
                    cts_host=normalize_express_cts_host(target.cts_host),
                ),
            )
        return unique

    def _seed_participant_huids(
        self,
        *,
        participant_targets: list[ResolvedParticipantTarget],
        initiator_huid: str | None,
        access_strategy: str,
        anchor_cts_host: str | None,
    ) -> list[str]:
        normalized_initiator_huid = (initiator_huid or "").strip() or None
        effective_anchor_cts_host = self._resolve_anchor_cts_host(anchor_cts_host)
        seed_huids: list[str] = []
        seen: set[str] = set()
        if normalized_initiator_huid:
            seed_huids.append(normalized_initiator_huid)
            seen.add(normalized_initiator_huid)
        if (access_strategy or "direct_add").strip().lower() != "direct_add":
            return seed_huids
        for target in participant_targets:
            if target.target_huid in seen:
                continue
            if (
                effective_anchor_cts_host is not None
                and target.cts_host is not None
                and normalize_express_cts_host(target.cts_host) != effective_anchor_cts_host
            ):
                continue
            seed_huids.append(target.target_huid)
            seen.add(target.target_huid)
        if not seed_huids and participant_targets:
            seed_huids.append(participant_targets[0].target_huid)
        return seed_huids

    def _partition_targets_by_cts_host(
        self,
        participant_targets: list[ResolvedParticipantTarget],
        *,
        anchor_cts_host: str | None,
    ) -> dict[str | None, list[ResolvedParticipantTarget]]:
        grouped: dict[str | None, list[ResolvedParticipantTarget]] = {}
        for target in participant_targets:
            route_cts_host = self._target_route_cts_host(
                participant_cts_host=target.cts_host,
                anchor_cts_host=anchor_cts_host,
            )
            grouped.setdefault(route_cts_host, []).append(target)
        return grouped

    def _target_route_cts_host(
        self,
        *,
        participant_cts_host: str | None,
        anchor_cts_host: str | None,
    ) -> str | None:
        normalized_participant_cts_host = normalize_express_cts_host(participant_cts_host)
        if normalized_participant_cts_host is not None:
            return normalized_participant_cts_host
        return self._resolve_anchor_cts_host(anchor_cts_host)

    def _resolve_anchor_cts_host(self, anchor_cts_host: str | None) -> str | None:
        normalized_anchor_cts_host = normalize_express_cts_host(anchor_cts_host)
        if normalized_anchor_cts_host is not None:
            return normalized_anchor_cts_host
        current_cts_host = normalize_express_cts_host(get_current_express_cts_host())
        if current_cts_host is not None:
            return current_cts_host
        primary_cts_host = normalize_express_cts_host(
            getattr(self._express_gateway, "primary_cts_host", None),
        )
        return primary_cts_host

    def _registered_cts_hosts(self) -> frozenset[str]:
        raw_hosts = getattr(self._express_gateway, "registered_cts_hosts", ())
        return frozenset(
            normalized
            for host in raw_hosts
            if (normalized := normalize_express_cts_host(host)) is not None
        )

    def _configured_bot_huid_for_cts_host(self, cts_host: str | None) -> str | None:
        getter = getattr(self._express_gateway, "bot_huid_for_cts_host", None)
        if callable(getter):
            normalized = normalize_express_cts_host(cts_host)
            value = getter(normalized)
            normalized_huid = (value or "").strip()
            return normalized_huid or None
        return None

    async def _resolved_helper_bot_huid_for_cts_host(
        self,
        cts_host: str | None,
    ) -> str | None:
        normalized_cts_host = normalize_express_cts_host(cts_host)
        if normalized_cts_host is None:
            return None
        learned_huid = await self._learned_bot_huid_for_cts_host(normalized_cts_host)
        if learned_huid:
            return learned_huid
        return self._configured_bot_huid_for_cts_host(normalized_cts_host)

    async def _learned_bot_huid_for_cts_host(
        self,
        cts_host: str,
    ) -> str | None:
        repository = self._express_bot_huid_binding_repository
        if repository is None:
            return None
        bot_id = self._configured_bot_id_for_cts_host(cts_host)
        if bot_id is None:
            return None
        record = await repository.get_by_bot_id(bot_id)
        if record is None:
            return None
        record_cts_host = normalize_express_cts_host(record.cts_host)
        if record_cts_host != cts_host:
            return None
        normalized_huid = record.bot_huid.strip()
        return normalized_huid or None

    def _configured_bot_id_for_cts_host(self, cts_host: str | None) -> str | None:
        getter = getattr(self._express_gateway, "bot_id_for_cts_host", None)
        if callable(getter):
            normalized = normalize_express_cts_host(cts_host)
            value = getter(normalized)
            normalized_bot_id = (value or "").strip()
            return normalized_bot_id or None
        return None

    def _is_cts_host_configured(self, cts_host: str | None) -> bool:
        registered_hosts = self._registered_cts_hosts()
        if not registered_hosts:
            return True
        normalized_cts_host = normalize_express_cts_host(cts_host)
        return normalized_cts_host is not None and normalized_cts_host in registered_hosts

    def _use_cts_host(self, cts_host: str | None) -> AbstractContextManager[None]:
        router_use_cts_host = getattr(self._express_gateway, "use_cts_host", None)
        if callable(router_use_cts_host):
            return router_use_cts_host(cts_host)
        if cts_host is None:
            return nullcontext()
        return use_express_cts_host(cts_host)

    def _members_synced_event_type(self, chat_kind: str) -> str:
        return "channel_members_synced" if chat_kind == "channel" else "target_chat_members_synced"

    def _members_sync_degraded_event_type(self, chat_kind: str) -> str:
        return "channel_members_sync_degraded" if chat_kind == "channel" else "target_chat_members_sync_degraded"

    def _members_sync_rerouted_event_type(self, chat_kind: str) -> str:
        return "channel_members_sync_rerouted" if chat_kind == "channel" else "target_chat_members_sync_rerouted"

    def _initiator_admin_synced_event_type(self, chat_kind: str) -> str:
        return "channel_initiator_admin_synced" if chat_kind == "channel" else "target_chat_initiator_admin_synced"

    def _initiator_admin_degraded_event_type(self, chat_kind: str) -> str:
        return "channel_initiator_admin_degraded" if chat_kind == "channel" else "target_chat_initiator_admin_degraded"

    def _access_link_prepared_event_type(self, chat_kind: str) -> str:
        return "channel_access_link_prepared" if chat_kind == "channel" else "target_chat_access_link_prepared"

    def _invite_dispatch_failed_event_type(self, chat_kind: str) -> str:
        return "channel_invite_dispatch_failed" if chat_kind == "channel" else "target_chat_invite_dispatch_failed"

    def _invite_dispatched_event_type(self, chat_kind: str) -> str:
        return "channel_invite_link_dispatched" if chat_kind == "channel" else "target_chat_invite_link_dispatched"

    def _invite_route_missing_event_type(self, chat_kind: str) -> str:
        return "channel_invite_route_missing" if chat_kind == "channel" else "target_chat_invite_route_missing"

    def _helper_bot_attached_event_type(self, chat_kind: str) -> str:
        return "channel_helper_bot_attached" if chat_kind == "channel" else "target_chat_helper_bot_attached"

    def _helper_bot_attach_degraded_event_type(self, chat_kind: str) -> str:
        return "channel_helper_bot_attach_degraded" if chat_kind == "channel" else "target_chat_helper_bot_attach_degraded"

    def _log(
        self,
        level: str,
        event: str,
        **payload: object,
    ) -> None:
        if self._logger is None:
            return
        log_method = getattr(self._logger, level, None)
        if callable(log_method):
            log_method(event, **payload)

    def _helper_bot_attach_skipped_event_type(self, chat_kind: str) -> str:
        return "channel_helper_bot_attach_skipped" if chat_kind == "channel" else "target_chat_helper_bot_attach_skipped"

    def _channel_admin_error(
        self,
        *,
        source_chat_id: str,
    ) -> str:
        return (
            "channel migration requires Telegram admin rights for the connected user session; "
            f"source_chat_id={source_chat_id}. Reconnect the Telegram account that is an admin "
            "of this channel or delegate the migration to a channel administrator."
        )

    def _require_channel_initiator_huid(
        self,
        *,
        source_chat_id: str,
        initiator_huid: str | None,
    ) -> str:
        normalized_initiator_huid = (initiator_huid or "").strip()
        if normalized_initiator_huid:
            return normalized_initiator_huid
        raise FatalItemError(
            "channel migration requires initiator_huid from the bot request context; "
            f"source_chat_id={source_chat_id}",
        )

    def _now(self) -> datetime:
        return datetime.now(tz=UTC)
