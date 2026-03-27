from __future__ import annotations

from dataclasses import dataclass

from extg_shared.contracts.models import SourceDialog, TelegramExportSnapshotRecord


@dataclass(frozen=True, slots=True)
class ParsedTelegramExportSnapshot:
    source_chat_id: str
    source_chat_title: str
    source_chat_type: str
    original_filename: str | None
    export_chat_id: str | None
    payload_sha256: str
    message_count: int
    media_count: int
    approximate_bytes: int

    def to_source_dialog(self) -> SourceDialog:
        return SourceDialog(
            dialog_id=self.source_chat_id,
            chat_type=self.source_chat_type,
            title=self.source_chat_title,
            message_count=self.message_count,
            media_count=self.media_count,
            approximate_bytes=self.approximate_bytes,
            has_topics=False,
        )

    def to_snapshot_record(
        self,
        *,
        archive_locator: str,
        uploaded_by_huid: str,
        created_at,
    ) -> TelegramExportSnapshotRecord:
        return TelegramExportSnapshotRecord(
            source_chat_id=self.source_chat_id,
            source_chat_title=self.source_chat_title,
            source_chat_type=self.source_chat_type,
            original_filename=self.original_filename,
            export_chat_id=self.export_chat_id,
            archive_locator=archive_locator,
            payload_sha256=self.payload_sha256,
            message_count=self.message_count,
            media_count=self.media_count,
            approximate_bytes=self.approximate_bytes,
            uploaded_by_huid=uploaded_by_huid,
            created_at=created_at,
            updated_at=created_at,
        )
