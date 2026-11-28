from datetime import datetime
from app.models.domain import (
    InboxMessage,
    OutboxMessage,
    OutboxStatus,
    from_iso,
    to_iso,
    utc_now,
)
from app.storage.db import Database


def _row_to_inbox(row) -> InboxMessage:
    return InboxMessage(
        id=row["id"],
        project_id=row["project_id"],
        channel=row["channel"],
        external_user_id=row["external_user_id"],
        idempotency_key=row["idempotency_key"],
        body=row["body"],
        processed=bool(row["processed"]),
        created_at=from_iso(row["created_at"]),
    )


def _row_to_outbox(row) -> OutboxMessage:
    return OutboxMessage(
        id=row["id"],
        project_id=row["project_id"],
        channel=row["channel"],
        recipient_id=row["recipient_id"],
        idempotency_key=row["idempotency_key"],
        body=row["body"],
        status=OutboxStatus(row["status"]),
        retry_count=row["retry_count"],
        created_at=from_iso(row["created_at"]),
        sent_at=from_iso(row["sent_at"]) if row["sent_at"] else None,
    )


class MessageRepository:
    def __init__(self, db: Database) -> None:
        self.db = db

    async def enqueue_inbox(
        self,
        channel: str,
        external_user_id: str,
        idempotency_key: str,
        body: str,
        project_id: str | None = None,
        now: datetime | None = None,
    ) -> tuple[InboxMessage, bool]:
        """Insert an incoming message idempotently. Returns (message, created_new)."""
        ts = to_iso(now or utc_now())
        async with self.db.connect() as conn:
            cursor = await conn.execute(
                """
                INSERT OR IGNORE INTO messages_inbox (
                    project_id, channel, external_user_id, idempotency_key, body, processed, created_at
                ) VALUES (?, ?, ?, ?, ?, 0, ?)
                """,
                (project_id, channel, external_user_id, idempotency_key, body, ts),
            )
            created = cursor.rowcount == 1
            await conn.commit()

            cursor = await conn.execute(
                "SELECT * FROM messages_inbox WHERE idempotency_key = ?",
                (idempotency_key,),
            )
            row = await cursor.fetchone()
            assert row is not None
            return _row_to_inbox(row), created

    async def get_pending_inbox(self, project_id: str) -> list[InboxMessage]:
        async with self.db.connect() as conn:
            cursor = await conn.execute(
                """
                SELECT * FROM messages_inbox
                WHERE project_id = ? AND processed = 0
                ORDER BY id ASC
                """,
                (project_id,),
            )
            rows = await cursor.fetchall()
            return [_row_to_inbox(r) for r in rows]

    async def mark_inbox_processed(self, message_id: int) -> None:
        async with self.db.connect() as conn:
            await conn.execute(
                "UPDATE messages_inbox SET processed = 1 WHERE id = ?",
                (message_id,),
            )
            await conn.commit()

    async def enqueue_outbox(
        self,
        project_id: str,
        channel: str,
        recipient_id: str,
        idempotency_key: str,
        body: str,
        now: datetime | None = None,
    ) -> tuple[OutboxMessage, bool]:
        """Queue an outgoing message idempotently so crash recovery never double-sends."""
        ts = to_iso(now or utc_now())
        async with self.db.connect() as conn:
            cursor = await conn.execute(
                """
                INSERT OR IGNORE INTO messages_outbox (
                    project_id, channel, recipient_id, idempotency_key, body, status, retry_count, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, 0, ?)
                """,
                (project_id, channel, recipient_id, idempotency_key, body, OutboxStatus.PENDING.value, ts),
            )
            created = cursor.rowcount == 1
            await conn.commit()

            cursor = await conn.execute(
                "SELECT * FROM messages_outbox WHERE idempotency_key = ?",
                (idempotency_key,),
            )
            row = await cursor.fetchone()
            assert row is not None
            return _row_to_outbox(row), created

    async def get_pending_outbox(self, limit: int = 20) -> list[OutboxMessage]:
        async with self.db.connect() as conn:
            cursor = await conn.execute(
                """
                SELECT * FROM messages_outbox
                WHERE status = ?
                ORDER BY id ASC
                LIMIT ?
                """,
                (OutboxStatus.PENDING.value, limit),
            )
            rows = await cursor.fetchall()
            return [_row_to_outbox(r) for r in rows]

    async def mark_outbox_sent(self, outbox_id: int, now: datetime | None = None) -> None:
        ts = to_iso(now or utc_now())
        async with self.db.connect() as conn:
            await conn.execute(
                """
                UPDATE messages_outbox
                SET status = ?, sent_at = ?
                WHERE id = ?
                """,
                (OutboxStatus.SENT.value, ts, outbox_id),
            )
            await conn.commit()

    async def mark_outbox_retry(self, outbox_id: int, max_retries: int = 5) -> None:
        async with self.db.connect() as conn:
            await conn.execute(
                """
                UPDATE messages_outbox
                SET retry_count = retry_count + 1,
                    status = CASE
                        WHEN retry_count + 1 >= ? THEN ?
                        ELSE status
                    END
                WHERE id = ?
                """,
                (max_retries, OutboxStatus.FAILED.value, outbox_id),
            )
            await conn.commit()

    # --- Cross-channel & Teammate Identity Mapping  ---

    async def add_member(
        self,
        project_id: str,
        channel: str,
        external_user_id: str,
        role: str = "teammate",
        now: datetime | None = None,
    ) -> None:
        ts = to_iso(now or utc_now())
        async with self.db.connect() as conn:
            await conn.execute(
                """
                INSERT OR IGNORE INTO project_members (project_id, channel, external_user_id, role, created_at)
                VALUES (?, ?, ?, ?, ?)
                """,
                (project_id, channel, external_user_id, role, ts),
            )
            await conn.commit()

    async def find_project_for_user(self, channel: str, external_user_id: str) -> str | None:
        async with self.db.connect() as conn:
            cursor = await conn.execute(
                """
                SELECT project_id FROM project_members
                WHERE channel = ? AND external_user_id = ?
                ORDER BY id DESC LIMIT 1
                """,
                (channel, external_user_id),
            )
            row = await cursor.fetchone()
            return row["project_id"] if row else None

    async def get_recipient_for_channel(self, project_id: str, channel: str) -> str | None:
        async with self.db.connect() as conn:
            cursor = await conn.execute(
                """
                SELECT external_user_id FROM project_members
                WHERE project_id = ? AND channel = ?
                ORDER BY CASE WHEN role = 'owner' THEN 0 ELSE 1 END, id ASC
                LIMIT 1
                """,
                (project_id, channel),
            )
            row = await cursor.fetchone()
            return row["external_user_id"] if row else None

    async def get_conversation_history(self, project_id:str) -> list[dict[str, str]]:
        """Reconstract durable chat history from inbox + outbox rows"""

        async with self.db.connect() as conn:
            cursor = await conn.execute(
                """
                SELECT 'user' AS role, body, created_at, id AS sort_id
                FROM messages_inbox
                WHERE project_id = ?
                UNION ALL
                SELECT 'assistant' AS role, body, created_at, id AS sort_id
                FROM messages_outbox
                WHERE project_id = ?
                ORDER BY created_at ASC, sort_id ASC
                """,
                (project_id, project_id)
            )
            rows = await cursor.fetchall()
            return [{"role": r["role"], "content": r["body"]} for r in rows]

            