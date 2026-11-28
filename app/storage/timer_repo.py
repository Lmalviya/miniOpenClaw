from datetime import datetime, timedelta
import json
from app.models.domain import Timer, TimerStatus, from_iso, to_iso, utc_now
from app.storage.db import Database


def _row_to_timer(row) -> Timer:
    return Timer(
        id=row["id"],
        project_id=row["project_id"],
        timer_type=row["timer_type"],
        fire_at=from_iso(row["fire_at"]),
        status=TimerStatus(row["status"]),
        payload=json.loads(row["payload_json"]),
        created_at=from_iso(row["created_at"]),
        fired_at=from_iso(row["fired_at"]) if row["fired_at"] else None,
    )


class TimerRepository:
    def __init__(self, db: Database) -> None:
        self.db = db

    async def schedule_timer(
        self,
        project_id: str,
        timer_type: str,
        fire_at: datetime,
        payload: dict | None = None,
        now: datetime | None = None,
    ) -> int:
        ts = to_iso(now or utc_now())
        fire_iso = to_iso(fire_at)
        payload_str = json.dumps(payload or {})
        async with self.db.connect() as conn:
            cursor = await conn.execute(
                """
                INSERT INTO timers (project_id, timer_type, fire_at, status, payload_json, created_at)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (project_id, timer_type, fire_iso, TimerStatus.PENDING.value, payload_str, ts),
            )
            await conn.commit()
            assert cursor.lastrowid is not None
            return cursor.lastrowid

    async def schedule_plan_approval_timers(
        self,
        project_id: str,
        day_seconds: float = 86400.0,
        now: datetime | None = None,
    ) -> list[int]:
        """Schedule Day 1, 3, 6 reminders + Day 7 auto-park timer."""
        base = now or utc_now()
        schedule = [
            ("PLAN_REMINDER_DAY_1", 1),
            ("PLAN_REMINDER_DAY_3", 3),
            ("PLAN_REMINDER_DAY_6", 6),
            ("PLAN_EXPIRE_DAY_7", 7),
        ]
        ids: list[int] = []
        for timer_type, days in schedule:
            fire_at = base + timedelta(seconds=days * day_seconds)
            timer_id = await self.schedule_timer(
                project_id, timer_type, fire_at, payload={"day": days}, now=base
            )
            ids.append(timer_id)
        return ids

    async def claim_due_timers(self, now: datetime | None = None) -> list[Timer]:
        """Atomically mark due PENDING timers as FIRED and return them."""
        now_iso = to_iso(now or utc_now())
        async with self.db.connect() as conn:
            cursor = await conn.execute(
                """
                SELECT * FROM timers
                WHERE status = ? AND fire_at <= ?
                ORDER BY fire_at ASC, id ASC
                """,
                (TimerStatus.PENDING.value, now_iso),
            )
            rows = await cursor.fetchall()
            if not rows:
                return []

            ids = [r["id"] for r in rows]
            placeholders = ",".join("?" for _ in ids)
            await conn.execute(
                f"""
                UPDATE timers
                SET status = ?, fired_at = ?
                WHERE id IN ({placeholders}) AND status = ?
                """,
                [TimerStatus.FIRED.value, now_iso, *ids, TimerStatus.PENDING.value],
            )
            await conn.commit()

            # Re-read updated rows so fired_at/status match DB
            cursor = await conn.execute(
                f"SELECT * FROM timers WHERE id IN ({placeholders}) ORDER BY fire_at ASC, id ASC",
                ids,
            )
            updated_rows = await cursor.fetchall()
            return [_row_to_timer(r) for r in updated_rows]

    async def cancel_pending_timers(self, project_id: str) -> int:
        async with self.db.connect() as conn:
            cursor = await conn.execute(
                """
                UPDATE timers
                SET status = ?
                WHERE project_id = ? AND status = ?
                """,
                (TimerStatus.CANCELLED.value, project_id, TimerStatus.PENDING.value),
            )
            await conn.commit()
            return cursor.rowcount

    async def schedule_review_timers(
        self,
        project_id: str,
        day_seconds: float = 86400.0,
        now: datetime | None = None,
    ) -> list[int]:
        """Schedule Day 2, 5 review reminders + Day 7 review expiration."""
        base = now or utc_now()
        schedule = [
            ("REVIEW_REMINDER_DAY_2", 2),
            ("REVIEW_REMINDER_DAY_5", 5),
            ("REVIEW_EXPIRE_DAY_7", 7),
        ]
        ids: list[int] = []
        for timer_type, days in schedule:
            fire_at = base + timedelta(seconds=days * day_seconds)
            timer_id = await self.schedule_timer(
                project_id, timer_type, fire_at, payload={"day": days}, now=base
            )
            ids.append(timer_id)
        return ids