from datetime import datetime, timedelta
import json
from app.models.domain import (
    EventLog,
    PlanStep,
    PlanStepCreate,
    Project,
    from_iso,
    to_iso,
    utc_now,
)
from app.models.state_machine import ProjectStatus, StepStatus, validate_transition
from app.storage.db import Database


class StateConflictError(RuntimeError):
    """Raised when a concurrent update changed the project's status first."""


def _row_to_project(row) -> Project:
    return Project(
        id=row["id"],
        title=row["title"],
        status=ProjectStatus(row["status"]),
        workspace_path=row["workspace_path"],
        preferred_channel=row["preferred_channel"],
        last_active_channel=row["last_active_channel"],
        build_round=row["build_round"],
        locked_by=row["locked_by"],
        lease_expires_at=from_iso(row["lease_expires_at"]) if row["lease_expires_at"] else None,
        heartbeat_at=from_iso(row["heartbeat_at"]) if row["heartbeat_at"] else None,
        created_at=from_iso(row["created_at"]),
        updated_at=from_iso(row["updated_at"]),
    )


def _row_to_step(row) -> PlanStep:
    return PlanStep(
        id=row["id"],
        project_id=row["project_id"],
        step_order=row["step_order"],
        build_round=row["build_round"],
        title=row["title"],
        description=row["description"],
        acceptance_criteria=row["acceptance_criteria"],
        status=StepStatus(row["status"]),
        git_commit_sha=row["git_commit_sha"],
        attempt_count=row["attempt_count"],
        last_error=row["last_error"],
        updated_at=from_iso(row["updated_at"]),
    )


class ProjectRepository:
    def __init__(self, db: Database) -> None:
        self.db = db

    async def create_project(
        self,
        project_id: str,
        title: str,
        workspace_path: str,
        channel: str,
        external_user_id: str,
        now: datetime | None = None,
    ) -> Project:
        ts = to_iso(now or utc_now())
        async with self.db.connect() as conn:
            await conn.execute(
                """
                INSERT INTO projects (
                    id, title, status, workspace_path,
                    preferred_channel, last_active_channel,
                    build_round, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, 1, ?, ?)
                """,
                (project_id, title, ProjectStatus.INTAKE.value, workspace_path, channel, channel, ts, ts),
            )
            await conn.execute(
                """
                INSERT INTO project_members (project_id, channel, external_user_id, role, created_at)
                VALUES (?, ?, ?, 'owner', ?)
                """,
                (project_id, channel, external_user_id, ts),
            )
            await self._insert_event(
                conn, project_id, "PROJECT_CREATED", {"channel": channel, "user": external_user_id}, ts
            )
            await conn.commit()

        project = await self.get_project(project_id)
        assert project is not None
        return project

    async def get_project(self, project_id: str) -> Project | None:
        async with self.db.connect() as conn:
            cursor = await conn.execute("SELECT * FROM projects WHERE id = ?", (project_id,))
            row = await cursor.fetchone()
            return _row_to_project(row) if row else None

    async def transition_status(
        self,
        project_id: str,
        expected_current: ProjectStatus,
        target: ProjectStatus,
        reason: str = "",
        now: datetime | None = None,
    ) -> Project:
        validate_transition(expected_current, target)
        ts = to_iso(now or utc_now())

        async with self.db.connect() as conn:
            cursor = await conn.execute(
                """
                UPDATE projects
                SET status = ?, updated_at = ?
                WHERE id = ? AND status = ?
                """,
                (target.value, ts, project_id, expected_current.value),
            )
            if cursor.rowcount == 0:
                raise StateConflictError(
                    f"Project {project_id} is no longer in status {expected_current}"
                )
            await self._insert_event(
                conn,
                project_id,
                "STATE_CHANGED",
                {"from": expected_current.value, "to": target.value, "reason": reason},
                ts,
            )
            await conn.commit()

        project = await self.get_project(project_id)
        assert project is not None
        return project

    # --- Task A1.2: Worker Leases & Crash Recovery Queries ---

    async def acquire_lease(
        self,
        project_id: str,
        worker_id: str,
        lease_seconds: int = 60,
        now: datetime | None = None,
    ) -> bool:
        current_dt = now or utc_now()
        now_iso = to_iso(current_dt)
        expires_iso = to_iso(current_dt + timedelta(seconds=lease_seconds))

        async with self.db.connect() as conn:
            cursor = await conn.execute(
                """
                UPDATE projects
                SET locked_by = ?, lease_expires_at = ?, heartbeat_at = ?, updated_at = ?
                WHERE id = ?
                  AND (locked_by IS NULL OR locked_by = ? OR lease_expires_at <= ?)
                """,
                (worker_id, expires_iso, now_iso, now_iso, project_id, worker_id, now_iso),
            )
            if cursor.rowcount == 1:
                await self._insert_event(
                    conn, project_id, "LEASE_ACQUIRED", {"worker_id": worker_id, "expires_at": expires_iso}, now_iso
                )
                await conn.commit()
                return True
            return False

    async def renew_lease(
        self,
        project_id: str,
        worker_id: str,
        lease_seconds: int = 60,
        now: datetime | None = None,
    ) -> bool:
        current_dt = now or utc_now()
        now_iso = to_iso(current_dt)
        expires_iso = to_iso(current_dt + timedelta(seconds=lease_seconds))

        async with self.db.connect() as conn:
            cursor = await conn.execute(
                """
                UPDATE projects
                SET lease_expires_at = ?, heartbeat_at = ?
                WHERE id = ? AND locked_by = ?
                """,
                (expires_iso, now_iso, project_id, worker_id),
            )
            await conn.commit()
            return cursor.rowcount == 1

    async def release_lease(
        self,
        project_id: str,
        worker_id: str,
        now: datetime | None = None,
    ) -> None:
        now_iso = to_iso(now or utc_now())
        async with self.db.connect() as conn:
            cursor = await conn.execute(
                """
                UPDATE projects
                SET locked_by = NULL, lease_expires_at = NULL, heartbeat_at = NULL, updated_at = ?
                WHERE id = ? AND locked_by = ?
                """,
                (now_iso, project_id, worker_id),
            )
            if cursor.rowcount == 1:
                await self._insert_event(
                    conn, project_id, "LEASE_RELEASED", {"worker_id": worker_id}, now_iso
                )
                await conn.commit()

    async def find_stale_active_projects(self, now: datetime | None = None) -> list[Project]:
        """Find BUILDING/PLANNING projects whose worker crashed or lost its lease."""
        now_iso = to_iso(now or utc_now())
        async with self.db.connect() as conn:
            cursor = await conn.execute(
                """
                SELECT * FROM projects
                WHERE status IN (?, ?)
                  AND (locked_by IS NULL OR lease_expires_at <= ?)
                """,
                (ProjectStatus.PLANNING.value, ProjectStatus.BUILDING.value, now_iso),
            )
            rows = await cursor.fetchall()
            return [_row_to_project(r) for r in rows]

    # --- Plan Steps & Events ---

    async def save_plan_steps(
        self,
        project_id: str,
        steps: list[PlanStepCreate],
        build_round: int = 1,
        now: datetime | None = None,
    ) -> list[PlanStep]:
        ts = to_iso(now or utc_now())
        async with self.db.connect() as conn:
            for step in steps:
                await conn.execute(
                    """
                    INSERT INTO plan_steps (
                        project_id, step_order, build_round, title,
                        description, acceptance_criteria, status, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        project_id,
                        step.step_order,
                        build_round,
                        step.title,
                        step.description,
                        step.acceptance_criteria,
                        StepStatus.PENDING.value,
                        ts,
                    ),
                )
            await self._insert_event(
                conn, project_id, "PLAN_SAVED", {"build_round": build_round, "step_count": len(steps)}, ts
            )
            await conn.commit()
        return await self.get_plan_steps(project_id, build_round)

    async def get_plan_steps(self, project_id: str, build_round: int = 1) -> list[PlanStep]:
        async with self.db.connect() as conn:
            cursor = await conn.execute(
                """
                SELECT * FROM plan_steps
                WHERE project_id = ? AND build_round = ?
                ORDER BY step_order ASC
                """,
                (project_id, build_round),
            )
            rows = await cursor.fetchall()
            return [_row_to_step(r) for r in rows]

    async def update_step_status(
        self,
        step_id: int,
        project_id: str,
        status: StepStatus,
        git_commit_sha: str | None = None,
        last_error: str | None = None,
        increment_attempt: bool = False,
        now: datetime | None = None,
    ) -> None:
        ts = to_iso(now or utc_now())
        attempt_expr = "attempt_count + 1" if increment_attempt else "attempt_count"
        async with self.db.connect() as conn:
            await conn.execute(
                f"""
                UPDATE plan_steps
                SET status = ?,
                    git_commit_sha = COALESCE(?, git_commit_sha),
                    last_error = ?,
                    attempt_count = {attempt_expr},
                    updated_at = ?
                WHERE id = ? AND project_id = ?
                """,
                (status.value, git_commit_sha, last_error, ts, step_id, project_id),
            )
            await self._insert_event(
                conn,
                project_id,
                "STEP_UPDATED",
                {"step_id": step_id, "status": status.value, "commit": git_commit_sha},
                ts,
            )
            await conn.commit()

    async def list_events(self, project_id: str) -> list[EventLog]:
        async with self.db.connect() as conn:
            cursor = await conn.execute(
                "SELECT * FROM events_log WHERE project_id = ? ORDER BY id ASC",
                (project_id,),
            )
            rows = await cursor.fetchall()
            return [
                EventLog(
                    id=r["id"],
                    project_id=r["project_id"],
                    event_type=r["event_type"],
                    details=json.loads(r["details_json"]),
                    created_at=from_iso(r["created_at"]),
                )
                for r in rows
            ]
    async def log_event(
        self,
        project_id: str,
        event_type: str,
        details: dict | None = None,
        now: datetime | None = None,
    ) -> None:
        ts = to_iso(now or utc_now())
        async with self.db.connect() as conn:
            await self._insert_event(conn, project_id, event_type, details or {}, ts)
            await conn.commit()

    async def update_last_active_channel(
        self,
        project_id: str,
        channel: str,
        now: datetime | None = None,
    ) -> None:
        ts = to_iso(now or utc_now())
        async with self.db.connect() as conn:
            await conn.execute(
                """
                UPDATE projects
                SET last_active_channel = ?, updated_at = ?
                WHERE id = ?
                """,
                (channel, ts, project_id),
            )
            await conn.commit()

    async def replace_plan_steps(
        self, 
        project_id:str,
        steps: list[PlanStepCreate],
        build_round:int = 1,
        now: datetime|None=None
    ) -> list[PlanStep]:
        async with self.db.connect() as conn:
            await conn.execute(
                "DELETE FROM plan_steps WHERE project_id = ? AND build_round = ?",
                (project_id, build_round)
            )
            await conn.commit()
        return await self.save_plan_steps(project_id, steps, build_round=build_round, now=now)

    async def _insert_event(
        self, conn, project_id: str, event_type: str, details: dict, ts: str
    ) -> None:
        await conn.execute(
            """
            INSERT INTO events_log (project_id, event_type, details_json, created_at)
            VALUES (?, ?, ?, ?)
            """,
            (project_id, event_type, json.dumps(details), ts),
        )