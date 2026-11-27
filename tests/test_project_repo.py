from datetime import datetime, timedelta, timezone
import pytest
from app.models.domain import PlanStepCreate
from app.models.state_machine import ProjectStatus, StepStatus
from app.storage.db import Database
from app.storage.project_repo import ProjectRepository, StateConflictError


@pytest.mark.asyncio
async def test_project_lifecycle_and_conflict(tmp_path):
    db = Database(tmp_path / "test.db")
    await db.init()
    repo = ProjectRepository(db)

    p = await repo.create_project("proj-1", "Salon App", "/tmp/proj-1", "whatsapp", "+15550001")
    assert p.status == ProjectStatus.INTAKE

    p = await repo.transition_status("proj-1", ProjectStatus.INTAKE, ProjectStatus.PLANNING)
    assert p.status == ProjectStatus.PLANNING

    # Simulates a stale timer/webhook expecting INTAKE after it already moved to PLANNING
    with pytest.raises(StateConflictError):
        await repo.transition_status("proj-1", ProjectStatus.INTAKE, ProjectStatus.PARKED)

    events = await repo.list_events("proj-1")
    assert [e.event_type for e in events] == ["PROJECT_CREATED", "STATE_CHANGED"]


@pytest.mark.asyncio
async def test_worker_lease_prevents_double_work_and_recovers_after_expiry(tmp_path):
    db = Database(tmp_path / "test.db")
    await db.init()
    repo = ProjectRepository(db)

    t0 = datetime(2026, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
    await repo.create_project("proj-2", "Booking App", "/tmp/proj-2", "slack", "U123", now=t0)
    await repo.transition_status("proj-2", ProjectStatus.INTAKE, ProjectStatus.PLANNING, now=t0)
    await repo.transition_status("proj-2", ProjectStatus.PLANNING, ProjectStatus.AWAITING_PLAN_APPROVAL, now=t0)
    await repo.transition_status("proj-2", ProjectStatus.AWAITING_PLAN_APPROVAL, ProjectStatus.BUILDING, now=t0)

    # Worker A claims a 60s lease
    assert await repo.acquire_lease("proj-2", "worker-A", lease_seconds=60, now=t0) is True
    # Worker B cannot steal it 30s later
    assert await repo.acquire_lease("proj-2", "worker-B", lease_seconds=60, now=t0 + timedelta(seconds=30)) is False
    assert await repo.find_stale_active_projects(now=t0 + timedelta(seconds=30)) == []

    # Fast-forward 61s: Worker A crashed without releasing!
    t_after_crash = t0 + timedelta(seconds=61)
    stale = await repo.find_stale_active_projects(now=t_after_crash)
    assert [p.id for p in stale] == ["proj-2"]

    # Worker B reclaims the expired lease
    assert await repo.acquire_lease("proj-2", "worker-B", lease_seconds=60, now=t_after_crash) is True
    await repo.release_lease("proj-2", "worker-B", now=t_after_crash)


@pytest.mark.asyncio
async def test_plan_steps_checkpointing(tmp_path):
    db = Database(tmp_path / "test.db")
    await db.init()
    repo = ProjectRepository(db)

    await repo.create_project("proj-3", "Test App", "/tmp/proj-3", "cli", "local-user")
    steps = await repo.save_plan_steps(
        "proj-3",
        [
            PlanStepCreate(
                step_order=1,
                title="Setup FastAPI",
                description="Create app skeleton",
                acceptance_criteria="GET /health returns 200",
            )
        ],
    )
    assert len(steps) == 1
    assert steps[0].status == StepStatus.PENDING

    await repo.update_step_status(
        steps[0].id,
        "proj-3",
        StepStatus.DONE,
        git_commit_sha="abc1234",
        increment_attempt=True,
    )
    updated = await repo.get_plan_steps("proj-3")
    assert updated[0].status == StepStatus.DONE
    assert updated[0].git_commit_sha == "abc1234"
    assert updated[0].attempt_count == 1