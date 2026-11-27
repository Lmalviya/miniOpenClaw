from datetime import datetime, timedelta, timezone
import pytest
from app.models.domain import PlanStepCreate
from app.models.state_machine import ProjectStatus, StepStatus
from app.sandbox.workspace import GitWorkspace, WorkspaceError
from app.scheduler.recovery import CrashRecoveryService
from app.storage.db import Database
from app.storage.project_repo import ProjectRepository


@pytest.mark.asyncio
async def test_workspace_blocks_path_traversal(tmp_path):
    ws = GitWorkspace(tmp_path / "proj")
    await ws.init_repo()
    with pytest.raises(WorkspaceError):
        ws.resolve_safe_path("../../secret.txt")


@pytest.mark.asyncio
async def test_crash_before_commit_resets_dirty_files_and_requeues_step(tmp_path):
    db = Database(tmp_path / "test.db")
    await db.init()
    repo = ProjectRepository(db)

    ws_dir = tmp_path / "ws-1"
    ws = GitWorkspace(ws_dir)
    await ws.init_repo()

    t0 = datetime(2026, 1, 1, 10, 0, 0, tzinfo=timezone.utc)
    await repo.create_project("p1", "Salon", str(ws_dir), "cli", "u1", now=t0)
    await repo.transition_status("p1", ProjectStatus.INTAKE, ProjectStatus.PLANNING, now=t0)
    await repo.transition_status("p1", ProjectStatus.PLANNING, ProjectStatus.AWAITING_PLAN_APPROVAL, now=t0)
    await repo.transition_status("p1", ProjectStatus.AWAITING_PLAN_APPROVAL, ProjectStatus.BUILDING, now=t0)

    steps = await repo.save_plan_steps(
        "p1",
        [
            PlanStepCreate(step_order=1, title="Init app", description="d1", acceptance_criteria="a1"),
            PlanStepCreate(step_order=2, title="Add routes", description="d2", acceptance_criteria="a2"),
        ],
        now=t0,
    )

    # Step 1 completed and committed cleanly
    await ws.write_file("main.py", "print('v1')\n")
    sha1 = await ws.commit_all("[round-1 step-1] Init app")
    await repo.update_step_status(steps[0].id, "p1", StepStatus.DONE, git_commit_sha=sha1, increment_attempt=True, now=t0)

    # Worker starts Step 2, edits main.py + creates untracked broken file, then CRASHES
    await repo.acquire_lease("p1", "dead-worker", lease_seconds=30, now=t0)
    await repo.update_step_status(steps[1].id, "p1", StepStatus.IN_PROGRESS, increment_attempt=True, now=t0)
    await ws.write_file("main.py", "BROKEN HALF-WRITTEN CODE")
    await ws.write_file("garbage_tmp.py", "crash leftover")

    # Server restarts 60s later (lease expired!)
    recovery = CrashRecoveryService(repo)
    recovered = await recovery.recover_stale_projects(now=t0 + timedelta(seconds=60))
    assert recovered == ["p1"]

    # Dirty edits & untracked file were wiped back to Step 1's clean state
    assert await ws.read_file("main.py") == "print('v1')\n"
    assert not (ws_dir / "garbage_tmp.py").exists()

    # Step 2 is back to PENDING ready to retry
    refreshed_steps = await repo.get_plan_steps("p1")
    assert refreshed_steps[0].status == StepStatus.DONE
    assert refreshed_steps[1].status == StepStatus.PENDING


@pytest.mark.asyncio
async def test_crash_after_commit_marks_step_done_without_rerun(tmp_path):
    db = Database(tmp_path / "test.db")
    await db.init()
    repo = ProjectRepository(db)

    ws_dir = tmp_path / "ws-2"
    ws = GitWorkspace(ws_dir)
    await ws.init_repo()

    t0 = datetime(2026, 1, 1, 10, 0, 0, tzinfo=timezone.utc)
    await repo.create_project("p2", "Salon", str(ws_dir), "cli", "u1", now=t0)
    await repo.transition_status("p2", ProjectStatus.INTAKE, ProjectStatus.PLANNING, now=t0)
    await repo.transition_status("p2", ProjectStatus.PLANNING, ProjectStatus.AWAITING_PLAN_APPROVAL, now=t0)
    await repo.transition_status("p2", ProjectStatus.AWAITING_PLAN_APPROVAL, ProjectStatus.BUILDING, now=t0)

    steps = await repo.save_plan_steps(
        "p2",
        [PlanStepCreate(step_order=1, title="Init app", description="d1", acceptance_criteria="a1")],
        now=t0,
    )

    # Worker commits Step 1, then crashes BEFORE updating SQLite
    await repo.acquire_lease("p2", "dead-worker", lease_seconds=30, now=t0)
    await repo.update_step_status(steps[0].id, "p2", StepStatus.IN_PROGRESS, increment_attempt=True, now=t0)
    await ws.write_file("app.py", "print('ok')\n")
    sha1 = await ws.commit_all("[round-1 step-1] Init app")

    recovery = CrashRecoveryService(repo)
    recovered = await recovery.recover_stale_projects(now=t0 + timedelta(seconds=60))
    assert recovered == ["p2"]

    refreshed_steps = await repo.get_plan_steps("p2")
    assert refreshed_steps[0].status == StepStatus.DONE
    assert refreshed_steps[0].git_commit_sha == sha1