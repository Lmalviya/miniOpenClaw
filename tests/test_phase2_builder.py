from datetime import datetime, timedelta, timezone
import pytest
from app.agent.coder_llm import SimulatedStepCoder
from app.agent.planner_llm import RuleBasedMockPlannerLLM
from app.channels.base import ChannelType
from app.channels.gateway import ChannelGateway
from app.channels.simulator import SimulatorChannelAdapter
from app.models.state_machine import ProjectStatus, StepStatus
from app.orchestrator.builder import Phase2Builder
from app.orchestrator.intake_planner import Phase1Coordinator
from app.sandbox.workspace import GitWorkspace
from app.scheduler.recovery import CrashRecoveryService
from app.storage.db import Database
from app.storage.message_repo import MessageRepository
from app.storage.project_repo import ProjectRepository
from app.storage.timer_repo import TimerRepository


async def _setup_approved_project(tmp_path):
    db = Database(tmp_path / "test.db")
    await db.init()
    proj_repo = ProjectRepository(db)
    msg_repo = MessageRepository(db)
    timer_repo = TimerRepository(db)

    wa = SimulatorChannelAdapter(ChannelType.WHATSAPP)
    gateway = ChannelGateway(proj_repo, msg_repo, workspaces_root=tmp_path / "ws")
    gateway.register(wa)

    p1 = Phase1Coordinator(proj_repo, msg_repo, timer_repo, RuleBasedMockPlannerLLM())
    proj_id, _ = await gateway.receive_webhook(
        ChannelType.WHATSAPP, {"user_id": "+1555", "message_id": "m1", "text": "Salon app"}
    )
    await p1.process_pending_messages(proj_id)
    await gateway.receive_webhook(
        ChannelType.WHATSAPP, {"user_id": "+1555", "message_id": "m2", "text": "Guest booking"}
    )
    await p1.process_pending_messages(proj_id)
    await gateway.receive_webhook(
        ChannelType.WHATSAPP, {"user_id": "+1555", "message_id": "m3", "text": "APPROVE"}
    )
    await p1.process_pending_messages(proj_id)
    await gateway.flush_outbox()
    return db, proj_repo, msg_repo, gateway, wa, proj_id


@pytest.mark.asyncio
async def test_phase2_self_healing_interrupts_pause_and_resume(tmp_path):
    db, proj_repo, msg_repo, gateway, wa, proj_id = await _setup_approved_project(tmp_path)

    coder = SimulatedStepCoder(self_heal_on_step=1)
    builder = Phase2Builder(proj_repo, msg_repo, coder)

    # 1. Run Step 1 only (self-heals failing test, updates progress.md, commits to git)
    status = await builder.run_build(proj_id, max_steps=1)
    assert status == ProjectStatus.BUILDING

    steps = await proj_repo.get_plan_steps(proj_id)
    assert steps[0].status == StepStatus.DONE
    assert steps[0].git_commit_sha is not None
    assert steps[1].status == StepStatus.PENDING

    # 2. User asks for status AND sends a style change mid-build
    await gateway.receive_webhook(
        ChannelType.WHATSAPP, {"user_id": "+1555", "message_id": "m4", "text": "what's the status?"}
    )
    await gateway.receive_webhook(
        ChannelType.WHATSAPP, {"user_id": "+1555", "message_id": "m5", "text": "use blue as the main colour"}
    )

    # Run Step 2 -> picks up both messages before starting Step 2
    status = await builder.run_build(proj_id, max_steps=1)
    assert status == ProjectStatus.BUILDING
    await gateway.flush_outbox()

    sent_texts = [m.body for m in wa.sent_messages]
    assert any("1/4 steps done" in t for t in sent_texts)
    assert any("use blue as the main colour" in t for t in sent_texts)
    # Step 2 received the override inside its fresh context
    _, step2_ctx = coder.seen_contexts[1]
    assert "use blue as the main colour" in step2_ctx.overrides_markdown

    # 3. User pauses build -> builder stops cleanly
    await gateway.receive_webhook(
        ChannelType.WHATSAPP, {"user_id": "+1555", "message_id": "m6", "text": "pause"}
    )
    status = await builder.run_build(proj_id)
    assert status == ProjectStatus.PAUSED

    # 4. User resumes -> finishes remaining steps 3 & 4 -> AWAITING_REVIEW
    await gateway.receive_webhook(
        ChannelType.WHATSAPP, {"user_id": "+1555", "message_id": "m7", "text": "resume"}
    )
    status = await builder.run_build(proj_id)
    assert status == ProjectStatus.AWAITING_REVIEW

    final_steps = await proj_repo.get_plan_steps(proj_id)
    assert all(s.status == StepStatus.DONE for s in final_steps)


@pytest.mark.asyncio
async def test_phase2_crash_mid_step_recovers_and_continues(tmp_path):
    db, proj_repo, msg_repo, _, _, proj_id = await _setup_approved_project(tmp_path)
    t0 = datetime(2026, 1, 1, 12, 0, 0, tzinfo=timezone.utc)

    coder = SimulatedStepCoder()
    builder = Phase2Builder(proj_repo, msg_repo, coder)

    # Step 1 completes
    await builder.run_build(proj_id, max_steps=1, now=t0)

    # Simulate Step 2 crashing partway through
    proj = await proj_repo.get_project(proj_id)
    assert proj is not None
    steps = await proj_repo.get_plan_steps(proj_id)
    await proj_repo.acquire_lease(proj_id, "crashed-worker", lease_seconds=30, now=t0)
    await proj_repo.update_step_status(steps[1].id, proj_id, StepStatus.IN_PROGRESS, increment_attempt=True, now=t0)
    ws = GitWorkspace(proj.workspace_path)
    await ws.write_file("step_2.py", "CORRUPT CRASH DATA")

    # Server restarts 60s later -> recovers dirty workspace, then resumes build to completion
    t_restart = t0 + timedelta(seconds=60)
    recovery = CrashRecoveryService(proj_repo)
    assert await recovery.recover_stale_projects(now=t_restart) == [proj_id]

    final_status = await builder.run_build(proj_id, worker_id="recovered-worker", now=t_restart)
    assert final_status == ProjectStatus.AWAITING_REVIEW
    assert all(s.status == StepStatus.DONE for s in await proj_repo.get_plan_steps(proj_id))