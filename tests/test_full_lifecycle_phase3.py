from datetime import datetime, timedelta, timezone
import pytest
from app.agent.coder_llm import SimulatedStepCoder
from app.agent.planner_llm import RuleBasedMockPlannerLLM
from app.channels.base import ChannelType
from app.channels.gateway import ChannelGateway
from app.channels.simulator import SimulatorChannelAdapter
from app.models.state_machine import ProjectStatus
from app.orchestrator.builder import Phase2Builder
from app.orchestrator.engine import AgentOrchestrator
from app.orchestrator.intake_planner import Phase1Coordinator
from app.orchestrator.reviewer import Phase3Reviewer
from app.sandbox.deployer import LocalSimulatedDeployer
from app.scheduler.timer_worker import TimerWorker
from app.storage.db import Database
from app.storage.message_repo import MessageRepository
from app.storage.project_repo import ProjectRepository
from app.storage.timer_repo import TimerRepository

from app.models.state_machine import StepStatus
from app.sandbox.deployer import Deployer, DeploymentResult
from app.sandbox.workspace import GitWorkspace
from app.scheduler.recovery import CrashRecoveryService

@pytest.mark.asyncio
async def test_full_end_to_end_whatsapp_to_slack_multi_round_deploy(tmp_path):
    db = Database(tmp_path / "test.db")
    await db.init()
    proj_repo = ProjectRepository(db)
    msg_repo = MessageRepository(db)
    timer_repo = TimerRepository(db)

    wa = SimulatorChannelAdapter(ChannelType.WHATSAPP)
    slack = SimulatorChannelAdapter(ChannelType.SLACK)
    gateway = ChannelGateway(proj_repo, msg_repo, workspaces_root=tmp_path / "ws")
    gateway.register(wa)
    gateway.register(slack)

    planner = RuleBasedMockPlannerLLM()
    coder = SimulatedStepCoder()
    deployer = LocalSimulatedDeployer()

    engine = AgentOrchestrator(
        proj_repo,
        msg_repo,
        Phase1Coordinator(proj_repo, msg_repo, timer_repo, planner),
        Phase2Builder(proj_repo, msg_repo, coder),
        Phase3Reviewer(proj_repo, msg_repo, timer_repo, planner, deployer),
    )
    timer_worker = TimerWorker(proj_repo, msg_repo, timer_repo)

    t0 = datetime(2026, 1, 1, 9, 0, 0, tzinfo=timezone.utc)

    # 1. Owner starts on WhatsApp -> Q&A -> Plan -> APPROVE -> Round 1 builds -> Preview URL sent to WhatsApp
    proj_id, _ = await gateway.receive_webhook(
        ChannelType.WHATSAPP, {"user_id": "+1555", "message_id": "w1", "text": "Salon booking app"}, now=t0
    )
    assert await engine.tick_project(proj_id, now=t0) == ProjectStatus.INTAKE

    await gateway.receive_webhook(
        ChannelType.WHATSAPP, {"user_id": "+1555", "message_id": "w2", "text": "Guest booking + 24h reminders"}, now=t0
    )
    assert await engine.tick_project(proj_id, now=t0) == ProjectStatus.AWAITING_PLAN_APPROVAL

    await gateway.receive_webhook(
        ChannelType.WHATSAPP, {"user_id": "+1555", "message_id": "w3", "text": "APPROVE"}, now=t0
    )
    assert await engine.tick_project(proj_id, now=t0) == ProjectStatus.AWAITING_REVIEW
    await gateway.flush_outbox(now=t0)
    assert f"preview-{proj_id}-r1" in wa.sent_messages[-1].body

    # 2. Day 2 review reminder goes to WhatsApp
    await timer_worker.process_due_timers(now=t0 + timedelta(days=2, minutes=1))
    await gateway.flush_outbox()
    assert "Review reminder (Day 2)" in wa.sent_messages[-1].body

    # 3. Day 5: Teammate links Slack and requests a change -> Round 2 builds & sends preview to Slack!
    t_day5 = t0 + timedelta(days=5)
    await gateway.receive_webhook(
        ChannelType.SLACK, {"user_id": "U_TEAM", "message_id": "s1", "text": f"link {proj_id}"}, now=t_day5
    )
    await gateway.receive_webhook(
        ChannelType.SLACK,
        {"user_id": "U_TEAM", "message_id": "s2", "text": "Also add email confirmation"},
        now=t_day5,
    )
    assert await engine.tick_project(proj_id, now=t_day5) == ProjectStatus.AWAITING_REVIEW
    await gateway.flush_outbox(now=t_day5)

    proj = await proj_repo.get_project(proj_id)
    assert proj is not None
    assert proj.build_round == 2
    assert any(f"preview-{proj_id}-r2" in m.body for m in slack.sent_messages)

    # 4. Teammate replies APPROVE on Slack -> Production deployed & tagged!
    await gateway.receive_webhook(
        ChannelType.SLACK, {"user_id": "U_TEAM", "message_id": "s3", "text": "APPROVE"}, now=t_day5
    )
    assert await engine.tick_project(proj_id, now=t_day5) == ProjectStatus.DEPLOYED
    await gateway.flush_outbox(now=t_day5)
    assert f"https://{proj_id}.openclaw.local" in slack.sent_messages[-1].body
    assert "prod-round-2" in slack.sent_messages[-1].body

class FailingPreviewDeployer(Deployer):
    async def deploy_preview(self, project_id: str, workspace_path: str, build_round: int) -> DeploymentResult:
        return DeploymentResult(
            environment="preview",
            url=f"https://preview-{project_id}-r{build_round}.openclaw.local",
            healthy=False,
            error="HTTP 500 on /health",
        )

    async def deploy_production(self, project_id: str, workspace_path: str, build_round: int) -> DeploymentResult:
        raise AssertionError("Production deploy must not run when preview fails")


def _build_stack(tmp_path, deployer: Deployer | None = None):
    db = Database(tmp_path / "test.db")
    proj_repo = ProjectRepository(db)
    msg_repo = MessageRepository(db)
    timer_repo = TimerRepository(db)

    wa = SimulatorChannelAdapter(ChannelType.WHATSAPP)
    slack = SimulatorChannelAdapter(ChannelType.SLACK)
    gateway = ChannelGateway(proj_repo, msg_repo, workspaces_root=tmp_path / "ws")
    gateway.register(wa)
    gateway.register(slack)

    planner = RuleBasedMockPlannerLLM()
    coder = SimulatedStepCoder()
    used_deployer = deployer or LocalSimulatedDeployer()

    engine = AgentOrchestrator(
        proj_repo,
        msg_repo,
        Phase1Coordinator(proj_repo, msg_repo, timer_repo, planner),
        Phase2Builder(proj_repo, msg_repo, coder),
        Phase3Reviewer(proj_repo, msg_repo, timer_repo, planner, used_deployer),
    )
    timer_worker = TimerWorker(proj_repo, msg_repo, timer_repo)
    return db, proj_repo, msg_repo, timer_repo, gateway, wa, slack, engine, timer_worker


@pytest.mark.asyncio
async def test_review_day7_timeout_parks_and_late_approve_deploys(tmp_path):
    db, proj_repo, _, _, gateway, wa, slack, engine, timer_worker = _build_stack(tmp_path)
    await db.init()

    t0 = datetime(2026, 1, 1, 9, 0, 0, tzinfo=timezone.utc)
    proj_id, _ = await gateway.receive_webhook(
        ChannelType.WHATSAPP, {"user_id": "+1555", "message_id": "w1", "text": "Salon app"}, now=t0
    )
    await engine.tick_project(proj_id, now=t0)
    await gateway.receive_webhook(
        ChannelType.WHATSAPP, {"user_id": "+1555", "message_id": "w2", "text": "Guest booking"}, now=t0
    )
    await engine.tick_project(proj_id, now=t0)
    await gateway.receive_webhook(
        ChannelType.WHATSAPP, {"user_id": "+1555", "message_id": "w3", "text": "APPROVE"}, now=t0
    )
    assert await engine.tick_project(proj_id, now=t0) == ProjectStatus.AWAITING_REVIEW

    # Status query during review must NOT trigger Round 2
    await gateway.receive_webhook(
        ChannelType.WHATSAPP, {"user_id": "+1555", "message_id": "w4", "text": "what's the status?"}, now=t0
    )
    assert await engine.tick_project(proj_id, now=t0) == ProjectStatus.AWAITING_REVIEW
    assert (await proj_repo.get_project(proj_id)).build_round == 1

    # Day 5 reminder & Day 7 auto-park
    await timer_worker.process_due_timers(now=t0 + timedelta(days=5, minutes=1))
    await gateway.flush_outbox()
    assert "Review reminder (Day 5)" in wa.sent_messages[-1].body

    await timer_worker.process_due_timers(now=t0 + timedelta(days=7, minutes=1))
    await gateway.flush_outbox()
    assert (await proj_repo.get_project(proj_id)).status == ProjectStatus.PARKED
    assert "parked this project" in wa.sent_messages[-1].body

    # Day 12: User replies APPROVE on Slack -> unparks to AWAITING_REVIEW -> deploys to production!
    t_day12 = t0 + timedelta(days=12)
    await gateway.receive_webhook(
        ChannelType.SLACK, {"user_id": "U1", "message_id": "s1", "text": f"link {proj_id}"}, now=t_day12
    )
    await gateway.receive_webhook(
        ChannelType.SLACK, {"user_id": "U1", "message_id": "s2", "text": "APPROVE"}, now=t_day12
    )
    assert await engine.tick_project(proj_id, now=t_day12) == ProjectStatus.DEPLOYED
    await gateway.flush_outbox()
    assert f"https://{proj_id}.openclaw.local" in slack.sent_messages[-1].body


@pytest.mark.asyncio
async def test_paused_mid_build_parked_resumes_building_and_round2_crash_recovery(tmp_path):
    db, proj_repo, _, _, gateway, _, _, engine, _ = _build_stack(tmp_path)
    await db.init()

    t0 = datetime(2026, 1, 1, 9, 0, 0, tzinfo=timezone.utc)
    proj_id, _ = await gateway.receive_webhook(
        ChannelType.WHATSAPP, {"user_id": "+1555", "message_id": "w1", "text": "Salon app"}, now=t0
    )
    await engine.tick_project(proj_id, now=t0)
    await gateway.receive_webhook(
        ChannelType.WHATSAPP, {"user_id": "+1555", "message_id": "w2", "text": "Guest booking"}, now=t0
    )
    await engine.tick_project(proj_id, now=t0)
    await gateway.receive_webhook(
        ChannelType.WHATSAPP, {"user_id": "+1555", "message_id": "w3", "text": "APPROVE"}, now=t0
    )

    # Build only Step 1, then pause and park
    assert await engine.tick_project(proj_id, max_build_steps=1, now=t0) == ProjectStatus.BUILDING
    await gateway.receive_webhook(
        ChannelType.WHATSAPP, {"user_id": "+1555", "message_id": "w4", "text": "pause"}, now=t0
    )
    assert await engine.tick_project(proj_id, now=t0) == ProjectStatus.PAUSED
    await proj_repo.transition_status(proj_id, ProjectStatus.PAUSED, ProjectStatus.PARKED, now=t0)

    # User replies later -> unparks straight to BUILDING and finishes remaining steps (not AWAITING_PLAN_APPROVAL)
    await gateway.receive_webhook(
        ChannelType.WHATSAPP, {"user_id": "+1555", "message_id": "w5", "text": "continue"}, now=t0
    )
    assert await engine.tick_project(proj_id, now=t0) == ProjectStatus.AWAITING_REVIEW

    # Start Round 2 change, simulate crash mid-Round 2, recover, and finish Round 2
    await gateway.receive_webhook(
        ChannelType.WHATSAPP, {"user_id": "+1555", "message_id": "w6", "text": "Add dark mode"}, now=t0
    )
    # Run Round 2 in REVIEW -> transitions to BUILDING and executes 0 steps (max_build_steps=0)
    assert await engine.tick_project(proj_id, max_build_steps=0, now=t0) == ProjectStatus.BUILDING

    proj = await proj_repo.get_project(proj_id)
    assert proj is not None and proj.build_round == 2
    r2_steps = await proj_repo.get_plan_steps(proj_id, build_round=2)
    await proj_repo.acquire_lease(proj_id, "dead-worker", lease_seconds=30, now=t0)
    await proj_repo.update_step_status(r2_steps[0].id, proj_id, StepStatus.IN_PROGRESS, increment_attempt=True, now=t0)
    await GitWorkspace(proj.workspace_path).write_file("broken_r2.py", "CRASH")

    t_recover = t0 + timedelta(seconds=60)
    assert await CrashRecoveryService(proj_repo).recover_stale_projects(now=t_recover) == [proj_id]
    assert not (tmp_path / "ws" / proj_id / "broken_r2.py").exists()
    assert await engine.tick_project(proj_id, worker_id="live-worker", now=t_recover) == ProjectStatus.AWAITING_REVIEW


@pytest.mark.asyncio
async def test_preview_healthcheck_failure_moves_to_failed_and_notifies_user(tmp_path):
    db, _, _, _, gateway, wa, _, engine, _ = _build_stack(tmp_path, deployer=FailingPreviewDeployer())
    await db.init()

    proj_id, _ = await gateway.receive_webhook(
        ChannelType.WHATSAPP, {"user_id": "+1555", "message_id": "w1", "text": "Salon app"}
    )
    await engine.tick_project(proj_id)
    await gateway.receive_webhook(
        ChannelType.WHATSAPP, {"user_id": "+1555", "message_id": "w2", "text": "Guest booking"}
    )
    await engine.tick_project(proj_id)
    await gateway.receive_webhook(
        ChannelType.WHATSAPP, {"user_id": "+1555", "message_id": "w3", "text": "APPROVE"}
    )

    assert await engine.tick_project(proj_id) == ProjectStatus.FAILED
    await gateway.flush_outbox()
    assert "healthcheck failed" in wa.sent_messages[-1].body