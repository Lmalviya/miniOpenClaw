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