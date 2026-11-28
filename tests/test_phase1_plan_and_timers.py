from datetime import datetime, timedelta, timezone
import json
import pytest
from app.agent.planner_llm import RuleBasedMockPlannerLLM
from app.channels.base import ChannelType
from app.channels.gateway import ChannelGateway
from app.channels.simulator import SimulatorChannelAdapter
from app.models.state_machine import ProjectStatus
from app.orchestrator.intake_planner import Phase1Coordinator
from app.scheduler.timer_worker import TimerWorker
from app.storage.db import Database
from app.storage.message_repo import MessageRepository
from app.storage.project_repo import ProjectRepository
from app.storage.timer_repo import TimerRepository


@pytest.mark.asyncio
async def test_phase1_intake_reminders_parking_and_unpark(tmp_path):
    db = Database(tmp_path / "test.db")
    await db.init()
    proj_repo = ProjectRepository(db)
    msg_repo = MessageRepository(db)
    timer_repo = TimerRepository(db)

    wa = SimulatorChannelAdapter(ChannelType.WHATSAPP)
    gateway = ChannelGateway(proj_repo, msg_repo, workspaces_root=tmp_path / "ws")
    gateway.register(wa)

    coordinator = Phase1Coordinator(proj_repo, msg_repo, timer_repo, RuleBasedMockPlannerLLM(), day_seconds=86400.0)
    timer_worker = TimerWorker(proj_repo, msg_repo, timer_repo)

    t0 = datetime(2026, 1, 1, 10, 0, 0, tzinfo=timezone.utc)

    # 1. User sends initial request -> agent asks clarifying questions
    proj_id, _ = await gateway.receive_webhook(
        ChannelType.WHATSAPP,
        {"user_id": "+1555", "message_id": "m1", "text": "Build me a booking app for my salon"},
        now=t0,
    )
    assert proj_id is not None
    status = await coordinator.process_pending_messages(proj_id, now=t0)
    assert status == ProjectStatus.INTAKE
    await gateway.flush_outbox(now=t0)
    assert "quick questions" in wa.sent_messages[-1].body

    # 2. User answers -> spec.md + plan.json written & committed, enters AWAITING_PLAN_APPROVAL
    await gateway.receive_webhook(
        ChannelType.WHATSAPP,
        {"user_id": "+1555", "message_id": "m2", "text": "Guest booking, 24h reminders, daily calendar"},
        now=t0 + timedelta(minutes=5),
    )
    status = await coordinator.process_pending_messages(proj_id, now=t0 + timedelta(minutes=5))
    assert status == ProjectStatus.AWAITING_PLAN_APPROVAL
    await gateway.flush_outbox()
    assert "Reply APPROVE" in wa.sent_messages[-1].body

    proj = await proj_repo.get_project(proj_id)
    assert proj is not None
    plan_data = json.loads((tmp_path / "ws" / proj_id / "plan.json").read_text(encoding="utf-8"))
    assert len(plan_data) == 4
    assert (tmp_path / "ws" / proj_id / "spec.md").exists()

    # 3. Day 1, 3, 6 reminders fire; Day 7 parks the project
    for days in (1, 3, 6):
        await timer_worker.process_due_timers(now=t0 + timedelta(days=days, minutes=10))
        await gateway.flush_outbox()
        assert f"Reminder (Day {days})" in wa.sent_messages[-1].body

    await timer_worker.process_due_timers(now=t0 + timedelta(days=7, minutes=10))
    await gateway.flush_outbox()
    assert "parked this project" in wa.sent_messages[-1].body
    assert (await proj_repo.get_project(proj_id)).status == ProjectStatus.PARKED

    # 4. User comes back on Day 10 and approves -> unparks and enters BUILDING
    await gateway.receive_webhook(
        ChannelType.WHATSAPP,
        {"user_id": "+1555", "message_id": "m3", "text": "APPROVE"},
        now=t0 + timedelta(days=10),
    )
    status = await coordinator.process_pending_messages(proj_id, now=t0 + timedelta(days=10))
    assert status == ProjectStatus.BUILDING


@pytest.mark.asyncio
async def test_phase1_plan_revision_and_cross_channel_approval(tmp_path):
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

    coordinator = Phase1Coordinator(proj_repo, msg_repo, timer_repo, RuleBasedMockPlannerLLM(), day_seconds=86400.0)
    timer_worker = TimerWorker(proj_repo, msg_repo, timer_repo)

    t0 = datetime(2026, 1, 1, 10, 0, 0, tzinfo=timezone.utc)
    proj_id, _ = await gateway.receive_webhook(
        ChannelType.WHATSAPP, {"user_id": "+1555", "message_id": "m1", "text": "Salon app"}, now=t0
    )
    await coordinator.process_pending_messages(proj_id, now=t0)
    await gateway.receive_webhook(
        ChannelType.WHATSAPP, {"user_id": "+1555", "message_id": "m2", "text": "Guest booking"}, now=t0
    )
    await coordinator.process_pending_messages(proj_id, now=t0)
    await gateway.flush_outbox()

    # User requests changes on Day 2 -> replans and resets 7-day timers
    t_day2 = t0 + timedelta(days=2)
    await gateway.receive_webhook(
        ChannelType.WHATSAPP,
        {"user_id": "+1555", "message_id": "m3", "text": "Also add SMS fallback"},
        now=t_day2,
    )
    status = await coordinator.process_pending_messages(proj_id, now=t_day2)
    assert status == ProjectStatus.AWAITING_PLAN_APPROVAL
    spec_text = (tmp_path / "ws" / proj_id / "spec.md").read_text(encoding="utf-8")
    assert "Also add SMS fallback" in spec_text

    # Teammate links Slack on Day 4 and approves -> cancels reminders, moves to BUILDING
    t_day4 = t0 + timedelta(days=4)
    await gateway.receive_webhook(
        ChannelType.SLACK, {"user_id": "U_BOSS", "message_id": "s1", "text": f"link {proj_id}"}, now=t_day4
    )
    await gateway.receive_webhook(
        ChannelType.SLACK, {"user_id": "U_BOSS", "message_id": "s2", "text": "APPROVE"}, now=t_day4
    )
    status = await coordinator.process_pending_messages(proj_id, now=t_day4)
    assert status == ProjectStatus.BUILDING
    await gateway.flush_outbox()
    assert "Starting the build now" in slack.sent_messages[-1].body

    # Even if time advances to Day 20, cancelled timers never fire
    assert await timer_worker.process_due_timers(now=t0 + timedelta(days=20)) == 0