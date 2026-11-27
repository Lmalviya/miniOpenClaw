from datetime import datetime, timedelta, timezone
import pytest
from app.storage.db import Database
from app.storage.message_repo import MessageRepository
from app.storage.project_repo import ProjectRepository
from app.storage.timer_repo import TimerRepository


@pytest.mark.asyncio
async def test_inbox_and_outbox_idempotency(tmp_path):
    db = Database(tmp_path / "test.db")
    await db.init()
    proj_repo = ProjectRepository(db)
    msg_repo = MessageRepository(db)

    await proj_repo.create_project("proj-1", "Salon App", "/tmp/proj-1", "whatsapp", "+1555111")

    # Same webhook delivered twice -> only first creates a new row
    m1, created1 = await msg_repo.enqueue_inbox(
        "whatsapp", "+1555111", "wa-msg-999", "pause", project_id="proj-1"
    )
    m2, created2 = await msg_repo.enqueue_inbox(
        "whatsapp", "+1555111", "wa-msg-999", "pause", project_id="proj-1"
    )
    assert created1 is True
    assert created2 is False
    assert m1.id == m2.id

    pending = await msg_repo.get_pending_inbox("proj-1")
    assert len(pending) == 1
    await msg_repo.mark_inbox_processed(m1.id)
    assert await msg_repo.get_pending_inbox("proj-1") == []

    # Outbox idempotency
    o1, o_created1 = await msg_repo.enqueue_outbox(
        "proj-1", "whatsapp", "+1555111", "reminder-day-1", "Still waiting for APPROVE"
    )
    _, o_created2 = await msg_repo.enqueue_outbox(
        "proj-1", "whatsapp", "+1555111", "reminder-day-1", "Still waiting for APPROVE"
    )
    assert o_created1 is True
    assert o_created2 is False

    await msg_repo.mark_outbox_sent(o1.id)
    assert await msg_repo.get_pending_outbox() == []


@pytest.mark.asyncio
async def test_seven_day_approval_timers_and_cancellation(tmp_path):
    db = Database(tmp_path / "test.db")
    await db.init()
    proj_repo = ProjectRepository(db)
    timer_repo = TimerRepository(db)

    await proj_repo.create_project("proj-2", "Salon App", "/tmp/proj-2", "slack", "U100")
    t0 = datetime(2026, 1, 1, 9, 0, 0, tzinfo=timezone.utc)

    await timer_repo.schedule_plan_approval_timers("proj-2", day_seconds=86400.0, now=t0)

    # At +12 hours: nothing due yet
    assert await timer_repo.claim_due_timers(now=t0 + timedelta(hours=12)) == []

    # At +1 day +1 second: only Day 1 fires
    due_day1 = await timer_repo.claim_due_timers(now=t0 + timedelta(days=1, seconds=1))
    assert [t.timer_type for t in due_day1] == ["PLAN_REMINDER_DAY_1"]

    # Claiming again at same timestamp returns empty (cannot fire twice)
    assert await timer_repo.claim_due_timers(now=t0 + timedelta(days=1, seconds=1)) == []

    # User approves on Day 2 -> cancel Day 3, 6, 7 timers
    cancelled = await timer_repo.cancel_pending_timers("proj-2")
    assert cancelled == 3

    # Even at Day 10, nothing left fires
    assert await timer_repo.claim_due_timers(now=t0 + timedelta(days=10)) == []