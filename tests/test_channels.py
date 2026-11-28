import pytest
from app.channels.base import ChannelType
from app.channels.gateway import ChannelGateway
from app.channels.simulator import SimulatorChannelAdapter
from app.storage.db import Database
from app.storage.message_repo import MessageRepository
from app.storage.project_repo import ProjectRepository


@pytest.mark.asyncio
async def test_cross_channel_teammate_linking_and_outbox_retry(tmp_path):
    db = Database(tmp_path / "test.db")
    await db.init()
    proj_repo = ProjectRepository(db)
    msg_repo = MessageRepository(db)

    wa = SimulatorChannelAdapter(ChannelType.WHATSAPP)
    slack = SimulatorChannelAdapter(ChannelType.SLACK)

    gateway = ChannelGateway(proj_repo, msg_repo, workspaces_root=tmp_path / "ws")
    gateway.register(wa)
    gateway.register(slack)

    # 1. Owner starts on WhatsApp
    proj_id, created = await gateway.receive_webhook(
        ChannelType.WHATSAPP,
        {"user_id": "+15551234", "message_id": "w1", "text": "Build me a salon booking app"},
    )
    assert proj_id is not None
    assert created is True

    # Duplicate webhook is ignored
    proj_id_dup, created_dup = await gateway.receive_webhook(
        ChannelType.WHATSAPP,
        {"user_id": "+15551234", "message_id": "w1", "text": "Build me a salon booking app"},
    )
    assert proj_id_dup == proj_id
    assert created_dup is False

    # 2. Teammate links from Slack 5 days later
    linked_id, link_created = await gateway.receive_webhook(
        ChannelType.SLACK,
        {"user_id": "U_TEAMMATE", "message_id": "s1", "text": f"link {proj_id}"},
    )
    assert linked_id == proj_id
    assert link_created is True

    # Verify last_active_channel switched to slack
    proj = await proj_repo.get_project(proj_id)
    assert proj is not None
    assert proj.last_active_channel == "slack"

    # 3. Teammate replies APPROVE on Slack -> routed to the same project inbox
    approve_proj_id, _ = await gateway.receive_webhook(
        ChannelType.SLACK,
        {"user_id": "U_TEAMMATE", "message_id": "s2", "text": "APPROVE"},
    )
    assert approve_proj_id == proj_id

    pending_inbox = await msg_repo.get_pending_inbox(proj_id)
    assert [m.body for m in pending_inbox] == ["Build me a salon booking app", "APPROVE"]

    # 4. Simulate transient Slack failure on first flush, then success on retry
    slack.fail_next_send = True
    assert await gateway.flush_outbox() == 0
    assert await gateway.flush_outbox() == 1
    assert len(slack.sent_messages) == 1
    assert "Linked to project" in slack.sent_messages[0].body