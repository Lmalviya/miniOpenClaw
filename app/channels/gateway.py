from datetime import datetime
from pathlib import Path
import uuid
from app.channels.base import ChannelAdapter, ChannelType
from app.models.domain import utc_now
from app.storage.message_repo import MessageRepository
from app.storage.project_repo import ProjectRepository


class ChannelGateway:
    def __init__(
        self,
        project_repo: ProjectRepository,
        message_repo: MessageRepository,
        workspaces_root: str | Path = "workspaces",
    ) -> None:
        self.project_repo = project_repo
        self.message_repo = message_repo
        self.workspaces_root = Path(workspaces_root)
        self.adapters: dict[ChannelType, ChannelAdapter] = {}

    def register(self, adapter: ChannelAdapter) -> None:
        self.adapters[adapter.channel_type] = adapter

    async def receive_webhook(
        self,
        channel: ChannelType,
        payload: dict,
        now: datetime | None = None,
    ) -> tuple[str | None, bool]:
        """
        Normalize and store an incoming message idempotently.
        Returns (project_id, is_new_message).
        """
        adapter = self.adapters[channel]
        msg = adapter.parse_webhook(payload)
        current = now or utc_now()

        # Handle explicit cross-channel or teammate linking: "link proj-123"
        if msg.body.lower().startswith("link "):
            target_project_id = msg.body.split(maxsplit=1)[1].strip()
            project = await self.project_repo.get_project(target_project_id)
            if project is None:
                # Ignore unknown project IDs safely
                return None, False

            await self.message_repo.add_member(
                target_project_id, channel.value, msg.external_user_id, role="teammate", now=current
            )
            await self.project_repo.update_last_active_channel(
                target_project_id, channel.value, now=current
            )
            _, created = await self.message_repo.enqueue_inbox(
                channel=channel.value,
                external_user_id=msg.external_user_id,
                idempotency_key=msg.idempotency_key,
                body=msg.body,
                project_id=target_project_id,
                now=current,
            )
            if created:
                await self.message_repo.mark_inbox_processed(
                    (await self.message_repo.get_pending_inbox(target_project_id))[-1].id
                )
                await self.message_repo.enqueue_outbox(
                    project_id=target_project_id,
                    channel=channel.value,
                    recipient_id=msg.external_user_id,
                    idempotency_key=f"link-ack:{msg.idempotency_key}",
                    body=f"Linked to project {target_project_id} ({project.title}). Current status: {project.status.value}.",
                    now=current,
                )
            return target_project_id, created

        # Look up existing project for this (channel, user)
        project_id = await self.message_repo.find_project_for_user(
            channel.value, msg.external_user_id
        )

        if project_id is None:
            # First message -> create a new project in INTAKE state
            project_id = f"proj-{uuid.uuid4().hex[:8]}"
            title = msg.body[:40] or "Untitled App"
            ws_path = str((self.workspaces_root / project_id).resolve())
            await self.project_repo.create_project(
                project_id=project_id,
                title=title,
                workspace_path=ws_path,
                channel=channel.value,
                external_user_id=msg.external_user_id,
                now=current,
            )
        else:
            await self.project_repo.update_last_active_channel(
                project_id, channel.value, now=current
            )

        _, created = await self.message_repo.enqueue_inbox(
            channel=channel.value,
            external_user_id=msg.external_user_id,
            idempotency_key=msg.idempotency_key,
            body=msg.body,
            project_id=project_id,
            now=current,
        )
        return project_id, created

    async def flush_outbox(self, limit: int = 20, now: datetime | None = None) -> int:
        """Deliver pending outbox messages via their registered channel adapters."""
        pending = await self.message_repo.get_pending_outbox(limit=limit)
        sent_count = 0

        for item in pending:
            adapter = self.adapters.get(ChannelType(item.channel))
            if adapter is None:
                await self.message_repo.mark_outbox_retry(item.id)
                continue

            try:
                await adapter.send(item.recipient_id, item.body)
                await self.message_repo.mark_outbox_sent(item.id, now=now)
                sent_count += 1
            except Exception:
                await self.message_repo.mark_outbox_retry(item.id)

        return sent_count