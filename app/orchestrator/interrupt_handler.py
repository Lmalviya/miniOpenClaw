from datetime import datetime
from app.models.domain import Project, utc_now
from app.models.state_machine import ProjectStatus, StepStatus
from app.sandbox.workspace import GitWorkspace
from app.storage.message_repo import MessageRepository
from app.storage.project_repo import ProjectRepository


class InterruptHandler:
    def __init__(
        self,
        project_repo: ProjectRepository,
        message_repo: MessageRepository,
    ) -> None:
        self.project_repo = project_repo
        self.message_repo = message_repo

    async def process_interrupts(
        self, project_id: str, now: datetime | None = None
    ) -> Project:
        current = now or utc_now()
        project = await self.project_repo.get_project(project_id)
        if project is None:
            raise ValueError(f"Unknown project: {project_id}")

        pending = await self.message_repo.get_pending_inbox(project_id)
        for msg in pending:
            text = msg.body.strip()
            lower = text.lower()

            if self._is_status_query(lower):
                await self._reply_status(project, msg.id, current)
            elif lower in {"pause", "stop"} and project.status == ProjectStatus.BUILDING:
                project = await self.project_repo.transition_status(
                    project.id, ProjectStatus.BUILDING, ProjectStatus.PAUSED, reason="User requested pause", now=current
                )
                await self._send_reply(
                    project, f"pause-ack:{msg.id}", "Build paused cleanly. Send RESUME anytime to continue.", current
                )
            elif lower in {"resume", "continue"} and project.status == ProjectStatus.PAUSED:
                project = await self.project_repo.transition_status(
                    project.id, ProjectStatus.PAUSED, ProjectStatus.BUILDING, reason="User requested resume", now=current
                )
                await self._send_reply(
                    project, f"resume-ack:{msg.id}", "Resuming build from the next pending step.", current
                )
            else:
                # Mid-build requirement / style override (e.g. "use blue as the main colour")
                ws = GitWorkspace(project.workspace_path)
                await ws.init_repo()
                existing = ""
                if (ws.root / "overrides.md").exists():
                    existing = await ws.read_file("overrides.md")
                updated = existing + f"- {text}\n"
                await ws.write_file("overrides.md", updated)
                await ws.commit_all(f"[round-{project.build_round} override] {text[:50]}")
                await self._send_reply(
                    project,
                    f"override-ack:{msg.id}",
                    f"Got it! Saved update for upcoming steps: '{text}'",
                    current,
                )

            await self.message_repo.mark_inbox_processed(msg.id)

        refreshed = await self.project_repo.get_project(project_id)
        assert refreshed is not None
        return refreshed

    def _is_status_query(self, lower: str) -> bool:
        return any(k in lower for k in ("status", "progress", "how far", "update"))

    async def _reply_status(self, project: Project, msg_id: int, now: datetime) -> None:
        steps = await self.project_repo.get_plan_steps(project.id, project.build_round)
        done = sum(1 for s in steps if s.status == StepStatus.DONE)
        total = len(steps)
        next_step = next((s for s in steps if s.status != StepStatus.DONE), None)
        next_label = f"Next: Step {next_step.step_order} ({next_step.title})" if next_step else "All steps complete"
        body = f"Status: {project.status.value} | {done}/{total} steps done. {next_label}."
        await self._send_reply(project, f"status-reply:{msg_id}", body, now)

    async def _send_reply(
        self, project: Project, idempotency_key: str, body: str, now: datetime
    ) -> None:
        channel = project.last_active_channel
        recipient = await self.message_repo.get_recipient_for_channel(project.id, channel)
        if recipient is None:
            channel = project.preferred_channel
            recipient = await self.message_repo.get_recipient_for_channel(project.id, channel)
        assert recipient is not None

        await self.message_repo.enqueue_outbox(
            project_id=project.id,
            channel=channel,
            recipient_id=recipient,
            idempotency_key=idempotency_key,
            body=body,
            now=now,
        )