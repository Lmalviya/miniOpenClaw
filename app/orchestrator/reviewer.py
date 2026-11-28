from datetime import datetime
import json
from app.agent.planner_llm import PlannerLLM
from app.models.domain import utc_now
from app.models.state_machine import ProjectStatus
from app.sandbox.deployer import Deployer
from app.sandbox.workspace import GitWorkspace
from app.storage.message_repo import MessageRepository
from app.storage.project_repo import ProjectRepository
from app.storage.timer_repo import TimerRepository

class Phase3Reviewer:
    def __init__(
        self,
        project_repo: ProjectRepository,
        message_repo: MessageRepository,
        timer_repo: TimerRepository,
        planner_llm: PlannerLLM,
        deployer: Deployer,
        day_seconds: float = 86400.0,
    ) -> None:
        self.project_repo = project_repo
        self.message_repo = message_repo
        self.timer_repo = timer_repo
        self.planner_llm = planner_llm
        self.deployer = deployer
        self.day_seconds = day_seconds

    async def publish_preview(
        self, project_id: str, now: datetime | None = None
    ) -> None:
        current = now or utc_now()
        project = await self.project_repo.get_project(project_id)
        assert project is not None

        result = await self.deployer.deploy_preview(
            project.id, project.workspace_path, project.build_round
        )
        if not result.healthy:
            await self.project_repo.transition_status(
                project.id,
                ProjectStatus.AWAITING_REVIEW,
                ProjectStatus.FAILED,
                reason=f"Preview healthcheck failed: {result.error}",
                now=current,
            )
            await self._send_to_active_channel(
                project.id,
                idempotency_key=f"preview-failed:{project.id}:r{project.build_round}",
                body=f"Build finished, but the preview healthcheck failed for round {project.build_round}.",
                now=current,
            )
            return

        await self.timer_repo.schedule_review_timers(
            project.id, day_seconds=self.day_seconds, now=current
        )
        await self.project_repo.log_event(
            project.id,
            "PREVIEW_DEPLOYED",
            {"build_round": project.build_round, "url": result.url},
            now=current,
        )
        await self._send_to_active_channel(
            project.id,
            idempotency_key=f"preview-ready:{project.id}:r{project.build_round}",
            body=(
                f"Your app (round {project.build_round}) is ready: {result.url}\n"
                "Reply APPROVE to go live, or tell me what to change."
            ),
            now=current,
        )

    async def handle_review_reply(
        self, project_id: str, now: datetime | None = None
    ) -> ProjectStatus:
        current = now or utc_now()
        project = await self.project_repo.get_project(project_id)
        assert project is not None

        pending = await self.message_repo.get_pending_inbox(project_id)
        if not pending or project.status != ProjectStatus.AWAITING_REVIEW:
            return project.status

        latest = pending[-1]
        text = latest.body.strip()
        for msg in pending:
            await self.message_repo.mark_inbox_processed(msg.id)

        if any(k in text.lower() for k in ("status", "progress")):
            await self._send_to_active_channel(
                project.id,
                idempotency_key=f"review-status:{latest.id}",
                body=(
                    f"Status: AWAITING_REVIEW (round {project.build_round}). "
                    "Reply APPROVE to go live, or tell me what to change."
                ),
                now=current,
            )
            return ProjectStatus.AWAITING_REVIEW

        await self.timer_repo.cancel_pending_timers(project_id)

        if text.upper() == "APPROVE":
            prod = await self.deployer.deploy_production(
                project.id, project.workspace_path, project.build_round
            )
            await self.project_repo.transition_status(
                project.id,
                ProjectStatus.AWAITING_REVIEW,
                ProjectStatus.DEPLOYED,
                reason=f"Approved for production ({prod.release_tag})",
                now=current,
            )
            await self._send_to_active_channel(
                project.id,
                idempotency_key=f"prod-live:{latest.id}",
                body=f"Approved! Your app is now live at {prod.url} (release {prod.release_tag}).",
                now=current,
            )
            return ProjectStatus.DEPLOYED

        # User/teammate requested changes -> increment build_round & start new build round
        project = await self.project_repo.increment_build_round(project.id, now=current)
        ws = GitWorkspace(project.workspace_path)
        await ws.init_repo()
        spec_md = await ws.read_file("spec.md") if (ws.root / "spec.md").exists() else ""
        overrides = await ws.read_file("overrides.md") if (ws.root / "overrides.md").exists() else ""
        await ws.write_file("overrides.md", overrides + f"- [Round {project.build_round}] {text}\n")

        new_steps = await self.planner_llm.plan_changes(spec_md, text, project.build_round)
        await ws.write_file(
            f"plan_round_{project.build_round}.json",
            json.dumps([s.model_dump() for s in new_steps], indent=2),
        )
        await ws.commit_all(f"[round-{project.build_round} plan] Review changes: {text[:40]}")

        await self.project_repo.save_plan_steps(
            project.id, new_steps, build_round=project.build_round, now=current
        )
        await self.project_repo.transition_status(
            project.id,
            ProjectStatus.AWAITING_REVIEW,
            ProjectStatus.BUILDING,
            reason=f"Starting build round {project.build_round} from review feedback",
            now=current,
        )
        await self._send_to_active_channel(
            project.id,
            idempotency_key=f"round-start:{latest.id}",
            body=f"Got your feedback! Starting build round {project.build_round} now.",
            now=current,
        )
        return ProjectStatus.BUILDING

    async def _send_to_active_channel(
        self, project_id: str, idempotency_key: str, body: str, now: datetime
    ) -> None:
        project = await self.project_repo.get_project(project_id)
        assert project is not None
        channel = project.last_active_channel
        recipient = await self.message_repo.get_recipient_for_channel(project_id, channel)
        if recipient is None:
            channel = project.preferred_channel
            recipient = await self.message_repo.get_recipient_for_channel(project_id, channel)
        assert recipient is not None

        await self.message_repo.enqueue_outbox(
            project_id=project_id,
            channel=channel,
            recipient_id=recipient,
            idempotency_key=idempotency_key,
            body=body,
            now=now,
        )