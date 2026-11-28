from datetime import datetime
import json
from app.agent.planner_llm import PlannerLLM
from app.models.domain import utc_now
from app.models.state_machine import ProjectStatus
from app.sandbox.workspace import GitWorkspace
from app.storage.message_repo import MessageRepository
from app.storage.project_repo import ProjectRepository
from app.storage.timer_repo import TimerRepository

class Phase1Coordinator:
    def __init__(
        self, 
        project_repo: ProjectRepository,
        message_repo: MessageRepository,
        timer_repo: TimerRepository,
        planner_llm: PlannerLLM,
        day_seconds: float = 86400.0
    ) ->None:
        self.project_repo = project_repo
        self.message_repo = message_repo
        self.timer_repo = timer_repo
        self.planner_llm = planner_llm
        self.day_seconds = day_seconds

    async def process_pending_messages(
        self, project_id: str, now: datetime|None=None
    ) -> ProjectStatus:

        current = now or utc_now()
        project = await self.project_repo.get_project(project_id)
        if project is None:
            raise ValueError(f"Unknown Project: {project_id}")

        pending = await self.message_repo.get_pending_inbox(project_id)
        if not pending:
            return project.status

        # Unpark automatically if the user replies after Day 7 expriation
        if project.status == ProjectStatus.PARKED:
            existing_steps = await self.project_repo.get_plan_steps(project_id, project.build_round)
            target = ProjectStatus.AWAITING_PLAN_APPROVAL if existing_steps else ProjectStatus.INTAKE
            
            project = await self.project_repo.transition_status(
                project_id, ProjectStatus.PARKED, target, reason="User replied to unpark", now=current
            )

        if project.status == ProjectStatus.INTAKE:
            await self._handle_intake(project_id, pending, current)
        elif project.status == ProjectStatus.AWAITING_PLAN_APPROVAL:
            await self._handle_approval_reply(project_id, pending, current)

        refreshed = await self.project_repo.get_project(project_id)
        assert refreshed is not None
        return refreshed.status

    async def _handle_intake(self, project_id: str, pending, now: datetime) -> None:
        conversation = await self.message_repo.get_conversation_history(project_id)
        decision = await self.planner_llm.plan(conversation)

        for msg in pending:
            await self.message_repo.mark_inbox_processed(msg.id)

        if decision.action == "ask_questions":
            await self._send_to_active_channel(
                project_id,
                idempotency_key=f"intake-q:{pending[-1].id}",
                body=decision.questions_text or "Could you share a bit more detail?",
                now=now
            )
            return

        # Generate spec.md + plan.json -> AWAITING_PLAN_APPROVAL
        await self.project_repo.transition_status(
            project_id, ProjectStatus.INTAKE, ProjectStatus.PLANNING, reason="Generating Plan", now=now
        )
        await self._write_plan_and_wait(
            project_id, decision, idempotency_suffix=f"plan:{pending[-1].id}", now=now
        )
        return

    async def _handle_approval_reply(self, project_id: str, pending, now:datetime)->None:
        latest = pending[-1]
        text = latest.body.strip()

        for msg in pending:
            await self.message_repo.mark_inbox_processed(msg.id)

        if text.upper() == "APPROVE":
            await self.timer_repo.cancel_pending_timers(project_id)
            await self.project_repo.transition_status(
                project_id,
                ProjectStatus.AWAITING_PLAN_APPROVAL,
                ProjectStatus.BUILDING,
                reason="Plan approved by user",
                now=now
            )

            await self._send_to_active_channel(
                project_id,
                idempotency_key=f"plan-approved:{latest.id}",
                body="Plan approved! Starting the build now. You can ask for status or send updates anytime.",
                now=now,
            )
            return

        # User requested changes -> cancel old timers, replan, reset 7-day window
        await self.timer_repo.cancel_pending_timers(project_id)
        await self.project_repo.transition_status(
            project_id,
            ProjectStatus.AWAITING_PLAN_APPROVAL,
            ProjectStatus.PLANNING,
            reason="Replanning from user feedback",
            now=now
        )
        conversation = await self.message_repo.get_conversation_history(project_id)
        decision = await self.planner_llm.plan(conversation)
        await self._write_plan_and_wait(project_id, decision, idempotency_suffix=f"replan:{latest.id}", now=now)

    
    async def _write_plan_and_wait(
        self,
        project_id: str,
        decision,
        idempotency_suffix: str,
        now: datetime
    ) -> None:
        project = await self.project_repo.get_project(project_id)
        assert project is not None

        ws = GitWorkspace(project.workspace_path)
        await ws.init_repo()
        await ws.write_file("spec.md", decision.spec_markdown or "# Sepcification\n")
        await ws.write_file(
            "plan.json",
            json.dumps([s.model_dump() for s in decision.steps], indent=2)
        )

        await ws.write_file("progress.md", "# Build Progress\n\n- plan generated, awaiting approval.\n")
        await ws.commit_all(f"[round-{project.build_round} plan] Save spec.md and plan.json")

        await self.project_repo.replace_plan_steps(
            project_id, decision.steps, build_round=project.build_round, now=now
        )
        await self.timer_repo.schedule_plan_approval_timers(
            project_id, day_seconds=self.day_seconds, now=now
        )
        await self.project_repo.transition_status(
            project_id,
            ProjectStatus.PLANNING,
            ProjectStatus.AWAITING_PLAN_APPROVAL,
            reason="Plan ready for approval",
            now=now,
        )
        await self._send_to_active_channel(
            project_id,
            idempotency_key=idempotency_suffix,
            body=decision.summary_for_user or "Here's the plan, reply APPROVE or send changes.",
            now=now,
        )


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



