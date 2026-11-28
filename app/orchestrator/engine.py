from datetime import datetime
from app.models.domain import utc_now
from app.models.state_machine import ProjectStatus, StepStatus
from app.orchestrator.builder import Phase2Builder
from app.orchestrator.intake_planner import Phase1Coordinator
from app.orchestrator.interrupt_handler import InterruptHandler
from app.orchestrator.reviewer import Phase3Reviewer
from app.storage.message_repo import MessageRepository
from app.storage.project_repo import ProjectRepository


class AgentOrchestrator:
    def __init__(
        self,
        project_repo: ProjectRepository,
        message_repo: MessageRepository,
        phase1: Phase1Coordinator,
        phase2: Phase2Builder,
        phase3: Phase3Reviewer,
    ) -> None:
        self.project_repo = project_repo
        self.message_repo = message_repo
        self.phase1 = phase1
        self.phase2 = phase2
        self.phase3 = phase3
        self.interrupts = InterruptHandler(project_repo, message_repo)

    async def tick_project(
        self,
        project_id: str,
        worker_id: str = "worker-1",
        max_build_steps: int | None = None,
        now: datetime | None = None,
    ) -> ProjectStatus:
        """Advance a project as far as it can go given current DB state & inbox."""
        current = now or utc_now()
        project = await self.project_repo.get_project(project_id)
        if project is None:
            raise ValueError(f"Unknown project: {project_id}")

        # Unpark intelligently based on how far the project had progressed
        if project.status == ProjectStatus.PARKED:
            pending = await self.message_repo.get_pending_inbox(project_id)
            if not pending:
                return project.status
            steps = await self.project_repo.get_plan_steps(project_id, project.build_round)
            if steps and all(s.status == StepStatus.DONE for s in steps):
                target = ProjectStatus.AWAITING_REVIEW
            elif steps:
                target = ProjectStatus.AWAITING_PLAN_APPROVAL
            else:
                target = ProjectStatus.INTAKE
            project = await self.project_repo.transition_status(
                project_id, ProjectStatus.PARKED, target, reason="Unparked by user reply", now=current
            )

        if project.status in {ProjectStatus.INTAKE, ProjectStatus.AWAITING_PLAN_APPROVAL}:
            status = await self.phase1.process_pending_messages(project_id, now=current)
            if status != ProjectStatus.BUILDING:
                return status
            project = await self.project_repo.get_project(project_id)
            assert project is not None

        if project.status == ProjectStatus.PAUSED:
            project = await self.interrupts.process_interrupts(project_id, now=current)

        if project.status == ProjectStatus.BUILDING:
            status = await self.phase2.run_build(
                project_id, worker_id=worker_id, max_steps=max_build_steps, now=current
            )
            if status == ProjectStatus.AWAITING_REVIEW:
                await self.phase3.publish_preview(project_id, now=current)
            refreshed = await self.project_repo.get_project(project_id)
            assert refreshed is not None
            return refreshed.status

        if project.status == ProjectStatus.AWAITING_REVIEW:
            status = await self.phase3.handle_review_reply(project_id, now=current)
            if status == ProjectStatus.BUILDING:
                status = await self.phase2.run_build(
                    project_id, worker_id=worker_id, max_steps=max_build_steps, now=current
                )
                if status == ProjectStatus.AWAITING_REVIEW:
                    await self.phase3.publish_preview(project_id, now=current)
            refreshed = await self.project_repo.get_project(project_id)
            assert refreshed is not None
            return refreshed.status

        return project.status