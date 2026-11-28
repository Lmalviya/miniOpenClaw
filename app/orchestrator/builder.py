from datetime import datetime
from app.agent.coder_llm import StepCoder, StepContext
from app.models.domain import utc_now
from app.models.state_machine import ProjectStatus, StepStatus
from app.orchestrator.interrupt_handler import InterruptHandler
from app.sandbox.executor import SandboxExecutor
from app.sandbox.workspace import GitWorkspace
from app.storage.message_repo import MessageRepository
from app.storage.project_repo import ProjectRepository


class Phase2Builder:
    def __init__(
        self,
        project_repo: ProjectRepository,
        message_repo: MessageRepository,
        step_coder: StepCoder,
        executor: SandboxExecutor | None = None,
        max_step_attempts: int = 3,
    ) -> None:
        self.project_repo = project_repo
        self.message_repo = message_repo
        self.step_coder = step_coder
        self.executor = executor or SandboxExecutor()
        self.interrupts = InterruptHandler(project_repo, message_repo)
        self.max_step_attempts = max_step_attempts

    async def run_build(
        self,
        project_id: str,
        worker_id: str = "builder-1",
        max_steps: int | None = None,
        now: datetime | None = None,
    ) -> ProjectStatus:
        """
        Execute pending plan steps under a worker lease.
        Checks user interrupts between every step and commits each passing step to Git.
        """
        current = now or utc_now()
        acquired = await self.project_repo.acquire_lease(
            project_id, worker_id, lease_seconds=120, now=current
        )
        if not acquired:
            project = await self.project_repo.get_project(project_id)
            assert project is not None
            return project.status

        steps_executed = 0
        try:
            while True:
                if max_steps is not None and steps_executed >= max_steps:
                    break

                # 1. Check mid-build user messages (status / pause / resume / overrides)
                project = await self.interrupts.process_interrupts(project_id, now=current)
                if project.status != ProjectStatus.BUILDING:
                    break

                # 2. Heartbeat lease renewal
                await self.project_repo.renew_lease(
                    project_id, worker_id, lease_seconds=120, now=current
                )

                # 3. Pick next PENDING step
                steps = await self.project_repo.get_plan_steps(project_id, project.build_round)
                next_step = next((s for s in steps if s.status == StepStatus.PENDING), None)

                if next_step is None:
                    # All steps done -> move to AWAITING_REVIEW
                    await self.project_repo.transition_status(
                        project_id,
                        ProjectStatus.BUILDING,
                        ProjectStatus.AWAITING_REVIEW,
                        reason="All build steps completed",
                        now=current,
                    )
                    break

                # 4. Execute step in sandbox
                await self.project_repo.update_step_status(
                    next_step.id,
                    project_id,
                    StepStatus.IN_PROGRESS,
                    increment_attempt=True,
                    now=current,
                )

                ws = GitWorkspace(project.workspace_path)
                await ws.init_repo()
                context = await self._load_context(ws)

                result = await self.step_coder.execute_step(ws, self.executor, next_step, context)

                if result.success:
                    progress_text = context.progress_markdown + (
                        f"- [x] Step {next_step.step_order}: {next_step.title} — {result.summary}\n"
                    )
                    await ws.write_file("progress.md", progress_text)
                    sha = await ws.commit_all(
                        f"[round-{project.build_round} step-{next_step.step_order}] {next_step.title}"
                    )
                    await self.project_repo.update_step_status(
                        next_step.id,
                        project_id,
                        StepStatus.DONE,
                        git_commit_sha=sha,
                        last_error=None,
                        now=current,
                    )
                    steps_executed += 1
                else:
                    await ws.reset_hard_clean()
                    refreshed_steps = await self.project_repo.get_plan_steps(
                        project_id, project.build_round
                    )
                    current_step = next(s for s in refreshed_steps if s.id == next_step.id)

                    if current_step.attempt_count >= self.max_step_attempts:
                        await self.project_repo.update_step_status(
                            next_step.id,
                            project_id,
                            StepStatus.FAILED,
                            last_error=result.error,
                            now=current,
                        )
                        await self.project_repo.transition_status(
                            project_id,
                            ProjectStatus.BUILDING,
                            ProjectStatus.FAILED,
                            reason=f"Step {next_step.step_order} failed max attempts",
                            now=current,
                        )
                        break
                    else:
                        await self.project_repo.update_step_status(
                            next_step.id,
                            project_id,
                            StepStatus.PENDING,
                            last_error=result.error,
                            now=current,
                        )
                        steps_executed += 1
        finally:
            await self.project_repo.release_lease(project_id, worker_id, now=current)

        final_project = await self.project_repo.get_project(project_id)
        assert final_project is not None
        return final_project.status

    async def _load_context(self, ws: GitWorkspace) -> StepContext:
        spec = await ws.read_file("spec.md") if (ws.root / "spec.md").exists() else ""
        progress = await ws.read_file("progress.md") if (ws.root / "progress.md").exists() else ""
        overrides = await ws.read_file("overrides.md") if (ws.root / "overrides.md").exists() else ""
        return StepContext(
            spec_markdown=spec,
            progress_markdown=progress,
            overrides_markdown=overrides,
        )