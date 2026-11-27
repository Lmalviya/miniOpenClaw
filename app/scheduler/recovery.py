from datetime import datetime
from app.models.domain import utc_now
from app.models.state_machine import ProjectStatus, StepStatus
from app.sandbox.workspace import GitWorkspace
from app.storage.project_repo import ProjectRepository


class CrashRecoveryService:
    def __init__(
        self,
        project_repo: ProjectRepository,
        worker_id: str = "recovery-worker",
        max_step_attempts: int = 3,
    ) -> None:
        self.project_repo = project_repo
        self.worker_id = worker_id
        self.max_step_attempts = max_step_attempts

    async def recover_stale_projects(self, now: datetime | None = None) -> list[str]:
        """Find projects whose worker died mid-task and restore clean state."""
        current = now or utc_now()
        stale_projects = await self.project_repo.find_stale_active_projects(now=current)
        recovered_ids: list[str] = []

        for project in stale_projects:
            acquired = await self.project_repo.acquire_lease(
                project.id, self.worker_id, lease_seconds=60, now=current
            )
            if not acquired:
                continue

            try:
                await self._recover_single_project(
                    project.id, project.status, project.workspace_path, project.build_round, current
                )
                recovered_ids.append(project.id)
            finally:
                await self.project_repo.release_lease(project.id, self.worker_id, now=current)

        return recovered_ids

    async def _recover_single_project(
        self,
        project_id: str,
        status: ProjectStatus,
        workspace_path: str,
        build_round: int,
        now: datetime,
    ) -> None:
        workspace = GitWorkspace(workspace_path)

        if status == ProjectStatus.BUILDING:
            steps = await self.project_repo.get_plan_steps(project_id, build_round)
            in_progress = [s for s in steps if s.status == StepStatus.IN_PROGRESS]

            for step in in_progress:
                committed_sha = await workspace.find_step_commit(build_round, step.step_order)
                if committed_sha:
                    # Edge case: crashed right after git commit, before SQLite update
                    await self.project_repo.update_step_status(
                        step.id,
                        project_id,
                        StepStatus.DONE,
                        git_commit_sha=committed_sha,
                        last_error=None,
                        now=now,
                    )
                else:
                    # Standard case: crashed mid-edit -> rollback dirty files & retry
                    if (workspace.root / ".git").exists():
                        await workspace.reset_hard_clean()

                    if step.attempt_count >= self.max_step_attempts:
                        await self.project_repo.update_step_status(
                            step.id,
                            project_id,
                            StepStatus.FAILED,
                            last_error="Exceeded max attempts after crash recovery",
                            now=now,
                        )
                        await self.project_repo.transition_status(
                            project_id,
                            ProjectStatus.BUILDING,
                            ProjectStatus.FAILED,
                            reason=f"Step {step.step_order} exceeded max crash retries",
                            now=now,
                        )
                    else:
                        await self.project_repo.update_step_status(
                            step.id,
                            project_id,
                            StepStatus.PENDING,
                            last_error="Recovered from worker crash",
                            now=now,
                        )

        await self.project_repo.log_event(
            project_id,
            "CRASH_RECOVERED",
            {"status": status.value, "build_round": build_round},
            now=now,
        )