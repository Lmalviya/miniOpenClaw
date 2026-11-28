from abc import ABC, abstractmethod
import sys
from typing import Literal
from pydantic import BaseModel
from app.sandbox.executor import SandboxExecutor
from app.sandbox.workspace import GitWorkspace


class DeploymentResult(BaseModel):
    environment: Literal["preview", "production"]
    url: str
    healthy: bool
    release_tag: str | None = None
    error: str | None = None


class Deployer(ABC):
    @abstractmethod
    async def deploy_preview(
        self, project_id: str, workspace_path: str, build_round: int
    ) -> DeploymentResult:
        """Deploy a preview build and verify health."""

    @abstractmethod
    async def deploy_production(
        self, project_id: str, workspace_path: str, build_round: int
    ) -> DeploymentResult:
        """Promote the reviewed build to production."""


class LocalSimulatedDeployer(Deployer):
    """Runs final pytest healthcheck in sandbox and tags Git releases."""

    def __init__(self, executor: SandboxExecutor | None = None) -> None:
        self.executor = executor or SandboxExecutor()

    async def deploy_preview(
        self, project_id: str, workspace_path: str, build_round: int
    ) -> DeploymentResult:
        check = await self.executor.run(
            workspace_path, [sys.executable, "-m", "pytest", "-q"]
        )
        return DeploymentResult(
            environment="preview",
            url=f"https://preview-{project_id}-r{build_round}.openclaw.local",
            healthy=check.ok,
            error=None if check.ok else (check.stdout + "\n" + check.stderr),
        )

    async def deploy_production(
        self, project_id: str, workspace_path: str, build_round: int
    ) -> DeploymentResult:
        ws = GitWorkspace(workspace_path)
        tag = f"prod-round-{build_round}"
        await ws.create_tag(tag)
        return DeploymentResult(
            environment="production",
            url=f"https://{project_id}.openclaw.local",
            healthy=True,
            release_tag=tag,
        )