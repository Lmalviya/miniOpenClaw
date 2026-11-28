from abc import ABC, abstractmethod
import sys
from pydantic import BaseModel
from app.models.domain import PlanStep
from app.sandbox.executor import SandboxExecutor
from app.sandbox.workspace import GitWorkspace


class StepContext(BaseModel):
    spec_markdown: str
    progress_markdown: str
    overrides_markdown: str = ""


class StepExecutionResult(BaseModel):
    success: bool
    summary: str
    error: str | None = None
    attempts_inside_step: int = 1


class StepCoder(ABC):
    @abstractmethod
    async def execute_step(
        self,
        workspace: GitWorkspace,
        executor: SandboxExecutor,
        step: PlanStep,
        context: StepContext,
    ) -> StepExecutionResult:
        """Write code, run tests in sandbox, fix failures, and return outcome."""


class SimulatedStepCoder(StepCoder):
    """
    Deterministic coding agent that writes real files + pytest tests.
    Demonstrates the Write -> Run Tests -> Fix -> Re-run self-healing loop.
    """

    def __init__(self, self_heal_on_step: int | None = None) -> None:
        self.self_heal_on_step = self_heal_on_step
        self.seen_contexts: list[tuple[int, StepContext]] = []

    async def execute_step(
        self,
        workspace: GitWorkspace,
        executor: SandboxExecutor,
        step: PlanStep,
        context: StepContext,
    ) -> StepExecutionResult:
        self.seen_contexts.append((step.step_order, context))
        module_name = f"step_{step.step_order}"

        # Write test for this step
        await workspace.write_file(
            f"tests/test_{module_name}.py",
            f"from {module_name} import get_status\n\n"
            f"def test_{module_name}():\n"
            f"    assert get_status() == 'ok'\n",
        )

        # Optional: simulate an initial bug on first try so we see the fix loop work
        if self.self_heal_on_step == step.step_order:
            await workspace.write_file(
                f"{module_name}.py",
                "def get_status():\n    return 'broken'\n",
            )
            first_run = await executor.run(
                workspace.root, [sys.executable, "-m", "pytest", "-q"]
            )
            assert not first_run.ok  # Failed test detected by agent!

            # Self-heal after reading test failure
            await workspace.write_file(
                f"{module_name}.py",
                "def get_status():\n    return 'ok'\n",
            )
            second_run = await executor.run(
                workspace.root, [sys.executable, "-m", "pytest", "-q"]
            )
            if not second_run.ok:
                return StepExecutionResult(
                    success=False,
                    summary=f"Step {step.step_order} failed after fix attempt",
                    error=second_run.stdout + "\n" + second_run.stderr,
                    attempts_inside_step=2,
                )
            return StepExecutionResult(
                success=True,
                summary=f"Implemented {step.title} (self-healed test failure)",
                attempts_inside_step=2,
            )

        # Normal single-pass implementation
        theme_comment = (
            f"# Active overrides:\n# {context.overrides_markdown.strip()}\n"
            if context.overrides_markdown.strip()
            else ""
        )
        await workspace.write_file(
            f"{module_name}.py",
            f"{theme_comment}def get_status():\n    return 'ok'\n",
        )
        test_run = await executor.run(
            workspace.root, [sys.executable, "-m", "pytest", "-q"]
        )
        if not test_run.ok:
            return StepExecutionResult(
                success=False,
                summary=f"Tests failed for step {step.step_order}",
                error=test_run.stdout + "\n" + test_run.stderr,
            )
        return StepExecutionResult(
            success=True,
            summary=f"Implemented {step.title}",
        )