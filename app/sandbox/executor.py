import asyncio
from dataclasses import dataclass
from pathlib import Path 

@dataclass
class CommandResult:
    exit_code: int
    stdout: str 
    stderr: str 
    timed_out: bool = False

    @property
    def ok(self) -> bool:
        return not self.timed_out and self.exit_code == 0

    
class SandboxExecutor:
    def __init__(self, max_output_chars: int=4000) -> None:
        self.max_output_chars = max_output_chars

    def _trancate(self, text: str) -> str:
        if len(text) <= self.max_output_chars:
            return text
        return "...[truncated]...\n" + text[-self.max_output_chars:]

    async def run(
        self, 
        workspace_root: str|Path,
        args: list[str],
        timeout_seconds: float = 30.0
    ) -> CommandResult:
        """Run a command inside workspace_root without invoking a shell"""

        proc = await asyncio.create_subprocess_exec(
            *args,
            cwd=str(Path(workspace_root).resolve()),
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE
        )

        try:
            stdout_b, stderr_b = await asyncio.wait_for(
                proc.communicate(), timeout=timeout_seconds
            )
            return CommandResult(
                exit_code=proc.returncode or 0,
                stdout=self._trancate(stdout_b.decode(errors="replace")),
                stderr=self._trancate(stderr_b.decode(errors="replace")),
                timed_out=False
            )

        except asyncio.TimeoutError:
            proc.kill()
            await proc.communicate()
            return CommandResult(
                exit_code=-1,
                stdout="",
                stderr=f"Command timed out after {timeout_seconds}s",
                timed_out=True,
            )