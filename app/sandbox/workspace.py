"""
How Crash Recovery Works (2 Edge Cases)

Suppose your build worker crashes (kill -9) while running Step 2:

1. Crash BEFORE git commit:
   - State: Workspace has half-written uncommitted files; SQLite says Step 2 is
     IN_PROGRESS.
   - Recovery: Run `git reset --hard HEAD` + `git clean -fd` to discard broken
     uncommitted edits, and reset Step 2 back to PENDING so the next worker
     retries cleanly.

2. Crash AFTER git commit, BEFORE updating SQLite:
   - State: Git already has `[round-1 step-2]` committed, but SQLite still says
     IN_PROGRESS.
   - Recovery: Check `git log` for `[round-1 step-2]`. If found, mark Step 2
     DONE with that commit SHA instead of re-running it.

------------------
This wraps per-project git operations using asyncio.create_subprocess_exec and guards against path traversal (../).

"""

import asyncio
from pathlib import Path 

class WorkspaceError(RuntimeError):
    pass

class GitWorkspace:
    def __init__(self, root_dir:str|Path)->None:
        self.root = Path(root_dir).resolve()

    def resolve_safe_path(self, relative_path: str) -> Path: # prevents path traversal outside self.root (is_relative_to)
        target = (self.root / relative_path).resolve()
        if not target.is_relative_to(self.root):
            raise WorkspaceError(f"Path escapes workspace: {relative_path}")
        return target

    async def _run_git(self, *args: str) -> str:
        """
        cwd, stdout, stderr: It sets the child process working directory to str(self.root) and 
        attaches PIPE streams so stdout and stderr can be read programmatically rather than 
        printing straight to the terminal.
        ---------------------------------------------------------------------------------------
        What it does not isolate: There is no container, VM, seccomp/namespace restriction, read-only filesystem 
        mount, CPU/memory/timeout limit, or network isolation. Inside self.root, a git command—or an agent that 
        runs arbitrary commands or edits files inside self.root—still runs with the host user's permissions, 
        can read/modify anything inside self.root, and can be affected by repo hooks (.git/hooks) or 
        environment variables.
        """
        proc = await asyncio.create_subprocess_exec(
            "git",
            *args,
            cwd=str(self.root), # keeps Git operations anchored to the workspace directory
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE
        )
        """
        It awaits both output streams until git finishes. Draining stdout and stderr via `communicate()`
        prevents pipe-buffer deadlocks when Git writes lots of output.
        """
        stdout, stderr = await proc.communicate()
        if proc.returncode != 0:
            raise WorkspaceError(
                f"git {' '.join(args)} failed: {stderr.decode(errors='replace').strip()}"
            )
        
        return stdout.decode(errors='replace').strip()


    async def init_repo(self) -> None:
        self.root.mkdir(parents=True, exist_ok=True)
        if not (self.root / ".git").exists():
            await self._run_git("init", "-b", "main")
            # Local repo identity so commits never fail on clean environments
            await self._run_git("config", "user.name", "OpenClaw Mini Agent")
            await self._run_git("config", "user.email", "agent@openclaw.local")

    async def write_file(self, relative_path: str, content: str) -> Path:
        target = self.resolve_safe_path(relative_path)
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(content, encoding="utf-8")
        return target

    
    async def read_file(self, relative_path: str) -> str:
        target = self.resolve_safe_path(relative_path)
        return target.read_text(encoding="utf-8")

    async def is_dirty(self) -> bool:
        status = await self._run_git("status", "--porcelain")
        return bool(status)

    async def has_commits(self) -> bool:
        proc = await asyncio.create_subprocess_exec(
            "git",
            "rev-parse",
            "--verify",
            "HEAD",
            cwd=str(self.root),
            stdout=asyncio.subprocess.DEVNULL,
            stderr=asyncio.subprocess.DEVNULL,
        )
        await proc.communicate()
        return proc.returncode == 0

    async def commit_all(self, message: str) -> str:
        await self._run_git("add", "-A")
        if await self.is_dirty():
            await self._run_git("commit", "-m", message)
        return await self._run_git("rev-parse", "HEAD")

    async def reset_hard_clean(self) -> None:
        """Discard all uncommitted edits and untracked files from a crashed step."""
        if await self.has_commits():
            await self._run_git("reset", "--hard", "HEAD")
        await self._run_git("clean", "-fd")

    async def find_step_commit(self, build_round: int, step_order: int) -> str | None:
        """Check if a step already committed before the server crashed."""
        if not await self.has_commits():
            return None
        tag = f"[round-{build_round} step-{step_order}]"
        output = await self._run_git(
            "log", "--grep", tag, "--fixed-strings", "-n", "1", "--format=%H"
        )
        return output or None

    async def create_tag(self, tag_name: str) -> None:
        if await self.has_commits():
            await self._run_git("tag", "-f", tag_name)

            

    