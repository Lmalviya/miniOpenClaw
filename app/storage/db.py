"""
================================================================================
DATABASE CONCURRENCY & RELIABILITY ARCHITECTURE
================================================================================

1. WAL Mode (`PRAGMA journal_mode = WAL`)
   - Problem: By default, SQLite locks the entire database file during any write.
     If a background worker is updating `plan_steps`, incoming webhooks trying to
     insert into `messages_inbox` will get blocked and fail with "database is locked".
   - Solution: Appends changes to a separate Write-Ahead Log (.wal) file instead of
     overwriting the main database immediately, allowing concurrent reads and writes.

2. Lease Locking on `projects` (`locked_by`, `lease_expires_at`, `heartbeat_at`)
   - Problem: If a worker claims a project, marks it "in_progress", and crashes or
     loses power mid-build, the project gets stuck in "in_progress" forever.
   - Solution: Workers must periodically update `heartbeat_at` to extend their
     `lease_expires_at` timestamp. If a server crashes, the lease expires
     automatically, allowing a restarted worker to safely reclaim the job.

3. Idempotency Keys (`idempotency_key` on `messages_inbox` & `messages_outbox`)
   - Problem: Webhook providers retry requests if they don't get a fast 200 OK, and
     a server might crash right after sending an outbound message before logging it.
     Both scenarios cause duplicate builds or double-messaging users.
   - Solution: Enforces a UNIQUE `idempotency_key` (event/message ID) at the database
     level. Any retry or duplicate attempt is safely ignored rather than re-processed.
================================================================================
"""

from contextlib import asynccontextmanager
from pathlib import Path 
import aiosqlite

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS projects (
    id TEXT PRIMARY KEY,
    title TEXT NOT NULL,
    status TEXT NOT NULL,
    workspace_path TEXT NOT NULL,
    preferred_channel TEXT NOT NULL,
    last_active_channel TEXT NOT NULL,
    build_round INTEGER NOT NULL DEFAULT 1,
    -- Worker lease fields for crash recovery (Task A1.2)
    locked_by TEXT,
    lease_expires_at TEXT,
    heartbeat_at TEXT,
    -- Timestamps (ISO-8601 UTC)
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS project_members (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    project_id TEXT NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
    channel TEXT NOT NULL,           -- 'cli', 'whatsapp', 'slack'
    external_user_id TEXT NOT NULL,  -- phone number, Slack user ID, etc.
    role TEXT NOT NULL DEFAULT 'owner',
    created_at TEXT NOT NULL,
    UNIQUE(project_id, channel, external_user_id)
);

CREATE TABLE IF NOT EXISTS plan_steps (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    project_id TEXT NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
    step_order INTEGER NOT NULL,
    build_round INTEGER NOT NULL DEFAULT 1,
    title TEXT NOT NULL,
    description TEXT NOT NULL,
    acceptance_criteria TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'PENDING',
    git_commit_sha TEXT,
    attempt_count INTEGER NOT NULL DEFAULT 0,
    last_error TEXT,
    updated_at TEXT NOT NULL,
    UNIQUE(project_id, build_round, step_order)
);

CREATE TABLE IF NOT EXISTS messages_inbox (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    project_id TEXT REFERENCES projects(id) ON DELETE CASCADE,
    channel TEXT NOT NULL,
    external_user_id TEXT NOT NULL,
    idempotency_key TEXT NOT NULL UNIQUE,
    body TEXT NOT NULL,
    processed INTEGER NOT NULL DEFAULT 0,  -- 0 = pending, 1 = handled
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS messages_outbox (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    project_id TEXT NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
    channel TEXT NOT NULL,
    recipient_id TEXT NOT NULL,
    idempotency_key TEXT NOT NULL UNIQUE,
    body TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'PENDING',  -- PENDING, SENT, FAILED
    retry_count INTEGER NOT NULL DEFAULT 0,
    created_at TEXT NOT NULL,
    sent_at TEXT
);

CREATE TABLE IF NOT EXISTS timers (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    project_id TEXT NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
    timer_type TEXT NOT NULL,        -- e.g., 'PLAN_REMINDER_DAY_1', 'PLAN_EXPIRE_DAY_7'
    fire_at TEXT NOT NULL,           -- ISO-8601 UTC timestamp
    status TEXT NOT NULL DEFAULT 'PENDING',  -- PENDING, FIRED, CANCELLED
    payload_json TEXT NOT NULL DEFAULT '{}',
    created_at TEXT NOT NULL,
    fired_at TEXT
);

CREATE TABLE IF NOT EXISTS events_log (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    project_id TEXT NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
    event_type TEXT NOT NULL,        -- e.g., 'STATE_CHANGED', 'STEP_COMMITTED', 'CRASH_RECOVERED'
    details_json TEXT NOT NULL DEFAULT '{}',
    created_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_timers_due ON timers(status, fire_at);
CREATE INDEX IF NOT EXISTS idx_inbox_pending ON messages_inbox(project_id, processed);
CREATE INDEX IF NOT EXISTS idx_outbox_pending ON messages_outbox(status, created_at);
"""

class Database: 
    def __init__(self, db_path: str|Path = "open_claw_mini.db") -> None:
        self.db_path = Path(db_path)

    async def init(self) -> None:
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        async with self.connect() as conn:
            await conn.executescript(SCHEMA_SQL)
            await conn.commit()

    @asynccontextmanager
    async def connect(self):
        conn = await aiosqlite.connect(self.db_path)
        conn.row_factory = aiosqlite.Row
        try:
            await conn.execute("PRAGMA journal_mode = WAL;")
            await conn.execute("PRAGMA busy_timeout = 5000;")
            await conn.execute("PRAGMA foreign_keys = ON;")
            await conn.execute("PRAGMA synchronous = NORMAL;")
            yield conn
        finally:
            await conn.close()
            