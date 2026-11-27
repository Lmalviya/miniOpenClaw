import pytest
from app.models.state_machine import (
    InvalidTransitionError, 
    ProjectStatus,
    validate_transition
)
from app.storage.db import Database

@pytest.mark.asyncio
async def test_db_initializes_with_wal_and_tables(tmp_path):
    db_file = tmp_path / "test.db"
    db = Database(db_file)
    await db.init()

    async with db.connect() as conn:
        cursor = await conn.execute("PRAGMA journal_mode;")
        mode = (await cursor.fetchone())[0]
        assert mode.lower() == "wal"

        cursor = await conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table';"
        )
        tables = {row["name"] for row in await cursor.fetchall()}

    expected =  {
        "projects",
        "project_members",
        "plan_steps",
        "messages_inbox",
        "messages_outbox",
        "timers",
        "events_log",
    }
    assert expected.issubset(tables)

def test_state_transitions():
    validate_transition(ProjectStatus.INTAKE, ProjectStatus.PLANNING)
    with pytest.raises(InvalidTransitionError):
        validate_transition(ProjectStatus.INTAKE, ProjectStatus.DEPLOYED)