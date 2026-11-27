from datetime import datetime, timezone
from pydantic import BaseModel, Field

from app.models.state_machine import ProjectStatus, StepStatus


def utc_now() -> datetime:
    return datetime.now(timezone.utc)

def to_iso(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")

def from_iso(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


class Project(BaseModel):
    id: str
    title: str
    status: ProjectStatus
    workspace_path: str
    preferred_channel: str
    last_active_channel: str
    build_round: int = 1
    locked_by: str | None = None
    lease_expires_at: datetime | None = None
    heartbeat_at: datetime | None = None
    created_at: datetime
    updated_at: datetime


class PlanStepCreate(BaseModel):
    step_order: int
    title: str
    description: str
    acceptance_criteria: str


class PlanStep(PlanStepCreate):
    id: int
    project_id: str
    build_round: int = 1
    status: StepStatus = StepStatus.PENDING
    git_commit_sha: str | None = None
    attempt_count: int = 0
    last_error: str | None = None
    updated_at: datetime


class EventLog(BaseModel):
    id: int
    project_id: str
    event_type: str
    details: dict = Field(default_factory=dict)
    created_at: datetime