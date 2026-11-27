from enum import StrEnum

class ProjectStatus(StrEnum):
    INTAKE = "INTAKE"
    PLANNING = "PLANNING"
    AWAITING_PLAN_APPROVAL = "AWAITING_PLAN_APPROVAL"
    BUILDING = "BUILDING"
    PAUSED = "PAUSED"
    AWAITING_REVIEW = "AWAITING_REVIEW"
    DEPLOYED = "DEPLOYED"
    PARKED = "PARKED"
    FAILED = "FAILED"

class StepStatus(StrEnum):
    PENDING = "PENDING"
    IN_PROGRESS = "IN_PROGRESS"
    DONE = "DONE"
    FAILED = "FAILED"
    SKIPPED = "SKIPPED"

ALLOWED_TRANSITIONS: dict[ProjectStatus, set[ProjectStatus]] = {
    ProjectStatus.INTAKE: {ProjectStatus.PLANNING, ProjectStatus.PARKED, ProjectStatus.FAILED},
    ProjectStatus.PLANNING = {ProjectStatus.AWAITING_PLAN_APPROVAL, ProjectStatus.FAILED},
    ProjectStatus.AWAITING_PLAN_APPROVAL: {
        ProjectStatus.PLANNING,
        ProjectStatus.BUILDING,
        ProjectStatus.PARKED,
        ProjectStatus.FAILED
    },
    ProjectStatus.BUILDING: {ProjectStatus.PAUSED, ProjectStatus.AWAITING_REVIEW, ProjectStatus.FAILED},
    ProjectStatus.PAUSED: {ProjectStatus.BUILDING, ProjectStatus.PARKED, ProjectStatus.FAILED},
    ProjectStatus.AWAITING_REVIEW: {ProjectStatus.BUILDING, ProjectStatus.DEPLOYED, ProjectStatus.PARKED, ProjectStatus.FAILED},
    ProjectStatus.PARKED: {ProjectStatus.INTAKE, ProjectStatus.AWAITING_PLAN_APPROVAL, ProjectStatus.BUILDING, ProjectStatus.AWAITING_REVIEW},
    ProjectStatus.DEPLOYED: {ProjectStatus.BUILDING},
    ProjectStatus.FAILED: {ProjectStatus.BUILDING, ProjectStatus.PLANNING}
}


class InvalidTransitionError(valueError):
    pass 

def validate_transition(current: ProjectStatus, target: ProjectStatus) -> None:
    if target not in ALLOWED_TRANSITIONS.get(current, set()):
        raise InvalidTransitionError(f"Cannot transition from {current} to {target}")

        