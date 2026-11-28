from datetime import datetime
from app.models.domain import utc_now
from app.models.state_machine import ProjectStatus
from app.storage.message_repo import MessageRepository
from app.storage.project_repo import ProjectRepository, StateConflictError
from app.storage.timer_repo import TimerRepository


class TimerWorker:
    def __init__(
        self,
        project_repo: ProjectRepository,
        message_repo: MessageRepository,
        timer_repo: TimerRepository,
    ) -> None:
        self.project_repo = project_repo
        self.message_repo = message_repo
        self.timer_repo = timer_repo

    async def process_due_timers(self, now: datetime | None = None) -> int:
        current = now or utc_now()
        due_timers = await self.timer_repo.claim_due_timers(now=current)
        handled = 0

        for timer in due_timers:
            project = await self.project_repo.get_project(timer.project_id)
            if project is None:
                continue

            # Guard: only process plan timers if project is still waiting for plan approval
            if timer.timer_type.startswith("PLAN_") and project.status != ProjectStatus.AWAITING_PLAN_APPROVAL:
                continue

            recipient = await self.message_repo.get_recipient_for_channel(
                project.id, project.last_active_channel
            )
            if recipient is None:
                continue

            if timer.timer_type.startswith("PLAN_REMINDER_"):
                day = timer.payload.get("day", "?")
                await self.message_repo.enqueue_outbox(
                    project_id=project.id,
                    channel=project.last_active_channel,
                    recipient_id=recipient,
                    idempotency_key=f"timer-outbox:{timer.id}",
                    body=f"Reminder (Day {day}): Your plan is still waiting. Reply APPROVE or send changes.",
                    now=current,
                )
                handled += 1

            elif timer.timer_type == "PLAN_EXPIRE_DAY_7":
                try:
                    await self.project_repo.transition_status(
                        project.id,
                        ProjectStatus.AWAITING_PLAN_APPROVAL,
                        ProjectStatus.PARKED,
                        reason="No plan approval after 7 days",
                        now=current,
                    )
                except StateConflictError:
                    # User approved right as timer fired; do nothing
                    continue

                await self.message_repo.enqueue_outbox(
                    project_id=project.id,
                    channel=project.last_active_channel,
                    recipient_id=recipient,
                    idempotency_key=f"timer-outbox:{timer.id}",
                    body="No reply after 7 days, so I've parked this project. Reply anytime to resume!",
                    now=current,
                )
                handled += 1

        return handled