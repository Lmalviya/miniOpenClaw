from abc import ABC, abstractmethod
from typing import Literal
from pydantic import BaseModel, Field 
from app.models.domain import PlanStepCreate

class PlannerDecision(BaseModel):
    action: Literal["ask_questions", "produce_plan"]
    questions_text: str | None = None
    spec_markdown: str |None = None
    steps: list[PlanStepCreate] = Field(default_factory=list)
    summary_for_user: str | None = None

class PlannerLLM(ABC):
    @abstractmethod
    async def plan(self, conversation: list[dict[str, str]]) -> PlannerDecision:
        """Decide whether to ask clarifying questions or generate spec + plan"""


class RuleBasedMockPlannerLLM(PlannerLLM):
    """Deterministic planner for learning and testing without an llm api key."""

    async def plan(self, conversation:list[dict[str, str]]) -> PlannerDecision:
        user_messages = [m["content"] for m in conversation if m["role"] == "user"]

        #Turn 1: ask clarification questions before generating a plan
        if len(user_messages) <= 1:
            return PlannerDecision(
                action="ask_questions",
                questions_text=(
                    "Before I design the plan, quick questions:\n"
                    "1. Do customers need accounts, or guest booking with phone/name?\n"
                    "2. Should WhatsApp reminders go out 24h before, 2h before, or both?\n"
                    "3. What should staff see in the admin panel (daily calendar, revenue, or both)?"
                ),
            )

        # Turn 2: Generat spec.md + plan.json steps incorporating all user messages
        requirements_block = "\n".join(f"- {msg}" for msg in user_messages)
        spec_md = (
            "# Project Specification\n\n"
            "## User Requirements & Clarifications\n"
            f"{requirements_block}\n\n"
            "## Core Modules\n"
            "- Service catalog & duration configuration\n"
            "- Time-slot availability & booking API\n"
            "- Automated reminder scheduler\n"
            "- Admin dashboard\n"
        )

        steps = [
            PlanStepCreate(
                step_order=1,
                title="Project skeleton & healthcheck",
                description="Initialize FastAPI app, config, and SQLite database.",
                acceptance_criteria="GET /health returns 200 OK.",
            ),
            PlanStepCreate(
                step_order=2,
                title="Services & time-slots CRUD",
                description="Add models and endpoints for salon services and available slots.",
                acceptance_criteria="Pytest tests pass for creating/listing services and slots.",
            ),
            PlanStepCreate(
                step_order=3,
                title="Booking flow & reminders",
                description="Implement booking creation and WhatsApp reminder scheduling.",
                acceptance_criteria="Double-booking is prevented and reminder job is queued.",
            ),
            PlanStepCreate(
                step_order=4,
                title="Admin panel UI & API",
                description="Build admin view for managing bookings and daily schedule.",
                acceptance_criteria="Admin route renders bookings list and status updates pass tests.",
            ),
        ]

        summary_lines = "\n".join(f"{s.step_order}. {s.title}" for s in steps)
        return PlannerDecision(
            action="produce_plan",
            spec_markdown=spec_md,
            steps=steps,
            summary_for_user=(
                f"Here's the plan ({len(steps)} steps):\n"
                f"{summary_lines}\n\n"
                "Reply APPROVE to start building, or send changes."
            ),
        )