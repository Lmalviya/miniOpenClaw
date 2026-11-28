from abc import ABC, abstractmethod
from enum import StrEnum
from pydantic import BaseModel, Field


class ChannelType(StrEnum):
    CLI = "cli"
    WHATSAPP = "whatsapp"
    SLACK = "slack"


class CanonicalIncoming(BaseModel):
    channel: ChannelType
    external_user_id: str
    idempotency_key: str
    body: str
    raw_payload: dict = Field(default_factory=dict)


class ChannelAdapter(ABC):
    channel_type: ChannelType

    @abstractmethod
    async def send(self, recipient_id: str, body: str) -> None:
        """Send an outbound text message to the external channel."""

    @abstractmethod
    def parse_webhook(self, payload: dict) -> CanonicalIncoming:
        """Normalize a channel-specific webhook payload into CanonicalIncoming."""