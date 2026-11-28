"""
Simulator to test CLI, Whatsapp, Slack channels locally and in automated tests
"""

from dataclasses import dataclass
from app.channels.base import CanonicalIncoming, ChannelAdapter, ChannelType


@dataclass
class SentRecord:
    channel: ChannelType
    recipient_id: str
    body: str


class SimulatorChannelAdapter(ChannelAdapter):
    def __init__(
        self,
        channel_type: ChannelType,
        print_to_console: bool = False,
        fail_next_send: bool = False,
    ) -> None:
        self.channel_type = channel_type
        self.print_to_console = print_to_console
        self.fail_next_send = fail_next_send
        self.sent_messages: list[SentRecord] = []

    async def send(self, recipient_id: str, body: str) -> None:
        if self.fail_next_send:
            self.fail_next_send = False
            raise RuntimeError(f"Simulated {self.channel_type} network error")

        record = SentRecord(channel=self.channel_type, recipient_id=recipient_id, body=body)
        self.sent_messages.append(record)
        if self.print_to_console:
            print(f"[{self.channel_type.upper()} -> {recipient_id}] {body}")

    def parse_webhook(self, payload: dict) -> CanonicalIncoming:
        return CanonicalIncoming(
            channel=self.channel_type,
            external_user_id=str(payload["user_id"]),
            idempotency_key=f"{self.channel_type.value}:{payload['message_id']}",
            body=str(payload["text"]).strip(),
            raw_payload=payload,
        )