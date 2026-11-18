import json
import time
from dataclasses import asdict, dataclass, field


# Event kinds that change what agents see in the transcript.
MESSAGE = "message"
TYPING = "typing"
ROUND_STARTED = "round_started"


@dataclass(frozen=True)
class Event:
    game_id: str
    kind: str
    sender: str
    text: str = ""
    ts: float = field(default_factory=time.time)
    # Recipient for private events (role assignment, kill request); None = everyone.
    to: str | None = None
    # Structured payload, e.g. {"vote": 3} or {"role": "Mafia"}.
    data: dict | None = None

    def to_bytes(self) -> bytes:
        return json.dumps(asdict(self)).encode("utf-8")

    @classmethod
    def from_bytes(cls, raw: bytes) -> "Event":
        return cls(**json.loads(raw.decode("utf-8")))
