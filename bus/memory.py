from bus.base import EventBus
from bus.events import Event


class InMemoryBus(EventBus):
    """Single-process bus. The default when Kafka isn't configured."""

    async def publish(self, event: Event) -> None:
        self._fan_out(event)
