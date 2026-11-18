import os

from bus.base import EventBus, Subscription
from bus.events import Event
from bus.memory import InMemoryBus


def create_bus_from_env() -> EventBus:
    """Pick the event bus backend from the environment.

    EVENT_BUS=kafka               use Kafka (requires aiokafka)
    KAFKA_BOOTSTRAP_SERVERS       default localhost:9092
    KAFKA_TOPIC                   default agent-events
    Anything else (or unset) uses the in-process bus.
    """
    if os.getenv("EVENT_BUS", "memory").lower() == "kafka":
        from bus.kafka import KafkaBus

        return KafkaBus(
            bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
            topic=os.getenv("KAFKA_TOPIC", "agent-events"),
        )
    return InMemoryBus()


__all__ = ["Event", "EventBus", "InMemoryBus", "Subscription", "create_bus_from_env"]
