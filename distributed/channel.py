import os
from collections.abc import AsyncIterator

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer, TopicPartition

from bus.events import Event
from bus.kafka import ensure_topic
from distributed.protocol import topic_for


def bootstrap_servers() -> str:
    return os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")


class GameChannel:
    """A process's connection to one game's Kafka topic.

    The topic has a single partition, so every process sees every event in the
    same order. Each process reads it from the very beginning: events that were
    already there when it connected are flagged as replay, which is how a
    restarted process rebuilds its state without re-acting to old requests.
    """

    def __init__(self, game_id: str, bootstrap: str | None = None):
        self.game_id = game_id
        self.topic = topic_for(game_id)
        self.bootstrap = bootstrap or bootstrap_servers()
        self._tp = TopicPartition(self.topic, 0)
        self._producer: AIOKafkaProducer | None = None
        self._consumer: AIOKafkaConsumer | None = None
        self._replay_until = 0

    async def start(self) -> None:
        await ensure_topic(self.bootstrap, self.topic, partitions=1)
        self._producer = AIOKafkaProducer(bootstrap_servers=self.bootstrap)
        await self._producer.start()
        self._consumer = AIOKafkaConsumer(bootstrap_servers=self.bootstrap, enable_auto_commit=False)
        await self._consumer.start()
        self._consumer.assign([self._tp])
        await self._consumer.seek_to_beginning(self._tp)
        self._replay_until = (await self._consumer.end_offsets([self._tp]))[self._tp]

    async def stop(self) -> None:
        if self._consumer:
            await self._consumer.stop()
        if self._producer:
            await self._producer.stop()

    async def publish(self, kind: str, sender: str, text: str = "", to: str | None = None, data: dict | None = None) -> None:
        event = Event(game_id=self.game_id, kind=kind, sender=sender, text=text, to=to, data=data)
        await self._producer.send_and_wait(self.topic, value=event.to_bytes())

    async def events(self) -> AsyncIterator[tuple[Event, bool]]:
        """Yield (event, is_replay) forever, oldest first."""
        async for record in self._consumer:
            yield Event.from_bytes(record.value), record.offset < self._replay_until
