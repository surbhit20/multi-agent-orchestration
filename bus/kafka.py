import asyncio
import logging

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer, TopicPartition
from aiokafka.admin import AIOKafkaAdminClient, NewTopic
from aiokafka.errors import TopicAlreadyExistsError

from bus.base import EventBus
from bus.events import Event

log = logging.getLogger(__name__)


class KafkaBus(EventBus):
    """Publishes every game event to a Kafka topic and feeds subscribers from it.

    Events are keyed by game_id, so all events for one game land on one
    partition and keep their order. The consumer is assigned to every
    partition and positioned at the end on start(), so it sees exactly the
    events produced from that point on — no consumer-group rebalancing delay.
    """

    def __init__(self, bootstrap_servers: str, topic: str, partitions: int = 3):
        super().__init__()
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic
        self.partitions = partitions
        self._producer: AIOKafkaProducer | None = None
        self._consumer: AIOKafkaConsumer | None = None
        self._consume_task: asyncio.Task | None = None

    async def start(self) -> None:
        partitions = await self._ensure_topic()

        self._producer = AIOKafkaProducer(bootstrap_servers=self.bootstrap_servers)
        await self._producer.start()

        self._consumer = AIOKafkaConsumer(bootstrap_servers=self.bootstrap_servers, enable_auto_commit=False)
        await self._consumer.start()
        tps = [TopicPartition(self.topic, p) for p in partitions]
        self._consumer.assign(tps)
        await self._consumer.seek_to_end(*tps)
        # Resolve positions now so nothing published after start() is skipped.
        for tp in tps:
            await self._consumer.position(tp)

        self._consume_task = asyncio.create_task(self._consume_loop())
        log.info("KafkaBus connected to %s, topic %r", self.bootstrap_servers, self.topic)

    async def stop(self) -> None:
        if self._consume_task:
            self._consume_task.cancel()
            try:
                await self._consume_task
            except asyncio.CancelledError:
                pass
        if self._consumer:
            await self._consumer.stop()
        if self._producer:
            await self._producer.stop()

    async def publish(self, event: Event) -> None:
        await self._producer.send_and_wait(self.topic, value=event.to_bytes(), key=event.game_id.encode("utf-8"))

    async def _consume_loop(self) -> None:
        async for record in self._consumer:
            try:
                event = Event.from_bytes(record.value)
            except Exception:
                log.warning("Skipping malformed event at offset %s", record.offset)
                continue
            self._fan_out(event)

    async def _ensure_topic(self) -> list[int]:
        """Create the topic if needed and return its partition ids."""
        admin = AIOKafkaAdminClient(bootstrap_servers=self.bootstrap_servers)
        await admin.start()
        try:
            try:
                await admin.create_topics([NewTopic(self.topic, num_partitions=self.partitions, replication_factor=1)])
            except TopicAlreadyExistsError:
                pass
            for _ in range(20):
                [desc] = await admin.describe_topics([self.topic])
                if desc.get("partitions"):
                    return sorted(p["partition"] for p in desc["partitions"])
                await asyncio.sleep(0.25)  # freshly created topic: wait for leaders
            raise RuntimeError(f"Kafka topic {self.topic!r} has no partitions after creation")
        finally:
            await admin.close()
