"""Print game events from Kafka as they happen.

    python -m bus.tail                 # new events only
    python -m bus.tail --from-beginning
    python -m bus.tail --game <game_id>
"""
import argparse
import asyncio
import os
from datetime import datetime

from aiokafka import AIOKafkaConsumer

from bus.events import Event


async def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--bootstrap-servers", default=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"))
    parser.add_argument("--topic", default=os.getenv("KAFKA_TOPIC", "agent-events"))
    parser.add_argument("--game", help="only show events for this game_id")
    parser.add_argument("--from-beginning", action="store_true")
    args = parser.parse_args()

    consumer = AIOKafkaConsumer(
        args.topic,
        bootstrap_servers=args.bootstrap_servers,
        auto_offset_reset="earliest" if args.from_beginning else "latest",
        enable_auto_commit=False,
    )
    await consumer.start()
    try:
        async for record in consumer:
            event = Event.from_bytes(record.value)
            if args.game and event.game_id != args.game:
                continue
            ts = datetime.fromtimestamp(event.ts).strftime("%H:%M:%S")
            sender = f"{event.sender} -> {event.to}" if event.to else event.sender
            print(f"[{ts}] {event.game_id[:8]} {event.kind:<13} {sender}: {event.text.replace(chr(10), ' | ')}", flush=True)
    finally:
        await consumer.stop()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
