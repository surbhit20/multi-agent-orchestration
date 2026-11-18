import asyncio
from abc import ABC, abstractmethod

from bus.events import Event


class Subscription:
    """A per-game stream of events, fed by an EventBus."""

    def __init__(self, bus: "EventBus", game_id: str):
        self._bus = bus
        self.game_id = game_id
        self._queue: asyncio.Queue[Event] = asyncio.Queue()

    def _deliver(self, event: Event) -> None:
        if event.game_id == self.game_id:
            self._queue.put_nowait(event)

    async def get(self) -> Event:
        return await self._queue.get()

    def drain(self) -> list[Event]:
        events = []
        while not self._queue.empty():
            events.append(self._queue.get_nowait())
        return events

    async def wait_for_activity(self, timeout: float) -> bool:
        """Block until at least one event arrives (True) or the timeout passes (False).

        Events that arrived in a burst are coalesced into a single wake-up, so
        callers react once per change rather than once per event.
        """
        try:
            await asyncio.wait_for(self._queue.get(), timeout=timeout)
        except asyncio.TimeoutError:
            return False
        self.drain()
        return True

    def close(self) -> None:
        self._bus._unsubscribe(self)


class EventBus(ABC):
    def __init__(self):
        self._subscriptions: list[Subscription] = []

    async def start(self) -> None:
        pass

    async def stop(self) -> None:
        pass

    @abstractmethod
    async def publish(self, event: Event) -> None: ...

    def subscribe(self, game_id: str) -> Subscription:
        sub = Subscription(self, game_id)
        self._subscriptions.append(sub)
        return sub

    def _unsubscribe(self, sub: Subscription) -> None:
        if sub in self._subscriptions:
            self._subscriptions.remove(sub)

    def _fan_out(self, event: Event) -> None:
        for sub in list(self._subscriptions):
            sub._deliver(event)
