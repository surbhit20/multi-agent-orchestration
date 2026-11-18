"""The referee for a distributed Mafia game.

    python -m distributed.referee --game <game_id> --players 5

It never calls a player directly. It publishes phase changes and requests to
the game's Kafka topic and waits for the players' answers to show up there.
It's also the "screen": it prints the public chat as it happens.
"""
import argparse
import asyncio
import random
import time
from collections import Counter
from collections.abc import Callable
from datetime import datetime, timedelta

from bus.events import Event
from distributed import protocol as p
from distributed.channel import GameChannel
from logger_config import setup_logging


class Referee:
    def __init__(self, channel: GameChannel, n_players: int, round_minutes: float, logger):
        self.channel = channel
        self.n_players = n_players
        self.round_minutes = round_minutes
        self.logger = logger

        self.joined: set[str] = set()
        self.typing: set[str] = set()
        self._waiters: list[tuple[Callable[[Event], bool], asyncio.Future]] = []
        self._pump: asyncio.Task | None = None

    # ---- event plumbing ----------------------------------------------------

    async def _pump_events(self) -> None:
        async for event, _ in self.channel.events():
            if event.kind == p.JOIN:
                self.joined.add(event.sender)
            elif event.kind == p.TYPING:
                self.typing.add(event.sender)
            elif event.kind == p.MESSAGE:
                self.typing.discard(event.sender)
                self._show(event)

            for predicate, future in list(self._waiters):
                if not future.done() and predicate(event):
                    future.set_result(event)

    def _show(self, event: Event) -> None:
        ts = datetime.fromtimestamp(event.ts).strftime("%H:%M:%S")
        self.logger.info(f"[{ts}] {event.sender}: {event.text}")

    def expect(self, predicate: Callable[[Event], bool]) -> tuple[Callable[[Event], bool], asyncio.Future]:
        """Start listening for an event matching `predicate`. Call before publishing the request."""
        entry = (predicate, asyncio.get_running_loop().create_future())
        self._waiters.append(entry)
        return entry

    async def receive(self, entry, timeout: float) -> Event | None:
        """Wait for an `expect`ed event, or None after `timeout` seconds."""
        try:
            return await asyncio.wait_for(entry[1], timeout)
        except asyncio.TimeoutError:
            return None
        finally:
            self._waiters.remove(entry)

    async def wait_for(self, predicate: Callable[[Event], bool], timeout: float) -> Event | None:
        return await self.receive(self.expect(predicate), timeout)

    async def publish(self, kind: str, text: str = "", to: str | None = None, data: dict | None = None) -> None:
        """Publish as the referee and wait until it comes back off the topic,
        so the referee's view (and console) stays in topic order."""
        marker = f"{time.time_ns()}-{random.random()}"
        data = {**(data or {}), "_id": marker}
        echo = self.expect(lambda e: e.sender == p.REFEREE and (e.data or {}).get("_id") == marker)
        await self.channel.publish(kind, p.REFEREE, text, to=to, data=data)
        await self.receive(echo, timeout=30)

    async def announce(self, text: str, **data) -> None:
        await self.publish(p.MESSAGE, text, data=data)

    # ---- game --------------------------------------------------------------

    async def run(self) -> None:
        await self.channel.start()
        self._pump = asyncio.create_task(self._pump_events())
        try:
            await self._play()
        finally:
            self._pump.cancel()
            await self.channel.stop()

    async def _play(self) -> None:
        self.logger.info(f"Waiting for {self.n_players} players to join game {self.channel.game_id}...")
        deadline = time.monotonic() + 180
        while len(self.joined) < self.n_players:
            if time.monotonic() > deadline:
                raise SystemExit(f"Only {len(self.joined)} of {self.n_players} players joined")
            await self.wait_for(lambda e: e.kind == p.JOIN, timeout=1)

        alive = list(range(self.n_players))
        mafia = random.choice(alive)
        for pid in alive:
            role = "Mafia" if pid == mafia else "Civilian"
            await self.publish(p.ROLE_ASSIGNED, to=p.player_name(pid), data={"role": role})
        self.logger.info(f"\n🎮 Game started with {self.n_players} players")
        self.logger.info(f"🕵️ Mafia is Player: {mafia}  (hidden from the players)\n")

        winner = None
        round_num = 1
        while len(alive) > 2:
            await self.discussion(round_num, alive)

            out = await self.voting(alive)
            if out is not None:
                alive.remove(out)
                if out == mafia:
                    winner = "Civilians"
                    self.logger.info(f"\n🎉 Mafia {mafia} has been eliminated! Civilians win!")
                    break

            if len(alive) <= 2:
                break
            victim = await self.night(mafia, alive)
            if victim is not None:
                alive.remove(victim)
            round_num += 1

        if winner is None and mafia in alive:
            winner = "Mafia"
            self.logger.info("\n💀 Mafia wins! Only one civilian remains.")
        await self.publish(p.GAME_OVER, data={"winner": winner})

    async def discussion(self, round_num: int, alive: list[int]) -> None:
        now = datetime.now()
        ts = now.strftime("%H:%M:%S")
        end = (now + timedelta(minutes=self.round_minutes)).strftime("%H:%M:%S")
        text = f"[{ts}] Round {round_num} has started! Round will end at {end}\n[{ts}] Players Alive: {alive}"
        self.logger.info(f"\n🔄 Round {round_num} ({self.round_minutes} min)\n{text}")
        await self.publish(p.ROUND_STARTED, text)

        # Players drive the conversation themselves; the referee just keeps time.
        await asyncio.sleep(self.round_minutes * 60)
        await self.publish(p.ROUND_ENDED)

        # Let anyone mid-message finish, like the in-process game waits for its tasks.
        grace_end = time.monotonic() + 60
        while self.typing and time.monotonic() < grace_end:
            await self.wait_for(lambda e: e.kind == p.MESSAGE, timeout=1)
        self.logger.info(f"✅ Round {round_num} completed")

    async def voting(self, alive: list[int]) -> int | None:
        self.logger.info("\n🗳️ Voting...")
        votes: Counter[int] = Counter()
        # One at a time, so later voters see earlier votes, as in the in-process game.
        for pid in alive:
            name = p.player_name(pid)
            ballot = self.expect(lambda e, name=name: e.kind == p.MESSAGE and e.sender == name and "vote" in (e.data or {}))
            await self.publish(p.VOTE_REQUEST, to=name)
            reply = await self.receive(ballot, timeout=120)
            choice = reply.data["vote"] if reply else None
            if choice in alive:
                votes[choice] += 1

        self.logger.info(f"📊 Voting results: {dict(votes)}")
        if not votes:
            await self.announce("No valid votes. No one is eliminated this round.")
            return None
        top = max(votes.values())
        leaders = [pid for pid, n in votes.items() if n == top]
        if len(leaders) > 1:
            await self.announce(f"Tie between players {leaders}. No one is eliminated this round.")
            return None
        out = leaders[0]
        await self.announce(f"Player {out} has been eliminated!", eliminated=p.player_name(out))
        return out

    async def night(self, mafia: int, alive: list[int]) -> int | None:
        self.logger.info("\n🔪 Mafia is choosing a victim...")
        choice = self.expect(lambda e: e.kind == p.KILL and e.to == p.REFEREE)
        await self.publish(p.KILL_REQUEST, to=p.player_name(mafia))
        reply = await self.receive(choice, timeout=120)
        target = reply.data.get("target") if reply else None
        if target not in alive or target == mafia:
            await self.announce("The night passes quietly. No one was killed.")
            return None
        await self.announce(f"Player {target} has been killed by the Mafia!", eliminated=p.player_name(target))
        return target


async def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--game", required=True, help="game id")
    parser.add_argument("--players", type=int, default=5)
    parser.add_argument("--round-minutes", type=float, default=1.5)
    args = parser.parse_args()

    logger = setup_logging(game_name="mafia-distributed")
    referee = Referee(GameChannel(args.game), args.players, args.round_minutes, logger)
    await referee.run()


if __name__ == "__main__":
    asyncio.run(main())
