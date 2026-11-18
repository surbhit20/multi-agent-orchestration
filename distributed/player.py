"""One Mafia player, running as its own process.

    python -m distributed.player --game <game_id> --id 3

The player knows nothing about the other players or the referee except what
arrives on the game's Kafka topic. It rebuilds the chat transcript from those
events, decides for itself when to speak, and posts its messages back.
"""
import argparse
import asyncio
import logging
import time
from datetime import datetime

import dotenv
from agents import Runner
from pydantic import BaseModel

from agent.generator import get_generator_agent
from agent.scheduler import get_scheduler_agent
from bus.events import Event
from distributed import protocol as p
from distributed.channel import GameChannel
from game.mafia.prompts import GENERATOR_INSTRUCTIONS, KILL_PROMPT, SCHEDULER_INSTRUCTIONS, VOTING_PROMPT

log = logging.getLogger("distributed.player")


class SelectPlayer(BaseModel):
    player_id: str


def _fmt(event: Event) -> str:
    if event.kind == p.ROUND_STARTED:
        return event.text  # already formatted, with its own timestamps
    ts = datetime.fromtimestamp(event.ts).strftime("%H:%M:%S")
    return f"[{ts}] {event.sender}: {event.text}"


class PlayerProcess:
    def __init__(self, channel: GameChannel, player_id: int, generator_model: str, scheduler_model: str):
        self.channel = channel
        self.name = p.player_name(player_id)
        self.generator_model = generator_model
        self.scheduler_model = scheduler_model

        self.role: str | None = None
        self.alive = True
        self.discussion_open = False
        self.transcript: list[str] = []
        self.typing: set[str] = set()

        self.generator = None
        self.scheduler = None
        self._thinking: asyncio.Task | None = None
        self._dirty = False
        self._tasks: set[asyncio.Task] = set()

    # ---- state -------------------------------------------------------------

    def context(self) -> str:
        """What this player 'sees': the transcript plus who is typing right now."""
        now = datetime.now().strftime("%H:%M:%S")
        lines = self.transcript + [f"[{now}] {user} is typing..." for user in sorted(self.typing)]
        return "".join(line + "\n" for line in lines)

    def apply(self, event: Event) -> None:
        """Update local state from an event. Runs for replayed events too."""
        if event.to not in (None, self.name):
            return  # someone else's private event

        if event.kind == p.ROLE_ASSIGNED:
            self._become(event.data["role"])
        elif event.kind == p.ROUND_STARTED:
            self.discussion_open = True
            self.typing.clear()
        elif event.kind == p.ROUND_ENDED:
            self.discussion_open = False
        elif event.kind == p.TYPING:
            self.typing.add(event.sender)
        elif event.kind == p.MESSAGE:
            self.typing.discard(event.sender)
            if (event.data or {}).get("eliminated") == self.name:
                self.alive = False

        if event.kind in p.TRANSCRIPT_KINDS and event.to is None:
            self.transcript.append(_fmt(event))

    def _become(self, role: str) -> None:
        self.role = role
        self.generator = get_generator_agent(
            self.name,
            instructions=GENERATOR_INSTRUCTIONS.format(name=self.name, role=role),
            model_name=self.generator_model,
        )
        self.scheduler = get_scheduler_agent(
            self.name,
            instructions=SCHEDULER_INSTRUCTIONS.format(name=self.name, role=role),
            model_name=self.scheduler_model,
        )

    # ---- reacting ----------------------------------------------------------

    def react(self, event: Event) -> None:
        """Decide what to do about a live (non-replayed) event."""
        if self.role is None or not self.alive:
            return
        if event.to == self.name and event.kind == p.VOTE_REQUEST:
            self._spawn(self.vote())
        elif event.to == self.name and event.kind == p.KILL_REQUEST:
            self._spawn(self.kill())
        elif event.to is None and event.kind in (p.ROUND_STARTED, p.MESSAGE, p.TYPING) and self.discussion_open:
            self._poke()

    def _spawn(self, coro) -> None:
        task = asyncio.create_task(coro)
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)

    def _poke(self) -> None:
        """The chat changed. Think about it now, or again right after the current thought."""
        if self._thinking and not self._thinking.done():
            self._dirty = True
            return
        self._thinking = asyncio.create_task(self._think_loop())

    async def _think_loop(self) -> None:
        while True:
            self._dirty = False
            try:
                await self.think_once()
            except Exception:
                log.exception("%s failed while thinking", self.name)
            if not (self._dirty and self.discussion_open and self.alive):
                return

    async def think_once(self) -> None:
        context = self.context()
        decision = await Runner.run(starting_agent=self.scheduler, input=context)
        if not decision.final_output.should_speak:
            return

        await self.channel.publish(p.TYPING, self.name)
        started = time.monotonic()
        reply = (await Runner.run(starting_agent=self.generator, input=context)).final_output

        # Simulate human typing speed: long messages arrive later.
        delay = (40 * len(reply.split(" "))) / 60
        await asyncio.sleep(max(0.0, delay - (time.monotonic() - started)))
        await self.channel.publish(p.MESSAGE, self.name, reply)

    async def _pick_player(self, instructions: str) -> int | None:
        agent = self.generator.clone(output_type=SelectPlayer)
        result = await Runner.run(starting_agent=agent, input=self.context() + instructions)
        try:
            return int(result.final_output.player_id)
        except ValueError:
            log.warning("%s gave an unparseable choice: %r", self.name, result.final_output.player_id)
            return None

    async def vote(self) -> None:
        choice = await self._pick_player(VOTING_PROMPT)
        await self.channel.publish(p.MESSAGE, self.name, f"Voted for Player {choice}", data={"vote": choice})

    async def kill(self) -> None:
        choice = await self._pick_player(KILL_PROMPT)
        await self.channel.publish(p.KILL, self.name, to=p.REFEREE, data={"target": choice})

    # ---- main loop ---------------------------------------------------------

    async def run(self) -> None:
        await self.channel.start()
        try:
            await self.channel.publish(p.JOIN, self.name)
            async for event, is_replay in self.channel.events():
                self.apply(event)
                if event.kind == p.GAME_OVER:
                    break
                if not is_replay:
                    self.react(event)
        finally:
            for task in [self._thinking, *self._tasks]:
                if task:
                    task.cancel()
            await self.channel.stop()


async def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--game", required=True, help="game id")
    parser.add_argument("--id", type=int, required=True, help="player number")
    parser.add_argument("--generator-model", default="gpt-4.1")
    parser.add_argument("--scheduler-model", default="gpt-4.1-mini")
    args = parser.parse_args()

    logging.basicConfig(level=logging.WARNING, format="%(asctime)s %(name)s %(levelname)s %(message)s")
    dotenv.load_dotenv(override=True)
    player = PlayerProcess(GameChannel(args.game), args.id, args.generator_model, args.scheduler_model)
    await player.run()


if __name__ == "__main__":
    asyncio.run(main())
