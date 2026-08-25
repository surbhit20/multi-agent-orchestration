# Multi-Agent Orchestration

A framework for running **social deduction games between LLM agents**. Instead of taking
turns in a fixed order, every player is an autonomous agent that watches a shared
conversation and decides for itself when to speak — so discussions unfold concurrently,
with interruptions, overlapping replies, and "is typing…" pressure, the way a real
group chat does.

Two games ship with the framework:

| Game | Description |
|------|-------------|
| **Mafia** | One hidden Mafia player eliminates a civilian each night; civilians vote someone out each day. |
| **Undercover** | Civilians share a secret word, impostors get nothing. Everyone gives a one-word clue, then the group votes out whoever seems to be faking. |

## How it works

### Two agents per player

Each player is not a single LLM call — it is a pair of agents with different jobs and
different models:

- **Scheduler** (cheap, fast model) — reads the current transcript and answers one
  question: *should I speak right now?* This is what makes conversation feel organic
  rather than round-robin.
- **Generator** (stronger model) — only runs once the scheduler says yes, and writes the
  actual message in character, given the player's hidden role and word.

The generator is also reused for structured decisions. For votes, kills, and clue words
it is cloned with a Pydantic `output_type` (`SelectPlayer`, `SayWord`), so the model must
return a parseable object instead of prose.

### One shared context

`context/Context.py` is an asyncio-guarded singleton holding the whole game transcript.
Everything a player knows comes from it — there is no private channel and no god view.
It also tracks a **typing buffer**: when a player commits to speaking, `X is typing...`
is appended to what every other agent sees, so agents can react to the fact that someone
is mid-sentence.

Before posting, a player sleeps for a length proportional to its message
(`40 * words / 60` seconds), simulating human typing speed. Short replies land first;
long speeches arrive late and can be overtaken.

### The round loop

The controller doesn't poll players on a schedule. It watches the shared context and,
whenever it *changes*, wakes every idle player so they can re-evaluate whether to jump
in. Each awakened player runs as its own `asyncio` task; the round ends on a wall-clock
timer, after which the controller gathers all outstanding tasks before moving to voting.

## Project layout

```
main.py                     entry point — picks a game, sets up logging, runs it
logger_config.py            per-game console + file logging
utils.py

agent/
  Player.py                 base player: scheduler → typing delay → generator → post
  scheduler.py              "should I speak?" agent (ShouldSpeak)
  generator.py              message-writing agent

context/
  Context.py                shared transcript, typing buffer, round announcements

game/
  mafia/
    controller.py           game loop: discussion → vote → night kill
    player.py               MafiaPlayer: vote(), kill()
    prompts.py
  undercover/
    controller.py           game loop: clue words → discussion → vote
    player.py               UndercoverPlayer: vote(), contribute_word()
    prompts.py
    words.txt               secret word pool

logs/                       transcripts from previous runs
```

## Setup

Requires Python 3.10+ (the code uses `X | None` type syntax).

```bash
pip install openai-agents pydantic python-dotenv
```

Add your key in a `.env` file at the project root:

```
OPENAI_API_KEY=your-key-here
```

## Running a game

Mafia is what `main.py` runs by default:

```bash
python main.py
```

To play Undercover, point `main.py` at the other controller:

```python
from game.undercover.controller import Game
from logger_config import setup_logging
import asyncio

logger = setup_logging(game_name="undercover")
game = Game(n_players=5, n_imposters=1)
asyncio.run(game.start_game())
```

The transcript is printed live and written to `logs/<game>_log_<timestamp>.log`. Hidden
information (who the Mafia is, what the secret word is) is printed to the console for
your benefit — the agents themselves never see it.

## Configuration

Models and prompts are set per game in the `config` dict at the top of each controller:

```python
config = {
    "generator_model_name": "gpt-4.1",       # writes messages
    "scheduler_model_name": "gpt-4.1-mini",  # decides when to speak
    ...
}
```

Mixing model tiers is the point — the scheduler fires constantly and should be cheap,
while the generator runs rarely and should be good. `game/undercover/controller.py` also
contains commented-out LiteLLM model strings for running the same games against
non-OpenAI providers.

Other knobs: `n_players` (and `n_imposters` for Undercover) on the `Game` constructor,
the discussion length per round — `round_duration` on Undercover's `Game`, the
`duration_minutes` argument to `run_round` in Mafia — and the word pool in
`game/undercover/words.txt`.
