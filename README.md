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

The controller doesn't poll players on a schedule. Every change to the shared context —
a message, a typing indicator, a round announcement — is published as an event on an
**event bus**, and the controller subscribes to it: whenever an event arrives it wakes
every idle player so they can re-evaluate whether to jump in. Bursts of events are
coalesced into a single wake-up. Each awakened player runs as its own `asyncio` task; the round ends on a wall-clock
timer, after which the controller gathers all outstanding tasks before moving to voting.

### Event bus and Kafka

The bus has two backends, chosen by the `EVENT_BUS` environment variable:

| Backend | `EVENT_BUS` | Notes |
|---------|-------------|-------|
| In-memory | `memory` (default) | Single process, no extra dependencies. |
| Kafka | `kafka` | Every event is written to a Kafka topic, so the game is observable, replayable and consumable by other services. |

With Kafka, each event is a JSON record keyed by the game's `game_id` (so one game's
events stay ordered on one partition):

```json
{"game_id": "84511eb4-…", "kind": "message", "sender": "Player 3", "text": "Voted for Player 1", "ts": 1795547410.1}
```

`kind` is one of `message`, `typing` or `round_started`. Votes, kills and eliminations
arrive as `message` events because they're posted to the transcript.

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
  Context.py                shared transcript, typing buffer, round announcements;
                            publishes every change to the event bus

bus/
  events.py                 Event record (game_id, kind, sender, text, ts)
  base.py                   EventBus interface + per-game Subscription
  memory.py                 in-process backend (default)
  kafka.py                  Kafka backend (aiokafka)
  tail.py                   CLI that prints a game's events live from Kafka
docker-compose.yml          single-node Kafka for local runs

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

For the optional Kafka backend, also install `aiokafka`:

```bash
pip install aiokafka
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

To play Undercover, set `GAME`:

```bash
GAME=undercover python main.py
```

### Running on GitHub Actions

`.github/workflows/run-game.yml` runs one game on a GitHub runner, with a Kafka service
container and the `OPENAI_API_KEY` repository secret (Settings → Secrets and variables →
Actions). Start it from the Actions tab ("Run game" → Run workflow) and pick the game;
the transcript and the Kafka event dump are uploaded as a run artifact.

### Running with Kafka

Start a local broker, then run with the Kafka bus enabled:

```bash
docker compose up -d
```

```bash
EVENT_BUS=kafka python main.py
```

In another terminal, watch the event stream (or replay past games with
`--from-beginning`, and filter to one game with `--game <game_id>`):

```bash
python -m bus.tail
```

`KAFKA_BOOTSTRAP_SERVERS` (default `localhost:9092`) and `KAFKA_TOPIC` (default
`agent-events`) point the game at a different cluster or topic. The topic is created on
first use if it doesn't exist.

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
