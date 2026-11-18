"""Start a distributed Mafia game on this machine: one referee + one process per player.

    python -m distributed.launch --players 5

Each process could just as well run on a different machine; all they share is
the Kafka broker (KAFKA_BOOTSTRAP_SERVERS) and the game id.
"""
import argparse
import asyncio
import subprocess
import sys
import uuid

from bus.kafka import ensure_topic
from distributed.channel import bootstrap_servers
from distributed.protocol import topic_for


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--players", type=int, default=5)
    parser.add_argument("--round-minutes", type=float, default=1.5)
    parser.add_argument("--generator-model", default="gpt-4.1")
    parser.add_argument("--scheduler-model", default="gpt-4.1-mini")
    parser.add_argument("--game", default=None, help="game id (default: a new random one)")
    args = parser.parse_args()

    game_id = args.game or str(uuid.uuid4())
    asyncio.run(ensure_topic(bootstrap_servers(), topic_for(game_id), partitions=1))
    print(f"Game {game_id} on topic {topic_for(game_id)}", flush=True)

    py = [sys.executable, "-m"]
    referee = subprocess.Popen(py + ["distributed.referee", "--game", game_id,
                                     "--players", str(args.players), "--round-minutes", str(args.round_minutes)])
    players = [
        subprocess.Popen(py + ["distributed.player", "--game", game_id, "--id", str(i),
                               "--generator-model", args.generator_model, "--scheduler-model", args.scheduler_model])
        for i in range(args.players)
    ]

    try:
        code = referee.wait()
    except KeyboardInterrupt:
        referee.terminate()
        code = 130
    for proc in players:
        try:
            proc.wait(timeout=30)  # players exit on their own when they see game_over
        except subprocess.TimeoutExpired:
            proc.terminate()
    sys.exit(code)


if __name__ == "__main__":
    main()
