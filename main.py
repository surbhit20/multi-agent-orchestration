import asyncio
import os

from logger_config import setup_logging

# GAME=mafia (default) or GAME=undercover
game_name = os.getenv("GAME", "mafia").lower()
logger = setup_logging(game_name=game_name)

if game_name == "undercover":
    from game.undercover.controller import Game
    game = Game(n_players=5, n_imposters=1)
elif game_name == "mafia":
    from game.mafia.controller import Game
    game = Game(n_players=5)
else:
    raise SystemExit(f"Unknown GAME {game_name!r}; use 'mafia' or 'undercover'")

logger.debug(f"{game_name} Game Starting...")
asyncio.run(game.start_game())
logger.debug("Game finished!")
