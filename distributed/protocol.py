"""Event kinds used on a game's Kafka topic.

Public events (to=None) are seen by every player and form the chat transcript.
Private events carry `to` = the recipient's name ("Game" for the referee).
"""

REFEREE = "Game"

# player -> referee
JOIN = "join"                    # a player process is up and listening
KILL = "kill"                    # private, data={"target": int}

# referee -> one player (private)
ROLE_ASSIGNED = "role_assigned"  # data={"role": "Mafia" | "Civilian"}
VOTE_REQUEST = "vote_request"    # it's your turn to vote
KILL_REQUEST = "kill_request"    # Mafia only: pick tonight's victim

# public
ROUND_STARTED = "round_started"  # discussion is open
ROUND_ENDED = "round_ended"      # discussion is closed; finish what you're typing
TYPING = "typing"
MESSAGE = "message"              # chat line; votes carry data={"vote": int},
                                 # eliminations data={"eliminated": name}
GAME_OVER = "game_over"          # data={"winner": "Mafia" | "Civilians"}

# Events that put a line in the transcript every player reads.
TRANSCRIPT_KINDS = {ROUND_STARTED, MESSAGE}


def player_name(player_id: int) -> str:
    return f"Player {player_id}"


def topic_for(game_id: str) -> str:
    return f"game.{game_id}"
