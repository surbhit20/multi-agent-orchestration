from game.mafia.player import MafiaPlayer
from context.Context import Context
import asyncio
import random
import time
import uuid
from bus import create_bus_from_env
from game.mafia.prompts import SCHEDULER_INSTRUCTIONS, VOTING_PROMPT, KILL_PROMPT, GENERATOR_INSTRUCTIONS
from collections import defaultdict
from logger_config import get_logger

config = {
    "generator_model_name": "gpt-4.1",
    "scheduler_model_name": "gpt-4.1-mini",
    "scheduler_instructions": SCHEDULER_INSTRUCTIONS,
    "generator_instructions": GENERATOR_INSTRUCTIONS,
    "vote_instructions": VOTING_PROMPT,
    "kill_instructions": KILL_PROMPT,
}

class Game:
    def __init__(self, n_players: int, game_name: str = "mafia"):
        self.n_players = n_players
        self.game_name = game_name
        self.mafia_idx = random.randint(0, n_players - 1) 
        self.players = {i: MafiaPlayer(f"Player {i}", "Civilian" if i != self.mafia_idx else "Mafia", config) for i in range(self.n_players)}
        self.context_manager = Context(game_name=game_name)
        self.logger = get_logger('controller', game_name=game_name)
        self.game_id = str(uuid.uuid4())
        self.bus = create_bus_from_env()
        self.context_manager.attach_bus(self.bus, self.game_id)

    async def start_game(self):
        await self.bus.start()
        try:
            await self._play()
        finally:
            await self.bus.stop()

    async def _play(self):
        round_num = 1
        print(f"\n🎮 Game started with {self.n_players} players")
        print(f"🕵️ Mafia is Player: {self.mafia_idx}\n")
        self.logger.info(f"Game started with {self.n_players} players")
        self.logger.debug(f"Mafia is Player: {self.mafia_idx}")
        
        while len(self.players) > 2:
            await self.run_round(round_num)
            
            await self.voting_round()
            if self.mafia_idx not in self.players: 
                print(f"\n🎉 Mafia {self.mafia_idx} has been eliminated! Civilians win!")
                self.logger.info(f"Mafia {self.mafia_idx} has been eliminated! Civilians win!")
                break
            
            await self.kill_round()
            round_num += 1

        if len(self.players) == 2 and self.mafia_idx in self.players:
            print("\n💀 Mafia wins! Only one civilian remains.")
            self.logger.info("Mafia wins! Only one civilian remains.")

    async def run_round(self, round_num: int, duration_minutes: int = 1.5):
        print(f"\n🔄 Starting round {round_num} (duration: {duration_minutes} minutes)")
        self.logger.info(f"Starting round {round_num} (duration: {duration_minutes} minutes)")
        # Subscribe before announcing the round so the announcement itself wakes everyone.
        events = self.bus.subscribe(self.game_id)
        await self.context_manager.start_round(round_num, duration_minutes, list(self.players.keys()))
        
        deadline = time.monotonic() + duration_minutes * 60
        player_tasks = []  # Track all player tasks
        
        try:
            # Wake idle players whenever the transcript changes (a message, a typing
            # indicator, or the round announcement) until the round's time is up.
            while (remaining := deadline - time.monotonic()) > 0:
                if not await events.wait_for_activity(timeout=remaining):
                    break
                idle_players = [player for player in self.players.values() if player.is_idle]
                for player in idle_players:
                    self.logger.debug(f"Starting task for idle player: {player.name}")
                    player_tasks.append(asyncio.create_task(player.run()))
        finally:
            events.close()
        
        # Wait for all player tasks to complete before ending the round
        if player_tasks:
            self.logger.debug(f"Waiting for {len(player_tasks)} player tasks to complete")
            await asyncio.gather(*player_tasks)
        
        print(f"✅ Round {round_num} completed")
        self.logger.info(f"Round {round_num} completed")
    
    async def voting_round(self):
        print("\n🗳️ Starting voting round...")
        self.logger.info("Starting voting round")
        votes = defaultdict(int)
        for player in self.players.values():
            vote = await player.vote()
            votes[int(vote)] += 1
            self.logger.debug(f"{player.name} voted for Player {vote}")
        
        # Find the player(s) with the maximum votes
        max_votes = max(votes.values())
        max_voted_players = [player for player, count in votes.items() if count == max_votes]
        
        print(f"📊 Voting results: {dict(votes)}")
        self.logger.debug(f"Voting results: {dict(votes)}")
        
        if len(max_voted_players) > 1:
            print(f"⚖️ Tie detected between players: {max_voted_players}. No one is eliminated this round.")
            self.logger.info(f"Tie detected between players: {max_voted_players}. No one is eliminated this round.")
            return  # No one is eliminated in case of a tie
        
        max_vote = max_voted_players[0]
        if max_vote in self.players: 
            del self.players[max_vote]
            print(f"❌ Player {max_vote} has been eliminated by vote!")
            self.logger.info(f"Player {max_vote} has been eliminated by vote!")

        await self.context_manager.add_message("Game", f"Player {max_vote} has been eliminated!")
    
    async def kill_round(self):
        print("\n🔪 Mafia is choosing a victim...")
        self.logger.info("Mafia is choosing a victim...")
        killed_idx = await self.players[self.mafia_idx].kill()
        
        if int(killed_idx) in self.players: 
            del self.players[int(killed_idx)]
            print(f"☠️ Player {killed_idx} has been killed by the Mafia!")
            self.logger.info(f"Player {killed_idx} has been killed by the Mafia!")

        await self.context_manager.add_message("Game", f"Player {killed_idx} has been killed by the Mafia!")
