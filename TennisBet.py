import pandas as pd
import time
import redis
import json

# Connect to Redis
redis_client = redis.StrictRedis(host='localhost', port=6379, db=0)

# Elo calculation functions
def k_factor(matches_played):
    K = 250
    offset = 5
    shape = 0.4
    return K / (matches_played + offset) ** shape

def calc_exp_score(playerA_elo, playerB_elo):
    return 1 / (1 + 10 ** ((playerB_elo - playerA_elo) / 400))

def update_elo(old_elo, k, actual_score, expected_score):
    return old_elo + k * (actual_score - expected_score)

def update_player_stats(row, k_factor_func):
    winner_id, loser_id = str(row['winner_id']), str(row['loser_id'])
    tourney_date = row['tourney_date']

    winner_data = redis_client.get(winner_id)
    loser_data = redis_client.get(loser_id)

    if not winner_data or not loser_data:
        print(f"Player ID missing: winner_id={winner_id}, loser_id={loser_id}")
        return

    winner_data = json.loads(winner_data)
    loser_data = json.loads(loser_data)

    winner_elo = winner_data['current_elo']
    loser_elo = loser_data['current_elo']

    exp_score_winner = calc_exp_score(winner_elo, loser_elo)
    exp_score_loser = 1 - exp_score_winner

    k_winner = k_factor_func(winner_data['matches_played'])
    k_loser = k_factor_func(loser_data['matches_played'])

    winner_data['matches_played'] += 1
    loser_data['matches_played'] += 1

    updated_winner_elo = update_elo(winner_elo, k_winner, 1, exp_score_winner)
    updated_loser_elo = update_elo(loser_elo, k_loser, 0, exp_score_loser)

    winner_data['current_elo'] = updated_winner_elo
    loser_data['current_elo'] = updated_loser_elo

    if updated_winner_elo > winner_data['peak_elo']:
        winner_data['peak_elo'] = updated_winner_elo
        winner_data['peak_elo_date'] = tourney_date

    # Store updated data back in Redis
    redis_client.set(winner_id, json.dumps(winner_data))
    redis_client.set(loser_id, json.dumps(loser_data))

# Load players data into Redis
players_df = pd.read_csv('csv/atp_players.csv', usecols=['player_id', 'name_first', 'name_last', 'ioc'])
players_df.rename(columns={'ioc': 'country'}, inplace=True)
players_df['current_elo'] = 1500
players_df['matches_played'] = 0
players_df['peak_elo'] = 1500
players_df['peak_elo_date'] = 'N/A'
players_df['player_id'] = players_df['player_id'].astype(str)

# Store each player's data in a single Redis hash named 'players_data'
for _, player in players_df.iterrows():
    redis_client.hset('players_data', player['player_id'], player.to_json())


# Process matches and update Elo ratings
for year in range(2010, 2025):
    print(f"Starting processing for {year}'s matches")
    for chunk in pd.read_csv(f'csv/matches/atp_matches_{year}.csv', chunksize=1000):
        start_time = time.time()
        chunk.apply(lambda row: update_player_stats(row, k_factor), axis=1)
        elapsed_time = time.time() - start_time
        print(f"Processed chunk in {elapsed_time:.2f} seconds")

        # Backup results to disk after each batch
        # Retrieve all players' data from the Redis hash
        players_backup = {
            key.decode(): json.loads(value) 
            for key, value in redis_client.hgetall('players_data').items()
        }


# Save updated player data to CSV
players_data = [json.loads(value) for key, value in redis_client.hgetall('players_data').items()]
players_df = pd.DataFrame(players_data)
players_df.to_csv('2023_YE_elo_rankings.csv', index=False)
