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

def update_player_stats(row, players_dict, k_factor_func):
    winner_id, loser_id = str(row['winner_id']), str(row['loser_id'])
    tourney_date = row['tourney_date']

    if winner_id not in players_dict or loser_id not in players_dict:
        print(f"Player ID missing: winner_id={winner_id}, loser_id={loser_id}")
        return

    winner_data = players_dict[winner_id]
    loser_data = players_dict[loser_id]

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

# Load players data into Python dictionary 
players_df = pd.read_csv('csv/atp_players.csv', usecols=['player_id', 'name_first', 'name_last', 'ioc'])
players_df.rename(columns={'ioc': 'country'}, inplace=True)
players_df['current_elo'] = 1500
players_df['matches_played'] = 0
players_df['peak_elo'] = 1500
players_df['peak_elo_date'] = 'N/A'
players_df['player_id'] = players_df['player_id'].astype(str)

# Initialize players dictionary
players_dict = players_df.set_index('player_id').to_dict('index')

# Process matches and update Elo ratings
for year in range(2010, 2025):
    print(f"Starting processing for {year}'s matches")
    for chunk in pd.read_csv(f'csv/matches/atp_matches_{year}.csv', chunksize=1000):
        start_time = time.time()
        chunk.apply(lambda row: update_player_stats(row, players_dict, k_factor), axis=1)
        elapsed_time = time.time() - start_time
        print(f"Processed chunk in {elapsed_time:.2f} seconds")

# Store updated player data in Redis (final persistence)
with redis_client.pipeline() as pipe:
    for player_id, player_data in players_dict.items():
        pipe.hset('players_data', player_id, json.dumps(player_data))
    pipe.execute()

# Save updated player data to CSV
players_df = pd.DataFrame.from_dict(players_dict, orient='index')
players_df.to_csv('2023_YE_elo_rankings.csv', index=False)
