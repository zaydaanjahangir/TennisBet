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

def update_player_stats(row, redis_client, k_factor_func):
    # Extract winner and loser IDs
    winner_id, loser_id = str(row['winner_id']), str(row['loser_id'])
    tourney_date = row['tourney_date']

    # Fetch player data from Redis
    winner_data = redis_client.hget('players_data', winner_id)
    loser_data = redis_client.hget('players_data', loser_id)

    if not winner_data or not loser_data:
        print(f"Player ID missing: winner_id={winner_id}, loser_id={loser_id}")
        return

    # Deserialize player data
    winner_data = json.loads(winner_data)
    loser_data = json.loads(loser_data)

    # Perform Elo calculations
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

    # Write updated player data back to Redis
    redis_client.hset('players_data', winner_id, json.dumps(winner_data))
    redis_client.hset('players_data', loser_id, json.dumps(loser_data))

# Initialize Redis with Player Data
def initialize_redis(redis_client, players_file):
    players_df = pd.read_csv(players_file, usecols=['player_id', 'name_first', 'name_last', 'ioc'])
    players_df.rename(columns={'ioc': 'country'}, inplace=True)
    players_df['current_elo'] = 1500
    players_df['matches_played'] = 0
    players_df['peak_elo'] = 1500
    players_df['peak_elo_date'] = 'N/A'
    players_df['player_id'] = players_df['player_id'].astype(str)

    # Store players in Redis hash
    with redis_client.pipeline() as pipe:
        for _, player in players_df.iterrows():
            pipe.hset('players_data', player['player_id'], player.to_json())
        pipe.execute()

# Process matches and update Elo ratings
def process_matches(redis_client, matches_file_pattern, start_year, end_year):
    for year in range(start_year, end_year + 1):
        print(f"Starting processing for {year}'s matches")
        for chunk in pd.read_csv(matches_file_pattern.format(year=year), chunksize=1000):
            start_time = time.time()
            chunk.apply(lambda row: update_player_stats(row, redis_client, k_factor), axis=1)
            elapsed_time = time.time() - start_time
            print(f"Processed chunk in {elapsed_time:.2f} seconds")

# Save Redis Data to CSV
def save_redis_to_csv(redis_client, output_file):
    # Retrieve all player data from Redis
    players_data = [
        json.loads(value)
        for value in redis_client.hvals('players_data')
    ]
    players_df = pd.DataFrame(players_data)
    players_df.to_csv(output_file, index=False)

# Main script execution
if __name__ == "__main__":
    initialize_redis(redis_client, 'csv/atp_players.csv')
    process_matches(redis_client, 'csv/matches/atp_matches_{year}.csv', 2010, 2024)
    save_redis_to_csv(redis_client, '2023_YE_elo_rankings.csv')
