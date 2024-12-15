import redis
import pandas as pd
import json

# Redis client connection
redis_client = redis.StrictRedis(host='localhost', port=6379, db=0)

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

def save_redis_to_csv(redis_client, output_file):
    # Retrieve all player data from Redis
    players_data = [
        json.loads(value)
        for value in redis_client.hvals('players_data')
    ]
    players_df = pd.DataFrame(players_data)
    players_df.to_csv(output_file, index=False)
    print(f"Results saved to {output_file}")
