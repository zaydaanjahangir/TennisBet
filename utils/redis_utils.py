import redis
import pandas as pd
import json

redis_client = redis.StrictRedis(host='localhost', port=6379, db=0)


def initialize_redis(redis_client, players_file):
    players_df = pd.read_csv(players_file, usecols=['player_id', 'name_first', 'name_last', 'ioc'])
    players_df.rename(columns={'ioc': 'country'}, inplace=True)
    players_df['current_elo'] = 1500
    players_df['matches_played'] = 0
    players_df['peak_elo'] = 1500
    players_df['peak_elo_date'] = 'N/A'
    players_df['player_id'] = players_df['player_id'].astype(str)

    players_df['name_first'] = players_df['name_first'].fillna('').str.strip().str.lower()
    players_df['name_last'] = players_df['name_last'].fillna('').str.strip().str.lower()

    players_df = players_df[players_df['name_first'] != '']
    players_df = players_df[players_df['name_last'] != '']

    with redis_client.pipeline() as pipe:
        for _, player in players_df.iterrows():
            full_name_key = f"{player['name_first']} {player['name_last']}"
            pipe.hset('players_data', player['player_id'], player.to_json())
            pipe.hset('player_name_to_id', full_name_key, player['player_id'])
        pipe.execute()

    print(f"Redis initialized with {len(players_df)} players.")

def save_redis_to_csv(redis_client, output_file):
    players_data = [
        json.loads(value)
        for value in redis_client.hvals('players_data')
    ]
    players_df = pd.DataFrame(players_data)
    players_df.to_csv(output_file, index=False)
    print(f"Results saved to {output_file}")

def get_player_data_by_name(redis_client, first_name, last_name):
    full_name = f"{first_name} {last_name}".strip()
    player_id = redis_client.hget('player_name_to_id', full_name)

    if player_id is None:
        print(f"Player with name '{full_name}' not found in Redis.")
        return None

    player_data = redis_client.hget('players_data', player_id)

    if player_data is None:
        print(f"Data for player ID '{player_id.decode()}' not found.")
        return None

    return json.loads(player_data)

def set_player_data_by_name(redis_client, first_name, last_name, updated_data):
    full_name = f"{first_name.lower().strip()} {last_name.lower().strip()}"

    player_id = redis_client.hget("player_name_to_id", full_name)
    if not player_id:
        print(f"Error: Player with name '{full_name}' not found in Redis.")
        return False

    player_id = player_id.decode("utf-8") 
    redis_client.hset("players_data", player_id, json.dumps(updated_data))
    print(f"Player data for '{full_name}' successfully updated in Redis.")
    return True
