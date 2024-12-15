import redis
import requests
from bs4 import BeautifulSoup
import pandas as pd
import json
from utils.redis_utils import get_player_data_by_name
from utils.redis_utils import set_player_data_by_name


redis_client = redis.StrictRedis(host='localhost', port=6379, db=0)

URL = "https://tennisabstract.com/reports/atp_elo_ratings.html"


def scrape_elo_ratings():
    data = requests.get(URL).text
    soup = BeautifulSoup(data, "html.parser")
    
    table = soup.find('table', class_='tablesorter')
    df = pd.DataFrame(columns=['Player', 'Age', 'Elo'])

    for row in table.tbody.find_all('tr'):
        columns = row.find_all('td')
        if columns:
            player = columns[1].text.strip()
            age = columns[2].text.strip()
            elo = columns[3].text.strip()
            
            new_data = pd.DataFrame([{'Player': player, 'Age': age, 'Elo': elo}])
            df = pd.concat([df, new_data], ignore_index=True)
    
    return df



def update_redis_with_elo(player_df):
    for _, player in player_df.iterrows():
        full_name = player["Player"]
        print(f"Processing player: {full_name}")
        
        name_parts = full_name.split()
        if len(name_parts) < 2:
            print(f"Skipping player {full_name} due to invalid name format.")
            continue
        first_name = name_parts[0]
        last_name = " ".join(name_parts[1:])
        
        redis_data = get_player_data_by_name(redis_client, first_name, last_name)
        if not redis_data:
            print(f"Player with name '{full_name}' not found in Redis.")
            continue
        
        redis_data["current_elo"] = player["Elo"]
        
        if not set_player_data_by_name(redis_client, first_name, last_name, redis_data):
            print(f"Failed to update data for player {full_name}.")
        else:
            print(f"Successfully updated data for player {full_name}.")



if __name__ == "__main__":
    player_df = scrape_elo_ratings()
    update_redis_with_elo(player_df)