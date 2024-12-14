import pandas as pd

score = 1

def k_factor(matches_played):
	K = 250
	offset = 5
	shape = 0.4
	return K/(matches_played + offset)**shape

def calc_exp_score(playerA_elo, playerB_elo):
	exp_score = 1/(1+(10**((playerB_elo - playerA_elo)/400)))
	return exp_score
	
def update_elo(old_elo, k, actual_score, expected_score):
	new_elo = old_elo + k *(actual_score - expected_score)	
	return new_elo

def update_player_stats(row, players_dict, k_factor_func, score):
    winner_id, loser_id = row['winner_id'], row['loser_id']
    tourney_date = row['tourney_date']
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

    return updated_winner_elo, updated_loser_elo

player_columns_to_read = ['player_id', 'name_first', 'name_last', 'ioc']
players_df = pd.read_csv('csv/atp_players.csv', usecols=player_columns_to_read) # Chunkify?
players_df.rename(columns={'ioc': 'country'}, inplace = True)
players_df['current_elo'] = 1500
players_df['matches_played'] = 0
players_df['peak_elo'] = 1500
players_df['peak_elo_date'] = 'N/A'

players_df['current_elo'] = players_df['current_elo'].astype(float)
players_df['peak_elo'] = players_df['peak_elo'].astype(float)

# What is this?
players_dict = players_df.set_index('player_id').to_dict('index')
elo_ratings_annual = {}
default_value = 1500
name_mapping = {row['player_id']: f"{row['name_first']} {row['name_last']}" for index, row in players_df.iterrows()}


for year in range(2000, 2002):
    print("Starting processing for " + str(year) + "'s matches")
    counter = 0
    for chunk in pd.read_csv(f'csv/matches/atp_matches_{year}.csv', chunksize=1000):
         print("Applying functions to " + str(year) + "'s matches")
         chunk.apply(lambda row: update_player_stats(row, players_dict, k_factor, score), axis=1)
        
         print("Updating players for " + str(year) + "'s matches")
        # updates the players csv?
         for player_id, player_data in players_dict.items():
            for key, value in player_data.items():
                players_df.loc[players_df['player_id'] == player_id, key] = value
    print(str(year) + "complete")

players_df.to_csv('2023_YE_elo_rankings.csv')
