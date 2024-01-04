<<<<<<< HEAD
import pandas as pd

score = 1

#define k factor assumptions
def k_factor(matches_played):
	K = 250
	offset = 5
	shape = 0.4
	return K/(matches_played + offset)**shape

#define a function for calculating the expected score of player_A
#expected score of player_B = 1 - expected score of player
def calc_exp_score(playerA_surface_elo, playerB_surface_elo):
	exp_score = 1/(1+(10**((playerB_surface_elo - playerA_surface_elo)/400)))
	return exp_score
	
#define a function for calculating new elo
def update_elo(old_elo_surface, k, actual_score, expected_score):
	new_elo_surface = old_elo_surface + k *(actual_score - expected_score)	
	return new_elo_surface

def update_player_stats(row, players_dict, k_factor_func, score):
    winner_id, loser_id, surface = row['winner_id'], row['loser_id'], row['surface']
    surface = row['surface'].lower()
    tourney_date = row['tourney_date']
    winner_data = players_dict[winner_id]
    loser_data = players_dict[loser_id]
    winner_elo = winner_data[f'current_elo_{surface}']
    loser_elo = loser_data[f'current_elo_{surface}']
    exp_score_winner = calc_exp_score(winner_elo, loser_elo)
    exp_score_loser = 1 - exp_score_winner
    k_winner = k_factor_func(winner_data['matches_played'])
    k_loser = k_factor_func(loser_data['matches_played'])
    winner_data['matches_played'] += 1
    loser_data['matches_played'] += 1
    updated_winner_elo = update_elo(winner_elo, k_winner, 1, exp_score_winner)
    updated_loser_elo = update_elo(loser_elo, k_loser, 0, exp_score_loser)
    winner_data[f'current_elo_{surface}'] = updated_winner_elo
    loser_data[f'current_elo_{surface}'] = updated_loser_elo
    winner_data['current_elo'] = (winner_data['current_elo_hard'] +  winner_data['current_elo_grass'] +  winner_data['current_elo_clay']) / 3
    loser_data['current_elo'] = (loser_data['current_elo_hard'] + loser_data['current_elo_grass'] + loser_data['current_elo_clay']) / 3

    if updated_winner_elo > winner_data['peak_elo']:
        winner_data['peak_elo'] = updated_winner_elo
        winner_data['peak_elo_date'] = tourney_date

    return updated_winner_elo, updated_loser_elo





player_columns_to_read = ['player_id', 'name_first', 'name_last', 'ioc']
players_df = pd.read_csv('atp_players.csv', usecols=player_columns_to_read)
players_df.rename(columns={'ioc': 'country'}, inplace = True)
players_df['current_elo'] = 1500
players_df['current_elo_hard'] = 1500
players_df['current_elo_clay'] = 1500
players_df['current_elo_grass'] = 1500
players_df['current_elo_carpet'] = 1500
players_df['matches_played'] = 0
players_df['peak_elo'] = 1500
players_df['peak_elo_date'] = 'N/A'

players_dict = players_df.set_index('player_id').to_dict('index')

for year in range(2000, 2023):
    matches_df = pd.read_csv(f'atp_matches_{year}.csv')
    matches_df.apply(lambda row: update_player_stats(row, players_dict, k_factor, score), axis=1)
    
    # Update players_df from players_dict
    for player_id, player_data in players_dict.items():
        for key, value in player_data.items():
            players_df.loc[players_df['player_id'] == player_id, key] = value

players_df.to_csv('2023_YE_elo_rankings.csv')
=======
import pandas as pd

score = 1

#define k factor assumptions
def k_factor(matches_played):
	K = 250
	offset = 5
	shape = 0.4
	return K/(matches_played + offset)**shape

#define a function for calculating the expected score of player_A
#expected score of player_B = 1 - expected score of player
def calc_exp_score(playerA_surface_elo, playerB_surface_elo):
	exp_score = 1/(1+(10**((playerB_surface_elo - playerA_surface_elo)/400)))
	return exp_score
	
#define a function for calculating new elo
def update_elo(old_elo_surface, k, actual_score, expected_score):
	new_elo_surface = old_elo_surface + k *(actual_score - expected_score)	
	return new_elo_surface

def update_player_stats(row, players_dict, k_factor_func, score):
    winner_id, loser_id, surface = row['winner_id'], row['loser_id'], row['surface']
    surface = row['surface'].lower()
    tourney_date = row['tourney_date']
    winner_data = players_dict[winner_id]
    loser_data = players_dict[loser_id]
    winner_elo = winner_data[f'current_elo_{surface}']
    loser_elo = loser_data[f'current_elo_{surface}']
    exp_score_winner = calc_exp_score(winner_elo, loser_elo)
    exp_score_loser = 1 - exp_score_winner
    k_winner = k_factor_func(winner_data['matches_played'])
    k_loser = k_factor_func(loser_data['matches_played'])
    winner_data['matches_played'] += 1
    loser_data['matches_played'] += 1
    updated_winner_elo = update_elo(winner_elo, k_winner, 1, exp_score_winner)
    updated_loser_elo = update_elo(loser_elo, k_loser, 0, exp_score_loser)
    winner_data[f'current_elo_{surface}'] = updated_winner_elo
    loser_data[f'current_elo_{surface}'] = updated_loser_elo
    winner_data['current_elo'] = (winner_data['current_elo_hard'] +  winner_data['current_elo_grass'] +  winner_data['current_elo_clay']) / 3
    loser_data['current_elo'] = (loser_data['current_elo_hard'] + loser_data['current_elo_grass'] + loser_data['current_elo_clay']) / 3

    if updated_winner_elo > winner_data['peak_elo']:
        winner_data['peak_elo'] = updated_winner_elo
        winner_data['peak_elo_date'] = tourney_date

    return updated_winner_elo, updated_loser_elo

player_columns_to_read = ['player_id', 'name_first', 'name_last', 'ioc']
players_df = pd.read_csv('atp_players.csv', usecols=player_columns_to_read)
players_df.rename(columns={'ioc': 'country'}, inplace = True)
players_df['current_elo'] = 1500
players_df['current_elo_hard'] = 1500
players_df['current_elo_clay'] = 1500
players_df['current_elo_grass'] = 1500
players_df['current_elo_carpet'] = 1500
players_df['matches_played'] = 0
players_df['peak_elo'] = 1500
players_df['peak_elo_date'] = 'N/A'

players_dict = players_df.set_index('player_id').to_dict('index')

for year in range(2000, 2023):
    matches_df = pd.read_csv(f'atp_matches_{year}.csv')
    matches_df.apply(lambda row: update_player_stats(row, players_dict, k_factor, score), axis=1)
    
    # Update players_df from players_dict
    for player_id, player_data in players_dict.items():
        for key, value in player_data.items():
            players_df.loc[players_df['player_id'] == player_id, key] = value

players_df.to_csv('2023_YE_elo_rankings.csv')
>>>>>>> master
