import pandas as pd
import matplotlib.pyplot as plt

score = 1

#define k factor assumptions
def k_factor(matches_played):
	K = 250
	offset = 5
	shape = 0.4
	return K/(matches_played + offset)**shape

#define a function for calculating the expected score of player_A
#expected score of player_B = 1 - expected score of player
def calc_exp_score(playerA_elo, playerB_elo):
	exp_score = 1/(1+(10**((playerB_elo - playerA_elo)/400)))
	return exp_score
	
#define a function for calculating new elo
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
players_df = pd.read_csv('atp_players.csv', usecols=player_columns_to_read)
players_df.rename(columns={'ioc': 'country'}, inplace = True)
players_df['current_elo'] = 1500
players_df['matches_played'] = 0
players_df['peak_elo'] = 1500
players_df['peak_elo_date'] = 'N/A'

players_df['current_elo'] = players_df['current_elo'].astype(float)
players_df['peak_elo'] = players_df['peak_elo'].astype(float)

players_dict = players_df.set_index('player_id').to_dict('index')
elo_ratings_annual = {}
default_value = 1500
name_mapping = {row['player_id']: f"{row['name_first']} {row['name_last']}" for index, row in players_df.iterrows()}


for year in range(2000, 2024):
    matches_df = pd.read_csv(f'atp_matches_{year}.csv')
    matches_df.apply(lambda row: update_player_stats(row, players_dict, k_factor, score), axis=1)
    print(year)

    # Update players_df from players_dict (outside the if statement)
    for player_id, player_data in players_dict.items():
        for key, value in player_data.items():
            players_df.loc[players_df['player_id'] == player_id, key] = value

    top_10_players = sorted(players_dict.items(), key=lambda x: x[1]['current_elo'], reverse=True)[:10]
    top_10_names = {name_mapping[player_id]: data['current_elo'] for player_id, data in top_10_players}
    elo_ratings_annual[year] = top_10_names

    # Print the top 10 players for the year
    # print(f"Top 10 players for {year}:")
    # for player_id, data in top_10_players:
    #     print(f"Player ID: {player_id}, Elo Rating: {data['current_elo']}")


# Initialize player_ratings_over_time with player names
player_ratings_over_time = {name: [] for name in top_10_names}

years = list(range(2000, 2024))
sorted_years = sorted(elo_ratings_annual.keys())
for year in sorted_years:
    for player_name in player_ratings_over_time:
        rating = elo_ratings_annual[year].get(player_name, default_value)
        player_ratings_over_time[player_name].append(rating)

# Plotting after completing the yearly loop
for player_name, ratings in player_ratings_over_time.items():
    plt.plot(years, ratings, marker='o', label=player_name)

plt.title('Top 10 Tennis Players Elo Ratings Over Years')
plt.xlabel('Year')
plt.ylabel('Elo Rating')
plt.xticks(years)
plt.legend()
plt.grid(True)
plt.show()

players_df.to_csv('2023_YE_elo_rankings.csv')
