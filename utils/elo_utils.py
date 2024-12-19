import json

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
    winner_id, loser_id = str(row['winner_id']), str(row['loser_id'])
    tourney_date = row['tourney_date']

    winner_data = redis_client.hget('players_data', winner_id)
    loser_data = redis_client.hget('players_data', loser_id)

    if not winner_data or not loser_data:
        print(f"Player ID missing: winner_id={winner_id}, loser_id={loser_id}")
        return

    winner_data = json.loads(winner_data)
    loser_data = json.loads(loser_data)

    winner_elo = float(winner_data['current_elo'])
    loser_elo = float(loser_data['current_elo'])
    winner_data['matches_played'] = int(winner_data['matches_played'])
    loser_data['matches_played'] = int(loser_data['matches_played'])

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

    redis_client.hset('players_data', winner_id, json.dumps(winner_data))
    redis_client.hset('players_data', loser_id, json.dumps(loser_data))

