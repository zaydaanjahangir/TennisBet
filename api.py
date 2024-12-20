import requests
import json
import os
from datetime import datetime, timezone
from dotenv import load_dotenv

load_dotenv(dotenv_path='secrets/.env')

def format_player_name(slug):
    parts = slug.split('-')
    first_name = parts[-1]
    last_name_parts = parts[:-1]
    last_name = ' '.join(last_name_parts)
    full_name = f"{first_name} {last_name}".strip().lower()
    return full_name

def get_final_matches():
    current_date = datetime.now()
    formatted_date = current_date.strftime('%d/%m/%Y')
    daily_categories_url = f"https://allsportsapi2.p.rapidapi.com/api/tennis/calendar/{formatted_date}/categories"
    headers = {
        "x-rapidapi-key": os.getenv('API_KEY'),
        "x-rapidapi-host": "allsportsapi2.p.rapidapi.com"
    }

    response = requests.get(daily_categories_url, headers=headers).json()
    atp_data = next(
        (
            category
            for category in response["categories"]
            if category["category"]["name"] == "ATP"
        ),
        None,
    )

    if not atp_data:
        return []

    unique_tournament_ids = atp_data["uniqueTournamentIds"]
    team_ids = atp_data["teamIds"]

    matches_by_id = {}
    final_matches = []

    for player in team_ids:
        tennis_team_last_events_url = f"https://allsportsapi2.p.rapidapi.com/api/tennis/team/{player}/events/previous/0"
        response = requests.get(tennis_team_last_events_url, headers=headers).json()

        if 'events' in response and response['events']:
            last_event = response['events'][-1]
            home_team = last_event.get('homeTeam', {})
            away_team = last_event.get('awayTeam', {})
            status = last_event.get('status', {})
            winner_code = last_event.get('winnerCode')
            event_id = last_event.get('id')

            timestamp = last_event.get('startTimestamp')
            utc_date = datetime.fromtimestamp(timestamp, tz=timezone.utc)
            formatted_date = utc_date.strftime('%Y%m%d')

            home_team_slug = home_team.get('slug')
            home_team_id = home_team.get('id')
            away_team_slug = away_team.get('slug')
            away_team_id = away_team.get('id')

            if status.get('code') == 100:
                winner = {
                    "id": str(home_team_id if winner_code == 1 else away_team_id),
                    "name": format_player_name(home_team_slug if winner_code == 1 else away_team_slug)
                }
                loser = {
                    "id": str(away_team_id if winner_code == 1 else home_team_id),
                    "name": format_player_name(away_team_slug if winner_code == 1 else home_team_slug)
                }

                final_entry = {
                    "match_id": event_id,
                    "tournament_date": formatted_date,
                    "winner": winner,
                    "loser": loser
                }
                final_matches.append(final_entry)

    return final_matches
