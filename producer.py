import pika
import pandas as pd
import json
from api import get_final_matches


def send_tournament_batches_to_queue():
    connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    channel = connection.channel()
    channel.queue_declare(queue='elo_tournament_queue')

    # Load match data and group by tournaments
    years = range(2010, 2024)
    for year in years:
        matches_file = f"data/matches/atp_matches_{year}.csv"
        all_matches = pd.read_csv(matches_file)
        grouped = all_matches.groupby('tourney_id')

        for tourney_id, group in grouped:
            batch = {
                'tourney_id': tourney_id,
                'matches': group.to_dict(orient='records')
            }
            channel.basic_publish(
                exchange='',
                routing_key='elo_tournament_queue',
                body=json.dumps(batch)
            )
            print(f"Sent tournament {tourney_id} from {year} to the queue.")
    
    connection.close()

def send_matches_to_queue():
    connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    channel = connection.channel()
    channel.queue_declare(queue='elo_tournament_queue')
    matches = get_final_matches()

    for match in matches:
        channel.basic_publish(
            exchange='',
            routing_key='elo_tournament_queue',
            body=json.dumps(match)  
        )
        print(f"Sent match {match['match_id']} to the queue.")

    connection.close()

if __name__ == "__main__":
    send_matches_to_queue()
