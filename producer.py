import pika
import pandas as pd
import json
import time
from flask import Flask, send_file
from api import get_final_matches
from utils.redis_utils import redis_client
from apscheduler.schedulers.background import BackgroundScheduler

app = Flask(__name__) 

scheduler = BackgroundScheduler()

def run_producer():
    print("Running Producer...")
    send_matches_to_queue()

def send_tournament_batches_to_queue():
    connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    channel = connection.channel()
    channel.queue_declare(queue='elo_tournament_queue')

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
        match_id = match["match_id"]

        if redis_client.sismember("processed_match_ids", match_id):
            print(f"Skipping duplicate match {match_id}")
            continue

        channel.basic_publish(
            exchange='',
            routing_key='elo_tournament_queue',
            body=json.dumps(match)
        )
        print(f"Sent match {match_id} to the queue.")

        redis_client.sadd("processed_match_ids", match_id)
        redis_client.expire("processed_match_ids", 7 * 24 * 60 * 60)

    connection.close()

@app.route("/current-elo.csv", methods=["GET"])
def serve_csv():
    try:
        return send_file("data/current_elo.csv", mimetype="text/csv")
    except Exception as e:
        return {"error": str(e)}, 500



if __name__ == "__main__":
    print("Running initial producer job...")
    run_producer()

    print("Starting Producer scheduler...")
    scheduler.add_job(
        run_producer,
        'interval',
        hours=6,
        max_instances=1,
        misfire_grace_time=3600
    )
    scheduler.start()
    print("Producer scheduler started. Flask app is running...")

    try:
        # Run Flask app, bind to all network interfaces
        app.run(host="0.0.0.0", port=5000)
    except (KeyboardInterrupt, SystemExit):
        scheduler.shutdown()
