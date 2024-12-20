import pika
import json
from utils.elo_utils import update_player_stats, k_factor, update_player_stats_from_match
from utils.redis_utils import redis_client
from redis.exceptions import LockError

def process_tournament_with_locks(batch):
    tourney_id = batch['tourney_id']
    matches = batch['matches']

    print(f"Processing tournament {tourney_id} with {len(matches)} matches.")

    # Get all unique player IDs and sort them
    player_ids = sorted(
        {match['winner_id'] for match in matches}.union(
            {match['loser_id'] for match in matches}
        )
    )
    locks = {}

    try:
        # Acquire locks in sorted order
        for player_id in player_ids:
            locks[player_id] = redis_client.lock(f"player_lock:{player_id}", timeout=30)
            if not locks[player_id].acquire(blocking=True):
                raise LockError(f"Failed to acquire lock for player {player_id}")

        # Process matches sequentially
        for match in matches:
            update_player_stats(match, redis_client, k_factor)

    finally:
        # Release all locks in reverse order
        for player_id in reversed(player_ids):
            if player_id in locks:
                locks[player_id].release()

def process_match_with_locks(match):
    winner_id = match['winner']['id']
    loser_id = match['loser']['id']

    print(f"Processing match {match['match_id']} between {match['winner']['name']} and {match['loser']['name']}.")

    locks = {}
    try:
        for player_id in [winner_id, loser_id]:
            locks[player_id] = redis_client.lock(f"player_lock:{player_id}", timeout=30)
            if not locks[player_id].acquire(blocking=True):
                raise LockError(f"Failed to acquire lock for player {player_id}")

        update_player_stats_from_match(match, redis_client, k_factor)

    finally:
        for player_id in locks:
            locks[player_id].release()


def on_message_received_tournament(ch, method, properties, body):
    batch = json.loads(body)
    try:
        process_tournament_with_locks(batch)
        ch.basic_ack(delivery_tag=method.delivery_tag)
    except Exception as e:
        print(f"Error processing tournament: {e}")
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)

def on_message_received(ch, method, properties, body):
    try:
        match = json.loads(body)
        process_match_with_locks(match)
        ch.basic_ack(delivery_tag=method.delivery_tag)
    except Exception as e:
        print(f"Error processing match: {e}")
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)

# RabbitMQ consumer setup
connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
channel = connection.channel()
channel.queue_declare(queue='elo_tournament_queue')
channel.basic_qos(prefetch_count=1)
channel.basic_consume(queue='elo_tournament_queue', on_message_callback=on_message_received)

print("Worker started. Waiting for tasks...")
channel.start_consuming()
