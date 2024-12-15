import redis
from utils.redis_utils import initialize_redis

players_csv = "data/atp_players.csv"
redis_client = redis.StrictRedis(host='localhost', port=6379, db=0)
initialize_redis(redis_client, players_csv)
print("Redis initialized with player data.")