from utils.redis_utils import redis_client, save_redis_to_csv

if __name__ == "__main__":
    save_redis_to_csv(redis_client, "final_elo_ratings.csv")
