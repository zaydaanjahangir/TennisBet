# Tennis Elo Distributed System

## Overview

This project implements a distributed system to calculate Tennis Elo ratings for professional players based on historical match data. The system processes tournaments in parallel using RabbitMQ for task distribution and Redis as an in-memory store for player data. The Elo calculation is modular and handles dependencies between player ratings using Redis locks to prevent race conditions or deadlocks.

### Key Features
- **Batch Processing**: Matches are grouped by tournaments and processed as independent tasks.
- **Parallel Consumers**: Multiple worker nodes (consumers) process tournaments in parallel, improving throughput.
- **Redis Integration**: Player Elo ratings and match statistics are stored and updated in Redis.
- **Deadlock Prevention**: Ordered locking ensures consistency in processing player ratings across overlapping tournaments.
- **Scalable Architecture**: RabbitMQ enables scaling of consumers to handle larger datasets.

---

## System Architecture

1. **Producer**: Sends tournament batches (tasks) to a RabbitMQ queue.
2. **Consumers**: Multiple worker nodes process tournament batches, calculate Elo updates, and store the results in Redis.
3. **Redis**: Acts as the primary data store for player Elo ratings and intermediate results.
4. **CSV Export**: After processing all tournaments, the final Elo ratings are saved to a CSV file for verification.

---

## File Structure
```
tennis_elo_distributed/ 
├── producer.py # Sends tournament batches to RabbitMQ 
├── consumer.py # Processes tournaments and updates Redis 
├── utils/ │ 
├── elo_utils.py # Elo calculation functions │ 
├── redis_utils.py # Redis connection and helper functions │ 
├── queue_utils.py # RabbitMQ utilities 
├── data/ │ ├── atp_players.csv # Player data 
│ └── matches/ # Yearly match data 
├── requirements.txt # Dependencies 
└── README.md 
```

---

## Setup

1. **Install Dependencies**:
   ```bash
   pip install -r requirements.txt
   ```
2. **Start Redis and RabbitMQ**:
   ```bash
   redis-server
   brew services start rabbitmq
   ```
3. **Initialize Redis**:
   ```bash
   python -c "from utils.redis_utils import initialize_redis; initialize_redis(redis_client, 'data/atp_players.csv')"
   ```
4. **Run Producer**:
   ```bash
   python producer.py
   ```
5. **Run Consumers**: Open multiple terminals and run:
   ```bash
   python consumer.py
   ```
6. **Export Results**:
   ```bash
   python save_to_csv.py
   ```

## Example Output

Sample Elo ratings after processing:

| Player ID | Name           | Country | Current Elo | Peak Elo | Peak Elo Date |
|-----------|----------------|---------|-------------|----------|---------------|
| 100001    | Novak Djokovic | SRB     | 2203        | 2470     | 2021-12-01    |
| 100002    | Rafael Nadal   | ESP     | 2187        | 2450     | 2019-05-20    |

---

## Notes

- Ensure Redis and RabbitMQ are running before starting.
- Use `redis-cli` and RabbitMQ’s management interface for monitoring.
- Add more consumers to process tasks faster.
