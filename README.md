# Tennis Elo Ranking Calculator

This Python script calculates and visualizes tennis player Elo ratings over time using match data from 2000 to 2023 (assuming the data files are named `atp_players.csv` and `atp_matches_{year}.csv` for each year).

## Functionality

### Reads Player Data:
- Reads player information (ID, names, country) from `atp_players.csv`.
- Initializes Elo rating, matches played, peak Elo, and peak Elo date for each player.

### Calculates Elo Ratings:
- Iterates through years (2000-2023):
  - Reads match data for each year from a separate CSV file (`atp_matches_{year}.csv`).
  - For each match:
    - Calculates expected win probability for each player using their current Elo.
    - Updates Elo ratings for both players based on the match outcome and their expected win probabilities (increased for winners, decreased for losers).
    - Tracks peak Elo and peak Elo date for each player.
- Handles potential KeyboardInterrupt to gracefully stop the script.

### Generates Annual Top 10 Players:
- Identifies the top 10 players with the highest Elo ratings at the end of each year.

### Visualizes Player Ratings:
- Creates a line plot that displays the Elo rating trends for the top 10 players over the years (2000-2023).

### Saves Final Rankings:
- Saves the final player Elo ratings data for the year 2023 to a CSV file (`2023_YE_elo_rankings.csv`).

## Dependencies

- pandas
- matplotlib.pyplot

## Instructions

1. Install dependencies: `pip install pandas matplotlib`
2. Ensure your data files are named correctly and placed in the same directory as the script.
3. Run the script: `python tennis_elo_rankings.py`
4. (Optional) You can stop the script by typing 'stop' during the processing loop.

## Notes

- The code uses a K-factor function to adjust the Elo update based on the number of matches a player has played.
- The script assumes successful file access and may require modifications if file paths or names differ.
