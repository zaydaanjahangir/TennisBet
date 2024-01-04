from flask import Flask, render_template, request
import pandas as pd

app = Flask(__name__)

# Load your dataset
elo_df = pd.read_csv('elo_ratings.csv')  # Adjust the filename as needed

@app.route('/', methods=['GET', 'POST'])
def index():
    elo_rating = None
    if request.method == 'POST':
        first_name = request.form.get('name_first', '').strip()  # Default to empty string if None
        last_name = request.form.get('name_last', '').strip()  # Default to empty string if None

        if first_name and last_name:  # Proceed only if both names are provided
            player = elo_df[(elo_df['name_first'].str.lower() == first_name.lower()) & 
                            (elo_df['name_last'].str.lower() == last_name.lower())]
            if not player.empty:
                elo_rating = player['current_elo'].iloc[0]
            else:
                elo_rating = "Player not found"
    return render_template('index.html', elo_rating=elo_rating)


if __name__ == '__main__':
    app.run(debug=True)
