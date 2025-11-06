import pandas as pd
import pickle
import json
from .plot_elos import get_elos
import os
import datetime


def make_prediction(date_string):
    date = datetime.date.fromisoformat(date_string)
    
    df = pd.read_csv('../../data/processed/buli_matches_rolling.csv', parse_dates=['date'])
    matches_df = df[df['date'].dt.date == date]

    with open('../training/features_simple_model.json', 'r') as f:
        features_simple = json.load(f)

    with open('../training/features_complex_model.json', 'r') as f:
        features_complex = json.load(f)

    simple_model_name = 'simple_model_retrained.pkl' if os.path.exists("../../models/simple_model_retrained.pkl") else 'model_win_loose.pkl'
    with open(f"../../models/{simple_model_name}", 'rb') as f:
        print(f"Chose simple model: {simple_model_name}")
        simple_model = pickle.load(f)

    complex_model_name = 'complex_model_retrained.pkl' if os.path.exists("../../models/complex_model_retrained.pkl") else 'model_win_draw_loose.pkl'
    with open(f"../../models/{complex_model_name}", 'rb') as f:
        print(f"Chose complex model: {complex_model_name}")
        complex_model = pickle.load(f)
    
    matches = []
    for _, match in matches_df.iterrows():
        print(f"{match['team_home']} vs. {match['team_away']} on {match['date']} with {match['result_home']}")
        X_simple = match[features_simple].to_numpy().reshape(1, -1)
        X_complex = match[features_complex].to_numpy().reshape(1, -1)
        result_simple = simple_model.predict_proba(X_simple)
        result_complex = complex_model.predict_proba(X_complex)

        print(f"Simple model prediction:\t{match['team_home']} wins: {result_simple[0][1] * 100:.2f}% | {match['team_home']} looses OR draw: {result_simple[0][0] * 100:.2f}%")
        print(f"Complex model prediction:\t{match['team_home']} wins: {result_complex[0][0] * 100:.2f}% | Draw: {result_complex[0][1] * 100:.2f}% | {match['team_away']} wins: {result_complex[0][2] * 100:.2f}%")
        print("\n")

        match_elo = get_elos([match['team_home'], match['team_away']], end=date_string)

        new_match = {
            "team_home": match['team_home'],
            "team_away": match['team_away'],
            "result_home": match['result_home'] if not pd.isna(match['result_home']) else None,
            "goals_home": match['goals_home'] if not pd.isna(match['goals_home']) else None,
            "goals_away": match['goals_away'] if not pd.isna(match['goals_away']) else None,
            "date": match['date'],
            "simple_model_results": {
                "home_team_win": str(result_simple[0][1] * 100),
                "home_team_draw_or_loss": str(result_simple[0][0] * 100)
            },
            "complex_model_results": {
                "home_team_win": str(result_complex[0][0] * 100),
                "draw": str(result_complex[0][1] * 100),
                "away_team_win": str(result_complex[0][2] * 100)
            },
            "elos": match_elo
        }
        matches.append(new_match)
    return matches

def main():
    make_prediction("2025-08-24")     

if __name__ == '__main__':
    main()