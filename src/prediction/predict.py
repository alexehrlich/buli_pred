import pandas as pd
import pickle
import json
import matplotlib.pyplot as plt
from plot_elos import plot_elo


def main():
    df = pd.read_csv('./data/processed/buli_matches_rolling.csv')

    #only load all not played matches to make prediction
    open_matches_df = df[df['result_home'].isna()]

    with open('./src/training/features_simple_model.json', 'r') as f:
        features_simple = json.load(f)

    with open('./src/training/features_complex_model.json', 'r') as f:
        features_complex = json.load(f)

    with open('./models/model_win_loose.pkl', 'rb') as f:
        simple_model = pickle.load(f)

    with open('./models/model_win_draw_loose.pkl', 'rb') as f:
        complex_model = pickle.load(f)

    for _, match in open_matches_df.iterrows():
        print(f"{match['team_home']} vs. {match['team_away']} on {match['date']}")
        X_simple = match[features_simple].to_numpy().reshape(1, -1)
        X_complex = match[features_complex].to_numpy().reshape(1, -1)
        result_simple = simple_model.predict_proba(X_simple)
        result_complex = complex_model.predict_proba(X_complex)
        print(f"Simple model prediction:\t{match['team_home']} wins: {result_simple[0][1] * 100:.2f}% | {match['team_home']} looses OR draw: {result_simple[0][0] * 100:.2f}%")
        print(f"Complex model prediction:\t{match['team_home']} wins: {result_complex[0][0] * 100:.2f}% | Draw: {result_complex[0][1] * 100:.2f}% | {match['team_away']} wins: {result_complex[0][2] * 100:.2f}%")
        print("\n")

        plot_elo([match['team_home'], match['team_away']])
        

if __name__ == '__main__':
    main()