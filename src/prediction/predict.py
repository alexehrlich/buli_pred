import pandas as pd
import pickle

def main():
    df = pd.read_csv('./data/processed/buli_matches_rolling.csv')
    open_matches_df = df[df['result_home'].isna()]

    features_complex = [
    "xg_home_rolling_3",
    "xg_away_rolling_3",
    "elo_home_rolling_3",
    "elo_away_rolling_3",
    "elo_rolling_diff",           # Combined team strength
    "xg_rolling_diff",            # Attack quality difference
    "goal_ratio_diff",            # Recent scoring form
    "ga_per_xga_diff",            # Defense efficiency diff
    "gf_per_xg_diff",             # Offense efficiency diff
    "elo_diff_x_xg_diff",         # Interaction term, might capture outliers --> draws?
    "elo_diff_sq",

    "days_since_home",            # Rest days (can impact performance)
    "days_since_away",

    "goal_ratio_home_rolling_1",
    "goal_ratio_away_rolling_1",

    "match_hour",
    "day_code",
    "week",

    "poss_home_rolling_3",
    "sot_home_rolling_3",
    "poss_away_rolling_3",
    "sot_away_rolling_3",
    "xg_away_rolling_3",
    "xg_home_rolling_3"
    ]

    features_simple = [
    "xg_home_rolling_3",
    "xg_away_rolling_3",
    "elo_home_rolling_3",
    "elo_away_rolling_3",
    "elo_rolling_diff",           # Combined team strength
    "xg_rolling_diff",            # Attack quality difference
    "goal_ratio_diff",            # Recent scoring form
    "ga_per_xga_diff",            # Defense efficiency diff
    "gf_per_xg_diff",             # Offense efficiency diff
    "elo_diff_x_xg_diff",         # Interaction term, might capture outliers --> draws?
    "elo_diff_sq",

    # "days_since_home",            # Rest days (can impact performance)
    # "days_since_away",

    "goal_ratio_home_rolling_1",
    "goal_ratio_away_rolling_1",

    # "match_hour",
    # "day_code",
    # "week",

    # "poss_home_rolling_3",
    # "sot_home_rolling_3",
    # "poss_away_rolling_3",
    # "sot_away_rolling_3",
    # "xg_away_rolling_3",
    # "xg_home_rolling_3"
    ]

    with open('./models/model_win_loose.pkl', 'rb') as f:
        simple_model = pickle.load(f)

    with open('./models/model_win_draw_loose.pkl', 'rb') as f:
        complex_model = pickle.load(f)

    for idx, match in open_matches_df.iterrows():
        print(f"{match['team_home']} vs. {match['team_away']} on {match['date']}")
        X_simple = match[features_simple].to_numpy().reshape(1, -1)
        X_complex = match[features_complex].to_numpy().reshape(1, -1)
        result_simple = simple_model.predict_proba(X_simple)
        resukt_complex = complex_model.predict_proba(X_complex)
        print(f"{match['team_home']} wins: {result_simple[0][1] * 100:.2f}%")
        print(f"{match['team_home']} looses OR draw: {result_simple[0][0] * 100:.2f}%")
        print(f"Actual home Team result: {match['result_home']}")
        print("\n")


if __name__ == '__main__':
    main()