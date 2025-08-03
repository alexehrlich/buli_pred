import pandas as pd

def rolling_stats(df, cols, window) -> pd.DataFrame:
    df = df.copy()
    for team in df['team_away'].unique():
        #get the data frame of one team with all matches (home and away) and also the indexes of the matches
        team_df = df[(df['team_away'] == team) | (df['team_home'] == team)].sort_values('date')
        match_indexes = list(team_df.index)

        #set all values befor our window to nan to delete later
        for early_idx in match_indexes[:window]:
            venue_early = 'home' if df.loc[early_idx]['team_home'] == team else 'away'
            for col in cols:
                df.at[early_idx, f"{col}_{venue_early}_rolling_{window}"] = float('nan')

        #iterate over all matches of a team starting the game after 'window' so we can calc
        # stats from the 'window' last matches
        for match_idx, row in team_df.iloc[window:].iterrows():
            rolling_cols = {f"{col}": 0.0 for col in cols}
            
            #get the indexes of the last matches before this match
            match_list_index = match_indexes.index(match_idx)
            last_matches_indexes = match_indexes[match_list_index - window:match_list_index]

            #sum up the stats from the last matches of this team for all rolling columns
            for last_match_index in last_matches_indexes:
                match = df.loc[last_match_index]
                for col in cols:
                    if match['team_home'] == team:
                        rolling_cols[col] += match[f"{col}_home"] / window
                    else:
                        rolling_cols[col] += match[f"{col}_away"] / window

            #set the averag roling stats to the original dataframe
            venue = 'home' if row['team_home'] == team else 'away'
            for col in cols:
                df.at[match_idx, f"{col}_{venue}_rolling_{window}"] = rolling_cols[col]
    return df

def calculate_rest_days(df) -> pd.DataFrame:
    df = df.copy()

    # Sort full match list chronologically
    df = df.sort_values('date').reset_index(drop=True)

    # Track last game dates per team
    last_game_date = {}

    # Only update `days_since_home` and `days_since_away` for Bundesliga matches
    df['days_since_home'] = None
    df['days_since_away'] = None

    for index, row in df.iterrows():
        match_date = row['date']
        home_team = row['team_home']
        away_team = row['team_away']
        comp = row['comp']

        # If it's a Bundesliga game → calculate days since last match
        if comp == 'Bundesliga':
            # Home team
            if home_team in last_game_date:
                df.at[index, 'days_since_home'] = (match_date - last_game_date[home_team]).days
            else:
                df.at[index, 'days_since_home'] = 0

            # Away team
            if away_team in last_game_date:
                df.at[index, 'days_since_away'] = (match_date - last_game_date[away_team]).days
            else:
                df.at[index, 'days_since_away'] = 0

        # Always update last seen match for both teams (regardless of competition)
        last_game_date[home_team] = match_date
        last_game_date[away_team] = match_date
    return df

def calculate_date_details(df) -> pd.DataFrame:
    df['date'] = pd.to_datetime(df['date'])
    df['match_hour'] = df['time'].replace(":.+", "", regex=True).astype('int')
    df['day_code'] = df['date'].dt.day_of_week
    df['week'] = df.apply(lambda x: x['round'].split(' ')[-1], axis=1)
    return df

def main():
    df = pd.read_csv('../../data/raw/matches_combined.csv', index_col=0)

    df = calculate_date_details(df)

    df = calculate_rest_days(df)
    
    df = df[df['comp'] == 'Bundesliga']
    df['goals_home'] = df['goals_home'].astype(float)
    df['goals_away'] = df['goals_away'].astype(float)

    df['goal_ratio_home'] = (df['goals_home'] + 1) / (df['goals_away'] + 1)
    df['goal_ratio_away'] = (df['goals_away'] + 1) / (df['goals_home'] + 1)

    df['ga_per_xGA_home'] = df['goals_away'] / df['xg_away']
    df['ga_per_xGA_away'] = df['goals_home'] / df['xg_home']
    df['gf_per_xG_home'] = df['goals_home'] / df['xg_home']
    df['gf_per_xG_away'] = df['goals_away'] / df['xg_away']

    df = rolling_stats(df, ['xg', 'sh', 'sot', 'poss', 'goal_ratio', 'elo', 'ga_per_xGA', 'gf_per_xG'], 3)
    df = rolling_stats(df, ['goal_ratio'], 1)

    df['elo_rolling_diff'] = df['elo_home_rolling_3'] - df['elo_away_rolling_3']
    df['goal_ratio_diff'] = df['goal_ratio_home_rolling_3'] - df['goal_ratio_away_rolling_3']
    df['ga_per_xGA_diff'] = df['ga_per_xGA_home_rolling_3'] - df['ga_per_xGA_away_rolling_3']
    df['gf_per_xG_diff'] = df['gf_per_xG_home_rolling_3'] - df['gf_per_xG_away_rolling_3']

    df.dropna().reset_index(drop=True).to_csv('../../data/processed/buli_matches_rolling.csv')

if __name__ == '__main__':
    main()