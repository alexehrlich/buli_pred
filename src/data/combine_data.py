import pandas as pd
from datetime import datetime
import json

def switch_home_away(row):
    if row['venue'] == 'Home':
        row['venue'] = 'Away'
    elif row['venue'] == 'Away':
        row['venue'] = 'Home'
    else:
        row['venue'] = 'Neutral'
    row['team'], row['opponent'] = row['opponent'], row['team']
    row['ga'], row['gf'] = row['gf'], row['ga']
    row['xg'], row['xga'] = row['xga'], row['xg']

    row['poss'] = 100.0 - row['poss']

    if row['result'] == 'W':
        row['result'] = 'L'
    elif row['result'] == 'L':
        row['result'] = 'W'
    else:
        row['result'] = 'D'
    return row

def create_missing_match_pairs(df) -> pd.DataFrame:
    '''
        Create artifical matches for those where only one exist (if e.g. Bayern played agains Lazio. we only downloaded the data for Bayern)
        But we need a match pair to merge into one big match based dataframe
    '''
    # create match id to make two rows per match to one by merging at a unique match id
    df['match_id'] = df.apply(
        lambda row: f"{row['date']}_{'_'.join(sorted([row['team'], row['opponent']]))}",
        axis=1
    )
    df = df[['date', 'season', 'round', 'day', 'comp', 'match_id', 'time', 'team', 'venue', 'result', 'gf', 'ga', 'opponent', 'xg', 'xga', 'poss', 'sh', 'sot', 'dist']]

    #set arbitrarily home and away for neutral matches --> we only wnat them to calculate days sonce last match and then kick them
    neutrals_df = df[df['venue'] == 'Neutral'].sort_values('match_id')
    for i, idx in enumerate(neutrals_df.index):
        new_venue = 'Home' if i % 2 == 0 else 'Away'
        df.loc[idx, 'venue'] = new_venue

    #get all the matches with just one match
    unique_ids = df['match_id'].value_counts()
    unique_ids = unique_ids[unique_ids == 1].index
    df_single_matches = df[df['match_id'].isin(unique_ids)]
    df_copied_matches = df_single_matches.copy()

    # create the matches
    df_copied_matches = df_single_matches.apply(switch_home_away, axis=1)

    #add the new created matches to our big data frame
    return pd.concat([df, df_copied_matches])

def merge_match_rows(df) -> pd.DataFrame:
    '''
        The data frame has one row per team per match. This function merges the two rows and creates one row per match.
    '''
    home_away_cols = ['poss', 'sh', 'sot', 'dist']
    home_cols = {col: f"{col}_home" for col in home_away_cols}
    away_cols = {col: f"{col}_away" for col in home_away_cols}

    df_away = df[df['venue'] == 'Away']
    df_home = df[df['venue'] == 'Home']

    df_home = df_home.rename(columns={"team": "team_home", 
                                    "opponent": "team_away", 
                                    "gf": "goals_home", 
                                    "ga": "goals_away", 
                                    "xg": "xg_home", 
                                    "xga": "xg_away",
                                    "result": "result_home"})

    df_home = df_home.rename(columns=home_cols)
    df_away = df_away.rename(columns=away_cols)


    columns = list(away_cols.values())
    columns.append('match_id')

    merged_df = df_home.merge(df_away[columns], on='match_id').drop(columns=['venue'])

    return merged_df.sort_values('date').reset_index(drop=True)

def add_elos_to_df(df) -> pd.DataFrame:

    with open('../../data/raw/elo_api_name_to_team_map.json') as f:
        elo_api_team_mapping = json.load(f)
        elos = {}
        for team in list(elo_api_team_mapping.keys()):
            mapped_name = elo_api_team_mapping[team]
            elos[mapped_name] = pd.read_csv(f"../../data/raw/team_elos/{team}.csv")[['Elo', 'From', 'To']]
            elos[mapped_name]['From'] = pd.to_datetime(elos[mapped_name]['From'], format='%Y-%m-%d')
            elos[mapped_name]['To'] = pd.to_datetime(elos[mapped_name]['To'], format='%Y-%m-%d')
            date_cutoff = datetime.strptime("01/01/2019", "%d/%m/%Y")
            elos[mapped_name] = elos[mapped_name][elos[mapped_name]['From'] > date_cutoff]
            elos[mapped_name] = elos[mapped_name][elos[mapped_name]['To'] > date_cutoff]

        def get_elo(date, elo):
            filtered_df = elo.loc[elo['To'] <= date]
            if filtered_df.empty:
                raise ValueError(f"No Elo available before {date} for team.")
            return filtered_df.iloc[-1]['Elo']

        def fill_elo_row(row):
            if row['comp'] != 'Bundesliga':
                return pd.Series({'elo_home': float('nan'), 'elo_away': float('nan')})
            date = row['date']
            away_team = row['team_away']
            home_team = row['team_home']
            elo_home = get_elo(date, elos[home_team])
            elo_away = get_elo(date, elos[away_team])
            return pd.Series({'elo_home': elo_home, 'elo_away': elo_away})

        df[['elo_home', 'elo_away']] = df.apply(fill_elo_row, axis=1)
        return df.drop(columns='match_id').sort_values('date').reset_index(drop=True)

def main():
    df = pd.read_csv("../../data/raw/matches.csv")

    df = create_missing_match_pairs(df)

    df = merge_match_rows(df)

    df = add_elos_to_df(df)

    df.to_csv('../../data/interim/matches_combined.csv')

if __name__ == '__main__':
    main()