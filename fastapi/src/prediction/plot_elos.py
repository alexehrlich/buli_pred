import matplotlib.pyplot as plt
import pandas as pd
import sys
import json

def get_elos(teams, end, start="2020-01-01"):
    """
        Loads the elo-values for the teams. Since they dont have the same date stamps, they must
        be summarized(groupby) on a monthly basis for comparison and plotting.
    """
    dates = pd.date_range(start=start, end=end, freq='MS')
    date_list = [d.strftime(format='%Y-%m') for d in dates]

    return_df = pd.DataFrame(index=date_list)

    with open('../../data/raw/elo_api_name_to_team_map.json') as f:
        elo_team_mapping = json.load(f)
        team_elo_mapping = {value: key for key, value in elo_team_mapping.items()}
    
    for team in teams:
        if not team in team_elo_mapping.keys():
            print(f"Team {team} not found.")
            continue
        elo_df = pd.read_csv(f"../../data/raw/team_elos/{team_elo_mapping[team]}.csv")
        elo_df['From'] = pd.to_datetime(elo_df['From'])
        elos_mean = elo_df.groupby(elo_df['From'].dt.to_period('M'))[['Elo']].mean()
        elo_dates = [str(p) for p in elos_mean.index]
        elo_values = [elos_mean.loc[pd.Period(date, freq='M'), 'Elo'] if date in elo_dates else float('nan') for date in date_list]
        return_df.insert(0, team, elo_values)
        return_df[team] = return_df[team].ffill().bfill()
        return_df = return_df.sort_index(ascending=True)
    return return_df.to_dict()

def plot_elo(teams, start=1990, end=2025):
    dates = pd.date_range(start=f"{start}-01-01", end=f"{end}-12-31", freq='MS')
    date_list = [d.strftime(format='%Y-%m') for d in dates]
    fig, ax = plt.subplots(figsize=(15,6))

    with open('../../data/raw/elo_api_name_to_team_map.json') as f:
        elo_team_mapping = json.load(f)
        team_elo_mapping = {value: key for key, value in elo_team_mapping.items()}
    
    for team in teams:
        if not team in team_elo_mapping.keys():
            print(f"Team {team} not found.")
            continue
        elo_df = pd.read_csv(f"../../data/raw/team_elos/{team_elo_mapping[team]}.csv")
        elo_df['From'] = pd.to_datetime(elo_df['From'])
        elos_mean = elo_df.groupby(elo_df['From'].dt.to_period('M'))[['Elo']].mean()
        elo_dates = [str(p) for p in elos_mean.index]
        elos = pd.DataFrame([
            elos_mean.loc[pd.Period(date, freq='M'), 'Elo'] if date in elo_dates else float('nan')
            for date in date_list
        ])
        elos = elos.sort_index()
        elos = elos.bfill()
        ax.plot(date_list, elos.to_numpy(), label=team)
    tick_locs = [i for i, date in enumerate(dates) if date.month == 1]
    tick_labels = [dates[i].strftime('%Y') for i in tick_locs]
    ax.set_xticks(tick_locs)
    ax.set_xticklabels(tick_labels, rotation=90)
    ax.legend()
    plt.show()

def main():

    if len(sys.argv) < 2:
        return
    
    team_list = sys.argv[1:]

    get_elos(team_list, end="2025-09-06")

if __name__ == '__main__':
    main()