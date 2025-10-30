from .Scraper import Scraper
import pandas as pd
import sys
import os

def fetch_newest():
    sc = Scraper()
    sc.download_team_elos()
    path = '../../data/raw/matches.csv'
    if os.path.exists(path):
        current_df = pd.read_csv(path).sort_values('date')
        current_df = current_df[current_df['result'].notna()]
        current_df.to_csv(path, index=False)
        last_date = pd.to_datetime(current_df.iloc[-1]['date'])
        print(f"Found last date: {last_date}")
        print(sc.scrape_match_data(years=[2026], from_date=last_date, save_to_filepath=path).head())
    else:
        return

def main():

    if len(sys.argv) == 2:
        print(f"Fetch {sys.argv[1]} match data...")
        mode = sys.argv[1]
    else:
        return
    
    sc = Scraper()
    sc.download_team_elos()

    if mode == 'newest':
        path = '../../data/raw/matches.csv'
        if os.path.exists(path):
            current_df = pd.read_csv(path).sort_values('date')
            current_df = current_df[current_df['result'].notna()]
            current_df.to_csv(path, index=False)
            last_date = pd.to_datetime(current_df.iloc[-1]['date'])
            print(f"Found last date: {last_date}")
            print(sc.scrape_match_data(years=[2026], from_date=last_date, to_date=pd.to_datetime('2025-08-24'), save_to_filepath=path).head())
        else:
            return
        
    elif mode == 'all':
        sc.scrape_match_data(years=[2025, 2024, 2023, 2022, 2021, 2020], from_date=last_date, save_to_filepath=path)

if __name__ == '__main__':
    main()