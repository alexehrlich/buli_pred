import pandas as pd
from bs4 import BeautifulSoup
import time
import requests
import os
import json
from io import StringIO
from playwright.sync_api import sync_playwright


class Scraper:
    def __init__(self):
        pass

    def download_team_elos(self):
        with open('../../data/raw/elo_api_name_to_team_map.json', 'r') as f:
            elo_teamname_mapping = json.load(f)

            if not os.path.exists('../../data/raw/team_elos/'):
                print(f"create folder ../../data/raw/team_elos/")
                os.makedirs('../../data/raw/team_elos/')

            for team in list(elo_teamname_mapping.keys()):
                print(f"Downloading elo for team {team}")
                r = requests.get(f"http://api.clubelo.com/{team}")
                if r.status_code == 200:
                    f = open(f"../../data/raw/team_elos/{team}.csv", 'w')
                    f.write(r.text)
                    f.close()
                else:
                    print(f"could not find {team}")

    def scrape_match_data(self, years, from_date=None, to_date=pd.to_datetime('today').normalize(), save_to_filepath=None):
        all_matches = []

        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)  # headless browser
            context = browser.new_context(
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
                viewport={'width': 1920, 'height': 1080},
            )
            page = context.new_page()

            latest_year = years[0]
            standings_url = f"https://fbref.com/en/comps/20/{latest_year - 1}-{latest_year}/{latest_year - 1}-{latest_year}-Bundesliga-Stats"

            for year in years:
                print(f"Year: {year}")
                page.goto(standings_url)
                time.sleep(3)  # let page fully render

                html = page.content()
                with open('out.html', 'w') as f:
                    f.write(html)

                soup = BeautifulSoup(html, 'html.parser')
                standings_table = soup.select('table.stats_table')[0]

                links = [a.get('href') for a in standings_table.find_all('a')]
                links = [l for l in links if '/squads' in l]
                team_urls = [f"http://fbref.com{l}" for l in links]

                # get previous season URL for next iteration
                previous_season = soup.select('a.prev')[0].get('href')
                standings_url = f"https://fbref.com{previous_season}"

                for team_url in team_urls:
                    team_name = team_url.split('/')[-1].replace('-Stats', '').replace('-', ' ')
                    print(f"Team: {team_name}")
                    page.goto(team_url)
                    time.sleep(3)

                    html = page.content()
                    matches = pd.read_html(StringIO(html), match='Scores & Fixtures')[0]
                    matches = matches.drop(matches[matches['Date'] == 'Date'].index)

                    soup = BeautifulSoup(html, 'html.parser')
                    links = [a.get('href') for a in soup.find_all('a')]
                    shooting_links = [l for l in links if l and '/all_comps/shooting' in l]

                    if shooting_links:
                        page.goto(f"http://fbref.com{shooting_links[0]}")
                        time.sleep(2)
                        html = page.content()
                        shooting = pd.read_html(StringIO(html), match='Shooting')[0]
                        shooting.columns = shooting.columns.droplevel()
                        required_cols = ['Date', 'Sh', 'SoT', 'Dist', 'PK', 'FK', 'PKatt']
                        for col in required_cols:
                            if col not in shooting.columns:
                                shooting[col] = pd.NA
                        shooting_sub = shooting[required_cols]
                        team_data = matches.merge(shooting_sub, on='Date', how='left')
                    else:
                        team_data = matches

                    team_data["Date"] = pd.to_datetime(team_data["Date"])
                    team_data["Season"] = year
                    team_data["Team"] = team_name

                    if from_date is not None:
                        team_data = team_data[team_data["Date"].between(from_date, to_date, inclusive='right')]

                    team_data = team_data.drop(team_data[team_data["Date"] == "Date"].index)
                    all_matches.append(team_data)
                    time.sleep(1)

            browser.close()

        if save_to_filepath:
            with open('../../data/raw/fbref_api_name_to_team_map.json') as f:
                match_df = pd.concat(all_matches)
                fbref_team_name_mapping = json.load(f)
                match_df.columns = [c.lower() for c in match_df.columns]
                match_df['team'] = match_df['team'].apply(lambda x: fbref_team_name_mapping[x])
                if os.path.exists(save_to_filepath):
                    existing_df = pd.read_csv(save_to_filepath)
                    match_df = pd.concat([existing_df, match_df], ignore_index=True)
                match_df['date'] = pd.to_datetime(match_df['date'])
                match_df.sort_values('date').to_csv(save_to_filepath, index=False)
            return match_df

        return pd.concat(all_matches)