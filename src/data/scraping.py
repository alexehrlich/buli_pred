import pandas as pd
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
import time
import requests
import os
import json
from io import StringIO

def download_team_elos():
    with open('../../data/raw/elo_api_name_to_team_map.json', 'r') as f:
        elo_teamname_mapping = json.load(f)

        if not os.path.exists('../../data/raw/team_elos/'):
            print(f"create folder ../../data/raw/team_elos/")
            os.makedirs('../../data/raw/team_elos/')

        for team in list(elo_teamname_mapping.keys()):
            if os.path.exists(f"../../data/raw/team_elos/{team}.csv"):
                print(f"Elo {team} alreday downloaded")
                continue
            print(f"Downloading elo for team {team}")
            r = requests.get(f"http://api.clubelo.com/{team}")
            if r.status_code == 200:
                f = open(f"../data/raw/team_elos/{team}.csv", 'w')
                f.write(r.text)
                f.close()
            else:
                print(f"could not find {team}")

def scrape_match_data():
    if not os.path.exists('../../data/raw/matches.csv'):
            print("Downloading match data from https://fbref.com")
            years = list(range(2025, 2019, -1))
            options = Options()
            options.add_argument("--headless=new")
            options.add_argument("--disable-gpu")
            options.add_argument("--no-sandbox")
            options.add_argument("user-agent=Mozilla/5.0 ... Chrome/123.0 ...")
            all_matches = []
            standings_url = "https://fbref.com/en/comps/20/Bundesliga-Stats"

            for year in years:
                driver = webdriver.Chrome(options=options)  
                print(f"Year: {year}")
                driver.get(standings_url)
                html = driver.page_source
                time.sleep(8)
                driver.quit()
                soup = BeautifulSoup(StringIO(html))
                standings_table = soup.select('table.stats_table')[0]
                
                links = standings_table.find_all('a')
                links = [l.get('href') for l in links]
                links = [l for l in links if '/squads' in l]
                team_urls = [f"http://fbref.com{l}" for l in links]

                previous_season = soup.select('a.prev')[0].get('href')
                standings_url = f"https://fbref.com{previous_season}"

                for team_url in team_urls:
                    team_name = team_url.split('/')[-1].replace('-Stats', '').replace('-', ' ')
                    #print(team_name)
                    driver = webdriver.Chrome(options=options)
                    driver.get(team_url)
                    html = driver.page_source
                    matches = pd.read_html(StringIO(html), match='Scores & Fixtures')[0]
                    soup = BeautifulSoup(html)
                    links = soup.find_all('a')
                    links = [l.get('href') for l in links]
                    links = [l for l in links if l and '/all_comps/shooting' in l]
                    driver = webdriver.Chrome(options=options)
                    driver.get(f"http://fbref.com{links[0]}")
                    html = driver.page_source
                    shooting = pd.read_html(StringIO(html), match='Shooting')[0]
                    shooting.columns = shooting.columns.droplevel()

                    try:
                        team_data = matches.merge(shooting[['Date', 'Sh', 'SoT', 'Dist', 'FK', 'PK', 'PKatt']], on='Date')

                    except ValueError:
                        continue

                    team_data["Season"] = year
                    team_data["Team"] = team_name
                    all_matches.append(team_data)
                    time.sleep(1)
                    driver.quit()

                    with open('../data/raw/fbref_api_name_to_team_map.json') as f:
                        fbref_team_name_mapping = json.load(f)
                        match_df = pd.concat(all_matches)
                        match_df.columns = [c.lower() for c in match_df.columns]
                        match_df['team'] = match_df['team'].apply(lambda x: fbref_team_name_mapping[x])
                        match_df.sort_values('date').to_csv('../data/raw/matches.csv', index=False)
    else:
        print("matches.csv already exists.")

def main():
    download_team_elos()
    scrape_match_data()

if __name__ == '__main__':
    main()