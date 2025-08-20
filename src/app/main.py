from fastapi import FastAPI
from prediction.predict import make_prediction
from data.Scraper import Scraper
import pandas as pd

app = FastAPI(title='Test')


@app.get("/")
async def root():
    return {"message": "Hello World"}

@app.get("/predict")
async def predict():

    sc = Scraper()
    path = '../../data/raw/matches.csv'
    current_df = pd.read_csv(path).sort_values('date')
    current_df = current_df[current_df['result'].notna()]
    current_df.to_csv(path, index=False)
    last_date = pd.to_datetime(current_df.iloc[-1]['date'])
    print(f"Found last date: {last_date}")
    print(sc.scrape_match_data(years=[2026], from_date=last_date, to_date=pd.to_datetime('2025-08-24'), save_to_filepath=path).head())

    results = make_prediction()
    return results