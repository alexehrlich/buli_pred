from fastapi import FastAPI
from prediction.predict import make_prediction
from data.scraping import fetch_newest
from data.combine_data import combine_matches
from data.feature_eng import engineer_features
from training.train import train
from fastapi import BackgroundTasks
import asyncio

app = FastAPI(title='Test')


@app.get("/")
async def root():
    return {"message": "Hello World"}

@app.get("/predict")
async def predict():
    results = make_prediction()
    return results

# Wrapper to run sync functions in a separate thread
async def run_in_thread(func, *args, **kwargs):
    await asyncio.to_thread(func, *args, **kwargs)

def run_all_in_order():
    fetch_newest()
    combine_matches()
    engineer_features()

@app.get("/update")
async def load_new_data(background_tasks: BackgroundTasks):
    # Schedule the tasks in background, safely
    background_tasks.add_task(run_all_in_order)
    
    return {"message": "Fetching new matches - this can take up to 10 minutes..."}

@app.get('/retrain_model')
async def retrain_model():
    results = train()
    return {'simple model acc': results[0], 'complex model acc': results[1]}