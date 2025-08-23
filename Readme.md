# Bundesliga Match predictor

## Overview
This small application fetches soccer data from different places to train a aimple and complex model to predict unplayed soccer matches of the german bundesliga. A small website running in an nginx container serves to request predictions, update match data and retrain the models via seperate end points.


### Frontend
Simple index.html located in a distinct nginx docker container shows the unplayed matches with an elo plot of both teams

![image]([https://github.com/alexehrlich/buli_pred/imag/screen.png](https://github.com/alexehrlich/buli_pred/blob/main/img/screen.png))

#### Endpoints:
- localhost:8080/ Show the predictions for all unplayed matches
- localhost:8080/update Download new match data from fbref and club elo to retrain the model
- localhost:8080/retrain_model Retrain the model based on the newly fetcehd data
