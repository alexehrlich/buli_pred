# Bundesliga Match predictor

## Overview
This small application fetches soccer data from different places to train a aimple and complex model to predict unplayed soccer matches of the german bundesliga. A small website running in an nginx container serves to request predictions, update match data and retrain the models via seperate end points.


### Frontend
Simple index.html located in a distinct nginx docker container shows the unplayed matches with an elo plot of both teams

![https://github.com/alexehrlich/buli_pred/imag/screen.png](https://github.com/alexehrlich/buli_pred/blob/main/img/screen.png)

#### Endpoints:
- localhost:8080/ Show the predictions for all unplayed matches
- localhost:8080/update Download new match data from fbref and club elo to retrain the model
- localhost:8080/predict raw prediction data of this match day's unplayed games
- localhost:8080/retrain_model Retrain the model based on the newly fetcehd data

### Backend
A fastapi-app is running in a distinct docker container. The nginx endpoints from above are forwarded over the docker network to make predictions, load new data and retrain the model. This includes also data cleaning and feature engineering.

### Model
Soccer is a random game. Trainign a XGBoost to classify Win, Draw, Loss reaches with my current data an accuracy of 50% which is an improvement compared to stupid guessing ~33% oder major class guessing ~42%.

A simpler model just trained on the target if the home team wins or not (away team wins OR draw) reaches an accuracy of ~69%, which is ok. Both predictions are shown on the website

#### Data
Data is fetched form fbreb.com and clubelo.com

#### Feature Engineering
With the fetched data a lot of features were calculated to find some, which seperate the target classes. Here it turned out, that soccer is a really random game which also relies strongly on ad hoc scenarios like missing players, changer of manacher etc. which is hart to get from olad data amd integrate into training.
The almost impossible clear class separation can be seen in this pairplot. its also shown, that with the current features it is very hard to predict Draws, since this class is underrepresented and overlapped by higher probabilites of win and loss:
![https://github.com/alexehrlich/buli_pred/imag/eda.png](https://github.com/alexehrlich/buli_pred/blob/main/img/eda.png)



## Usage

A running docker deamon is neccessary

- `git clone ...`
- `docker-compose up --build`
- go to the endpoints on localhost described above
