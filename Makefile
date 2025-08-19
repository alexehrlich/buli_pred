.PHONY: scrape_all scrape_newest build_matches engineer_features prepare_prediction training prediction

all: prepare_prediction prediction

scrape_all:
	python3 ./src/data/scraping.py all

scrape_newest:
	python3 ./src/data/scraping.py newest

build_matches:
	python3 ./src/data/combine_data.py

engineer_features:
	python3 ./src/data/feature_eng.py

prepare_prediction: scrape_newest build_matches engineer_features

training:
		python3 ./src/training/train.py

prediction:
	python3 ./src/prediction/predict.py