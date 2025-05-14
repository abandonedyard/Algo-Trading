# Zone Predictor

## Project Overview  
This repository implements a pipeline to detect and model “green zones” (rapid price run-ups) in 1-second crypto data, train a predictor for green-zone duration, and visualize real-time triggers via a Bokeh app. The folder contains three main scripts:

1. **`collection.py`** — extract per-trade features around each 4%+ 1-minute trigger, compute zone metrics, and save a timestamped CSV of features.  
2. **`model.py`** — locate the generated CSV, train a LightGBM regression model on green-zone duration, evaluate MAE, and serialize both model and scaler to `.pkl`.  
3. **`combined_app.py`** — a Bokeh server application that:  
   - streams 1-second data from PostgreSQL  
   - applies the trained model+scaler to predict green-zone end times  
   - renders candlestick-style percent-change charts with annotated pump/dump/stabilization/purple zones  
   - steps through each new 4% trigger in the last 24 h  

## File Descriptions  
- **`collection.py`**  
  - Connects to `crypto_data_1m` and `crypto_data_1s` tables  
  - Finds minute bars where `(close–open)/open × 100 ≥ 4%` over a given look-back (default 10 days)  
  - Computes pre-zone, green-zone, and volatility metrics per trigger  
  - Emits `green_zone_sel_features_<timestamp>.csv` into a configurable output folder  

- **`model.py`**  
  - Reads exactly one features CSV from a configured directory  
  - Splits into train/test, scales features, trains LightGBM regressor with early stopping  
  - Prints test MAE and feature importances  
  - Saves `model_<timestamp>.pkl` and `scaler_<timestamp>.pkl` alongside the CSV  

- **`combined_app.py`**  
  - Auto-discovers the latest `model_*.pkl` and `scaler_*.pkl`  
  - Connects to PostgreSQL’s `crypto_data_1s` and `crypto_data_1m`  
  - Loads all 4% triggers from the last day  
  - Runs a Bokeh server that visualizes each trigger in sequence, streaming 1s data and updating zone annotations  

## Requirements  
- Python 3.8+  
- PostgreSQL with tables:  
  - `crypto_data_1m(time TIMESTAMP, open, high, low, close, volume, symbol)`  
  - `crypto_data_1s(time TIMESTAMP, close, symbol)`  
- Python packages (install via `pip install -r requirements.txt`):  
  ```text
  pandas
  numpy
  psycopg2-binary
  python-dotenv
  lightgbm
  scikit-learn
  joblib
  bokeh
