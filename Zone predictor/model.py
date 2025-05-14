#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
model.py

Скрипт автоматически находит единственный CSV-файл с признаками в папке
/Users/danil/Desktop/DataScience/O2/Разметка/DE/,
читает его, обучает LightGBM-регрессор и сохраняет модель + скейлер.
"""

import os
import glob
import sys
from datetime import datetime

import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler
import lightgbm as lgb
from sklearn.metrics import mean_absolute_error
import joblib

# Папка, где лежит ровно один CSV-файл с признаками
DATA_DIR = "/Users/danil/Desktop/DataScience/O2/Разметка/DE/"

def find_csv_file(data_dir):
    """Ищет ровно один CSV в папке и возвращает его путь."""
    pattern = os.path.join(data_dir, "*.csv")
    files = glob.glob(pattern)
    if not files:
        raise FileNotFoundError(f"No CSV files found in {data_dir}")
    if len(files) > 1:
        raise ValueError(f"Multiple CSV files found in {data_dir}: {files}")
    return files[0]

def main():
    # 1. Найти и прочитать CSV
    csv_path = find_csv_file(DATA_DIR)
    print(f"Reading features from {csv_path}")
    df = pd.read_csv(csv_path)

    # 2. Проверка наличия целевого столбца
    TARGET = "green_duration_sec"
    if TARGET not in df.columns:
        print(f"Error: target column '{TARGET}' not found in CSV")
        sys.exit(1)

    # 3. Выбор признаков
    FEATURES = [
        "pre_volatility",
        "pre_range_pct",
        "pre_max_candle_pct",
        "green_pct_diff",
        "green_speed_pct_sec",
        "speed_ratio_green_vs_pre",
        "volatility_ratio_green_vs_pre"
    ]
    missing = set(FEATURES) - set(df.columns)
    if missing:
        print(f"Error: missing feature columns: {missing}")
        sys.exit(1)

    X = df[FEATURES]
    y = df[TARGET]

    # 4. Разбиение на train/test
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=42
    )

    # 5. Масштабирование признаков
    scaler = StandardScaler()
    X_train_scaled = scaler.fit_transform(X_train)
    X_test_scaled  = scaler.transform(X_test)

    # 6. Обучение LightGBM-регрессора с явным num_leaves и ранней остановкой
    model = lgb.LGBMRegressor(
        n_estimators=500,
        learning_rate=0.05,
        max_depth=6,
        num_leaves=64,       # 2**max_depth
        random_state=42
    )
    model.fit(
        X_train_scaled, y_train,
        eval_set=[(X_test_scaled, y_test)],
        eval_metric="mae",
        callbacks=[
            lgb.early_stopping(stopping_rounds=50),
            lgb.log_evaluation(period=50)
        ]
    )

    # 7. Оценка качества
    y_pred = model.predict(X_test_scaled)
    mae = mean_absolute_error(y_test, y_pred)
    print(f"MAE on test set: {mae:.2f} seconds")

    # 8. Важность признаков
    print("\nFeature importances:")
    for feat, imp in zip(FEATURES, model.feature_importances_):
        print(f"  {feat}: {imp}")

    # 9. Сохранение модели и скейлера
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    model_path  = os.path.join(DATA_DIR, f"model_{timestamp}.pkl")
    scaler_path = os.path.join(DATA_DIR, f"scaler_{timestamp}.pkl")
    joblib.dump(model,  model_path)
    joblib.dump(scaler, scaler_path)
    print(f"\nModel saved to {model_path}")
    print(f"Scaler saved to {scaler_path}")

if __name__ == "__main__":
    main()
