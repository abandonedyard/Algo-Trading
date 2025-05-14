#!/usr/bin/env python3
import os
import pandas as pd
from datetime import datetime, timedelta
from binance.client import Client
from dotenv import load_dotenv
from sqlalchemy import create_engine

# ------------------------------------------------------------------------------
# 1) Загружаем переменные окружения из файла /opt/binance_scripts/.env
# ------------------------------------------------------------------------------
load_dotenv("/opt/binance_scripts/.env")

# ------------------------------------------------------------------------------
# 2) Считываем параметры для подключения к базе PostgreSQL из переменных окружения
# ------------------------------------------------------------------------------
DB_NAME     = os.getenv("DB_NAME")
DB_USER     = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST     = os.getenv("DB_HOST")
DB_PORT     = os.getenv("DB_PORT")

# Формируем строку подключения для SQLAlchemy
DB_URL = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
engine = create_engine(DB_URL)

# ------------------------------------------------------------------------------
# 3) Подключение к Binance (без API-ключей)
# ------------------------------------------------------------------------------
client = Client()

# ------------------------------------------------------------------------------
# 4) Путь для сохранения CSV-файла
# ------------------------------------------------------------------------------
OUTPUT_DIR  = "/opt/crypto_data"
OUTPUT_FILE = os.path.join(OUTPUT_DIR, "futures_symbols.csv")

# ------------------------------------------------------------------------------
# Функция для обновления списка криптовалют и сохранения их в CSV и БД
# ------------------------------------------------------------------------------
def update_crypto_symbols():
    try:
        os.makedirs(OUTPUT_DIR, exist_ok=True)

        info = client.futures_exchange_info()
        active_symbols = {s['symbol'] for s in info['symbols'] if s['status'] == 'TRADING'}

        tickers = client.futures_mark_price()

        symbols = []
        for i in tickers:
            s = i['symbol']
            # отсекаем USDCUSDT, внутренние пары, неактивные,
            # а также ровно BTCUSDT и ETHUSDT
            if (
                s == 'USDCUSDT'
                or 'USDT_' in s
                or s not in active_symbols
                or s == 'BTCUSDT'
                or s == 'ETHUSDT'
            ):
                continue
            symbols.append(s)

        df = pd.DataFrame(symbols, columns=["Symbol"])
        df.to_csv(OUTPUT_FILE, index=False)
        print(f"✅ Сохранено {len(symbols)} символов в файл: {OUTPUT_FILE}")

        df.to_sql("futures_symbols", con=engine, if_exists="replace", index=False)
        print(f"✅ Сохранено {len(symbols)} символов в базу данных (таблица: futures_symbols)")

    except Exception as e:
        print(f"❌ Ошибка при обновлении символов: {e}")

# ------------------------------------------------------------------------------
# ЗАПУСК СКРИПТА
# ------------------------------------------------------------------------------
if __name__ == "__main__":
    update_crypto_symbols()
