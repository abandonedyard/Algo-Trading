#!/usr/bin/env python3
import logging
import ccxt
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.types import TIMESTAMP, Float, String
from dotenv import load_dotenv
import os
from urllib.parse import quote_plus
from datetime import datetime, timedelta

# ------------------------------------------------------------------------------
# Конфиг из .env
# ------------------------------------------------------------------------------
load_dotenv("/opt/binance_scripts/.env")
DB_NAME     = os.getenv("DB_NAME")
DB_USER     = os.getenv("DB_USER")
DB_PASSWORD = quote_plus(os.getenv("DB_PASSWORD"))
DB_HOST     = os.getenv("DB_HOST")
DB_PORT     = os.getenv("DB_PORT")
DB_URL      = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

# ------------------------------------------------------------------------------
# Логгирование
# ------------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# ------------------------------------------------------------------------------
# Подключения
# ------------------------------------------------------------------------------
engine   = create_engine(DB_URL)
exchange = ccxt.binance()

# ------------------------------------------------------------------------------
# Константы
# ------------------------------------------------------------------------------
SYMBOLS_FILE      = "/opt/crypto_data/futures_symbols.csv"
SECONDS_BACK_MS   = 10 * 24 * 60 * 60 * 1000   # 10 дней
LIMIT             = 500
TIMEFRAME         = "1s"
TABLE             = "crypto_data_1s"
TF_SEC            = 1

# ------------------------------------------------------------------------------
# Читаем символы
# ------------------------------------------------------------------------------
def load_symbols(path):
    df = pd.read_csv(path)
    return df["Symbol"].tolist() if "Symbol" in df.columns else []

# ------------------------------------------------------------------------------
# Существующие секунды из БД
# ------------------------------------------------------------------------------
def get_existing_seconds(symbol):
    sql = text(f"""
        SELECT EXTRACT(EPOCH FROM time)::BIGINT AS ts
          FROM {TABLE}
         WHERE symbol = :sym
      ORDER BY ts
    """)
    with engine.connect() as conn:
        return conn.execute(sql, {"sym": symbol.replace("/", "")}).scalars().all()

# ------------------------------------------------------------------------------
# Ищем пробелы
# ------------------------------------------------------------------------------
def find_gaps(ts_list_ms):
    gaps = []
    if not ts_list_ms:
        return gaps
    prev = ts_list_ms[0]
    for curr in ts_list_ms[1:]:
        if curr > prev + 1000:
            gaps.append((prev + 1000, curr - 1000))
        prev = curr
    return gaps

# ------------------------------------------------------------------------------
# Запрос и вставка свечек
# ------------------------------------------------------------------------------
def fetch_and_insert(symbol, start_ms, end_ms):
    start = int(start_ms)
    end   = int(end_ms)
    # собираем существующие и сразу умножаем на 1000 → миллисекунды
    existing = set(ts * 1000 for ts in get_existing_seconds(symbol))
    while start <= end:
        chunk_end = min(end, start + LIMIT * TF_SEC * 1000)
        logging.info(f"{symbol}: запрашиваю {TIMEFRAME} с {start} до {chunk_end}")
        candles = exchange.fetch_ohlcv(symbol, timeframe=TIMEFRAME, since=start, limit=LIMIT)
        if not candles:
            logging.warning(f"{symbol}: нет данных {TIMEFRAME} с {start}")
            break

        df = pd.DataFrame(candles, columns=["time","open","high","low","close","volume"])
        df["time"] = pd.to_datetime(df["time"], unit="ms", utc=True)
        df.insert(0, "symbol", symbol.replace("/", ""))

        # округление до 4 знаков
        df[["open","high","low","close","volume"]] = df[["open","high","low","close","volume"]].round(4)

        # фильтруем дубликаты по (symbol, time)
        df["ts"] = (df["time"].astype("int64") // 10**6).astype(int)
        df = df[~df["ts"].isin(existing)]
        if df.empty:
            logging.info(f"{symbol}: все {TIMEFRAME} в этом чанке уже в БД")
            start = chunk_end + 1
            continue

        # добавляем новые ts в existing
        existing.update(df["ts"].tolist())
        df = df.drop(columns=["ts"])

        # пишем в БД
        df.to_sql(
            name=TABLE, con=engine,
            if_exists="append", index=False,
            dtype={
                "symbol": String(50),
                "time":   TIMESTAMP,
                "open":   Float,
                "high":   Float,
                "low":    Float,
                "close":  Float,
                "volume": Float,
            }
        )
        logging.info(f"{symbol}: вставил {len(df)} строк")

        # следующий сегмент
        last_ts = int(df["time"].astype("int64").max() // 10**6)
        start = (last_ts + 1) * 1000

# ------------------------------------------------------------------------------
# Основная логика
# ------------------------------------------------------------------------------
def main():
    symbols = load_symbols(SYMBOLS_FILE)
    logging.info(f"Начинаю проверку {len(symbols)} символов")
    now_ms = exchange.milliseconds()

    for sym in symbols:
        sym_clean = sym.replace("/", "")
        logging.info(f"=== {sym_clean} ===")
        existing_secs = get_existing_seconds(sym)
        if not existing_secs:
            fetch_and_insert(sym, now_ms - SECONDS_BACK_MS, now_ms)
        else:
            gaps = find_gaps([ts*1000 for ts in existing_secs])
            if not gaps:
                logging.info(f"{sym_clean}: пропусков нет")
            for start_ms, end_ms in gaps:
                fetch_and_insert(sym, start_ms, end_ms)

    logging.info("Готово.")

if __name__ == "__main__":
    main()
