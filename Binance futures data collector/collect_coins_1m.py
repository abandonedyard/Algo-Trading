#!/usr/bin/env python3
import os
import csv
import asyncio
import websockets
import json
from datetime import datetime, timezone, timedelta
from dotenv import load_dotenv
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker
from sqlalchemy import text
from urllib.parse import quote_plus
import logging
from decimal import Decimal, ROUND_HALF_UP

# ------------------------------------------------------------------------------
# Настройка логирования: выводятся только сообщения уровня ERROR
# ------------------------------------------------------------------------------
logging.basicConfig(
    level=logging.ERROR,  
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

# ------------------------------------------------------------------------------
# 1) Загрузка переменных окружения
# ------------------------------------------------------------------------------
load_dotenv("/opt/binance_scripts/.env")

DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = quote_plus(os.getenv("DB_PASSWORD"))
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")

# ------------------------------------------------------------------------------
# 2) Асинхронное подключение к PostgreSQL через asyncpg
# ------------------------------------------------------------------------------
ASYNC_DB_URL = f"postgresql+asyncpg://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
async_engine = create_async_engine(ASYNC_DB_URL, pool_pre_ping=True)
AsyncSessionLocal = sessionmaker(async_engine, class_=AsyncSession, expire_on_commit=False)

# ------------------------------------------------------------------------------
# 3) Константы
# ------------------------------------------------------------------------------
FUTURES_SYMBOLS_CSV = "/opt/crypto_data/futures_symbols.csv"
RECONNECT_DELAY_SEC = 5
BINANCE_WS_BASE = "wss://fstream.binance.com/ws/"

# ------------------------------------------------------------------------------
# 4) Функция чтения списка символов из CSV
# ------------------------------------------------------------------------------
def load_symbols_from_csv(filepath: str):
    symbols = []
    try:
        with open(filepath, newline="", encoding="utf-8") as f:
            for row in csv.reader(f):
                if row and (symbol := row[0].strip()):
                    symbols.append(symbol)
    except Exception as e:
        logger.error(f"Ошибка чтения CSV файла: {e}")
    return symbols

# ------------------------------------------------------------------------------
# 5) Асинхронная функция записи свечи в базу данных
# ------------------------------------------------------------------------------
async def save_candle_to_db(symbol, ts, o, h, l, c, v):
    dt = datetime.fromtimestamp(ts, tz=timezone.utc)
    query = text("""
        INSERT INTO crypto_data_1m(symbol, time, open, high, low, close, volume)
        VALUES(:symbol, :time, :open, :high, :low, :close, :volume)
    """)
    async with AsyncSessionLocal() as session:
        try:
            await session.execute(query, {
                "symbol": symbol,
                "time": dt,
                "open": o,
                "high": h,
                "low": l,
                "close": c,
                "volume": v
            })
            await session.commit()
        except Exception as e:
            logger.error(f"[{symbol}] Ошибка записи свечи в БД: {e}")

# ------------------------------------------------------------------------------
# 6) Асинхронная функция прослушивания свечей kline_1m для одного символа
# ------------------------------------------------------------------------------
async def listen_kline(symbol):
    url = f"{BINANCE_WS_BASE}{symbol.lower()}@kline_1m"
    while True:
        try:
            async with websockets.connect(
                url,
                open_timeout=30,
                ping_interval=30,
                ping_timeout=20
            ) as ws:
                async for message in ws:
                    try:
                        parsed_msg = json.loads(message)
                        data = parsed_msg.get("data", parsed_msg)
                    except Exception as e:
                        logger.error(f"[{symbol}] Ошибка декодирования JSON: {e}")
                        continue
                    
                    if data.get("e") == "kline":
                        k = data["k"]
                        if not k.get("x", False):
                            continue

                        # Преобразуем значения в Decimal и округляем до 4 знаков
                        open_ = Decimal(k["o"]).quantize(Decimal('0.0001'), rounding=ROUND_HALF_UP)
                        high_ = Decimal(k["h"]).quantize(Decimal('0.0001'), rounding=ROUND_HALF_UP)
                        low_  = Decimal(k["l"]).quantize(Decimal('0.0001'), rounding=ROUND_HALF_UP)
                        close_ = Decimal(k["c"]).quantize(Decimal('0.0001'), rounding=ROUND_HALF_UP)
                        volume_ = Decimal(k["v"]).quantize(Decimal('0.0001'), rounding=ROUND_HALF_UP)

                        await save_candle_to_db(
                            symbol,
                            k["t"] // 1000,
                            open_,
                            high_,
                            low_,
                            close_,
                            volume_
                        )
        except Exception as e:
            logger.error(f"[{symbol}] WS error: {e}. Переподключаемся через {RECONNECT_DELAY_SEC} секунд...")
            await asyncio.sleep(RECONNECT_DELAY_SEC)

# ------------------------------------------------------------------------------
# 7) Асинхронная функция обновления подключений по CSV (ежедневно в 01:00 МСК)
# ------------------------------------------------------------------------------
async def update_connections_periodically(active_tasks: dict):
    while True:
        try:
            now_utc = datetime.now(timezone.utc)
            moscow_now = now_utc.astimezone(timezone(timedelta(hours=3)))
            next_run = moscow_now.replace(hour=1, minute=0, second=0, microsecond=0)
            if next_run <= moscow_now:
                next_run += timedelta(days=1)
            sleep_seconds = (next_run - moscow_now).total_seconds()
            await asyncio.sleep(sleep_seconds)
            
            new_symbols = set(load_symbols_from_csv(FUTURES_SYMBOLS_CSV))
            for sym in list(active_tasks.keys()):
                if sym not in new_symbols:
                    active_tasks[sym].cancel()
                    del active_tasks[sym]
            for sym in new_symbols:
                if sym not in active_tasks:
                    active_tasks[sym] = asyncio.create_task(listen_kline(sym))
        except Exception as e:
            logger.error(f"Ошибка обновления подключений: {e}")

# ------------------------------------------------------------------------------
# 8) Основная функция
# ------------------------------------------------------------------------------
async def main():
    symbols = set(load_symbols_from_csv(FUTURES_SYMBOLS_CSV))
    active_tasks = {}
    for sym in symbols:
        active_tasks[sym] = asyncio.create_task(listen_kline(sym))
        await asyncio.sleep(0.05)
    update_task = asyncio.create_task(update_connections_periodically(active_tasks))
    await asyncio.gather(*list(active_tasks.values()), update_task)

# ------------------------------------------------------------------------------
# Точка входа
# ------------------------------------------------------------------------------
if __name__ == "__main__":
    asyncio.run(main())
