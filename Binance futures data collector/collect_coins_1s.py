#!/usr/bin/env python3
import os
import csv
import asyncio
import json
import logging
from datetime import datetime, timezone
from urllib.parse import quote_plus
from decimal import Decimal, ROUND_HALF_UP

import aiohttp
import websockets
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker
from sqlalchemy import text
from binance.client import Client
from dotenv import load_dotenv

# ------------------------------------------------------------------------------
# Логирование
# ------------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

# ------------------------------------------------------------------------------
# Конфиг из .env
# ------------------------------------------------------------------------------
load_dotenv("/opt/binance_scripts/.env")
DB_NAME            = os.getenv("DB_NAME")
DB_USER            = os.getenv("DB_USER")
DB_PASSWORD        = quote_plus(os.getenv("DB_PASSWORD"))
DB_HOST            = os.getenv("DB_HOST")
DB_PORT            = os.getenv("DB_PORT")
BINANCE_API_KEY    = os.getenv("BINANCE_API_KEY")
BINANCE_API_SECRET = os.getenv("BINANCE_API_SECRET")

DB_URL = f"postgresql+asyncpg://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
engine = create_async_engine(DB_URL, pool_pre_ping=True)
AsyncSessionLocal = sessionmaker(engine, expire_on_commit=False, class_=AsyncSession)

client = Client(BINANCE_API_KEY, BINANCE_API_SECRET)

# ------------------------------------------------------------------------------
# Настройки
# ------------------------------------------------------------------------------
FUTURES_SYMBOLS_CSV = "/opt/crypto_data/futures_symbols.csv"
RECONNECT_DELAY_SEC  = 5
BUFFER_DELAY_SEC     = 5
MONITOR_INTERVAL     = 5 * 60
QUEUE_MAXSIZE        = 0  # неограниченная очередь

# ------------------------------------------------------------------------------
# Глобальные структуры
# ------------------------------------------------------------------------------
bar_queue       = asyncio.Queue(maxsize=QUEUE_MAXSIZE)
last_written    = {}  # symbol → посл. записанная секунда
last_close      = {}  # symbol → посл. цена закрытия
last_agg_id     = {}  # symbol → посл. агрег. trade id
missing_counts  = {}
current_symbols = set()

# ------------------------------------------------------------------------------
# Чтение списка символов
# ------------------------------------------------------------------------------
def load_symbols_from_csv(path):
    with open(path, newline="", encoding="utf-8") as f:
        return [row[0].strip() for row in csv.reader(f) if row and row[0].strip()]

# ------------------------------------------------------------------------------
# Инициализация из БД (last_written = сейчас)
# ------------------------------------------------------------------------------
async def init_from_db(symbol):
    sql = text("""
        SELECT EXTRACT(EPOCH FROM time)::BIGINT AS epoch, close
          FROM crypto_data_1s
         WHERE symbol = :sym
         ORDER BY time DESC
         LIMIT 1
    """)
    async with AsyncSessionLocal() as session:
        res = await session.execute(sql, {"sym": symbol})
        row = res.fetchone()
    now = int(datetime.now(timezone.utc).timestamp())
    last_written[symbol] = now
    last_close[symbol] = float(row.close) if row else 0.0

# ------------------------------------------------------------------------------
# Сохранить свечу в БД
# ------------------------------------------------------------------------------
async def save_candle_to_db(symbol, ts, o, h, l, c, v):
    # округление до 4 знаков после запятой через Decimal
    o = Decimal(str(o)).quantize(Decimal("0.0001"), ROUND_HALF_UP)
    h = Decimal(str(h)).quantize(Decimal("0.0001"), ROUND_HALF_UP)
    l = Decimal(str(l)).quantize(Decimal("0.0001"), ROUND_HALF_UP)
    c = Decimal(str(c)).quantize(Decimal("0.0001"), ROUND_HALF_UP)
    v = Decimal(str(v)).quantize(Decimal("0.0001"), ROUND_HALF_UP)

    dt = datetime.fromtimestamp(ts, tz=timezone.utc)
    sql = text("""
        INSERT INTO crypto_data_1s (symbol, time, open, high, low, close, volume)
        VALUES (:symbol, :time, :open, :high, :low, :close, :volume)
        ON CONFLICT (symbol, time) DO NOTHING
    """)
    async with AsyncSessionLocal() as session:
        async with session.begin():
            await session.execute(sql, {
                "symbol": symbol, "time": dt,
                "open": o, "high": h, "low": l,
                "close": c, "volume": v,
            })

# ------------------------------------------------------------------------------
# Получить пропущенные агрегированные сделки через REST
# ------------------------------------------------------------------------------
async def fetch_missing_agg(symbol, from_id):
    params = {"symbol": symbol, "fromId": from_id, "limit": 1000}
    url = "https://fapi.binance.com/fapi/v1/aggTrades"
    async with aiohttp.ClientSession() as session:
        async with session.get(url, params=params) as resp:
            data = await resp.json()
    for item in data:
        await process_trade(symbol, item)

# ------------------------------------------------------------------------------
# Обработка одной сделки/аггрегированной сделки
# ------------------------------------------------------------------------------
async def process_trade(symbol, data):
    t   = data["T"] // 1000
    pid = data["a"]
    price = float(data["p"])
    vol   = float(data["q"])
    prev_id = last_agg_id.get(symbol)
    if prev_id is not None and pid != prev_id + 1:
        await fetch_missing_agg(symbol, prev_id + 1)
    last_agg_id[symbol] = pid

    prev = getattr(process_trade, f"prev_bar_{symbol}", None)
    if prev and prev["second"] != t:
        await bar_queue.put((symbol, prev))
        setattr(process_trade, f"prev_bar_{symbol}", None)
    bar = getattr(process_trade, f"prev_bar_{symbol}", None)
    if not bar or bar["second"] != t:
        bar = {"second": t, "open": price, "high": price, "low": price, "close": price, "volume": vol}
        setattr(process_trade, f"prev_bar_{symbol}", bar)
    else:
        bar["high"]   = max(bar["high"], price)
        bar["low"]    = min(bar["low"], price)
        bar["close"]  = price
        bar["volume"] += vol

# ------------------------------------------------------------------------------
# Корригирующая заполнение пропусков и запись баров из очереди
# ------------------------------------------------------------------------------
async def flush_bars():
    while True:
        symbol, bar = await bar_queue.get()
        sym = symbol
        ts = bar["second"]
        # заполняем пропуски между last_written и текущим ts
        for missing in range(last_written[sym] + 1, ts):
            price = last_close.get(sym, 0.0)
            await save_candle_to_db(sym, missing, price, price, price, price, 0.0)
            missing_counts[sym] = missing_counts.get(sym, 0) + 1
            last_written[sym] = missing
            last_close[sym]   = price
        # сохраняем сам бар
        o, h, l, c, v = bar["open"], bar["high"], bar["low"], bar["close"], bar["volume"]
        await save_candle_to_db(sym, ts, o, h, l, c, v)
        last_written[sym] = ts
        last_close[sym]   = c

# ------------------------------------------------------------------------------
# Периодический флаш пустых баров каждую секунду
# ------------------------------------------------------------------------------
async def periodic_flush_empty():
    while True:
        await asyncio.sleep(1)
        now = int(datetime.now(timezone.utc).timestamp())
        for sym in current_symbols:
            ts = last_written.get(sym, 0)
            # вставляем пропущенные пустые бары до текущей секунды
            while ts < now:
                next_ts = ts + 1
                price = last_close.get(sym, 0.0)
                await save_candle_to_db(sym, next_ts, price, price, price, price, 0.0)
                missing_counts[sym] = missing_counts.get(sym, 0) + 1
                last_written[sym] = next_ts
                ts = next_ts

# ------------------------------------------------------------------------------
# Поддержание соединения (ping/pong)
# ------------------------------------------------------------------------------
async def keepalive(ws):
    while True:
        try:
            pong = await ws.ping()
            await asyncio.wait_for(pong, timeout=20)
        except Exception as e:
            logging.warning(f"Ping failed: {e}")
            return

# ------------------------------------------------------------------------------
# Обработка WS-стрима aggTrade
# ------------------------------------------------------------------------------
async def listen_agg(symbol):
    url = f"wss://fstream.binance.com/ws/{symbol.lower()}@aggTrade"
    while True:
        try:
            async with websockets.connect(
                url, ping_interval=20, ping_timeout=20, open_timeout=30
            ) as ws:
                ping_task = asyncio.create_task(keepalive(ws))
                async for msg in ws:
                    data = json.loads(msg)
                    if data.get("e") != "aggTrade":
                        continue
                    await process_trade(symbol, data)
                ping_task.cancel()
        except websockets.ConnectionClosed as e:
            logging.warning(f"[{symbol}] WS closed: {e}, reconnect in {RECONNECT_DELAY_SEC}s")
            await asyncio.sleep(RECONNECT_DELAY_SEC)
        except Exception as e:
            logging.warning(f"[{symbol}] WS error: {e}, reconnect in {RECONNECT_DELAY_SEC}s")
            await asyncio.sleep(RECONNECT_DELAY_SEC)

# ------------------------------------------------------------------------------
# Основная точка входа
# ------------------------------------------------------------------------------
async def main():
    syms = load_symbols_from_csv(FUTURES_SYMBOLS_CSV)
    for s in syms:
        await init_from_db(s)
        current_symbols.add(s)
        missing_counts[s] = 0
        last_agg_id[s] = None
        asyncio.create_task(listen_agg(s))
        await asyncio.sleep(0.05)
    # несколько воркеров для очереди
    for _ in range(4):
        asyncio.create_task(flush_bars())
    # периодический флаш пустых баров
    asyncio.create_task(periodic_flush_empty())
    await asyncio.Event().wait()

if __name__ == "__main__":
    asyncio.run(main())
