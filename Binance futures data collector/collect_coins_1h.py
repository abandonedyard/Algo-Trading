#!/usr/bin/env python3
import os
from dotenv import load_dotenv
from urllib.parse import quote_plus
import csv
import asyncio
import websockets
import json
import psycopg2
from datetime import datetime, timezone  # добавлен timezone
from collections import defaultdict
from sqlalchemy import create_engine, text

# ------------------------------------------------------------------------------
# 1) Загружаем переменные окружения из файла /opt/binance_scripts/.env
# ------------------------------------------------------------------------------
load_dotenv("/opt/binance_scripts/.env")

# ------------------------------------------------------------------------------
# 2) Считываем параметры для подключения к БД из переменных окружения
# ------------------------------------------------------------------------------
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = quote_plus(os.getenv("DB_PASSWORD"))  # Кодируем пароль
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")

# ------------------------------------------------------------------------------
# 3) Формируем строку подключения к PostgreSQL на основе переменных
# ------------------------------------------------------------------------------
DB_URL = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

# -------------------------------------------------
# ПУТЬ К CSV-ФАЙЛУ С ФЬЮЧЕРСНЫМИ СИМВОЛАМИ
# -------------------------------------------------
FUTURES_SYMBOLS_CSV = "/opt/crypto_data/futures_symbols.csv"

# -------------------------------------------------
# КОЛИЧЕСТВО СИМВОЛОВ В ОДНОМ WEBSOCKET (примерно)
# -------------------------------------------------
SYMBOLS_PER_CONNECTION = 100

# -------------------------------------------------
# ЗАДЕРЖКА ПЕРЕД ПОВТОРОМ ПОДКЛЮЧЕНИЯ ПРИ ОШИБКЕ
# -------------------------------------------------
RECONNECT_DELAY_SEC = 5

# -------------------------------------------------
# BINANCE FUTURES WebSocket endpoint
# -------------------------------------------------
BINANCE_FUTURES_WS = "wss://fstream.binance.com/stream"

# -------------------------------------------------
# Создаём engine SQLAlchemy для PostgreSQL
# -------------------------------------------------
engine = create_engine(DB_URL, pool_pre_ping=True)

# ------------------------------------------------------------------------------------
# ФУНКЦИЯ ДЛЯ ЧТЕНИЯ СПИСКА СИМВОЛОВ ИЗ CSV
# ------------------------------------------------------------------------------------
def load_symbols_from_csv(filepath: str):
    symbols = []
    with open(filepath, "r", newline="", encoding="utf-8") as f:
        reader = csv.reader(f)
        for row in reader:
            if row:
                symbol = row[0].strip()
                if symbol:
                    symbols.append(symbol)
    return symbols

# ------------------------------------------------------------------------------------
# ВСПОМОГАТЕЛЬНАЯ ФУНКЦИЯ: РАЗБИВАЕМ СПИСОК СИМВОЛОВ НА НЕСКОЛЬКО "ПАКЕТОВ"
# ------------------------------------------------------------------------------------
def chunk_symbols(symbols, chunk_size):
    for i in range(0, len(symbols), chunk_size):
        yield symbols[i:i+chunk_size]

# ------------------------------------------------------------------------------------
# ГЛОБАЛЬНЫЙ АГРЕГАТОР СВЕЧЕЙ (для каждого символа)
# ------------------------------------------------------------------------------------
# Будем хранить данные свечи по ключу "hour"
candles_data = defaultdict(dict)

# ------------------------------------------------------------------------------------
# СОХРАНЕНИЕ СФОРМИРОВАННОЙ СВЕЧИ В БД
# ------------------------------------------------------------------------------------
def save_candle_to_db(symbol, candle_timestamp, copen, chigh, clow, cclose, cvolume):
    """
    Сохраняет одну часовую свечу в таблицу public.crypto_data_1h.
    
    Колонки:
      - symbol (character varying)
      - time   (timestamp with time zone)
      - open, high, low, close, volume (numeric)
    """
    # Переводим timestamp (начало часа) в tz-aware datetime в UTC
    dt = datetime.fromtimestamp(candle_timestamp, tz=timezone.utc)
    
    # SQL-запрос
    query = text("""
        INSERT INTO crypto_data_1h (symbol, time, open, high, low, close, volume)
        VALUES(:symbol, :time, :open, :high, :low, :close, :volume)
    """)

    params = {
        "symbol": symbol,
        "time": dt,
        "open": copen,
        "high": chigh,
        "low": clow,
        "close": cclose,
        "volume": cvolume
    }

    try:
        with engine.begin() as conn:
            conn.execute(query, params)
    except Exception as e:
        print(f"[{symbol}] Ошибка записи свечи в БД: {e}")

# ------------------------------------------------------------------------------------
# АГРЕГАЦИЯ СВЕЧЕЙ ИЗ СООБЩЕНИЙ aggTrade
# ------------------------------------------------------------------------------------
def process_agg_trade_message(symbol, msg):
    trade_price = float(msg["p"])
    trade_volume = float(msg["q"])
    trade_time_ms = msg["T"]
    trade_time_s = trade_time_ms // 1000  # Перевод миллисекунд в секунды
    # Округляем до начала часа (то есть, 00 минут 00 секунд)
    trade_time_h = (trade_time_s // 3600) * 3600

    candle = candles_data[symbol]

    # Если новая метка часа — закрываем предыдущую свечу (если была) и открываем новую
    if ("hour" not in candle) or (candle["hour"] != trade_time_h):
        if "hour" in candle:
            save_candle_to_db(
                symbol=symbol,
                candle_timestamp=candle["hour"],
                copen=candle["open"],
                chigh=candle["high"],
                clow=candle["low"],
                cclose=candle["close"],
                cvolume=candle["volume"]
            )
        # Открываем новую свечу
        candle["hour"] = trade_time_h
        candle["open"]   = trade_price
        candle["high"]   = trade_price
        candle["low"]    = trade_price
        candle["close"]  = trade_price
        candle["volume"] = trade_volume
    else:
        # Продолжаем ту же свечу
        if trade_price > candle["high"]:
            candle["high"] = trade_price
        if trade_price < candle["low"]:
            candle["low"] = trade_price
        candle["close"] = trade_price
        candle["volume"] += trade_volume

# ------------------------------------------------------------------------------------
# ФУНКЦИЯ, КОТОРАЯ ПО КОНЦУ КАЖДОГО ЧАСА СБРАСЫВАЕТ (FLUSH) НАКОПЛЕННЫЕ ДАННЫЕ
# ------------------------------------------------------------------------------------
async def flush_candles_on_hour():
    """
    Фоновая задача, которая ожидает наступления следующего полного часа (UTC)
    и для каждого символа, у которого есть накопленная свеча, сохраняет её в БД и сбрасывает данные.
    """
    while True:
        now = datetime.utcnow()
        # Вычисляем, сколько осталось секунд до следующего полного часа
        seconds_until_next_hour = 3600 - (now.minute * 60 + now.second)
        await asyncio.sleep(seconds_until_next_hour)
        # В момент наступления полного часа (00:00:00) сбрасываем накопленные свечи
        for symbol, candle in list(candles_data.items()):
            if "hour" in candle:
                save_candle_to_db(
                    symbol=symbol,
                    candle_timestamp=candle["hour"],
                    copen=candle["open"],
                    chigh=candle["high"],
                    clow=candle["low"],
                    cclose=candle["close"],
                    cvolume=candle["volume"]
                )
                # Сбрасываем данные для символа
                candles_data[symbol] = {}
        print("[FLUSH] Свечи сброшены по наступлению полного часа (UTC)")

# ------------------------------------------------------------------------------------
# ASYNC-ФУНКЦИЯ ДЛЯ ОБРАБОТКИ ОДНОГО WS-ПОДКЛЮЧЕНИЯ (ГРУППЫ СИМВОЛОВ)
# ------------------------------------------------------------------------------------
async def listen_agg_trades_for_symbols(symbols_chunk):
    """
    Подписываемся на aggTrade для символов из symbols_chunk,
    бесконечно слушаем поток.
    """
    streams_part = "/".join([f"{sym.lower()}@aggTrade" for sym in symbols_chunk])
    ws_url = f"{BINANCE_FUTURES_WS}?streams={streams_part}"

    print(f"\n[WS] Подключение для {len(symbols_chunk)} символов:\n{ws_url}\n")

    while True:
        try:
            async with websockets.connect(ws_url) as ws:
                while True:
                    message = await ws.recv()
                    data = json.loads(message)
                    
                    # Формат "обёртки": {"stream":"btcusdt@aggTrade","data":{...}}
                    if "data" in data and data["data"].get("e") == "aggTrade":
                        stream_symbol = data["data"]["s"]  # например, "BTCUSDT"
                        process_agg_trade_message(stream_symbol, data["data"])

        except Exception as e:
            print(f"[WS] Ошибка: {e}. Переподключаемся через {RECONNECT_DELAY_SEC} секунд...")
            await asyncio.sleep(RECONNECT_DELAY_SEC)

# ------------------------------------------------------------------------------------
# ОСНОВНАЯ ФУНКЦИЯ, ЗАПУСКАЮЩАЯ ВСЕ ПОДПИСКИ
# ------------------------------------------------------------------------------------
async def main():
    # 1. Загружаем все символы из CSV
    all_symbols = load_symbols_from_csv(FUTURES_SYMBOLS_CSV)
    print(f"Найдено символов: {len(all_symbols)}")

    # 2. Разбиваем на группы
    chunks = list(chunk_symbols(all_symbols, SYMBOLS_PER_CONNECTION))
    print(f"Сформировано {len(chunks)} WebSocket-подключений (по {SYMBOLS_PER_CONNECTION} символов максимум)")

    # 3. Создаём асинхронные таски для каждой группы и для фонового сброса свечей
    tasks = [asyncio.create_task(listen_agg_trades_for_symbols(chunk)) for chunk in chunks]
    tasks.append(asyncio.create_task(flush_candles_on_hour()))

    # 4. Запускаем все таски параллельно (до бесконечности)
    await asyncio.gather(*tasks)

# ------------------------------------------------------------------------------------
# ЗАПУСК
# ------------------------------------------------------------------------------------
if __name__ == "__main__":
    asyncio.run(main())
