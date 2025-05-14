#!/usr/bin/env python3
import logging
import ccxt
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.types import TIMESTAMP, Float, String
from dotenv import load_dotenv
import os
from urllib.parse import quote_plus

# Загружаем переменные окружения из .env
load_dotenv("/opt/binance_scripts/.env")

# Параметры подключения к базе данных
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = quote_plus(os.getenv("DB_PASSWORD"))  # Кодируем пароль
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_URL = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

# Настройка логов
logging.basicConfig(
    level=logging.INFO,  # Вывод логов в терминал
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# Подключение к базе данных через SQLAlchemy
engine = create_engine(DB_URL)

# Подключение к Binance через ccxt (без API-ключей)
exchange = ccxt.binance()

# Путь к CSV-файлу со списком символов (например, фьючерсные пары)
SYMBOLS_FILE = "/opt/crypto_data/futures_symbols.csv"

# Временной интервал для сбора данных: последние 30 дней (в миллисекундах)
PERIOD_MS = 30 * 24 * 60 * 60 * 1000  # 30 дней

# Лимит записей за один запрос к API (максимум 500)
LIMIT = 500

def load_symbols_from_csv(filepath: str):
    """Загружает список символов из CSV-файла."""
    symbols = []
    df = pd.read_csv(filepath)
    if "Symbol" in df.columns:
        symbols = df["Symbol"].tolist()
    return symbols

def fetch_and_save_candles(timeframe: str, table_name: str, timeframe_seconds: int):
    """
    Собирает свечи за последние 30 дней для заданного таймфрейма и сохраняет их в указанную таблицу.
    
    :param timeframe: Интервал свечи, например "1h"
    :param table_name: Имя таблицы для сохранения данных (crypto_data_1h)
    :param timeframe_seconds: Длительность свечи в секундах (3600 для часовых)
    """
    symbols = load_symbols_from_csv(SYMBOLS_FILE)
    logging.info(f"Найдено символов: {len(symbols)} для сбора свечей {timeframe} в таблицу '{table_name}'")
    
    for symbol in symbols:
        try:
            logging.info(f"Начало сбора данных для символа: {symbol} ({timeframe})")
            end_time = exchange.milliseconds()  # Текущее время в мс
            start_time = end_time - PERIOD_MS     # Время начала интервала (30 дней назад)
            
            while start_time < end_time:
                # Вычисляем верхнюю границу выборки (500 свечей)
                fetch_until = start_time + LIMIT * timeframe_seconds * 1000
                logging.info(f"Сбор данных для {symbol}: с {start_time} по {fetch_until} (в мс)")
                
                candles = exchange.fetch_ohlcv(
                    symbol,
                    timeframe=timeframe,
                    since=start_time,
                    limit=LIMIT
                )
                
                if not candles:
                    logging.warning(f"Нет данных для {symbol} на интервале {start_time}. Пропускаем...")
                    break
                
                # Создаем DataFrame; ожидается, что столбцы: [time, open, high, low, close, volume]
                df = pd.DataFrame(candles, columns=["time", "open", "high", "low", "close", "volume"])
                # Преобразуем время в datetime с привязкой к UTC+0
                df["time"] = pd.to_datetime(df["time"], unit="ms", utc=True)
                
                # Добавляем колонку с символом (убираем символ "/" если есть)
                df.insert(0, "symbol", symbol.replace("/", ""))
                
                # Определяем типы колонок для БД
                dtype_mapping = {
                    "symbol": String(50),
                    "time": TIMESTAMP,  # TIMESTAMP WITH TIME ZONE (при передаче timezone-aware объектов)
                    "open": Float,
                    "high": Float,
                    "low": Float,
                    "close": Float,
                    "volume": Float,
                }
                
                # Сохраняем данные в PostgreSQL (append)
                df.to_sql(
                    name=table_name,
                    con=engine,
                    if_exists='append',
                    index=False,
                    dtype=dtype_mapping
                )
                logging.info(f"Данные для {symbol} успешно загружены в '{table_name}'.")
                
                # Переходим к следующему блоку: сдвигаем start_time на количество свечей, равное LIMIT
                start_time += LIMIT * timeframe_seconds * 1000
        except Exception as e:
            logging.error(f"Ошибка при обработке {symbol} ({timeframe}): {e}")

def main():
    logging.info("Скрипт запущен для сбора свечей за последние 30 дней.")
    
    # Собираем часовые свечи: timeframe "1h" (3600 секунд) в таблицу crypto_data_1h  
    fetch_and_save_candles(timeframe="1h", table_name="crypto_data_1h", timeframe_seconds=3600)
    
    logging.info("Скрипт завершил сбор данных.")

if __name__ == '__main__':
    try:
        main()
    except (KeyboardInterrupt, SystemExit):
        logging.info("Скрипт остановлен пользователем.")
