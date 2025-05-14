# Binance Futures Data Collector

## Project Overview
This project contains a set of Python scripts to download, aggregate and backfill candle (OHLCV) data from Binance Futures into PostgreSQL. All scripts live together in one folder and share a common `.env` configuration.

## Scripts

- **`collect_coins_1h.py`**  
  Connects to Binance Futures WebSocket streams of aggregated trades, builds hourly candles in memory, and writes them into `crypto_data_1h`.

- **`collect_coins_1m.py`**  
  Listens to 1-minute Kline streams, waits for each minute-bar to close, and inserts finished candles into `crypto_data_1m`.

- **`collect_coins_1s.py`**  
  Streams aggTrades per symbol, builds 1-second bars, fills any missing seconds by fetching missing aggTrades or inserting empty bars, and writes into `crypto_data_1s`.

- **`fill_data_gap_h.py`**  
  Uses CCXT REST API to fetch the last 30 days of hourly candles for each symbol and bulk-inserts them into `crypto_data_1h` (for historical backfill).

- **`fill_data_gap_s.py`**  
  Scans existing 1-second timestamps in `crypto_data_1s`, finds gaps, fetches missing bars via CCXT and fills in missing seconds up to now.

- **`futures_symbols.py`**  
  Queries Binance REST API for active futures symbols, saves them to `futures_symbols.csv` and to the `futures_symbols` table.

## Requirements

- Python 3.8 or higher  
- PostgreSQL database  
- Create a `requirements.txt` with at least:  
  ```text
  asyncio
  websockets
  aiohttp
  sqlalchemy
  asyncpg
  psycopg2
  python-dotenv
  binance
  ccxt
  pandas

