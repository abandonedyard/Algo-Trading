# Telegram Trigger Screener

## Overview
This script (`de4danil.py`) scans your PostgreSQL `crypto_data_1m` table for symbols whose 1-minute close price jumped over 4% compared to the previous minute. It saves new triggers to two tables (`alexey_nikolaev.de4` and `alexey_nikolaev.all4`), builds a summary report, and sends it (with charts) to a Telegram channel.

## Requirements
- Python 3.8+  
- PostgreSQL server with tables:
  - `crypto_data_1m`
  - `alexey_nikolaev.de4` (unique on symbol+time)
  - `alexey_nikolaev.all4` (unique on symbol+time)
- Telegram Bot token & chat ID  
- Environment variables in `.env`  

## Installation
1. Clone repo / put `de4danil.py` in your project folder.  
2. Install dependencies:
   ```bash
   pip install psycopg2-binary python-dotenv requests matplotlib
