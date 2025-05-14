# Potential of Coins Analyzer

## Overview
`potential_of_coins.py` connects to your PostgreSQL `crypto_data_1h` table, calculates each symbol’s long and short potential over the last 10 days, builds charts and summary, then sends results to a Telegram channel.

## Requirements
- Python 3.8+  
- PostgreSQL with table `crypto_data_1h` containing columns:  
  `time, open, high, low, close, volume, symbol`  
- Telegram Bot token & chat ID  
- Environment variables via `python-dotenv`

## Installation
1. Place `potential_of_coins.py` in your project folder.  
2. Install dependencies:
   ```bash
   pip install python-dotenv telebot pandas numpy matplotlib psycopg2-binary
