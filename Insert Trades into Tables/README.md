# Insert Trades into Tables

## Overview
The `insert_from_tmm.py` script connects to your PostgreSQL database, reads recent trades from `money.tmm_small`, and inserts them into two target tables:

- **`public.de_1_39_2_2`** — for records where `model_name` starts with `1233`  
- **`public.de_1_39_3_2`** — for records where `model_name` starts with `1232`  

Only trades from the last 24 hours are processed, and duplicates (same symbol + minute) are skipped.

## Requirements
- Python 3.8+  
- `psycopg2-binary`  
- PostgreSQL with tables:
  - `money.tmm_small`
  - `public.de_1_39_2_2`
  - `public.de_1_39_3_2`

Install Python dependency:
```bash
pip install psycopg2-binary
