#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Файл: features_green.py
Назначение: посчитать ключевые показатели для ML-модели,
предсказывающей конец зелёной зоны, и сохранить их в CSV
"""

import os
import sys
from datetime import datetime, timedelta

import pandas as pd
import psycopg2

# ПАРАМЕТРЫ БД
PG_HOST = "212.67.12.174"
PG_PORT = 5432
PG_DBNAME = "o2"
PG_USER = "postgres"
PG_PASSWORD = "9mX!dA@45NzP#qLt"

# КАТАЛОГ ДЛЯ CSV
OUTPUT_DIR = "/Users/danil/Desktop/DataScience/O2/Разметка/DE/"


def get_symbol_data(conn, symbol, start_time, end_time):
    """Возвращает списки times и prices из таблицы crypto_data_1s"""
    query = """
        SELECT time, close
        FROM crypto_data_1s
        WHERE symbol = %s
          AND time >= %s
          AND time <= %s
        ORDER BY time
    """
    with conn.cursor() as cur:
        cur.execute(query, (symbol, start_time, end_time))
        rows = cur.fetchall()
    if not rows:
        return [], []
    times, prices = zip(*rows)
    times = [pd.to_datetime(t).tz_localize(None) if getattr(t, 'tzinfo', None) else t for t in times]
    prices = [float(p) for p in prices]
    return list(times), list(prices)


def zone_metrics(df):
    """Базовые метрики зоны: длительность, % изменение, скорость"""
    if df.empty:
        return {'duration_sec': 0, 'pct_diff': 0.0, 'diff_per_sec': 0.0}
    start_price = df['close'].iloc[0]
    end_price = df['close'].iloc[-1]
    pct_diff = (end_price - start_price) / start_price * 100 if start_price else 0.0
    duration_sec = (df['time'].iloc[-1] - df['time'].iloc[0]).total_seconds()
    diff_per_sec = pct_diff / duration_sec if duration_sec else 0.0
    return {'duration_sec': duration_sec, 'pct_diff': pct_diff, 'diff_per_sec': diff_per_sec}


def zone_volatility(df):
    """Волатильность: std(close)/mean(close)*100%"""
    mean_price = df['close'].mean()
    return df['close'].std() / mean_price * 100 if (not df.empty and mean_price) else 0.0


def pre_zone_extra(df, baseline_price):
    """Признаки пред-зоны: диапазон и максимальная свеча"""
    if df.empty or baseline_price == 0:
        return {'range_pct': 0.0, 'max_candle': 0.0}
    price_range = df['close'].max() - df['close'].min()
    range_pct = price_range / baseline_price * 100
    max_candle = df['close'].diff().abs().max() / baseline_price * 100
    return {'range_pct': range_pct, 'max_candle': max_candle}


def detect_green_zone(df, entry_time):
    """Находит индексы начала и конца зелёной зоны"""
    if hasattr(entry_time, 'tzinfo') and entry_time.tzinfo:
        entry_time = entry_time.replace(tzinfo=None)
    df_idx = df.set_index('time')
    if df_idx.empty:
        return None, None
    window = max(1, len(df_idx) // 40)
    df_idx['pct_change'] = df_idx['close'].pct_change(periods=window).fillna(0) * 100
    search_from = entry_time - timedelta(minutes=2)
    signals = df_idx[(df_idx['pct_change'] > 2.0) & (df_idx.index >= search_from)].index
    if signals.empty:
        return None, None
    sig_ts = signals[0]
    lookback_start = max(df_idx.index[0], sig_ts - timedelta(seconds=15))
    start_ts = df_idx.loc[lookback_start:sig_ts]['close'].idxmin()
    end_window = df_idx.loc[start_ts:start_ts + timedelta(minutes=5)]
    end_ts = end_window['close'].idxmax()
    start_i = int(df[df['time'] == start_ts].index[0])
    end_i = int(df[df['time'] == end_ts].index[0])
    return start_i, end_i


def compute_trade_features(conn, symbol, entry_time, close_time):
    """Считает признаки для одной сделки, включая длительность зелёной зоны"""
    start = entry_time - timedelta(minutes=15)
    end = entry_time + timedelta(minutes=10)
    times, prices = get_symbol_data(conn, symbol, start, end)
    if not times:
        return None
    df = pd.DataFrame({'time': times, 'close': prices}).sort_values('time').reset_index(drop=True)
    baseline = df['close'].iloc[0]
    si, ei = detect_green_zone(df, entry_time)
    if si is None:
        return None
    pre_df = df.iloc[:si]
    green_df = df.iloc[si:ei+1]
    green_m = zone_metrics(green_df)
    pre_m = zone_metrics(pre_df)
    pre_v = zone_volatility(pre_df)
    pre_ex = pre_zone_extra(pre_df, baseline)
    speed_ratio = (green_m['diff_per_sec'] / (pre_m['diff_per_sec'] or 1) - 1) * 100
    vol_ratio = (zone_volatility(green_df) / (pre_v or 1) - 1) * 100
    return {
        'symbol': symbol,
        'entry_time': entry_time,
        'green_duration_sec': int(green_m['duration_sec']),
        'green_pct_diff': green_m['pct_diff'],
        'green_speed_pct_sec': green_m['diff_per_sec'],
        'speed_ratio_green_vs_pre': speed_ratio,
        'volatility_ratio_green_vs_pre': vol_ratio,
        'pre_volatility': pre_v,
        'pre_range_pct': pre_ex['range_pct'],
        'pre_max_candle_pct': pre_ex['max_candle']
    }


def collect_trades(conn, days_back=10):
    """Триггеры ≥4% за минуту за последние days_back дней"""
    query = """
        SELECT symbol, time AS entry_time,
               time + INTERVAL '1 minute' AS close_time
        FROM crypto_data_1m
        WHERE time >= NOW() - INTERVAL %s
          AND (close - open)/open*100 >= 4
    """
    with conn.cursor() as cur:
        cur.execute(query, (f"{days_back} day",))
        return cur.fetchall()


def main(days_back=10):
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    try:
        conn = psycopg2.connect(
            host=PG_HOST, port=PG_PORT, dbname=PG_DBNAME,
            user=PG_USER, password=PG_PASSWORD, connect_timeout=30
        )
    except Exception as e:
        print("Ошибка подключения к БД:", e)
        sys.exit(1)
    trades = collect_trades(conn, days_back)
    print(f"Найдено триггеров: {len(trades)}")
    rows = []
    for idx, (sym, ent, clo) in enumerate(trades, 1):
        print(f"Обрабатываю {idx}/{len(trades)}: {sym} @ {ent}")
        feats = compute_trade_features(conn, sym, ent, clo)
        if feats:
            rows.append(feats)
    if not rows:
        print("Ни для одной сделки не получилось рассчитать признаки")
        return
    df_out = pd.DataFrame(rows)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    path = os.path.join(OUTPUT_DIR, f"green_zone_sel_features_{ts}.csv")
    df_out.to_csv(path, index=False)
    print(f"Сохранено {len(df_out)} строк в {path}")


if __name__ == '__main__':
    days = int(sys.argv[1]) if len(sys.argv) > 1 else 10
    print(f"Запуск за последние {days} дн.")
    main(days_back=days)
