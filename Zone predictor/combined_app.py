import sys
import os
import psycopg2
import pandas as pd
import numpy as np
from datetime import datetime, timedelta

from bokeh.plotting import figure, curdoc
from bokeh.models import (ColumnDataSource, HoverTool,
                          BoxAnnotation, Range1d, Div)
from bokeh.layouts import column, row
import joblib
from datetime import timedelta

# Автоматический рекурсивный поиск .pkl-файлов модели и скейлера по подстрокам
import glob
root_dir = os.path.dirname(os.path.abspath(__file__))
pkl_files = glob.glob(os.path.join(root_dir, '**', '*.pkl'), recursive=True)
model_files = [f for f in pkl_files if 'model' in os.path.basename(f).lower()]
scaler_files = [f for f in pkl_files if 'scaler' in os.path.basename(f).lower()]
if not model_files:
    raise FileNotFoundError("Model .pkl not found (filename must contain 'model')")
if not scaler_files:
    raise FileNotFoundError("Scaler .pkl not found (filename must contain 'scaler')")
MODEL_PATH = model_files[0]
SCALER_PATH = scaler_files[0]

# Load model and scaler
model = joblib.load(MODEL_PATH)
scaler = joblib.load(SCALER_PATH)

from bokeh.driving import count

####################################
#   ПАРАМЕТРЫ ПОДКЛЮЧЕНИЯ К БД
####################################
PG_HOST = "212.67.12.174"
PG_PORT = 5432
PG_DBNAME = "o2"
PG_USER = "postgres"
PG_PASSWORD = "9mX!dA@45NzP#qLt"

####################################
#   СОЕДИНЕНИЕ С БД (соединение не закрываем до завершения работы)
####################################
conn = psycopg2.connect(
    host=PG_HOST,
    port=PG_PORT,
    dbname=PG_DBNAME,
    user=PG_USER,
    password=PG_PASSWORD,
    connect_timeout=30
)
cur = conn.cursor()

####################################
# ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ
####################################
def get_symbol_data(symbol, start_time, end_time):
    query = """
        SELECT time, close
        FROM crypto_data_1s
        WHERE symbol = %s
          AND time >= %s
          AND time <= %s
        ORDER BY time
    """
    try:
        cur.execute(query, (symbol, start_time, end_time))
    except psycopg2.OperationalError as e:
        print(f"Error executing query for symbol {symbol}: {e}")
        return [], []
    rows = cur.fetchall()
    times, prices = [], []
    for r in rows:
        naive_time = pd.to_datetime(r[0]).tz_localize(None) if r[0].tzinfo else r[0]
        times.append(naive_time)
        prices.append(float(r[1]))
    return times, prices

def nearest_index(df, target_time):
    if target_time.tzinfo is not None:
        target_time = target_time.replace(tzinfo=None)
    return min(range(len(df)), key=lambda i: abs(df.loc[i, 'time'] - target_time))

def format_minutes(minutes_float):
    m = int(minutes_float)
    s = int(round((minutes_float - m) * 60))
    return f"{m}.{s:02d}"

def format_duration_min_sec(duration_min):
    minutes = int(duration_min)
    seconds = int(round((duration_min - minutes) * 60))
    return f"{minutes} мин {seconds} сек"

####################################
# ФУНКЦИИ РАСЧЁТА ЗОН
####################################
def get_zone_data(df, start_pos, end_pos):
    if start_pos > end_pos:
        return pd.DataFrame(columns=df.columns)
    return df.loc[range(start_pos, end_pos + 1)]

def calc_zone_metrics_in_percents(zone_df):
    if zone_df.empty:
        return {'start_price': None, 'end_price': None, 'duration_min': 0,
                'pct_diff': 0, 'diff_per_sec': 0}
    start_price = zone_df['close'].iloc[0]
    end_price   = zone_df['close'].iloc[-1]
    start_time  = zone_df['time'].iloc[0]
    end_time    = zone_df['time'].iloc[-1]
    duration_sec = (end_time - start_time).total_seconds()
    duration_min = duration_sec / 60.0
    pct_diff = 100.0 * (end_price - start_price) / start_price if start_price != 0 else 0.0
    diff_per_sec = pct_diff / duration_sec if duration_sec != 0 else 0
    return {'start_price': start_price, 'end_price': end_price,
            'duration_min': duration_min, 'pct_diff': pct_diff,
            'diff_per_sec': diff_per_sec}

def calc_pre_zone_metrics(zone_df, baseline_price):
    if zone_df.empty:
        return {'duration_min': 0, 'mean_volatility': 0, 'max_candle': 0,
                'range_percent': 0, 'time_range_min': 0, 'pct_diff': 0,
                'diff_per_sec': 0}
    base = calc_zone_metrics_in_percents(zone_df)
    start_price_zone = zone_df['close'].iloc[0]
    mean_volatility = (zone_df['close'].std() / start_price_zone * 100) if start_price_zone != 0 else 0
    max_candle = zone_df['close'].diff().abs().max()
    max_candle_percent = (max_candle / baseline_price) * 100 if baseline_price != 0 else 0
    range_val = zone_df['close'].max() - zone_df['close'].min()
    range_percent = (range_val / baseline_price) * 100 if baseline_price != 0 else 0
    min_time = zone_df.loc[zone_df['close'].idxmin(), 'time']
    max_time = zone_df.loc[zone_df['close'].idxmax(), 'time']
    time_range_min = abs((max_time - min_time).total_seconds()) / 60.0
    return {
        'duration_min': base['duration_min'],
        'mean_volatility': mean_volatility,
        'max_candle': max_candle_percent,
        'range_percent': range_percent,
        'time_range_min': time_range_min,
        'pct_diff': base['pct_diff'],
        'diff_per_sec': base['diff_per_sec']
    }

def calc_green_zone_metrics(zone_df, entry_price):
    if zone_df.empty:
        return {'duration_min': 0, 'pct_diff': 0, 'diff_per_sec': 0}
    base = calc_zone_metrics_in_percents(zone_df)
    green_pct_diff = base['pct_diff'] if base['pct_diff'] > 0 else 0
    return {
        'duration_min': base['duration_min'],
        'pct_diff': green_pct_diff,
        'diff_per_sec': base['diff_per_sec']
    }

def calc_red_zone_metrics(zone_df):
    base = calc_zone_metrics_in_percents(zone_df)
    return {
        'duration_min': base['duration_min'],
        'pct_diff': base['pct_diff'],
        'diff_per_sec': base['diff_per_sec']
    }

def calc_yellow_zone_metrics(zone_df, purple_zone_df):
    base = calc_zone_metrics_in_percents(zone_df)
    if purple_zone_df.empty:
        direction = False
    else:
        p_start = purple_zone_df['close'].iloc[0]
        p_end = purple_zone_df['close'].iloc[-1]
        direction = (p_end > p_start)
    return {
        'duration_min': base['duration_min'],
        'pct_diff': base['pct_diff'],
        'diff_per_sec': base['diff_per_sec'],
        'went_up_after': direction
    }

def calc_purple_zone_metrics(zone_df):
    if zone_df.empty:
        return {'duration_min': 0, 'pct_diff': 0, 'went_up': False}
    base = calc_zone_metrics_in_percents(zone_df)
    went_up = (base['end_price'] > base['start_price'])
    return {
        'duration_min': base['duration_min'],
        'pct_diff': base['pct_diff'],
        'went_up': went_up
    }

def calc_post_purple_zone_metrics(zone_df):
    if zone_df.empty:
        return {'duration_min': 0, 'pct_diff': 0, 'diff_per_sec': 0}
    base = calc_zone_metrics_in_percents(zone_df)
    return {
        'duration_min': base['duration_min'],
        'pct_diff': base['pct_diff'],
        'diff_per_sec': base['diff_per_sec']
    }

def calc_zone_volatility(zone_df):
    if zone_df.empty or zone_df['close'].mean() == 0:
        return 0
    return zone_df['close'].std() / zone_df['close'].mean() * 100

####################################
# Функция вычисления границы фиолетовой зоны
####################################
def compute_purple_zone_boundary(full_df, entry_time, pump_shift, dump_shift):
    df_idx = full_df.set_index('time').copy()
    window_size = max(1, len(df_idx) // 40)
    df_idx['pct_change'] = df_idx['close'].pct_change(periods=window_size) * 100
    df_idx['sma'] = df_idx['close'].rolling(window=window_size, min_periods=1).mean()
    df_idx['volatility'] = df_idx['close'].rolling(window=window_size, min_periods=1).std()
    df_idx['roc'] = df_idx['close'].diff(periods=window_size)
    df_idx.bfill(inplace=True)
    df_idx.ffill(inplace=True)

    threshold_pct = 2.0
    min_green_search = entry_time - pd.Timedelta(minutes=2)

    pump_signals = df_idx[df_idx['pct_change'] > threshold_pct].index
    pump_signals = pump_signals[pump_signals >= min_green_search]
    if pump_signals.size > 0:
        pump_signal_idx = pump_signals[0]
        lookback_seconds = 15
        lookback_start = pump_signal_idx - pd.Timedelta(seconds=lookback_seconds)
        if lookback_start < df_idx.index[0]:
            lookback_start = df_idx.index[0]
        df_lookback = df_idx.loc[lookback_start:pump_signal_idx]
        pump_start_idx = df_lookback['close'].idxmin()
    else:
        pump_start_idx = min_green_search

    pump_start_idx += pd.Timedelta(seconds=pump_shift)

    three_min_window_end = pump_start_idx + pd.Timedelta(minutes=5)
    green_zone_data = df_idx.loc[(df_idx.index >= pump_start_idx) & (df_idx.index <= three_min_window_end)]
    peak_time = green_zone_data['close'].idxmax() if not green_zone_data.empty else pump_start_idx

    five_min_later = peak_time + pd.Timedelta(minutes=5)
    post_peak_5min = df_idx.loc[(df_idx.index >= peak_time) & (df_idx.index <= five_min_later)]
    if not post_peak_5min.empty:
        tolerance_pct = 0.1
        min_value = post_peak_5min['close'].min()
        tol = (tolerance_pct / 100) * min_value
        similar_levels = post_peak_5min[abs(post_peak_5min['close'] - min_value) <= tol]
        dump_end_idx = similar_levels.index[0]
    else:
        dump_end_idx = peak_time

    dump_end_idx += pd.Timedelta(seconds=dump_shift)

    purple_window = full_df[(full_df['time'] >= dump_end_idx) & 
                            (full_df['time'] <= dump_end_idx + pd.Timedelta(minutes=3))]
    if not purple_window.empty:
        if purple_window['close'].iloc[-1] > purple_window['close'].iloc[0]:
            chosen_time = purple_window.loc[purple_window['close'].idxmax(), 'time']
        else:
            chosen_time = purple_window.loc[purple_window['close'].idxmin(), 'time']
    else:
        chosen_time = dump_end_idx + pd.Timedelta(minutes=3)

    if chosen_time in full_df['time'].values:
        purple_boundary = full_df[full_df['time'] == chosen_time].index[0]
    else:
        purple_boundary = full_df['time'].sub(chosen_time).abs().idxmin()

    return purple_boundary

####################################
# Функция создания графика для сделки
####################################
def create_trade_plot(trade, on_finished=None):
    red_zone_started_time = None
    fibo = None
    placed_green_cross = False
    placed_red_cross = False
    green_end_change_count = 0

    max_gap_red_end_sec = 0
    prev_calculated_red_end_dt = None
    max_gap_green_sec = 0
    prev_calculated_green_end_dt = None

    green_dot_price = None
    red_dot_price = None

    symbol, entry_time, close_time, entry_price = trade
    print(f"Подгружаем данные по сделке: {symbol}, вход={entry_time}, выход={close_time}")
    if entry_time.tzinfo is not None:
        entry_time = entry_time.replace(tzinfo=None)
    if close_time.tzinfo is not None:
        close_time = close_time.replace(tzinfo=None)
    
    start_time = entry_time - timedelta(minutes=15)
    end_time = close_time + timedelta(minutes=30)
    times, prices = get_symbol_data(symbol, start_time, end_time)
    if not times:
        print(f"Нет котировок в заданный период для сделки: {trade}. Пропускаем сделку.")
        if on_finished is not None:
            on_finished()
        return column([])
    full_df = pd.DataFrame({'time': times, 'close': prices}).reset_index(drop=True)
    full_df = full_df.sort_values(by='time').reset_index(drop=True)
    baseline_price = full_df.iloc[0]['close']

    full_df['percent'] = ((full_df['close'] - baseline_price) / baseline_price) * 100

    entry_idx = nearest_index(full_df, entry_time)
    exit_idx = nearest_index(full_df, close_time)
    entry_point = full_df.loc[entry_idx, 'time']
    exit_point = full_df.loc[exit_idx, 'time']

    initial_df = full_df[full_df['time'] <= entry_time]

    source = ColumnDataSource(data={
        'x': initial_df['time'],
        'y': initial_df['percent'],
        'z': initial_df['close'],
    })

    incsource = ColumnDataSource(data=dict(x=[], top=[], bottom=[], open=[], close=[]))
    decsource = ColumnDataSource(data=dict(x=[], top=[], bottom=[], open=[], close=[]))

    p = figure(x_axis_type='datetime', 
               title=f"{symbol} – Рост цены (%)", 
               width=1650, height=900, 
               tools="pan,wheel_zoom,box_zoom,reset,save")
    hover = HoverTool(tooltips=[
            ("Время", "@x{%F %T}"), 
            ("Разница", "@y{0.2f}%"), 
            ("Цена", "@z{0.8f}")
        ], formatters={'@x': 'datetime'})
    p.add_tools(hover)
    # Predict green zone end time and add annotation
    pre_metrics = calc_pre_zone_metrics(full_df[full_df['time'] <= entry_time], baseline_price)
    features = [
        pre_metrics['mean_volatility'],
        pre_metrics['range_percent'],
        pre_metrics['max_candle'],
        0,  # green_pct_diff (unknown)
        0,  # green_speed_pct_sec
        0,  # speed_ratio_green_vs_pre
        0   # volatility_ratio_green_vs_pre
    ]
    X_scaled = scaler.transform([features])
    duration_sec = model.predict(X_scaled)[0]
    predicted_time = entry_time + timedelta(seconds=float(duration_sec))
    pred_box = BoxAnnotation(
        left=predicted_time,
        right=predicted_time + timedelta(seconds=1),
        fill_alpha=0.2,
        fill_color='green',
        line_color='green',
        line_width=3
    )
    p.add_layout(pred_box)

    p.segment('x', 'top', 'x', 'bottom', source=incsource, color="green")
    p.vbar('x', 500, 'open', 'close', source=incsource,
           fill_color="green", line_color="green")
    p.segment('x', 'top', 'x', 'bottom', source=decsource, color="red")
    p.vbar('x', 500, 'open', 'close', source=decsource,
           fill_color="red", line_color="red")

    y_min, y_max = initial_df['percent'].min(), initial_df['percent'].max()
    y_range = y_max - y_min if (y_max - y_min) != 0 else 1
    p.y_range = Range1d(y_min - y_range * 0.05, y_max + y_range * 0.15)

    pump_box = BoxAnnotation(fill_alpha=0.1, fill_color='green', line_color='green',
                             line_dash='dashed', line_width=1)
    dump_box = BoxAnnotation(fill_alpha=0.1, fill_color='red', line_color='red',
                             line_dash='dashed', line_width=1)
    stab_box = BoxAnnotation(fill_alpha=0.1, fill_color='yellow', line_color='yellow',
                             line_dash='dashed', line_width=1)
    new_box = BoxAnnotation(fill_alpha=0.1, fill_color='purple', line_color='purple',
                            line_dash='dashed', line_width=1)
    p.add_layout(pump_box)
    p.add_layout(dump_box)
    p.add_layout(stab_box)
    p.add_layout(new_box)
    p.legend.location = "top_left"

    purple_zone_boundary = compute_purple_zone_boundary(full_df, entry_time, 0, 0)

    info_div = Div(text="", width=600, height=900,
                   styles={'overflow-y': 'scroll', 'font-size': '120%'})

    if not initial_df.empty:
        inc_x, inc_top, inc_bottom, inc_open, inc_close = [], [], [], [], []
        dec_x, dec_top, dec_bottom, dec_open, dec_close = [], [], [], [], []
        for i in range(1, len(initial_df)):
            cur_time = initial_df['time'].iloc[i]
            prev_y = initial_df['percent'].iloc[i - 1]
            cur_y = initial_df['percent'].iloc[i]
            o_ = prev_y
            c_ = cur_y
            hi_ = max(o_, c_)
            lo_ = min(o_, c_)
            if c_ >= o_:
                inc_x.append(cur_time)
                inc_open.append(o_)
                inc_close.append(c_)
                inc_top.append(hi_)
                inc_bottom.append(lo_)
            else:
                dec_x.append(cur_time)
                dec_open.append(o_)
                dec_close.append(c_)
                dec_top.append(hi_)
                dec_bottom.append(lo_)
        if inc_x:
            inc_patch = dict(x=inc_x, open=inc_open, close=inc_close,
                             top=inc_top, bottom=inc_bottom)
            incsource.stream(inc_patch)
        if dec_x:
            dec_patch = dict(x=dec_x, open=dec_open, close=dec_close,
                             top=dec_top, bottom=dec_bottom)
            decsource.stream(dec_patch)
        last_y_for_candle = initial_df['percent'].iloc[-1]
    else:
        last_y_for_candle = 0

    def update_zones_and_info():
        nonlocal placed_green_cross, placed_red_cross
        nonlocal max_gap_red_end_sec, prev_calculated_red_end_dt
        nonlocal max_gap_green_sec, prev_calculated_green_end_dt, green_end_change_count
        nonlocal green_dot_price
        nonlocal red_dot_price
        nonlocal pred_box

        cur_data = pd.DataFrame({
            'time': source.data['x'],
            'percent': source.data['y'],
            'close': source.data['z'],
        })
        if cur_data.empty:
            return
        df_idx = cur_data.set_index('time').copy()
        window_size = max(1, len(df_idx) // 40)
        df_idx['pct_change'] = df_idx['close'].pct_change(periods=window_size) * 100
        df_idx['sma'] = df_idx['close'].rolling(window=window_size, min_periods=1).mean()
        df_idx['volatility'] = df_idx['close'].rolling(window=window_size, min_periods=1).std()
        df_idx['roc'] = df_idx['close'].diff(periods=window_size)
        df_idx.bfill(inplace=True)
        df_idx.ffill(inplace=True)
        
        def time_to_pos(t, df_):
            return min(range(len(df_)), key=lambda i: abs(df_.loc[i, 'time'] - t))
        
        pump_shift_secs = 0
        pump_signals = df_idx[df_idx['pct_change'] > 2.0].index
        min_green_search = entry_time - pd.Timedelta(minutes=2)
        pump_signals = pump_signals[pump_signals >= min_green_search]
        if pump_signals.size > 0:
            pump_signal_idx = pump_signals[0]
            lookback_seconds = 15
            lookback_start = pump_signal_idx - pd.Timedelta(seconds=lookback_seconds)
            if lookback_start < df_idx.index[0]:
                lookback_start = df_idx.index[0]
            df_lookback = df_idx.loc[lookback_start:pump_signal_idx]
            pump_start_idx = df_lookback['close'].idxmin()
        else:
            pump_start_idx = min_green_search
        pump_start_idx += pd.Timedelta(seconds=pump_shift_secs)

        three_min_window_end = pump_start_idx + pd.Timedelta(minutes=5)
        green_zone_data = df_idx.loc[(df_idx.index >= pump_start_idx) & (df_idx.index <= three_min_window_end)]
        peak_time = green_zone_data['close'].idxmax() if not green_zone_data.empty else pump_start_idx

        df_reset = df_idx.reset_index()
        new_green_end_time = df_reset.loc[time_to_pos(peak_time, df_reset), 'time']
        if prev_calculated_green_end_dt is not None and new_green_end_time != prev_calculated_green_end_dt:
            green_end_change_count += 1
            gap_green = abs((new_green_end_time - prev_calculated_green_end_dt).total_seconds())
            if gap_green > max_gap_green_sec:
                max_gap_green_sec = gap_green
        prev_calculated_green_end_dt = new_green_end_time

        five_min_later = peak_time + pd.Timedelta(minutes=5)
        post_peak_5min = df_idx.loc[(df_idx.index >= peak_time) & (df_idx.index <= five_min_later)]
        if not post_peak_5min.empty:
            tolerance_pct = 0.1
            min_value = post_peak_5min['close'].min()
            tol = (tolerance_pct / 100) * min_value
            similar_levels = post_peak_5min[abs(post_peak_5min['close'] - min_value) <= tol]
            dump_end_idx = similar_levels.index[0]
        else:
            dump_end_idx = peak_time
        dump_shift_secs = 0
        dump_end_idx += pd.Timedelta(seconds=dump_shift_secs)

        purple_window = df_idx.loc[dump_end_idx : dump_end_idx + pd.Timedelta(minutes=3)]
        if not purple_window.empty:
            if purple_window['close'].iloc[-1] > purple_window['close'].iloc[0]:
                purple_end_idx = purple_window['close'].idxmax()
            else:
                purple_end_idx = purple_window['close'].idxmin()
        else:
            purple_end_idx = dump_end_idx + pd.Timedelta(minutes=3)

        def clamp_time(t):
            if t < df_idx.index[0]:
                return df_idx.index[0]
            if t > df_idx.index[-1]:
                return df_idx.index[-1]
            return t

        pump_start_idx = clamp_time(pump_start_idx)
        peak_time = clamp_time(peak_time)
        dump_end_idx = clamp_time(dump_end_idx)
        purple_end_idx = clamp_time(purple_end_idx)

        cur_data_reset = df_idx.reset_index()
        pump_start_pos = time_to_pos(pump_start_idx, cur_data_reset)
        peak_pos = time_to_pos(peak_time, cur_data_reset)
        dump_end_pos = time_to_pos(dump_end_idx, cur_data_reset)
        purple_zone_end_pos = time_to_pos(purple_end_idx, cur_data_reset)
        new_zone_end_pos = time_to_pos(df_idx.index[-1], cur_data_reset)

        pre_start_pos = 0
        pre_end_pos = pump_start_pos - 1 if pump_start_pos > 0 else 0

        green_start_pos = pump_start_idx
        green_end_pos = peak_pos if peak_pos < len(cur_data_reset) else len(cur_data_reset)-1

        red_start_pos = peak_pos
        red_end_pos = dump_end_pos if dump_end_pos < len(cur_data_reset) else len(cur_data_reset)-1

        yellow_start_pos = time_to_pos(dump_end_idx, cur_data_reset)
        yellow_zone_data = df_idx.loc[dump_end_idx:]
        if not yellow_zone_data.empty:
            min_price = yellow_zone_data['close'].min()
            threshold_pct = 1.0
            yellow_end_idx_dynamic = None
            for idx in yellow_zone_data.index:
                if yellow_zone_data.loc[idx, 'close'] > min_price * (1 + threshold_pct / 100):
                    yellow_end_idx_dynamic = idx
                    break
            if yellow_end_idx_dynamic is None:
                yellow_end_idx_dynamic = yellow_zone_data.index[-1]
        else:
            yellow_end_idx_dynamic = dump_end_idx

        if (yellow_end_idx_dynamic - dump_end_idx).total_seconds() < 10:
            yellow_end_idx_dynamic = dump_end_idx + pd.Timedelta(seconds=10)
        yellow_end_pos = time_to_pos(yellow_end_idx_dynamic, cur_data_reset)

        purple_start_pos = yellow_end_pos
        purple_end_pos = purple_zone_end_pos if purple_zone_end_pos < len(cur_data_reset) else len(cur_data_reset)-1

        post_purple_start_pos = purple_end_pos
        post_purple_end_pos = new_zone_end_pos if new_zone_end_pos < len(cur_data_reset) else len(cur_data_reset)-1

        # --- ниже был расчёт и вывод текстового блока, удалён по требованию ---

        def safe_time(t):
            if isinstance(t, (int, np.integer)):
                return cur_data_reset.loc[t, 'time'] if 0 <= t < len(cur_data_reset) else None
            else:
                return t

        pump_box.left  = safe_time(pump_start_pos)
        pump_box.right = safe_time(green_end_pos)
        dump_box.left  = safe_time(red_start_pos)
        dump_box.right = safe_time(red_end_pos)
        stab_box.left  = safe_time(yellow_start_pos)
        stab_box.right = safe_time(yellow_end_pos)
        new_box.left   = safe_time(purple_start_pos)
        new_box.right  = safe_time(purple_end_idx)
        
        red_end_time = safe_time(red_end_pos)
        now_time = cur_data_reset.loc[len(cur_data_reset)-1, 'time']
        green_end_time = safe_time(green_end_pos)
        
        if not placed_green_cross and now_time >= green_end_time + timedelta(seconds=6) and now_time >= close_time:
            current_price = df_idx['close'].iloc[-1]
            current_pct   = df_idx['percent'].iloc[-1]
            placed_green_cross = True
            green_dot_price   = current_price

        if placed_green_cross and not placed_red_cross:
            red_end_time = safe_time(red_end_pos)
            if red_end_time is not None and now_time >= red_end_time + timedelta(seconds=30):
                current_price = df_idx['close'].iloc[-1]
                if current_price < green_dot_price:
                    current_pct = df_idx['percent'].iloc[-1]
                    placed_red_cross = True
                    red_dot_price = current_price

        # --- пересчёт прогноза конца зелёной зоны ---
        now_df = cur_data  # текущие данные, отображённые на графике
        pre_df = now_df[now_df['time'] < entry_time]
        green_df = now_df[now_df['time'] >= entry_time]

        pre_m = calc_pre_zone_metrics(pre_df, baseline_price)
        green_m = calc_green_zone_metrics(green_df, entry_price)

        speed_ratio = (green_m['diff_per_sec'] / pre_m['diff_per_sec']) if pre_m['diff_per_sec'] else 0
        volatility_ratio = (calc_zone_volatility(green_df) / pre_m['mean_volatility']) if pre_m['mean_volatility'] else 0

        features = [
            pre_m['mean_volatility'],
            pre_m['range_percent'],
            pre_m['max_candle'],
            green_m['pct_diff'],
            green_m['diff_per_sec'],
            speed_ratio,
            volatility_ratio
        ]
        X_scaled = scaler.transform([features])
        duration_sec = model.predict(X_scaled)[0]
        new_end = entry_time + timedelta(seconds=float(duration_sec))

        # Обновляем границы предсказанной зелёной зоны
        pred_box.left = new_end
        pred_box.right = new_end + timedelta(seconds=1)

    callback_removed = False
    callback_id = [None]

    def stream_new_data():
        nonlocal current_pos, callback_removed, last_y_for_candle
        if current_pos >= final_pos:
            if not callback_removed:
                try:
                    curdoc().remove_periodic_callback(callback_id[0])
                except Exception as e:
                    print("Error removing callback:", e)
                callback_removed = True
                print("Достигли 30 минут после выхода — остановка потока данных.")
                if on_finished is not None:
                    on_finished()
            return
        if purple_zone_boundary is not None and current_pos < purple_zone_boundary:
            next_target = current_pos + 5
            if next_target >= purple_zone_boundary:
                next_pos = final_pos
            else:
                next_pos = next_target
        else:
            next_pos = final_pos

        new_chunk = full_df.iloc[current_pos:next_pos]
        if new_chunk.empty:
            if not callback_removed:
                try:
                    curdoc().remove_periodic_callback(callback_id[0])
                except Exception as e:
                    print("Error removing callback:", e)
                callback_removed = True
            return

        new_data_for_source = {
            'x': new_chunk['time'],
            'y': new_chunk['percent'],
            'z': new_chunk['close'],
        }
        source.stream(new_data_for_source)

        inc_x, inc_top, inc_bottom, inc_open, inc_close = [], [], [], [], []
        dec_x, dec_top, dec_bottom, dec_open, dec_close = [], [], [], [], []
        rows = new_chunk.to_dict('records')
        for row in rows:
            cur_time = row['time']
            cur_y = row['percent']
            o_ = last_y_for_candle
            c_ = cur_y
            hi_ = max(o_, c_)
            lo_ = min(o_, c_)
            if c_ >= o_:
                inc_x.append(cur_time)
                inc_open.append(o_)
                inc_close.append(c_)
                inc_top.append(hi_)
                inc_bottom.append(lo_)
            else:
                dec_x.append(cur_time)
                dec_open.append(o_)
                dec_close.append(c_)
                dec_top.append(hi_)
                dec_bottom.append(lo_)
            last_y_for_candle = c_
        if inc_x:
            inc_patch = dict(x=inc_x, open=inc_open, close=inc_close,
                             top=inc_top, bottom=inc_bottom)
            incsource.stream(inc_patch)
        if dec_x:
            dec_patch = dict(x=dec_x, open=dec_open, close=dec_close,
                             top=dec_top, bottom=dec_bottom)
            decsource.stream(dec_patch)

        current_pos = next_pos
        update_zones_and_info()

    current_pos = len(initial_df)
    final_pos = len(full_df[full_df['time'] <= (close_time + timedelta(minutes=30))])
    callback_id[0] = curdoc().add_periodic_callback(stream_new_data, 500)

    layout_trade = row(p, info_div)
    return layout_trade

####################################
# Глобальные переменные и загрузка сделок/триггеров
####################################
all_trades = None
current_trade_index = 0
layout_container = column()

def load_next_trade():
    global current_trade_index, all_trades
    if current_trade_index >= len(all_trades):
        print("Все сделки обработаны")
        return
    trade = all_trades[current_trade_index]
    print(f"Загружается сделка {current_trade_index+1} из {len(all_trades)}")
    current_trade_index += 1
    trade_layout = create_trade_plot(trade, on_finished=lambda: load_next_trade())
    layout_container.children.append(trade_layout)

####################################
# Загружаем триггеры и формируем layout
####################################
cur.execute("""
    SELECT symbol, time AS entry_time, time + INTERVAL '1 minute' AS close_time, open AS entry_price
    FROM crypto_data_1m
    WHERE time >= NOW() - INTERVAL '1 day'
      AND ((close - open) / open * 100) >= 4
""")
all_trades = cur.fetchall()
print("Найдено триггеров:", len(all_trades))
if not all_trades:
    raise ValueError("Нет триггеров для демонстрации примера.")

load_next_trade()
curdoc().add_root(layout_container)
