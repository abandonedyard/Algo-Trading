import os
import psycopg2
import requests
from datetime import datetime, timedelta
from dotenv import load_dotenv

# Для рисования графиков "в памяти"
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import io

# Инструменты для форматирования дат
import matplotlib.dates as mdates

DOTENV_PATH = "/opt/screener_scripts/.env"
load_dotenv(dotenv_path=DOTENV_PATH)

DB_CONFIG = {
    "dbname": os.getenv("DB_NAME"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "host": os.getenv("DB_HOST"),
    "port": os.getenv("DB_PORT")
}

# Добавляем нужные ключи:
TELEGRAM_BOT_TOKEN = "7982425369:AAEl3UkIc0SJww8uTyuLNsLJmsBsCLobqnw"
TELEGRAM_CHAT_ID = "-1002263492890"

MAX_TELEGRAM_LENGTH = 4000

def get_triggers():
    """
    Возвращает список кортежей:
      (symbol, time, trigger_type, t_priority)
    за последние 24 часа, где:
      - 'trigger_type' = 'close'
      - (time - prev_time) = 1 минута (ровно)
      - прирост > 4% (сравниваем current.close vs prev_close)
      - НЕТ записи в таблицах:
         de_1_39_2_2,
         de_1_39_3_2
        на диапазон [time - 70 секунд, time + 10 секунд].
    """
    conn = None
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        with conn.cursor() as cur:
            query = """
                WITH prev_data AS (
                    SELECT 
                        symbol,
                        time,
                        close,
                        LAG(close) OVER (PARTITION BY symbol ORDER BY time) AS prev_close,
                        LAG(time)  OVER (PARTITION BY symbol ORDER BY time) AS prev_time
                    FROM crypto_data_1m
                    WHERE time >= NOW() - INTERVAL '24 HOURS'
                )
                SELECT 
                    pd.symbol,
                    pd.time,
                    'close' AS trigger_type,
                    1 AS t_priority
                FROM prev_data pd
                WHERE pd.prev_close IS NOT NULL
                  AND pd.time - pd.prev_time = INTERVAL '1 minute'
                  AND pd.close > pd.prev_close * 1.04
                  AND NOT EXISTS (
                      SELECT 1
                      FROM de_1_39_2_2 t1
                      WHERE t1.symbol = pd.symbol
                        AND t1.entry_time BETWEEN pd.time - INTERVAL '70 seconds'
                                             AND pd.time + INTERVAL '10 seconds'
                  )
                  AND NOT EXISTS (
                      SELECT 1
                      FROM de_1_39_3_2 t2
                      WHERE t2.symbol = pd.symbol
                        AND t2.entry_time BETWEEN pd.time - INTERVAL '70 seconds'
                                             AND pd.time + INTERVAL '10 seconds'
                  )
                ORDER BY symbol, time, t_priority;
            """
            cur.execute(query)
            rows = cur.fetchall()
        return rows
    except Exception as e:
        print(f"[get_triggers] Ошибка при работе с БД: {e}")
        return []
    finally:
        if conn:
            conn.close()

def get_all_triggers():
    """
    Возвращает список кортежей:
      (symbol, time, trigger_type, t_priority)
    за последние 24 часа, где:
      - 'trigger_type' = 'close'
      - (time - prev_time) = 1 минута (ровно)
      - прирост > 4% (сравниваем current.close vs prev_close)
    (без проверки наличия записей в таблицах de_1_39_2_2 и de_1_39_3_2)
    """
    conn = None
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        with conn.cursor() as cur:
            query = """
                WITH prev_data AS (
                    SELECT 
                        symbol,
                        time,
                        close,
                        LAG(close) OVER (PARTITION BY symbol ORDER BY time) AS prev_close,
                        LAG(time)  OVER (PARTITION BY symbol ORDER BY time) AS prev_time
                    FROM crypto_data_1m
                    WHERE time >= NOW() - INTERVAL '24 HOURS'
                )
                SELECT 
                    pd.symbol,
                    pd.time,
                    'close' AS trigger_type,
                    1 AS t_priority
                FROM prev_data pd
                WHERE pd.prev_close IS NOT NULL
                  AND pd.time - pd.prev_time = INTERVAL '1 minute'
                  AND pd.close > pd.prev_close * 1.04
                ORDER BY symbol, time, t_priority;
            """
            cur.execute(query)
            rows = cur.fetchall()
        return rows
    except Exception as e:
        print(f"[get_all_triggers] Ошибка при работе с БД: {e}")
        return []
    finally:
        if conn:
            conn.close()

def load_24h_closes(symbol):
    """
    Загружает (time, close) за последние 24 часа для указанного symbol.
    Возвращает список кортежей (time, close).
    """
    conn = None
    data = []
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        with conn.cursor() as cur:
            query = """
                SELECT time, close
                FROM crypto_data_1m
                WHERE symbol = %s
                  AND time >= NOW() - INTERVAL '24 HOURS'
                ORDER BY time ASC
            """
            cur.execute(query, (symbol,))
            for row in cur.fetchall():
                data.append(row)
    except Exception as e:
        print(f"[load_24h_closes] Ошибка БД для {symbol}: {e}")
    finally:
        if conn:
            conn.close()
    return data

# -----------------------------
#  Блок Telegram
# -----------------------------
def send_telegram_message(message_text):
    """
    Отправляет простое текстовое сообщение в Telegram.
    """
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
        response = requests.post(url, json={
            "chat_id": TELEGRAM_CHAT_ID,
            "text": message_text,
            "parse_mode": "HTML"
        })
        print("Telegram status code:", response.status_code)
        print("Telegram response text:", response.text)
        if response.status_code == 200:
            resp_data = response.json()
            message_id = resp_data.get("result", {}).get("message_id")
            return True, message_id
        else:
            return False, None
    except Exception as e:
        print(f"Ошибка при отправке в Telegram (text): {e}")
        return False, None

def pin_telegram_message(message_id):
    """
    Закрепляет сообщение в чате Telegram.
    """
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/pinChatMessage"
        data = {
            "chat_id": TELEGRAM_CHAT_ID,
            "message_id": message_id,
            "disable_notification": True
        }
        r = requests.post(url, data=data)
        print("Pin status code:", r.status_code)
        print("Pin response text:", r.text)
        if r.status_code == 200:
            print("Сообщение закреплено.")
        else:
            print("Ошибка при закреплении сообщения.")
    except Exception as e:
        print(f"Ошибка при попытке закрепить сообщение: {e}")

def unpin_all_messages():
    """
    Снимает закреп со всех сообщений в чате.
    """
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/unpinAllChatMessages"
        data = {"chat_id": TELEGRAM_CHAT_ID}
        r = requests.post(url, data=data)
        print("UnpinAll status code:", r.status_code)
        print("UnpinAll response text:", r.text)
        if r.status_code == 200:
            print("Все сообщения откреплены.")
        else:
            print("Ошибка при откреплении сообщений.")
    except Exception as e:
        print(f"Ошибка при откреплении всех сообщений: {e}")

def send_long_message(long_text):
    """
    Отправляет длинное сообщение по частям (если нужно),
    снимая все старые закрепы и закрепляя только последнюю часть.
    """
    unpin_all_messages()
    start_idx = 0
    length = len(long_text)
    last_msg_id = None
    while start_idx < length:
        end_idx = min(start_idx + MAX_TELEGRAM_LENGTH, length)
        chunk = long_text[start_idx:end_idx]
        start_idx = end_idx
        success, msg_id = send_telegram_message(chunk)
        if not success:
            return False
        last_msg_id = msg_id
    if last_msg_id is not None:
        pin_telegram_message(last_msg_id)
    return True

def send_telegram_photo(image_data, caption=""):
    """
    Отправляет фото (график) в чат Telegram.
    """
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendPhoto"
        files = {"photo": ("chart.png", image_data)}
        data = {
            "chat_id": TELEGRAM_CHAT_ID,
            "caption": caption,
            "parse_mode": "HTML"
        }
        response = requests.post(url, data=data, files=files)
        print("Telegram status code (photo):", response.status_code)
        print("Telegram response text (photo):", response.text)
        return (response.status_code == 200)
    except Exception as e:
        print(f"Ошибка при отправке в Telegram (photo): {e}")
        return False

# -----------------------------
#   Функция рисует график
# -----------------------------
def plot_symbol_triggers(symbol, triggers):
    """
    Рисует график Close за последние 24 часа 
    и отмечает точки, где возникают триггеры (close).
    Возвращает бинарное содержимое PNG-графика.
    """
    data_24h = load_24h_closes(symbol)
    if not data_24h:
        return None
    times = [row[0] for row in data_24h]
    closes = [float(row[1]) for row in data_24h]
    fig, ax = plt.subplots(figsize=(12, 6))
    ax.plot(times, closes, label='Close', color='blue')
    for (t, trig_type) in triggers:
        if t in times:
            idx = times.index(t)
        else:
            closest_time = min(times, key=lambda x: abs(x - t))
            idx = times.index(closest_time)
        x_val = times[idx]
        y_val = closes[idx]
        ax.scatter(x_val, y_val, color='green', marker='o', s=100, label='CLOSE trigger')
    ax.set_title(f"{symbol}")
    ax.set_xlabel("Time (HH:MM)")
    ax.set_ylabel("Close Price (%)")
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%H:%M'))
    first_price = closes[0]
    from matplotlib.ticker import FuncFormatter
    formatter = FuncFormatter(lambda x, pos: f'{(x/first_price - 1)*100:.1f}%')
    ax.yaxis.set_major_formatter(formatter)
    ax.legend()
    ax.grid(True)
    fig.autofmt_xdate(rotation=30)
    buf = io.BytesIO()
    plt.savefig(buf, format='png')
    plt.close(fig)
    buf.seek(0)
    return buf.getvalue()

# -----------------------------
#   Функция сохранения данных в таблицу de4
# -----------------------------
def save_triggers_to_de4(triggers_list):
    """
    Сохраняет в таблицу alexey_nikolaev.de4 строки с (symbol, time),
    избегая дублирующих записей.
    Для корректной работы необходимо, чтобы в таблице было уникальное ограничение
    на (symbol, time).
    """
    if not triggers_list:
        return
    conn = None
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        with conn.cursor() as cur:
            for (symbol, t, trig_type, t_prio) in triggers_list:
                insert_query = """
                    INSERT INTO alexey_nikolaev.de4 (symbol, time)
                    VALUES (%s, %s)
                    ON CONFLICT (symbol, time) DO NOTHING;
                """
                cur.execute(insert_query, (symbol, t))
        conn.commit()
    except Exception as e:
        print(f"Ошибка при сохранении триггеров в таблицу de4: {e}")
    finally:
        if conn:
            conn.close()

# -----------------------------
#   Функция сохранения данных в таблицу all4
# -----------------------------
def save_triggers_to_all4(triggers_list):
    """
    Сохраняет в таблицу alexey_nikolaev.all4 строки с (symbol, time),
    избегая дублирующих записей.
    Для корректной работы необходимо, чтобы в таблице было уникальное ограничение
    на (symbol, time).
    """
    if not triggers_list:
        return
    conn = None
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        with conn.cursor() as cur:
            for (symbol, t, trig_type, t_prio) in triggers_list:
                insert_query = """
                    INSERT INTO alexey_nikolaev.all4 (symbol, time)
                    VALUES (%s, %s)
                    ON CONFLICT (symbol, time) DO NOTHING;
                """
                cur.execute(insert_query, (symbol, t))
        conn.commit()
    except Exception as e:
        print(f"Ошибка при сохранении триггеров в таблицу all4: {e}")
    finally:
        if conn:
            conn.close()

# -----------------------------
#   Основная функция
# -----------------------------
def main():
    triggers_data = get_triggers()
    if not triggers_data:
        print("Нет триггеров (или ошибка БД).")
        return

    # Сохраняем данные о триггерах в таблицу de4 (без дублей)
    save_triggers_to_de4(triggers_data)
    # Сохраняем все 4% приросты (без фильтрации по de_1_39_2_2 и de_1_39_3_2)
    all_triggers_data = get_all_triggers()
    save_triggers_to_all4(all_triggers_data)

    # Все триггеры — только close
    close_triggers = {}
    for (symbol, t, trig_type, t_prio) in triggers_data:
        close_triggers.setdefault(symbol, []).append(t)

    yesterday = datetime.now() - timedelta(days=1)
    date_str = yesterday.strftime('%Y-%m-%d')
    total_count = len(triggers_data)

    # Формируем сообщение с двумя блоками: по одному на таблицу
    msg_de_1_39_2_2 = "<b>🔸 Незадействованные триггеры по модели de_1_39_2_2 (close):</b>\n"
    if close_triggers:
        for symb, times_list in close_triggers.items():
            time_str = ", ".join(t.strftime('%H:%M') for t in times_list)
            msg_de_1_39_2_2 += f"• <b>{symb}</b>: {time_str}\n"
    else:
        msg_de_1_39_2_2 += "• Нет\n"
    msg_de_1_39_2_2 += "\n"

    msg_de_1_39_3_2 = "<b>🔹 Незадействованные триггеры по модели de_1_39_3_2 (close):</b>\n"
    if close_triggers:
        for symb, times_list in close_triggers.items():
            time_str = ", ".join(t.strftime('%H:%M') for t in times_list)
            msg_de_1_39_3_2 += f"• <b>{symb}</b>: {time_str}\n"
    else:
        msg_de_1_39_3_2 += "• Нет\n"
    msg_de_1_39_3_2 += "\n"

    message_header = (
        f"<b>📅 Дата отчета:</b> {date_str}\n"
        f"<b>Всего незадействованных триггеров (close):</b> {total_count}\n\n"
    )

    full_message = message_header + msg_de_1_39_2_2 + msg_de_1_39_3_2

    if send_long_message(full_message):
        print("Текстовый отчёт отправлен и закреплён!")
    else:
        print("Ошибка отправки текстового отчёта.")
        return

    symbols_set = set(close_triggers.keys())
    for symbol in symbols_set:
        chart_data = plot_symbol_triggers(
            symbol, [(t, 'close') for t in close_triggers[symbol]]
        )
        if chart_data:
            caption = f"<b>{symbol}</b> ({date_str})\n"
            for t in close_triggers[symbol]:
                caption += f"• CLOSE в {t.strftime('%H:%M')}\n"
            ok = send_telegram_photo(chart_data, caption=caption)
            if not ok:
                print(f"Ошибка отправки графика для {symbol}.")
        else:
            print(f"Нет данных для построения графика {symbol}.")

if __name__ == "__main__":
    main()
