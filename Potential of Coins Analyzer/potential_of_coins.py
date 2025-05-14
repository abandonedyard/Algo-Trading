import time
import math
import io
import os
from dotenv import load_dotenv
import telebot
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import psycopg2
import psycopg2.extras
from datetime import datetime, timedelta

# ------------------------------------------------------------------------------
# Загружаем переменные окружения из файла /opt/screener_scripts/.env
# ------------------------------------------------------------------------------
load_dotenv("/opt/screener_scripts/.env")

# ------------------------------------------------------------------------------
# Считываем параметры для подключения к БД из переменных окружения
# ------------------------------------------------------------------------------
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")  # <-- убрали quote_plus
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")

# ----------------------------------
# Параметры Telegram-бота
# ----------------------------------
# ВАЖНО: Токен должен содержать двоеточие в формате вида 123456789:ABC-DEF....
TG_BOT_TOKEN = "7896558929:AAEV3zi86WME9cBi_7VZOafvrkpXSfZTOwI"
CHAT_ID = "-1002296167815"  # Укажите нужный Chat ID
bot = telebot.TeleBot(TG_BOT_TOKEN)

# ----------------------------------
# Названия столбцов в вашей таблице
# ----------------------------------
TIME_COLUMN = "time"
OPEN_COLUMN = "open"
HIGH_COLUMN = "high"
LOW_COLUMN = "low"
CLOSE_COLUMN = "close"
VOLUME_COLUMN = "volume"
SYMBOL_COLUMN = "symbol"

# ----------------------------------
# Количество дней для выборки
# ----------------------------------
LAST_DAYS = 10

# -----------------------------------------------------------------------------
# Вспомогательные функции для Телеграм (обработка ошибок "Too Many Requests")
# -----------------------------------------------------------------------------
def extract_retry_after(e: Exception):
    """
    Извлекаем значение 'retry after' из сообщения об ошибке Telegram,
    если оно присутствует.
    """
    msg = str(e)
    if "retry after" in msg:
        try:
            parts = msg.split("retry after")
            retry_str = parts[1].strip()
            retry_sec = int(retry_str)
            return retry_sec
        except:
            return None
    return None

def send_telegram_message(text: str, pin=False):
    """
    Отправляет текстовое сообщение в Telegram.
    Если pin=True, сообщение будет закреплено без уведомления (и снимется предыдущий пин).
    """
    try:
        msg = bot.send_message(CHAT_ID, text)
        time.sleep(1)

        if pin:
            # Снимаем старый пин (если был)
            try:
                bot.unpin_all_chat_messages(CHAT_ID)
            except telebot.apihelper.ApiTelegramException as e:
                print(f"Ошибка при откреплении старых сообщений: {e}")

            # Закрепляем новое сообщение без уведомления
            bot.pin_chat_message(CHAT_ID, msg.message_id, disable_notification=True)

    except telebot.apihelper.ApiTelegramException as e:
        if "Too Many Requests" in str(e):
            # Сработал лимит: пытаемся найти "retry after"
            retry_sec = extract_retry_after(e)
            if retry_sec is not None:
                print(f"Превышен лимит запросов в Telegram. Ждём {retry_sec} секунд...")
                time.sleep(retry_sec + 1)
                msg = bot.send_message(CHAT_ID, text)
                if pin:
                    try:
                        bot.unpin_all_chat_messages(CHAT_ID)
                    except telebot.apihelper.ApiTelegramException as e_unpin:
                        print(f"Ошибка при повторном откреплении: {e_unpin}")
                    bot.pin_chat_message(CHAT_ID, msg.message_id, disable_notification=True)
        else:
            print(f"Ошибка при отправке сообщения: {e}")

def send_chart_to_telegram(image_data: bytes, caption: str):
    """
    Отправляет график (в виде фото) в Telegram (не закрепляем).
    """
    try:
        bot.send_photo(CHAT_ID, image_data, caption=caption)
        time.sleep(1)
    except telebot.apihelper.ApiTelegramException as e:
        if "Too Many Requests" in str(e):
            retry_sec = extract_retry_after(e)
            if retry_sec is not None:
                print(f"Превышен лимит запросов в Telegram. Ждём {retry_sec} секунд...")
                time.sleep(retry_sec + 1)
                bot.send_photo(CHAT_ID, image_data, caption=caption)
        else:
            print(f"Ошибка при отправке графика: {e}")

# -----------------------------------------------------------------------------
# Функции для работы с базой PostgreSQL
# -----------------------------------------------------------------------------
def get_connection():
    """
    Возвращает новое соединение с базой данных PostgreSQL.
    """
    return psycopg2.connect(
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        host=DB_HOST,
        port=DB_PORT
    )

def get_symbols_for_last_n_days(n_days: int):
    """
    Возвращает список уникальных symbol, для которых есть данные за последние n_days дней.
    """
    query = f"""
        SELECT DISTINCT {SYMBOL_COLUMN}
        FROM crypto_data_1h
        WHERE {TIME_COLUMN} >= NOW() - INTERVAL '{n_days} DAYS'
    """
    conn = get_connection()
    try:
        with conn.cursor() as cursor:
            cursor.execute(query)
            rows = cursor.fetchall()
        symbols = [row[0] for row in rows]
        return symbols
    finally:
        conn.close()

def load_data_for_symbol(symbol: str, n_days: int) -> pd.DataFrame:
    """
    Загружает данные из таблицы crypto_data_1h за последние n_days дней для указанного symbol.
    Возвращает DataFrame, отсортированный по времени.
    """
    query = f"""
        SELECT
            {TIME_COLUMN},
            {OPEN_COLUMN},
            {HIGH_COLUMN},
            {LOW_COLUMN},
            {CLOSE_COLUMN},
            {VOLUME_COLUMN}
        FROM crypto_data_1h
        WHERE {SYMBOL_COLUMN} = %s
          AND {TIME_COLUMN} >= NOW() - INTERVAL '{n_days} DAYS'
        ORDER BY {TIME_COLUMN} ASC
    """
    conn = get_connection()
    try:
        df = pd.read_sql_query(query, conn, params=[symbol])
    finally:
        conn.close()

    # Переименовываем столбец времени, чтобы потом было "Time"
    df.rename(columns={TIME_COLUMN: "Time"}, inplace=True)
    return df

# -----------------------------------------------------------------------------
# Основная логика обработки
# -----------------------------------------------------------------------------
def process_symbol(symbol: str) -> dict or None:
    """
    Загружает данные за последние LAST_DAYS для указанного symbol,
    вычисляет показатели и строит график (возвращает словарь с результатами).
    Если данных нет или данные некорректны, вернёт None.
    """
    df = load_data_for_symbol(symbol, LAST_DAYS)

    if df.empty:
        print(f"[{symbol}] Нет данных за последние {LAST_DAYS} дней. Пропускаем.")
        return None

    # Проверка на наличие NaN
    if df.isnull().any().any():
        print(f"[{symbol}] Данные содержат NaN. Пропускаем.")
        return None

    # Проверка на наличие необходимых столбцов
    required_columns = ["Time", OPEN_COLUMN, HIGH_COLUMN, LOW_COLUMN, CLOSE_COLUMN]
    missing_columns = [col for col in required_columns if col not in df.columns]
    if missing_columns:
        print(f"[{symbol}] Отсутствуют нужные столбцы: {missing_columns}. Пропускаем.")
        return None

    # Сортируем по Time (на всякий случай)
    df.sort_values(by="Time", inplace=True)
    df.reset_index(drop=True, inplace=True)

    # 1) Находим максимум за весь период
    max_idx = df[HIGH_COLUMN].idxmax()
    max_price = df.loc[max_idx, HIGH_COLUMN]
    max_time = df.loc[max_idx, "Time"]

    # 2) Минимум после максимума
    df_after_max = df.loc[max_idx:].copy()
    if df_after_max.empty:
        print(f"[{symbol}] Нет данных после максимума. Пропускаем.")
        return None

    min_idx_after_max = df_after_max[LOW_COLUMN].idxmin()
    min_price_after_max = df_after_max.loc[min_idx_after_max, LOW_COLUMN]
    min_time_after_max = df_after_max.loc[min_idx_after_max, "Time"]

    # 3) Открытие периода (первая цена OPEN в выборке)
    open_price = df.iloc[0][OPEN_COLUMN]

    # 4) Потенциал (лонга)
    potential = (max_price - open_price) / open_price * 100

    # 5) Потенциал шорта
    short_potential = (max_price - min_price_after_max) / max_price * 100

    # ---------------- Построение графика ----------------
    fig, ax = plt.subplots(figsize=(14, 7))

    ax.plot(df["Time"], df[CLOSE_COLUMN], label="Close", color='blue')
    ax.plot(df["Time"], df[HIGH_COLUMN], linestyle='--', alpha=0.5, label="High")
    ax.plot(df["Time"], df[LOW_COLUMN], linestyle='--', alpha=0.5, label="Low")
    ax.tick_params(axis='x', labelsize=9, labelrotation=30)

    # Собираем все цены для масштабирования
    all_prices = pd.concat([df[CLOSE_COLUMN], df[HIGH_COLUMN], df[LOW_COLUMN]])
    
    # Добавляем горизонтальные уровни от стартовой цены (Close на первой свече)
    first_close_price = df.iloc[0][CLOSE_COLUMN]
    level_20 = first_close_price * 1.2
    level_50 = first_close_price * 1.5
    level_100 = first_close_price * 2.0

    extended_min = min(all_prices.min(), level_20, level_50, level_100)
    extended_max = max(all_prices.max(), level_20, level_50, level_100)
    ax.set_ylim([extended_min * 0.98, extended_max * 1.02])

    # Горизонтальные линии
    ax.axhline(y=level_20,  color='red', linestyle='--', alpha=0.3, label="100%")
    ax.axhline(y=level_50,  color='red', linestyle='--', alpha=0.6, label="50%")
    ax.axhline(y=level_100, color='red', linestyle='--', alpha=0.9, label="20%")

    # Отмечаем максимум и минимум
    ax.scatter(max_time, max_price, color='red', marker='^', label="Max after open")
    ax.scatter(min_time_after_max, min_price_after_max, color='purple', marker='v', label="Min after max")

    start_date_str = df["Time"].min().strftime('%d-%m-%Y %H:%M')
    end_date_str = df["Time"].max().strftime('%d-%m-%Y %H:%M')

    ax.set_title(
        f"{symbol}\n"
        f"Период: {start_date_str} – {end_date_str}\n"
        f"Потенциал шорта: {short_potential:.2f}% | Потенциал: {potential:.2f}%"
    )

    ax.legend()
    ax.grid(True)

    # Сохраняем рисунок в буфер, чтобы отправить в Telegram
    buf = io.BytesIO()
    plt.savefig(buf, format='png')
    plt.close(fig)
    buf.seek(0)
    image_data = buf.getvalue()

    return {
        "symbol": symbol,
        "start_date": df["Time"].min(),
        "end_date": df["Time"].max(),
        "potential": potential,
        "short_potential": short_potential,
        "image_data": image_data
    }

def main():
    # 1. Получаем список всех монет за последние 10 дней
    symbols = get_symbols_for_last_n_days(LAST_DAYS)
    if not symbols:
        send_telegram_message(f"Нет монет с данными за последние {LAST_DAYS} дней.")
        return

    results = []
    for symbol in symbols:
        print(f"Обработка символа: {symbol}")
        res = process_symbol(symbol)
        if res is not None:
            results.append(res)

    if not results:
        send_telegram_message("Не удалось собрать корректные данные по монетам.")
        return

    # 2. Сортируем по потенциалу шорта (убывание)
    results.sort(key=lambda x: x["short_potential"], reverse=True)

    # 3. Определяем общий период (минимальная и максимальная дата среди всех монет)
    overall_min_start = min(r["start_date"] for r in results)
    overall_max_end = max(r["end_date"] for r in results)
    overall_min_str = overall_min_start.strftime('%d-%m-%Y %H:%M')
    overall_max_str = overall_max_end.strftime('%d-%m-%Y %H:%M')

    header_line = f"Период: {overall_min_str} – {overall_max_str}"
    lines = [header_line, ""]

    print("\n------ Результаты по монетам (в порядке убывания потенциала шорта) ------")
    print(header_line + "\n")

    # 4. Формируем список строк для отправки в Телеграм
    for r in results:
        coin_line = (
            f"{r['symbol']}: "
            f"Потенциал шорта: {r['short_potential']:.2f}%, "
            f"Потенциал: {r['potential']:.2f}%"
        )
        lines.append(coin_line)
        print(coin_line)

    # 5. Разбиваем на части, чтобы не отправлять слишком большое сообщение
    total_lines = len(lines)
    chunk_size = math.ceil(total_lines / 5)  # Делим примерно на 5 сообщений
    start_idx = 0

    message_count = 0
    while start_idx < total_lines:
        chunk_lines = lines[start_idx:start_idx + chunk_size]
        start_idx += chunk_size
        message_count += 1

        text_to_send = "\n".join(chunk_lines)

        # Закрепляем только первое сообщение
        pin_it = (message_count == 1)
        send_telegram_message(text_to_send, pin=pin_it)

    # 6. Отправляем графики
    for item in results:
        symbol = item["symbol"]
        start_date_str = item["start_date"].strftime('%d-%m-%Y %H:%M')
        end_date_str = item["end_date"].strftime('%d-%m-%Y %H:%M')
        short_potential = item["short_potential"]
        potential = item["potential"]
        image_data = item["image_data"]

        caption = (
            f"{symbol}\n"
            f"Период: {start_date_str} – {end_date_str}\n"
            f"Потенциал шорта: {short_potential:.2f}%\n"
            f"Потенциал: {potential:.2f}%"
        )

        send_chart_to_telegram(image_data, caption=caption)

if __name__ == "__main__":
    main()
