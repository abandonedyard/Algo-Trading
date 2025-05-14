#!/usr/bin/env python3
import psycopg2

def main():
    # Параметры подключения к PostgreSQL
    conn = psycopg2.connect(
        dbname="",
        user="",
        password="",
        host="",
        port=""
    )
    cur = conn.cursor()

    # 1) Обработка model_name, начинающегося с '1233', вставляем в de_1_39_2_2
    sql_for_1233 = """
        INSERT INTO public.de_1_39_2_2
        (
          time, 
          symbol, 
          link, 
          alert_price, 
          entry_price, 
          entry_time, 
          close_price, 
          close_time, 
          pnl, 
          is_automated, 
          maximum_drawdown, 
          from_tmm, 
          trades_count
        )
        SELECT
            open_time + INTERVAL '3 hour' AS time,
            symbol,
            NULL AS link,
            avg_price_entry AS alert_price,
            ARRAY[avg_price_entry] AS entry_price,
            open_time + INTERVAL '3 hour' AS entry_time,
            avg_price_exit AS close_price,
            close_time + INTERVAL '3 hour' AS close_time,
            unit_percent AS pnl,
            TRUE AS is_automated,
            maximum_drawdown,
            TRUE AS from_tmm,
            trades_count::integer
        FROM money.tmm_small
        WHERE model_name LIKE '1233%'
          AND open_time >= now() - INTERVAL '24 hour'
          AND NOT EXISTS (
              SELECT 1
              FROM public.de_1_39_2_2 t
              WHERE t.symbol = money.tmm_small.symbol
                AND date_trunc('minute', t.entry_time) = date_trunc('minute', money.tmm_small.open_time + INTERVAL '3 hour')
          );
    """

    # 2) Обработка model_name, начинающегося с '1232', вставляем в de_1_39_3_2
    sql_for_1232 = """
        INSERT INTO public.de_1_39_3_2
        (
          time, 
          symbol, 
          link, 
          alert_price, 
          entry_price, 
          entry_time, 
          close_price, 
          close_time, 
          pnl, 
          is_automated, 
          maximum_drawdown, 
          from_tmm, 
          trades_count
        )
        SELECT
            open_time + INTERVAL '3 hour' AS time,
            symbol,
            NULL AS link,
            avg_price_entry AS alert_price,
            ARRAY[avg_price_entry] AS entry_price,
            open_time + INTERVAL '3 hour' AS entry_time,
            avg_price_exit AS close_price,
            close_time + INTERVAL '3 hour' AS close_time,
            unit_percent AS pnl,
            TRUE AS is_automated,
            maximum_drawdown,
            TRUE AS from_tmm,
            trades_count::integer
        FROM money.tmm_small
        WHERE model_name LIKE '1232%'
          AND open_time >= now() - INTERVAL '24 hour'
          AND NOT EXISTS (
              SELECT 1
              FROM public.de_1_39_3_2 t
              WHERE t.symbol = money.tmm_small.symbol
                AND date_trunc('minute', t.entry_time) = date_trunc('minute', money.tmm_small.open_time + INTERVAL '3 hour')
          );
    """

    # Выполняем вставки
    cur.execute(sql_for_1233)
    cur.execute(sql_for_1232)

    # Сохраняем изменения
    conn.commit()

    # Закрываем соединение
    cur.close()
    conn.close()

if __name__ == "__main__":
    main()
