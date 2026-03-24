import pandas 
import json
from utils import db_conn
from datetime import datetime

def transform(id, time_id):
    conn = None
    cursor = None

    try:
        conn = db_conn.conn()
        cursor = conn.cursor()

        cursor.execute("""
        SELECT * 
        FROM raw_air_quality 
        WHERE city_id = %s 
        AND insert_at > (
            SELECT last_run FROM runs ORDER BY last_run DESC LIMIT 1
        );
        """, (id,))
        air_data = cursor.fetchall()

        cursor.execute("""
        SELECT * 
        FROM raw_weather 
        WHERE city_id = %s 
        AND insert_at > (
            SELECT last_run FROM runs ORDER BY last_run DESC LIMIT 1
        );
        """, (id,))
        weather_data = cursor.fetchall()

        if not air_data or not weather_data:
            return {
                "status": False,
                "message": f"No data found for city {id}"
            }

        data = [row[2]['data'] for row in air_data]
        weather = [row[2] for row in weather_data]

        df = pandas.json_normalize(data)
        df_weather = pandas.json_normalize(weather)

        if df.empty or df_weather.empty:
            return {
                "status": False,
                "message": f"Empty dataframe for city {id}"
            }

        temp = float(df_weather['main.temp'].iloc[0])
        feel = float(df_weather['main.feels_like'].iloc[0])
        pressure = int(df_weather['main.pressure'].iloc[0])
        humidity = int(df_weather['main.humidity'].iloc[0])
        cloud = int(df_weather['clouds.all'].iloc[0])
        wind = float(df_weather['wind.speed'].iloc[0])
        aqi = int(df["aqi"].iloc[0])

        cursor.execute("""
        INSERT INTO fact_weather(
            temperature, feels_like, pressure, humidity,
            cloud, wind_speed, aqi, city_id, time_id
        )
        VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (temp, feel, pressure, humidity, cloud, wind, aqi, id, time_id))

        conn.commit()

        return {
            "status": True,
            "message": f"Success for city {id}"
        }

    except Exception as e:
        print("Transform error:", e)
        return {
            "status": False,
            "message": str(e)
        }

    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()