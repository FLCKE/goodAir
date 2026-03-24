from utils import raw_extract, transform
from utils import db_conn
from datetime import datetime
import traceback
import sys


def run_transform(cities, time_id):
    for city in cities:
        result = transform.transform(city[0], time_id)
        if result.get("status") is False:
            print(result.get('message'))
    return True


def main():
    conn = None
    cursor = None

    try:
        print("Connexion DB...")
        conn = db_conn.conn()
        cursor = conn.cursor()

        # Vérifier les villes
        cursor.execute("SELECT id, name, country FROM dim_city;")
        cities = cursor.fetchall()

        if not cities:
            raise Exception(" Aucune ville trouvée dans dim_city")

        print(f"{len(cities)} villes trouvées")

        # Insert time
        date = datetime.now()
        cursor.execute("""
            INSERT INTO dim_time(datetime, hour, day, month, year)
            VALUES(%s,%s,%s,%s,%s) RETURNING id
        """, (date, date.hour, date.day, date.month, date.year))

        time_id = cursor.fetchone()[0]  # 
        conn.commit()

        print(f"time_id = {time_id}")

        # Extract
        for city in cities:
            raw_extract.getAirQuality(city[1], city[0])
            raw_extract.getWeather(city[1], city[0])

        # Transform
        result = run_transform(cities, time_id)

        if result:
            cursor.execute("""
                INSERT INTO runs(last_run)
                VALUES(%s)
            """, (datetime.now(),))
            conn.commit()

        print(" ETL terminé avec succès")

    except Exception as e:
        print("ERREUR:", e)
        traceback.print_exc()
        sys.exit(1)

    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()


if __name__ == "__main__":
    main()