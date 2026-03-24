import requests
from psycopg2.extras import Json
from datetime import datetime
from  utils import db_conn
import os 
def getAirQuality(city,id):
    conn = None
    cursor = None
    try:
        API_AIR=os.getenv("API_AIR")
        results=requests.get(f"https://api.waqi.info/feed/{city}/?token={API_AIR}")
        if results.status_code!=200:
            results.json()
            return {
                    "status": "error",
                    "message": results["message"]
                    }
        
        results = results.json()

        if results.get("status")!="ok":
            return {
                    "status": "error",
                    "message": results["message"]
                    }
        

        #Ajouter la donnée à la base de donné 
        conn=db_conn.conn()
        cursor=conn.cursor()

        
        cursor.execute("""
        INSERT INTO raw_air_quality(city_id,data,insert_at) VALUES
        (%s,%s,%s) RETURNING id
        """,(id,Json(results),datetime.now()))
        conn.commit()
        print("data value insert at :",cursor.rowcount)
        #verifier si la donnée a été bie inserer 
        if cursor.rowcount > 0:
            print(f"Succès ! {cursor.rowcount} ligne(s) insérée(s).")
            return {
            "status":True,
            "message":f"Air data recupered and added success for {city}"
        }
        else:
            print("Aucune donnée n'a été insérée (peut-être un conflit ?)")
            return {
            "status":False,
            "message":f" Air data not recupered and not added success for {city}"
        }
    except Exception as e:
        return e
    finally:
        if cursor :
            cursor.close()
        if conn:
            conn.close()



def getWeather(city,id):
    conn = None
    cursor = None
    try:
        API_OPEN=os.getenv("API_OPEN")
        conn=db_conn.conn()
        cursor=conn.cursor()
        
        results=requests.get(f"https://api.openweathermap.org/data/2.5/weather?q={city}&appid={API_OPEN}")
        if results.status_code!=200:
            results.json()
            return {
                    "status": "error",
                    "message": results.get("message")
                    }
        results=results.json()
        if results.get("cod")!=200:
            return {
                    "status": "error",
                    "message": results.get("message")
                    }
         #Ajouter la donnée à la base de donné 
        print(results)
        cursor.execute("""
        INSERT INTO raw_weather(city_id,data,insert_at) VALUES
        (%s,%s,%s) RETURNING id
        """,(id,Json(results),datetime.now()))
        conn.commit()
        print("insert id ",cursor.rowcount)

        #verifier si la donnée a été bie inserer 
        if cursor.rowcount > 0:
            print(f"Succès ! {cursor.rowcount} ligne(s) insérée(s).")
            return {
            "status":True,
            "message":f" weather data recupered and added success for {city}"
        }
        else:
            print("Aucune donnée n'a été insérée (peut-être un conflit ?)")
            return {
            "status":False,
            "message":f" weather data not recupered and not added success for {city}"
        }
       

        
    except Exception as e:
        return e
    finally:
        if cursor :
            cursor.close()
        if conn:
            conn.close()
