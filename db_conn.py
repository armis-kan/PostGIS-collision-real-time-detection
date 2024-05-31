import psycopg2
import os
import select
from dotenv import load_dotenv

################################################
#    module for managing GPS data (postGIS)    #
################################################


# učitaj .env datoteku
load_dotenv()


# poveži se na bazu podataka
def connect_to_database():
    """Poveži se na bazu podataka."""

    return psycopg2.connect(
            database=os.getenv("DB_NAME"),
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASSWORD"),
            host=os.getenv("DB_HOST"),
            port=os.getenv("DB_PORT")
        )


# funkcija za unos podataka o vozilu
def insert_gps_data(vehicle_id, latitude, longitude, time):
    """Uvezi GPS podatke u bazu podataka."""

    conn = connect_to_database()
    cur = conn.cursor()
    try:
        sql = f"""
            INSERT INTO vehicle_gps_datas (vehicle_id, latitude, longitude, time, location_geometry)
            VALUES (%s, %s, %s, %s, ST_Transform(ST_SetSRID(ST_Point(%s, %s), 4326), 3857))
            """
        cur.execute(sql, (vehicle_id, latitude, longitude, time, longitude, latitude))
        conn.commit()
    except psycopg2.Error as e:
        print(f"An error occurred: {e}")
    finally:
        cur.close()
        conn.close()


# slušaj notifikacije iz baze podataka
def listen_notifications(channel_names):
    """Slušaj notifikacije iz baze podataka."""

    conn = connect_to_database()
    conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    cur = conn.cursor()
    for channel in channel_names:
        cur.execute(f"LISTEN {channel};")

    try:
        while True:
            select.select([conn], [], [])
            conn.poll()
            while conn.notifies:
                notification = conn.notifies.pop(0)
                if notification.payload:
                    print(f"{notification.payload}")
    finally:
        cur.close()
        conn.close()
