import threading
import time
import db_conn
import sys

arguments = sys.argv

if len(arguments) > 2 or len(arguments) == 1:
    print("Usage: main.py [file]")
    sys.exit(2)


traces_file = arguments[1]


def notification_listener():
    """Funkcija koja inicira slušanje notifikacija iz baze podataka."""

    print()
    channels = ["wrong_direction_channel", "risk_of_crash_channel"]
    db_conn.listen_notifications(channels)


def insert_rows():
    """Funkcija koja učitava podatke iz datoteke i unosi ih u bazu podataka."""

    with open("data/" + traces_file, "r") as file:
        for line in file:
            parts = line.strip().split(",")

            vehicle_id = parts[0].strip()
            latitude = parts[1].strip()
            longitude = parts[2].strip()
            timestamp = parts[3].strip()

            db_conn.insert_gps_data(vehicle_id, latitude, longitude, timestamp)

            time.sleep(300 / 1000)  # 300ms


listener_thread = threading.Thread(target=notification_listener)
listener_thread.start()

insert_rows()
