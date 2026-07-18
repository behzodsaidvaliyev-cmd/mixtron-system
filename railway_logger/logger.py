import os
import sqlite3
import time
import paho.mqtt.client as mqtt

MQTT_BROKER = os.environ.get("MQTT_BROKER", "5a03687ae2394725ba4e934337264c51.s1.eu.hivemq.cloud")
MQTT_PORT = int(os.environ.get("MQTT_PORT", "8883"))
MQTT_USER = os.environ["MQTT_USER"]
MQTT_PASSWORD = os.environ["MQTT_PASSWORD"]
MQTT_TOPIC = os.environ.get("MQTT_TOPIC", "mixtron2/data")

DB_PATH = os.environ.get("DB_PATH", "/data/mixtron.db")


def ensure_db():
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS readings (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            received_at TEXT DEFAULT (datetime('now')),
            status TEXT,
            volt REAL,
            amp REAL,
            watt REAL,
            motosoat REAL,
            energy REAL,
            freq REAL,
            pf REAL
        )
    """)
    conn.commit()
    return conn


def on_connect(client, userdata, flags, rc, properties=None):
    print("[MQTT] connected, rc =", rc)
    client.subscribe(MQTT_TOPIC)


def on_message(client, userdata, msg):
    conn = userdata["conn"]
    line = msg.payload.decode("utf-8", "ignore")
    if not line.startswith("DATA|"):
        return
    try:
        parts = line.split("|")
        status, volt, amp, watt, motosoat, energy, freq, pf = parts[1:9]
        conn.execute(
            "INSERT INTO readings (status, volt, amp, watt, motosoat, energy, freq, pf) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            (status, float(volt), float(amp), float(watt), float(motosoat), float(energy), float(freq), float(pf)),
        )
        conn.commit()
        print("[DB] saved:", line)
    except Exception as e:
        print("[DB] parse/save error:", e, "line:", line)


def main():
    conn = ensure_db()
    client = mqtt.Client(
        callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
        client_id="railway-mixtron-logger",
        userdata={"conn": conn},
    )
    client.username_pw_set(MQTT_USER, MQTT_PASSWORD)
    client.tls_set()
    client.on_connect = on_connect
    client.on_message = on_message

    while True:
        try:
            client.connect(MQTT_BROKER, MQTT_PORT, 60)
            client.loop_forever()
        except Exception as e:
            print("[MQTT] connection error:", e)
            time.sleep(5)


if __name__ == "__main__":
    main()
