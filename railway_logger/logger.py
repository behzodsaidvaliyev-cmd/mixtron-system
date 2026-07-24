import os
import sqlite3
import time
import json
import calendar
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlparse, parse_qs
import paho.mqtt.client as mqtt

MQTT_BROKER = os.environ.get("MQTT_BROKER", "5a03687ae2394725ba4e934337264c51.s1.eu.hivemq.cloud")
MQTT_PORT = int(os.environ.get("MQTT_PORT", "8883"))
MQTT_USER = os.environ["MQTT_USER"]
MQTT_PASSWORD = os.environ["MQTT_PASSWORD"]

# Har qanday zavod/qurilma "<nom>/data" va "<nom>/events" topic'lariga yozsa, avtomatik qabul qilinadi
# (masalan mixtron2/data, zavod1/data - kodga tegmasdan yangi qurilma qo'shsa bo'ladi)
DATA_TOPIC_FILTER = "+/data"
EVENTS_TOPIC_FILTER = "+/events"

DB_PATH = os.environ.get("DB_PATH", "/data/mixtron.db")
HTTP_PORT = int(os.environ.get("PORT", "8080"))
UZ_OFFSET = 5 * 3600  # O'zbekiston UTC+5
DEFAULT_DEVICE = os.environ.get("DEFAULT_DEVICE", "mixtron2")  # ?zavod= berilmasa, shu ishlatiladi

db_lock = threading.Lock()


def get_conn():
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS readings (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            received_at TEXT DEFAULT (datetime('now')),
            device TEXT,
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
    conn.execute("""
        CREATE TABLE IF NOT EXISTS cycle_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            received_at TEXT DEFAULT (datetime('now')),
            device TEXT,
            event_ts INTEGER,
            event_time_local TEXT,
            event_type TEXT,
            motosoat REAL
        )
    """)
    for stmt in (
        "ALTER TABLE readings ADD COLUMN received_ts INTEGER",
        "ALTER TABLE readings ADD COLUMN device TEXT",
        "ALTER TABLE cycle_events ADD COLUMN device TEXT",
    ):
        try:
            conn.execute(stmt)
        except sqlite3.OperationalError:
            pass  # ustun allaqachon bor
    conn.commit()

    # eski yozuvlarda device bo'sh - bular birinchi ESP32'dan (mixtron2) kelgani ma'lum
    conn.execute("UPDATE readings SET device = 'mixtron2' WHERE device IS NULL")
    conn.execute("UPDATE cycle_events SET device = 'mixtron2' WHERE device IS NULL")
    conn.commit()

    # eski yozuvlarda received_ts bo'sh qolgan - received_at matnidan orqaga qarab to'ldiramiz
    rows = conn.execute("SELECT id, received_at FROM readings WHERE received_ts IS NULL").fetchall()
    for row_id, received_at in rows:
        try:
            ts = calendar.timegm(time.strptime(received_at, "%Y-%m-%d %H:%M:%S"))
            conn.execute("UPDATE readings SET received_ts = ? WHERE id = ?", (ts, row_id))
        except Exception:
            pass
    if rows:
        conn.commit()
        print("[DB] {} ta eski yozuvga vaqt to'ldirildi".format(len(rows)))

    return conn


def on_connect(client, userdata, flags, rc, properties=None):
    print("[MQTT] connected, rc =", rc)
    client.subscribe(DATA_TOPIC_FILTER)
    client.subscribe(EVENTS_TOPIC_FILTER)


def on_message(client, userdata, msg):
    conn = userdata["conn"]
    device = msg.topic.split("/")[0]
    line = msg.payload.decode("utf-8", "ignore")

    if line.startswith("DATA|"):
        try:
            parts = line.split("|")
            status, volt, amp, watt, motosoat, energy, freq, pf = parts[1:9]
            with db_lock:
                conn.execute(
                    "INSERT INTO readings (device, status, volt, amp, watt, motosoat, energy, freq, pf, received_ts) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    (device, status, float(volt), float(amp), float(watt), float(motosoat), float(energy), float(freq), float(pf), int(time.time())),
                )
                conn.commit()
            print("[DB] saved ({}): {}".format(device, line))
        except Exception as e:
            print("[DB] parse/save error:", e, "line:", line)

    elif line.startswith("EVENT|"):
        try:
            parts = line.split("|")
            event_ts, event_type, motosoat = parts[1], parts[2], parts[3]
            event_time_local = time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime(int(event_ts) + UZ_OFFSET))
            with db_lock:
                conn.execute(
                    "INSERT INTO cycle_events (device, event_ts, event_time_local, event_type, motosoat) VALUES (?, ?, ?, ?, ?)",
                    (device, int(event_ts), event_time_local, event_type, float(motosoat)),
                )
                conn.commit()
            print("[EVENT] saved ({}): {} at {} (mahalliy vaqt) motosoat = {}".format(device, event_type, event_time_local, motosoat))
        except Exception as e:
            print("[EVENT] parse/save error:", e, "line:", line)


def local_midnight_utc_ts(date_str):
    """'YYYY-MM-DD' (mahalliy sana) ni o'sha kunning mahalliy 00:00'i uchun Unix (UTC) vaqtiga aylantiradi."""
    dt = time.strptime(date_str, "%Y-%m-%d")
    return calendar.timegm(dt) - UZ_OFFSET


def local_str_to_utc_ts(value):
    """Mahalliy sana yoki sana+vaqt satrini Unix (UTC) vaqtiga aylantiradi.
    Qabul qilinadigan formatlar: 'YYYY-MM-DD', 'YYYY-MM-DD HH:MM', 'YYYY-MM-DD HH:MM:SS'."""
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M", "%Y-%m-%d"):
        try:
            dt = time.strptime(value, fmt)
            return calendar.timegm(dt) - UZ_OFFSET
        except ValueError:
            continue
    raise ValueError("noto'g'ri sana/vaqt format: " + value)


def motosoat_at(conn, device, target_utc_ts):
    """Berilgan zavod uchun, berilgan vaqtga eng yaqin (undan oldingi) motosoat qiymatini topadi."""
    with db_lock:
        row = conn.execute(
            "SELECT motosoat FROM readings WHERE device = ? AND received_ts <= ? ORDER BY received_ts DESC LIMIT 1",
            (device, target_utc_ts),
        ).fetchone()
        if row:
            return row[0]

        row2 = conn.execute(
            "SELECT motosoat FROM readings WHERE device = ? AND received_ts >= ? ORDER BY received_ts ASC LIMIT 1",
            (device, target_utc_ts),
        ).fetchone()
        return row2[0] if row2 else 0.0


def compute_hours_range(conn, device, from_ts, to_ts):
    start_motosoat = motosoat_at(conn, device, from_ts)
    end_motosoat = motosoat_at(conn, device, to_ts)
    return max(0.0, end_motosoat - start_motosoat)


def compute_hours_today(conn, device):
    now_utc = time.time()
    today_local = time.strftime("%Y-%m-%d", time.gmtime(now_utc + UZ_OFFSET))
    return compute_hours_range(conn, device, local_midnight_utc_ts(today_local), now_utc)


def parse_time_param(value, default):
    """Qiymat Unix vaqt yoki mahalliy sana/vaqt satri ('YYYY-MM-DD HH:MM') bo'lishi mumkin."""
    if value is None:
        return default
    try:
        return float(value)
    except ValueError:
        return local_str_to_utc_ts(value.replace("T", " "))


class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        parsed = urlparse(self.path)
        params = parse_qs(parsed.query)

        device = params.get("zavod", [DEFAULT_DEVICE])[0]

        if parsed.path == "/today_hours":
            hours = compute_hours_today(self.server.db_conn, device)
            body = json.dumps({"zavod": device, "hours_today": round(hours, 3)}).encode()

        elif parsed.path == "/hours":
            try:
                now_utc = time.time()
                from_ts = parse_time_param(params.get("from", [None])[0], now_utc - 86400)
                to_ts = parse_time_param(params.get("to", [None])[0], now_utc)
                hours = compute_hours_range(self.server.db_conn, device, from_ts, to_ts)
                body = json.dumps({"zavod": device, "from": from_ts, "to": to_ts, "hours": round(hours, 3)}).encode()
            except Exception as e:
                self.send_response(400)
                self.end_headers()
                self.wfile.write(str(e).encode())
                return

        else:
            self.send_response(404)
            self.end_headers()
            return

        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, format, *args):
        pass  # standart konsolni shovqindan tozalash


def run_http_server(conn):
    server = ThreadingHTTPServer(("0.0.0.0", HTTP_PORT), Handler)
    server.db_conn = conn
    print("[HTTP] /today_hours xizmati port", HTTP_PORT, "da ishga tushdi")
    server.serve_forever()


def main():
    conn = get_conn()

    http_thread = threading.Thread(target=run_http_server, args=(conn,), daemon=True)
    http_thread.start()

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
