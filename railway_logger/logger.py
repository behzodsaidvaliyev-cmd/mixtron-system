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
MQTT_TOPIC = os.environ.get("MQTT_TOPIC", "mixtron2/data")
MQTT_EVENTS_TOPIC = os.environ.get("MQTT_EVENTS_TOPIC", "mixtron2/events")

DB_PATH = os.environ.get("DB_PATH", "/data/mixtron.db")
HTTP_PORT = int(os.environ.get("PORT", "8080"))
UZ_OFFSET = 5 * 3600  # O'zbekiston UTC+5

db_lock = threading.Lock()


def get_conn():
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
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
    conn.execute("""
        CREATE TABLE IF NOT EXISTS cycle_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            received_at TEXT DEFAULT (datetime('now')),
            event_ts INTEGER,
            event_time_local TEXT,
            event_type TEXT,
            motosoat REAL
        )
    """)
    try:
        conn.execute("ALTER TABLE readings ADD COLUMN received_ts INTEGER")
    except sqlite3.OperationalError:
        pass  # ustun allaqachon bor
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
    client.subscribe(MQTT_TOPIC)
    client.subscribe(MQTT_EVENTS_TOPIC)


def on_message(client, userdata, msg):
    conn = userdata["conn"]
    line = msg.payload.decode("utf-8", "ignore")

    if line.startswith("DATA|"):
        try:
            parts = line.split("|")
            status, volt, amp, watt, motosoat, energy, freq, pf = parts[1:9]
            with db_lock:
                conn.execute(
                    "INSERT INTO readings (status, volt, amp, watt, motosoat, energy, freq, pf, received_ts) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    (status, float(volt), float(amp), float(watt), float(motosoat), float(energy), float(freq), float(pf), int(time.time())),
                )
                conn.commit()
            print("[DB] saved:", line)
        except Exception as e:
            print("[DB] parse/save error:", e, "line:", line)

    elif line.startswith("EVENT|"):
        try:
            parts = line.split("|")
            event_ts, event_type, motosoat = parts[1], parts[2], parts[3]
            event_time_local = time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime(int(event_ts) + UZ_OFFSET))
            with db_lock:
                conn.execute(
                    "INSERT INTO cycle_events (event_ts, event_time_local, event_type, motosoat) VALUES (?, ?, ?, ?)",
                    (int(event_ts), event_time_local, event_type, float(motosoat)),
                )
                conn.commit()
            print("[EVENT] saved:", event_type, "at", event_time_local, "(mahalliy vaqt) motosoat =", motosoat)
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


def motosoat_at(conn, target_utc_ts):
    """Berilgan vaqtga eng yaqin (undan oldingi) motosoat qiymatini topadi."""
    with db_lock:
        row = conn.execute(
            "SELECT motosoat FROM readings WHERE received_ts <= ? ORDER BY received_ts DESC LIMIT 1",
            (target_utc_ts,),
        ).fetchone()
        if row:
            return row[0]

        row2 = conn.execute(
            "SELECT motosoat FROM readings WHERE received_ts >= ? ORDER BY received_ts ASC LIMIT 1",
            (target_utc_ts,),
        ).fetchone()
        return row2[0] if row2 else 0.0


def compute_hours_range(conn, from_ts, to_ts):
    start_motosoat = motosoat_at(conn, from_ts)
    end_motosoat = motosoat_at(conn, to_ts)
    return max(0.0, end_motosoat - start_motosoat)


def compute_hours_today(conn):
    now_utc = time.time()
    today_local = time.strftime("%Y-%m-%d", time.gmtime(now_utc + UZ_OFFSET))
    return compute_hours_range(conn, local_midnight_utc_ts(today_local), now_utc)


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

        if parsed.path == "/today_hours":
            hours = compute_hours_today(self.server.db_conn)
            body = json.dumps({"hours_today": round(hours, 3)}).encode()

        elif parsed.path == "/hours":
            try:
                now_utc = time.time()
                from_ts = parse_time_param(params.get("from", [None])[0], now_utc - 86400)
                to_ts = parse_time_param(params.get("to", [None])[0], now_utc)
                hours = compute_hours_range(self.server.db_conn, from_ts, to_ts)
                body = json.dumps({"from": from_ts, "to": to_ts, "hours": round(hours, 3)}).encode()
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
