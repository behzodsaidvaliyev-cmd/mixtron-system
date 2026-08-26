import os
import sqlite3
import time
import json
import calendar
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlparse, parse_qs, quote
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
DEFAULT_DEVICE = os.environ.get("DEFAULT_DEVICE", "zavod3")  # ?zavod= berilmasa, shu ishlatiladi

# RLock (oddiy Lock emas): qulf olgan funksiya ichidan yana qulf
# oladigan funksiya chaqirilsa, oddiy Lock butun xizmatni abadiy
# qotirib qo'yadi. RLock bunday holatda xavfsiz ishlaydi.
db_lock = threading.RLock()


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
        "ALTER TABLE cycle_events ADD COLUMN event_time_local TEXT",
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

    # eski voqealarda event_time_local bo'sh qolgan - event_ts'dan qayta hisoblab to'ldiramiz
    event_rows = conn.execute("SELECT id, event_ts FROM cycle_events WHERE event_time_local IS NULL").fetchall()
    for row_id, event_ts in event_rows:
        try:
            t = time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime(event_ts + UZ_OFFSET))
            conn.execute("UPDATE cycle_events SET event_time_local = ? WHERE id = ?", (t, row_id))
        except Exception:
            pass
    if event_rows:
        conn.commit()
        print("[DB] {} ta eski voqeaga mahalliy vaqt to'ldirildi".format(len(event_rows)))

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
            event_ts, event_type, motosoat = int(parts[1]), parts[2], parts[3]
            # Qurilma vaqtni aniqlay olmagan bo'lsa (NTP bloklangan tarmoq)
            # 0 yuboradi - bunda serverning O'Z soati ishlatiladi, aks holda
            # voqea 1970-yil sanasi bilan yozilib, hisobotlarni buzardi.
            #
            # Ishonchsiz KELAJAK sanasi ham shu yerda ushlanadi: NTP protokoli
            # 2036-02-07 da "aylanadi" va ba'zi javoblar 136 yil noto'g'ri vaqt
            # beradi. Bazada aynan shunday yozuv topilgan (zavod1, 2036-02-07).
            # Qurilmada ham himoya bor, lekin eski kodli qurilma ulanib qolsa
            # server oxirgi to'siq bo'lib turadi.
            now = int(time.time())
            if event_ts <= 0:
                event_ts = now
                print("[EVENT] qurilmada vaqt yo'q edi - server vaqti qo'yildi")
            elif event_ts > now + 86400 or event_ts < 1700000000:
                print("[EVENT] ishonchsiz sana", event_ts, "- server vaqti qo'yildi")
                event_ts = now
            event_time_local = time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime(event_ts + UZ_OFFSET))
            with db_lock:
                conn.execute(
                    "INSERT INTO cycle_events (device, event_ts, event_time_local, event_type, motosoat) VALUES (?, ?, ?, ?, ?)",
                    (device, event_ts, event_time_local, event_type, float(motosoat)),
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


def compute_hours_month(conn, device):
    now_utc = time.time()
    month_start_local = time.strftime("%Y-%m-01", time.gmtime(now_utc + UZ_OFFSET))
    return compute_hours_range(conn, device, local_midnight_utc_ts(month_start_local), now_utc)


def soat_matn(soat):
    """Kasr soatni odam o'qiydigan ko'rinishga aylantiradi.

    17.83 -> "17.83 soat (17:50)". Kasr son hisob-kitobga qulay, lekin
    "17.83 soat" necha daqiqa ekanini ko'z bilan chamalash qiyin.
    """
    try:
        s = float(soat)
    except (TypeError, ValueError):
        return "-- soat"
    butun = int(s)
    daqiqa = int(round((s - butun) * 60))
    if daqiqa == 60:                 # yaxlitlash "17:60" bo'lib qolmasin
        butun += 1
        daqiqa = 0
    return "{:.2f} soat ({}:{:02d})".format(s, butun, daqiqa)


def parse_time_param(value, default):
    """Qiymat Unix vaqt yoki mahalliy sana/vaqt satri ('YYYY-MM-DD HH:MM') bo'lishi mumkin."""
    if value is None:
        return default
    try:
        return float(value)
    except ValueError:
        return local_str_to_utc_ts(value.replace("T", " "))


EVENTS_TABLE_STYLE = """
        body{font-family:Arial,sans-serif;background:#1c2530;color:#ecf0f1;padding:24px}
        h2{color:#f1c40f}
        a.download{display:inline-block;margin-bottom:16px;color:#fff;background:#2ecc71;
            padding:8px 14px;border-radius:4px;text-decoration:none;font-weight:bold}
        table{border-collapse:collapse;width:100%;background:#2c3e50}
        th,td{border:1px solid #445;padding:8px 12px;text-align:left}
        th{background:#34495e}
        tr:nth-child(even){background:#25303d}
        .jami{background:#2c3e50;border-left:5px solid #f1c40f;padding:12px 18px;
            margin:0 0 16px 0;font-size:18px;border-radius:4px;display:inline-block}
        .jami b{color:#f1c40f;font-size:22px}
        label.chk{display:inline-block;margin:0 18px 16px 0;cursor:pointer;
            background:#2c3e50;padding:9px 14px;border-radius:4px}
        label.chk input{margin-right:6px;transform:scale(1.2)}
        .izoh{color:#8aa0b3;font-size:12px;margin-left:10px}
        tr.boot td{color:#8aa0b3;font-style:italic}
"""


class Handler(BaseHTTPRequestHandler):
    def _send_json(self, obj):
        body = json.dumps(obj).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _send_error(self, code, message):
        body = str(message).encode()
        self.send_response(code)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        parsed = urlparse(self.path)
        params = parse_qs(parsed.query)
        conn = self.server.db_conn
        device = params.get("zavod", [DEFAULT_DEVICE])[0]

        if parsed.path == "/today_hours":
            hours = compute_hours_today(conn, device)
            self._send_json({"zavod": device, "hours_today": round(hours, 3)})

        elif parsed.path == "/month_hours":
            hours = compute_hours_month(conn, device)
            self._send_json({"zavod": device, "hours_month": round(hours, 3)})

        elif parsed.path == "/hours":
            try:
                now_utc = time.time()
                from_ts = parse_time_param(params.get("from", [None])[0], now_utc - 86400)
                to_ts = parse_time_param(params.get("to", [None])[0], now_utc)
                hours = compute_hours_range(conn, device, from_ts, to_ts)
                self._send_json({"zavod": device, "from": from_ts, "to": to_ts, "hours": round(hours, 3)})
            except Exception as e:
                self._send_error(400, e)

        elif parsed.path == "/events":
            limit = int(params.get("limit", ["200"])[0])
            with db_lock:
                rows = conn.execute(
                    "SELECT event_time_local, event_type, motosoat FROM cycle_events "
                    "WHERE device = ? ORDER BY event_ts DESC LIMIT ?",
                    (device, limit),
                ).fetchall()
            self._send_json({
                "zavod": device,
                "events": [{"vaqt": r[0], "holat": r[1], "motosoat": r[2]} for r in rows],
            })

        elif parsed.path == "/events.csv":
            # from/to berilmasa - butun tarix (eski xatti-harakat saqlanadi)
            try:
                from_ts = parse_time_param(params.get("from", [None])[0], 0)
                to_ts = parse_time_param(params.get("to", [None])[0], time.time())
            except Exception as e:
                self._send_error(400, e)
                return
            # boot=0 -> BOOT qatorlari chiqarilmaydi. Brauzerdagi kalochka
            # shu parametrni qo'shadi, shunda Excel ekranda ko'rinayotgan
            # ro'yxat bilan bir xil bo'ladi.
            boot_kerak = params.get("boot", ["1"])[0] != "0"
            shart = "" if boot_kerak else " AND event_type != 'BOOT'"
            with db_lock:
                rows = conn.execute(
                    "SELECT event_time_local, event_type, motosoat FROM cycle_events "
                    "WHERE device = ? AND event_ts BETWEEN ? AND ?" + shart +
                    " ORDER BY event_ts ASC",
                    (device, from_ts, to_ts),
                ).fetchall()
            lines = ["Vaqt;Holat;Motosoat"]
            for r in rows:
                lines.append("{};{};{}".format(r[0], r[1], str(r[2]).replace(".", ",")))
            body = b"\xef\xbb\xbf" + ("\n".join(lines)).encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "text/csv; charset=utf-8")
            self.send_header("Content-Disposition", 'attachment; filename="{}_voqealar.csv"'.format(device))
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        elif parsed.path == "/readings.csv":
            try:
                now_utc = time.time()
                from_ts = parse_time_param(params.get("from", [None])[0], now_utc - 3600)
                to_ts = parse_time_param(params.get("to", [None])[0], now_utc)
            except Exception as e:
                self._send_error(400, e)
                return

            with db_lock:
                rows = conn.execute(
                    "SELECT received_ts, status, volt, amp, watt, motosoat, energy, freq, pf FROM readings "
                    "WHERE device = ? AND received_ts BETWEEN ? AND ? ORDER BY received_ts ASC",
                    (device, from_ts, to_ts),
                ).fetchall()
            lines = ["Vaqt;Status;Volt;Amper;Watt;Motosoat;Energiya;Chastota;CosPhi"]
            for r in rows:
                vaqt = time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime(r[0] + UZ_OFFSET)) if r[0] else ""
                lines.append(";".join([vaqt] + [str(v).replace(".", ",") for v in r[1:]]))
            body = b"\xef\xbb\xbf" + ("\n".join(lines)).encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "text/csv; charset=utf-8")
            self.send_header("Content-Disposition", 'attachment; filename="{}_malumotlar.csv"'.format(device))
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        elif parsed.path == "/events.html":
            try:
                from_ts = parse_time_param(params.get("from", [None])[0], 0)
                to_ts = parse_time_param(params.get("to", [None])[0], time.time())
            except Exception as e:
                self._send_error(400, e)
                return
            with db_lock:
                rows = conn.execute(
                    "SELECT event_time_local, event_type, motosoat FROM cycle_events "
                    "WHERE device = ? AND event_ts BETWEEN ? AND ? "
                    "ORDER BY event_ts DESC LIMIT 1000",
                    (device, from_ts, to_ts),
                ).fetchall()
            # Ro'yxatning o'zi "jami qancha ishladi" degan savolga javob
            # bermaydi - odam ON/OFF larni qo'lda qo'shib chiqishi kerak
            # bo'lardi. Shuning uchun jami vaqt shu yerda hisoblanadi.
            #
            # DIQQAT: bu chaqiruv db_lock ICHIDA bo'lmasligi SHART -
            # compute_hours_range() ichida qulf qaytadan olinadi.
            jami_soat = compute_hours_range(conn, device, from_ts, to_ts)
            # BOOT qatorlari alohida belgilanadi - foydalanuvchi ularni
            # kalochka bilan yashira oladi. BOOT drobilkaning ishi emas,
            # qurilmaning qayta yoqilgani, shuning uchun ro'yxatni chalg'itadi.
            table_rows = "".join(
                "<tr class='{}'><td>{}</td><td>{}</td><td>{:.4f}</td></tr>".format(
                    "boot" if r[1] == "BOOT" else "ish", r[0], r[1], r[2])
                for r in rows
            )
            boot_soni = sum(1 for r in rows if r[1] == "BOOT")
            # Yuklab olish havolasi HAM shu davrga tegishli bo'lishi kerak,
            # aks holda jadvalda bir davr, Excel'da butun tarix chiqadi.
            span = ""
            if params.get("from") or params.get("to"):
                span = "&from={}&to={}".format(
                    quote(params.get("from", [""])[0]), quote(params.get("to", [""])[0]))
                sarlavha = ("{} - {} oralig&#39;idagi voqealar "
                            "(<span id='soni'>{}</span> ta)").format(
                    params.get("from", ["boshidan"])[0],
                    params.get("to", ["hozirgacha"])[0], len(rows))
            else:
                sarlavha = ("ON/OFF voqealari "
                            "(oxirgi <span id='soni'>{}</span> ta)").format(len(rows))
            html = (
                "<!doctype html><html><head><meta charset='utf-8'>"
                "<title>{device} - voqealar</title><style>{style}</style></head><body>"
                "<h2>{device} - {sarlavha}</h2>"
                "<div class='jami'>Jami ishlagan vaqt: <b>{jami}</b></div><br>"
                "<label class='chk'>"
                "<input type='checkbox' id='bootChk' checked onchange='bootToggle()'>"
                " BOOT ({boot_soni} ta) ko&#39;rsatilsin"
                "<span class='izoh'>BOOT - qurilma qayta yoqilgani, drobilkaning ishi emas</span>"
                "</label>"
                "<a class='download' id='yuk' href='/events.csv?zavod={device}{span}'>"
                "Excel (CSV) yuklab olish</a>"
                "<table><tr><th>Vaqt</th><th>Holat</th><th>Motosoat</th></tr>{rows}</table>"
                "<script>"
                "var JAMI={jami_qator},BOOTLAR={boot_soni},"
                "ASOS=\"/events.csv?zavod={device}{span}\";"
                "function bootToggle(){{"
                "var ko=document.getElementById('bootChk').checked;"
                "var q=document.querySelectorAll('tr.boot');"
                "for(var i=0;i<q.length;i++){{q[i].style.display=ko?'':'none';}}"
                "var s=document.getElementById('soni');"
                "if(s){{s.textContent=ko?JAMI:JAMI-BOOTLAR;}}"
                "document.getElementById('yuk').href=ASOS+(ko?'':'&boot=0');"
                "}}"
                "</script>"
                "</body></html>"
            ).format(device=device, style=EVENTS_TABLE_STYLE, rows=table_rows,
                     sarlavha=sarlavha, span=span, jami=soat_matn(jami_soat),
                     boot_soni=boot_soni, jami_qator=len(rows))
            body = html.encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        else:
            self._send_error(404, "not found")

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
