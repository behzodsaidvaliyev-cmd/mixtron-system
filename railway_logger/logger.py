import os
import hmac
import shutil
import socket
import sqlite3
import tempfile
import time
import json
import calendar
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import urllib.request
from urllib.parse import urlparse, parse_qs, quote, urlencode
import paho.mqtt.client as mqtt

import hisobot

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

# --- Jimlik nazorati ---
# Qurilma o'chsa yoki internetdan uzilsa, buni hech kim sezmasdan kunlab
# ma'lumot yo'qolishi mumkin - bir marta uchala qurilma olti kun jim qolgan
# va buni ancha keyin payqaganmiz. Shuning uchun jimlik alohida kuzatiladi.
# Bundan oldingi sana = qurilmada soat bo'lmagan (2000-yil) yoki NTP
# aylanishiga tushgan (2036-yil) degani. Loyiha 2026-yilda boshlangan.
EVENT_TS_MIN = 1750000000       # ~2025-06-15

# Bir martalik tuzatish. zavod3 da 2026-09-06 21:31 da yozilgan uchta voqea
# 5 soatlik uzilishdan keyin yuborilgan va serverda QABUL QILINGAN lahza
# (02:40:05) bilan saqlangan. Haqiqiy vaqt taxmin emas - o'sha motosoat
# qiymatlari o'lchovlarda aynan quyidagi soniyalarda qayd etilgan.
# Sabab qurilma kodida tuzatilgan, shuning uchun bu ro'yxat o'smaydi.
# Ustunlar: zavod, bazadagi sana, holat, motosoat, haqiqiy sana
BIR_MARTALIK_TUZATISH = [
    ("zavod3", "2026-09-07 02:40:05", "BOOT", 197.8959, "2026-09-06 21:31:44"),
    ("zavod3", "2026-09-07 02:40:05", "OFF",  197.8959, "2026-09-06 21:31:44"),
    ("zavod3", "2026-09-07 02:40:05", "ON",   197.8964, "2026-09-06 21:31:50"),
]

SILENCE_LIMIT_S = int(os.environ.get("SILENCE_LIMIT_S", "600"))   # 10 daqiqa
SILENCE_CHECK_S = int(os.environ.get("SILENCE_CHECK_S", "60"))    # tekshirish oralig'i
SILENCE_FORGET_S = 7 * 86400        # shuncha vaqt ko'rinmagan qurilma kuzatilmaydi
TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN", "")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "")
# "MIXTRON ZAVOD HOLATI" kanali: zavod yondi/o'chdi xabarlari va hisobotlar.
# Aloqa uzilishi ogohlantirishlari faqat shaxsiy chatga (TELEGRAM_CHAT_ID) boradi -
# foydalanuvchi so'rovi bilan kanaldan olib tashlangan (2026-10-26).
TELEGRAM_CHANNEL_ID = os.environ.get("TELEGRAM_CHANNEL_ID", "")
KECHIKKAN_VOQEA_S = 300     # voqea bundan kech kelsa "kechikib keldi" deb belgilanadi

# Kunlik infografika hisobot (hisobot.py): har kuni shu vaqtda o'tgan kun uchun
HISOBOT_VAQTI = os.environ.get("HISOBOT_VAQTI", "00:02")
# Smena hisoboti: 08:00 dan keyingi kun 08:00 gacha, shu vaqtda yuboriladi
SMENA_VAQTI = os.environ.get("SMENA_VAQTI", "08:01")
SMENA_BOSHI_SOAT = 8
HISOBOT_ZAVODLAR = [z.strip() for z in os.environ.get("HISOBOT_ZAVODLAR", "zavod1,zavod2,zavod3").split(",") if z.strip()]
HAFTA_KUNLARI = ["dushanba", "seshanba", "chorshanba", "payshanba", "juma", "shanba", "yakshanba"]

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

    # Indekslarsiz har bir /today_hours, /month_hours, /readings.csv so'rovi
    # millionlab qatorni boshidan oxirigacha o'qirdi. Dashboard bularni har
    # 5 soniyada so'ragani uchun CPU doim band bo'lib, Railway hisobi oyiga
    # $9 ga chiqib ketgan. Birinchi ishga tushishda bir necha soniya oladi.
    conn.execute("CREATE INDEX IF NOT EXISTS idx_readings_device_ts ON readings (device, received_ts)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_readings_ts ON readings (received_ts)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_events_device_ts ON cycle_events (device, event_ts)")
    # Kunlik hisobot qaysi sana uchun yuborilgani - qayta ishga tushganda takrorlanmasin
    conn.execute("CREATE TABLE IF NOT EXISTS hisobot_yuborilgan (sana TEXT PRIMARY KEY, yuborilgan_ts INTEGER)")
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

    # ISHONCHSIZ SANALI voqealarni tarix boshiga ko'chirish.
    #
    # Qurilmalarni sozlash kunlarida soat himoyasi hali yo'q edi: soati
    # sozlanmagan qurilma voqealarni 2000-yil sanasi bilan, NTP aylanishiga
    # tushgani esa 2036-yil sanasi bilan yozib yuborgan. 2036-yilgi yozuv
    # ro'yxatda eng tepaga chiqib, haqiqiy voqealarni pastga suradi.
    #
    # Yozuv O'CHIRILMAYDI: faqat sanasi shu qurilmaning eng birinchi haqiqiy
    # o'lchovi vaqtiga qo'yiladi. Motosoat va holat qiymatlari saqlanadi.
    hozir = time.time()
    yomon = conn.execute(
        "SELECT id, device, event_ts FROM cycle_events "
        "WHERE event_ts < ? OR event_ts > ?",
        (EVENT_TS_MIN, hozir + 86400),
    ).fetchall()
    if yomon:
        birinchi = {}
        for row_id, device, eski_ts in yomon:
            if device not in birinchi:
                r = conn.execute(
                    "SELECT MIN(received_ts) FROM readings WHERE device = ? AND received_ts >= ?",
                    (device, EVENT_TS_MIN),
                ).fetchone()
                birinchi[device] = (r[0] if r and r[0] else hozir)
            yangi_ts = birinchi[device]
            conn.execute(
                "UPDATE cycle_events SET event_ts = ?, event_time_local = ? WHERE id = ?",
                (yangi_ts,
                 time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime(yangi_ts + UZ_OFFSET)),
                 row_id),
            )
            print("[DB] {} : {} -> {} (ishonchsiz sana to'g'rilandi)".format(
                device,
                time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime(eski_ts + UZ_OFFSET)),
                time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime(yangi_ts + UZ_OFFSET))))
        conn.commit()
        print("[DB] jami {} ta ishonchsiz sanali voqea to'g'rilandi".format(len(yomon)))

    # Aniq ma'lum bo'lgan yakka yozuvlarni to'g'rilash (yuqoridagi ro'yxat).
    # Sana bazadagi qiymatga TO'LIQ mos kelsagina almashtiriladi - shu sabab
    # ikkinchi ishga tushirishda hech narsa o'zgarmaydi.
    tuzatildi = 0
    for device, eski_local, tur, ms, yangi_local in BIR_MARTALIK_TUZATISH:
        try:
            eski_ts = local_str_to_utc_ts(eski_local)
            yangi_ts = local_str_to_utc_ts(yangi_local)
        except Exception:
            continue
        cur = conn.execute(
            "UPDATE cycle_events SET event_ts = ?, event_time_local = ? "
            "WHERE device = ? AND event_ts = ? AND event_type = ? "
            "AND ABS(motosoat - ?) < 0.00005",
            (yangi_ts,
             time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime(yangi_ts + UZ_OFFSET)),
             device, eski_ts, tur, ms),
        )
        if cur.rowcount:
            tuzatildi += cur.rowcount
            print("[DB] {} : {} {} -> {} (aniqlangan haqiqiy vaqt)".format(
                device, tur, eski_local, yangi_local))
    if tuzatildi:
        conn.commit()
        print("[DB] jami {} ta voqea sanasi aniqlashtirildi".format(tuzatildi))

    return conn


def on_connect(client, userdata, flags, rc, properties=None):
    print("[MQTT] connected ({}), rc =".format(userdata.get("broker")), rc)
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
            # Qo'shimcha maydonlar (masalan BOOT uchun "rc=3" - qayta yuklanish
            # sababi: 1 quvvat/brownout, 2 dastur o'zi (machine.reset), 3 WDT) faqat logda ko'rinadi.
            print("[EVENT] saved ({}): {} at {} (mahalliy vaqt) motosoat = {}{}".format(
                device, event_type, event_time_local, motosoat,
                " " + " ".join(parts[4:]) if len(parts) > 4 else ""))
            if event_type in ("ON", "OFF"):          # BOOT - qurilma yoqilgani, drobilka ishi emas
                kanalga_yubor(holat_xabari(conn, device, event_type, event_ts, now))
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
            # Dashboard "BOOT siz" ochsa (&boot=0), sahifa DARROV shu holatda
            # ochilishi kerak - foydalanuvchi qayta bosib o'tirmasin.
            boot_kerak = params.get("boot", ["1"])[0] != "0"
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
                "<input type='checkbox' id='bootChk' {boot_belgi} onchange='bootToggle()'>"
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
                "bootToggle();"          # sahifa ochilishida holat qo'llaniladi
                "</script>"
                "</body></html>"
            ).format(device=device, style=EVENTS_TABLE_STYLE, rows=table_rows,
                     sarlavha=sarlavha, span=span, jami=soat_matn(jami_soat),
                     boot_soni=boot_soni, jami_qator=len(rows),
                     boot_belgi="checked" if boot_kerak else "")
            body = html.encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        elif parsed.path == "/export.db":
            # Faqat ko'chirish uchun. EXPORT_TOKEN o'rnatilmagan bo'lsa
            # endpoint umuman yo'qdek javob beradi.
            token = os.environ.get("EXPORT_TOKEN", "")
            if not token or not hmac.compare_digest(params.get("token", [""])[0], token):
                self._send_error(404, "not found")
                return
            fd, nusxa = tempfile.mkstemp(suffix=".db")
            os.close(fd)
            try:
                # backup() yozilayotgan baza'dan ham butun, izchil nusxa oladi
                dst = sqlite3.connect(nusxa)
                with db_lock:
                    # Ochiq (commit qilinmagan) tranzaksiya bo'lsa backup
                    # cheksiz kutib qoladi - get_conn() dagi 0 qatorli
                    # UPDATE ham shunday tranzaksiya ochib qoldiradi.
                    if conn.in_transaction:
                        conn.commit()
                    conn.backup(dst)
                dst.close()
                self.send_response(200)
                self.send_header("Content-Type", "application/octet-stream")
                self.send_header("Content-Length", str(os.path.getsize(nusxa)))
                self.end_headers()
                with open(nusxa, "rb") as f:
                    shutil.copyfileobj(f, self.wfile, 1024 * 1024)
            finally:
                os.remove(nusxa)

        elif parsed.path == "/latest":
            # Dashboard faqat oxirgi o'lchovni ko'rish uchun butun soatlik
            # /readings.csv ni yuklab olardi. Bu bitta qatorni qaytaradi.
            with db_lock:
                r = conn.execute(
                    "SELECT received_ts, status, volt, amp, watt, motosoat, energy, freq, pf FROM readings "
                    "WHERE device = ? ORDER BY received_ts DESC LIMIT 1",
                    (device,),
                ).fetchone()
            if not r:
                self._send_error(404, "olchov yoq")
                return
            self._send_json({
                "zavod": device,
                "received_ts": r[0],
                "vaqt": time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime(r[0] + UZ_OFFSET)) if r[0] else "",
                "status": r[1], "volt": r[2], "amp": r[3], "watt": r[4],
                "motosoat": r[5], "energy": r[6], "freq": r[7], "pf": r[8],
            })

        elif parsed.path == "/hisobot.png":
            # Hisobot rasmini ko'rish (kanalga yuborishdan oldin tekshirish uchun).
            # ?sana=YYYY-MM-DD, berilmasa - kecha. ?tur=smena - sana 08:00 dan
            # keyingi kun 08:00 gacha bo'lgan smena hisoboti.
            try:
                sana = params.get("sana", [None])[0] or kechagi_sana()
                if params.get("tur", [""])[0] == "smena":
                    png, _ = smena_hisoboti(conn, sana)
                else:
                    png, _ = kunlik_hisobot(conn, sana)
            except Exception as e:
                self._send_error(400, e)
                return
            self.send_response(200)
            self.send_header("Content-Type", "image/png")
            self.send_header("Content-Length", str(len(png)))
            self.end_headers()
            self.wfile.write(png)

        elif parsed.path == "/holat":
            self._send_json({
                "chegara_daqiqa": SILENCE_LIMIT_S // 60,
                "zavodlar": jimlik_holati(conn),
            })

        else:
            self._send_error(404, "not found")

    def log_message(self, format, *args):
        pass  # standart konsolni shovqindan tozalash


def telegram_yubor(matn, chat_id=None):
    """Xabar Telegramga yuboriladi. Sozlanmagan bo'lsa xizmat baribir ishlaydi -
    xabar faqat jurnalga tushadi, shunda sozlashdan oldin sinab ko'rish mumkin."""
    chat_id = chat_id or TELEGRAM_CHAT_ID
    print("[OGOH]" if chat_id == TELEGRAM_CHAT_ID else "[KANAL]", matn.replace(chr(10), " | "))
    if not (TELEGRAM_TOKEN and chat_id):
        return False
    try:
        url = "https://api.telegram.org/bot{}/sendMessage".format(TELEGRAM_TOKEN)
        data = urlencode({"chat_id": chat_id, "text": matn}).encode("utf-8")
        with urllib.request.urlopen(url, data=data, timeout=15) as r:
            r.read()
        return True
    except Exception as e:
        print("[OGOH] Telegramga yuborilmadi:", e)
        return False


def kanalga_yubor(matn):
    """Kanalga ALOHIDA oqimda yuboradi: Telegram sekin javob bersa ham MQTT
    xabarlarini qabul qilish va bazaga yozish to'xtab qolmasin."""
    if not TELEGRAM_CHANNEL_ID:
        return
    threading.Thread(target=telegram_yubor, args=(matn, TELEGRAM_CHANNEL_ID), daemon=True).start()


def zavod_nomi(device):
    """zavod2 -> Zavod 2"""
    if device.startswith("zavod") and device[5:].isdigit():
        return "Zavod " + device[5:]
    return device


def holat_xabari(conn, device, event_type, event_ts, hozir):
    belgi, soz = ("🟢", "YONDI") if event_type == "ON" else ("🔴", "O'CHDI")
    vaqt = time.gmtime(event_ts + UZ_OFFSET)
    bugun = time.gmtime(hozir + UZ_OFFSET)
    if vaqt[:3] == bugun[:3]:
        soat = time.strftime("%H:%M", vaqt)
    else:
        soat = time.strftime("%d.%m %H:%M", vaqt)
    qatorlar = ["{} {} — {}".format(belgi, zavod_nomi(device), soz), "🕐 " + soat]
    try:
        qatorlar.append("Bugun ishlagan: " + hisobot.soat_daqiqa(compute_hours_today(conn, device)))
    except Exception as e:
        print("[KANAL] bugungi soat hisoblanmadi:", e)
    if hozir - event_ts > KECHIKKAN_VOQEA_S:
        qatorlar.append("⏳ kechikib keldi (aloqa uzilgan edi)")
    return chr(10).join(qatorlar)


def kechagi_sana(hozir=None):
    hozir = time.time() if hozir is None else hozir
    return time.strftime("%Y-%m-%d", time.gmtime(hozir + UZ_OFFSET - 86400))


def kunlik_hisobot(conn, sana):
    """sana - 'YYYY-MM-DD' (mahalliy). (png, izoh) qaytaradi."""
    kun_boshi = local_midnight_utc_ts(sana)
    statlar = [
        hisobot.kun_statistikasi(conn, db_lock, z, kun_boshi,
                                 lambda d, a, b: compute_hours_range(conn, d, a, b))
        for z in HISOBOT_ZAVODLAR
    ]
    t = time.strptime(sana, "%Y-%m-%d")
    qisqa = time.strftime("%d.%m.%Y", t)
    return (hisobot.rasm_chizish(statlar, qisqa + ", " + HAFTA_KUNLARI[t.tm_wday]),
            hisobot.izoh_matni(statlar, qisqa))


def smena_hisoboti(conn, sana):
    """sana - smena boshlangan kun ('YYYY-MM-DD'): sana 08:00 -> keyingi kun 08:00."""
    boshi = local_midnight_utc_ts(sana) + SMENA_BOSHI_SOAT * 3600
    statlar = [
        hisobot.kun_statistikasi(conn, db_lock, z, boshi,
                                 lambda d, a, b: compute_hours_range(conn, d, a, b))
        for z in HISOBOT_ZAVODLAR
    ]
    t1 = time.gmtime(boshi + UZ_OFFSET)
    t2 = time.gmtime(boshi + 86400 + UZ_OFFSET)
    oraliq = "{} {:02d}:00 – {} {:02d}:00".format(
        time.strftime("%d.%m", t1), SMENA_BOSHI_SOAT, time.strftime("%d.%m.%Y", t2), SMENA_BOSHI_SOAT)
    return (hisobot.rasm_chizish(statlar, oraliq, sarlavha="smena hisoboti",
                                 boshlanish_daq=SMENA_BOSHI_SOAT * 60),
            hisobot.izoh_matni(statlar, oraliq))


def telegram_rasm(png, izoh, chat_id):
    """sendPhoto - multipart/form-data, faqat standart kutubxona bilan."""
    if not (TELEGRAM_TOKEN and chat_id):
        return False
    chegara = "----mixtron" + str(int(time.time() * 1000))
    qismlar = []
    maydonlar = [("chat_id", str(chat_id))]
    if izoh:
        maydonlar.append(("caption", izoh))
    for nom, qiymat in maydonlar:
        qismlar.append(('--{}\r\nContent-Disposition: form-data; name="{}"\r\n\r\n{}\r\n'
                        .format(chegara, nom, qiymat)).encode("utf-8"))
    qismlar.append(('--{}\r\nContent-Disposition: form-data; name="photo"; filename="hisobot.png"\r\n'
                    'Content-Type: image/png\r\n\r\n'.format(chegara)).encode("utf-8") + png + b"\r\n")
    qismlar.append("--{}--\r\n".format(chegara).encode("utf-8"))
    req = urllib.request.Request(
        "https://api.telegram.org/bot{}/sendPhoto".format(TELEGRAM_TOKEN),
        data=b"".join(qismlar),
        headers={"Content-Type": "multipart/form-data; boundary=" + chegara})
    try:
        with urllib.request.urlopen(req, timeout=60) as r:
            r.read()
        return True
    except Exception as e:
        print("[HISOBOT] Telegramga yuborilmadi:", e)
        return False


# (nom, yuborish vaqti, shu soatdan keyin o'tkazib yuboriladi, baza kaliti, yasovchi)
# Kalit: kunlik - "YYYY-MM-DD" (avvalgi yozuvlar bilan mos), smena - "smena:YYYY-MM-DD".
def _hisobot_turlari():
    return (
        ("kunlik", HISOBOT_VAQTI, 6, lambda sana: sana, kunlik_hisobot),
        ("smena", SMENA_VAQTI, 12, lambda sana: "smena:" + sana, smena_hisoboti),
    )


def hisobot_kuzatuvchisi(conn):
    """Fon oqimi: har kuni kunlik (00:02) va smena (08:01) hisobotlarini
    kanalga BIR MARTADAN yuboradi. Ikkalasi ham kechagi sana bo'yicha: kunlik -
    kechagi 00:00-24:00, smena - kechagi 08:00 dan bugungi 08:00 gacha.
    Yuborilgani bazada belgilanadi - deploy yoki qayta ishga tushish takror
    yuborishga olib kelmaydi. Kechikib ishga tushsa ham kunlik 06:00 gacha,
    smena 12:00 gacha yuboriladi; undan keyin o'tkazib yuboriladi (eski
    hisobot kun o'rtasida chiqib qolmasin)."""
    print("[HISOBOT] yoqildi: kunlik {}, smena {} da; zavodlar: {}".format(
        HISOBOT_VAQTI, SMENA_VAQTI, ",".join(HISOBOT_ZAVODLAR)))
    oxirgi_urinish = {}
    while True:
        time.sleep(30)
        if not TELEGRAM_CHANNEL_ID:
            continue
        for nom, vaqt, chegara_soat, kalit_fn, yasovchi in _hisobot_turlari():
            try:
                hozir = time.time()
                mahalliy = time.gmtime(hozir + UZ_OFFSET)
                if not (time.strftime("%H:%M", mahalliy) >= vaqt and mahalliy.tm_hour < chegara_soat):
                    continue
                if hozir - oxirgi_urinish.get(nom, 0) < 300:   # xato bo'lsa 5 daqiqada qayta
                    continue
                sana = kechagi_sana(hozir)
                kalit = kalit_fn(sana)
                with db_lock:
                    bor = conn.execute("SELECT 1 FROM hisobot_yuborilgan WHERE sana = ?", (kalit,)).fetchone()
                if bor:
                    continue
                oxirgi_urinish[nom] = hozir
                png, _ = yasovchi(conn, sana)
                # rasm matnsiz yuboriladi - foydalanuvchi so'rovi (hamma narsa rasmda)
                if telegram_rasm(png, "", TELEGRAM_CHANNEL_ID):
                    with db_lock:
                        conn.execute("INSERT OR REPLACE INTO hisobot_yuborilgan VALUES (?, ?)", (kalit, int(hozir)))
                        conn.commit()
                    print("[HISOBOT] yuborildi:", kalit)
            except Exception as e:
                print("[HISOBOT] xato ({}):".format(nom), e)


def jimlik_holati(conn, hozir=None):
    """Har bir qurilma uchun: oxirgi ma'lumot qachon kelgan va jim qolganmi."""
    hozir = time.time() if hozir is None else hozir
    with db_lock:
        rows = conn.execute(
            "SELECT device, MAX(received_ts) FROM readings "
            "WHERE received_ts > ? GROUP BY device",
            (hozir - SILENCE_FORGET_S,),
        ).fetchall()
    natija = []
    for device, oxirgi in rows:
        if not oxirgi:
            continue
        jim_s = hozir - oxirgi
        natija.append({
            "zavod": device,
            "oxirgi": time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime(oxirgi + UZ_OFFSET)),
            "jim_daqiqa": int(jim_s // 60),
            "jim": jim_s >= SILENCE_LIMIT_S,
        })
    natija.sort(key=lambda x: x["zavod"])
    return natija


_jim_belgilangan = {}      # qurilma -> ogohlantirish yuborilganmi


def jimlik_kuzatuvchisi(conn):
    """Fon oqimi: jimlik boshlanganda va tugaganda BITTADAN xabar yuboradi.
    Holat o'zgarmasa qayta yubormaydi - aks holda har daqiqada bezovta qilardi."""
    print("[OGOH] jimlik nazorati yoqildi: chegara {} daqiqa, Telegram {}".format(
        SILENCE_LIMIT_S // 60,
        "sozlangan" if (TELEGRAM_TOKEN and TELEGRAM_CHAT_ID) else "SOZLANMAGAN"))
    while True:
        time.sleep(SILENCE_CHECK_S)
        try:
            for h in jimlik_holati(conn):
                zavod = h["zavod"]
                oldin = _jim_belgilangan.get(zavod, False)
                if h["jim"] and not oldin:
                    _jim_belgilangan[zavod] = True
                    telegram_yubor(
                        "⚠ {} - {} daqiqadan beri ma'lumot yo'q".format(
                            zavod, h["jim_daqiqa"])
                        + chr(10) + "Oxirgi ma'lumot: " + h["oxirgi"])
                elif (not h["jim"]) and oldin:
                    _jim_belgilangan[zavod] = False
                    telegram_yubor("✅ {} - aloqa tiklandi".format(zavod))
        except Exception as e:
            print("[OGOH] kuzatuvda xato:", e)


class DualStackServer(ThreadingHTTPServer):
    # Railway ichki tarmog'i (*.railway.internal) IPv6 orqali ishlaydi,
    # tashqi domen esa IPv4. "::" + V6ONLY=0 ikkalasini ham qabul qiladi.
    address_family = socket.AF_INET6

    def server_bind(self):
        self.socket.setsockopt(socket.IPPROTO_IPV6, socket.IPV6_V6ONLY, 0)
        super().server_bind()


def run_http_server(conn):
    try:
        server = DualStackServer(("::", HTTP_PORT), Handler)
    except OSError:
        server = ThreadingHTTPServer(("0.0.0.0", HTTP_PORT), Handler)
    server.db_conn = conn
    print("[HTTP] /today_hours xizmati port", HTTP_PORT, "da ishga tushdi")
    server.serve_forever()


def bazani_urug_dan_olish():
    """Ko'chirish uchun: baza fayli hali yo'q bo'lsa va SEED_URL berilgan
    bo'lsa, eski servisning /export.db sidan bazani yuklab oladi.
    Baza allaqachon bor bo'lsa hech narsa qilmaydi - qayta ishga tushganda
    ustidan yozib yubormaydi."""
    seed_url = os.environ.get("SEED_URL", "")
    if not seed_url or os.path.exists(DB_PATH):
        return
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    qism = DB_PATH + ".part"
    url = seed_url + ("&" if "?" in seed_url else "?") + urlencode({"token": os.environ.get("SEED_TOKEN", "")})
    print("[SEED] baza yuklanmoqda...")
    with urllib.request.urlopen(url, timeout=600) as r, open(qism, "wb") as f:
        shutil.copyfileobj(r, f, 1024 * 1024)
    tekshir = sqlite3.connect(qism)
    natija = tekshir.execute("PRAGMA quick_check").fetchone()[0]
    soni = tekshir.execute("SELECT COUNT(*) FROM readings").fetchone()[0]
    tekshir.close()
    if natija != "ok":
        os.remove(qism)
        raise RuntimeError("[SEED] yuklangan baza buzuq: " + natija)
    os.replace(qism, DB_PATH)
    print("[SEED] tayyor: {} MB, readings = {}".format(os.path.getsize(DB_PATH) // 2**20, soni))


def main():
    bazani_urug_dan_olish()
    conn = get_conn()

    http_thread = threading.Thread(target=run_http_server, args=(conn,), daemon=True)
    http_thread.start()

    ogoh_thread = threading.Thread(target=jimlik_kuzatuvchisi, args=(conn,), daemon=True)
    ogoh_thread.start()

    threading.Thread(target=hisobot_kuzatuvchisi, args=(conn,), daemon=True).start()

    # Ikkinchi broker (o'zimizning mixtron-broker, ichki tarmoq). Qurilmalar
    # ulardan biriga yuboradi - avval o'zimiznikiga, u ishlamasa HiveMQ'ga -
    # shuning uchun logger ikkalasini birdan tinglaydi. Bir xabar ikki
    # brokerga tushmaydi, demak takroriy yozuv bo'lmaydi.
    broker2 = os.environ.get("MQTT_BROKER2", "")
    if broker2:
        threading.Thread(
            target=mqtt_tsikli,
            args=(conn, broker2, int(os.environ.get("MQTT_PORT2", "1883")),
                  os.environ.get("MQTT_TLS2", "0") == "1", "-2"),
            daemon=True,
        ).start()

    mqtt_tsikli(conn, MQTT_BROKER, MQTT_PORT, True, "")


def mqtt_tsikli(conn, broker, port, tls, qoshimcha):
    client = mqtt.Client(
        callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
        # Ikki servis bir xil client_id bilan ulansa broker ularni navbatma-navbat
        # uzib turadi - ko'chirish paytida yangisiga boshqa id beriladi.
        client_id=os.environ.get("MQTT_CLIENT_ID", "railway-mixtron-logger") + qoshimcha,
        userdata={"conn": conn, "broker": broker},
    )
    client.username_pw_set(MQTT_USER, MQTT_PASSWORD)
    if tls:
        client.tls_set()
    client.on_connect = on_connect
    client.on_message = on_message

    while True:
        try:
            client.connect(broker, port, 60)
            client.loop_forever()
        except Exception as e:
            print("[MQTT] connection error ({}):".format(broker), e)
            time.sleep(5)


if __name__ == "__main__":
    main()
