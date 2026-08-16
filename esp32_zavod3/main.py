"""
ZAVOD 3 - Drobilka runtime & energy monitor
ESP32 / MicroPython v1.28.0
PZEM-004T (UART2, TX=17, RX=16) -> moto-hours -> HiveMQ Cloud (MQTT/SSL)
"""

import ujson
import machine
import network
import time
import ssl
import gc
import os
import sys
import select
import socket
from machine import UART, Pin, WDT

try:
    from umqtt.simple import MQTTClient
except ImportError:
    raise RuntimeError("umqtt.simple topilmadi - mip.install('umqtt.simple')")

# ---------------------------------------------------------------------------
# CONFIG
# ---------------------------------------------------------------------------

DEVICE = "zavod3"

WIFI_CONFIG_FILE = "wifi_config.json"
MQTT_CONFIG_FILE = "mqtt_config.json"   # faqat qurilmada, GitHub'ga hech qachon ketmaydi
MOTOHOURS_FILE = "motohours.txt"
EVENTS_QUEUE_FILE = "events_queue.txt"  # internet yo'q paytda ON/OFF voqealari
THRESHOLD_FILE = "threshold.txt"
STATUS_FILE = "last_status.txt"         # reboot'dan keyin soxta voqea yozilmasligi uchun
BOOT_ID_FILE = "boot_id.txt"            # oflayn voqea qaysi yoqilishda yozilganini bilish uchun

MQTT_PORT = 8883
MQTT_KEEPALIVE = 60
MQTT_TOPIC = DEVICE + "/data"
MQTT_EVENTS_TOPIC = DEVICE + "/events"
MQTT_COMMAND_TOPIC = DEVICE + "/command"

DEFAULT_AMP_THRESHOLD = 1.5
POLL_INTERVAL_S = 2             # PZEM o'qish oralig'i
PUBLISH_INTERVAL_S = 5          # MQTT yuborish oralig'i
MOTOHOURS_SAVE_INTERVAL_S = 60  # flesh xotiraga yozish oralig'i
CHECK_MSG_INTERVAL_S = 1        # MQTT buyruqlarni tekshirish oralig'i

WIFI_CONNECT_TIMEOUT_S = 15
WIFI_RETRY_INTERVAL_S = 20      # ulanmasa, shuncha soniyada qayta urinadi
WIFI_HEALTH_INTERVAL_S = 30     # ish paytida WiFi uzilmaganini tekshirish oralig'i
NTP_RETRY_INTERVAL_S = 60       # vaqt sozlanmagan bo'lsa, qayta urinish oralig'i
MAX_SANE_DT_S = 300             # bundan katta vaqt farqi = soat sakragan, hisobga olinmaydi

PZEM_SLAVE_ADDR = 0xF8
PZEM_DEBUG = False              # True qilinsa xom baytlarni chop etadi (nosozlik izlashda)
PZEM_REINIT_AFTER_FAILS = 15    # ~30 soniya javobsizlikdan keyin UART qayta ishga tushadi

SOCKET_TIMEOUT_S = 15           # HECH QANDAY tarmoq amali bundan uzoq kutmaydi
WDT_TIMEOUT_MS = 120000         # asosiy tsikl shuncha qotib qolsa - majburiy reboot
BOOT_SAFE_WINDOW_S = 5          # WDT yoqilishidan oldingi "qutqaruv oynasi"

EVENTS_QUEUE_MAX_LINES = 500    # navbat cheksiz o'smasligi uchun chegara
PRINT_DATA_LINES = True         # serial'ga har o'qishni chop etish (nosozlik izlashda qulay)

OTA_ENABLED = True
OTA_URL = "https://raw.githubusercontent.com/behzodsaidvaliyev-cmd/mixtron-system/main/esp32_zavod3/main.py"
OTA_CHECK_INTERVAL_S = 86400    # kuniga bir marta
OTA_END_MARKER = "OTA-FAYL-OXIRI"  # fayl oxirida turadi; yuklash to'liqligini isbotlaydi

UNIX_EPOCH_OFFSET = 946684800   # MicroPython 2000-yildan, Unix 1970-yildan sanaydi

# ---------------------------------------------------------------------------
# TARMOQ: har bir socket'ga majburiy vaqt chegarasi
# ---------------------------------------------------------------------------
# MUHIM: umqtt/urequests o'zi socket yaratganda timeout QO'YMAYDI. Timeout'siz
# socket "yarim ochiq" ulanishda (router qayta yuklangandan keyin tez-tez
# uchraydi) ABADIY kutib qoladi -> WDT reboot -> yana o'sha holat -> cheksiz
# qayta yuklanish tsikli. Aynan shu sabab qurilmalar "o'lgan"dek ko'rinadi.
#
# Ikkinchi tuzoq: umqtt'ning check_msg() ichida setblocking(True) chaqiriladi,
# bu MicroPython'da timeout'ni BEKOR QILADI. Shu sabab timeout har bir
# check_msg()'dan keyin qayta tiklanadi (restore_socket_timeout()).

def _install_socket_timeout(mod):
    """Modul ichidagi socket yaratuvchini timeout qo'yadigan variantga almashtiradi."""
    if getattr(mod, "_timeout_patched", False):
        return
    orig = mod.socket

    def _patched(*args, **kwargs):
        s = orig(*args, **kwargs)
        try:
            s.settimeout(SOCKET_TIMEOUT_S)
        except Exception:
            pass
        return s

    mod.socket = _patched
    mod._timeout_patched = True


# MUHIM: umqtt "usocket" nomi bilan, urequests esa "socket" nomi bilan import
# qilishi mumkin. Ba'zi MicroPython versiyalarida bular AYRIM modul obyektlari -
# faqat bittasini tuzatsak, ikkinchisi himoyasiz qoladi (aynan MQTT qotib
# qolardi). Shu sabab ikkalasi ham tuzatiladi.
_install_socket_timeout(socket)
try:
    import usocket as _usocket
    if _usocket is not socket:
        _install_socket_timeout(_usocket)
except ImportError:
    pass


def restore_socket_timeout(client):
    """check_msg()/wait_msg() setblocking(True) qilib timeout'ni o'chiradi -
    har safar qayta tiklanadi, aks holda publish() abadiy qotib qolishi mumkin."""
    if client is None:
        return
    try:
        client.sock.settimeout(SOCKET_TIMEOUT_S)
    except Exception:
        pass


# ---------------------------------------------------------------------------
# KONFIG FAYLLAR
# ---------------------------------------------------------------------------

def load_mqtt_config():
    with open(MQTT_CONFIG_FILE) as f:
        cfg = ujson.load(f)
    return cfg["broker"], cfg["user"], cfg["password"]


def load_wifi_config():
    with open(WIFI_CONFIG_FILE) as f:
        cfg = ujson.load(f)
    return cfg["ssid"].strip(), cfg["password"].strip()


def set_wifi(ssid, password):
    _atomic_write(WIFI_CONFIG_FILE, ujson.dumps({"ssid": ssid, "password": password}))


def _replace(src, dst):
    """src -> dst ko'chirish, dst mavjud bo'lsa ham.
    MUHIM: littlefs'da os.rename mavjud faylni bosib yozadi, lekin FAT bilan
    formatlangan qurilmada XATO beradi. Bunga tayanib qolsak, motosoat/holat
    saqlash JIMGINA ishlamay qolishi mumkin edi."""
    try:
        os.rename(src, dst)
        return
    except OSError:
        pass
    try:
        os.remove(dst)
    except OSError:
        pass
    os.rename(src, dst)


def _atomic_write(path, text):
    """Yozish yarmida quvvat uzilsa ham fayl buzilmasligi uchun."""
    tmp = path + ".tmp"
    with open(tmp, "w") as f:
        f.write(text)
    _replace(tmp, path)


def _read_float(path, default):
    try:
        with open(path) as f:
            return float(f.read().strip())
    except (OSError, ValueError):
        return default


def load_threshold():
    return _read_float(THRESHOLD_FILE, DEFAULT_AMP_THRESHOLD)


def save_threshold(value):
    _atomic_write(THRESHOLD_FILE, str(value))


def load_motohours():
    return _read_float(MOTOHOURS_FILE, 0.0)


def save_motohours(hours):
    _atomic_write(MOTOHOURS_FILE, "{:.6f}".format(hours))


def load_last_status():
    """Reboot'dan keyin takroriy soxta ON/OFF voqeasi yozilmasligi uchun."""
    try:
        with open(STATUS_FILE) as f:
            v = f.read().strip()
            return v if v in ("ON", "OFF") else None
    except OSError:
        return None


def save_last_status(status):
    try:
        _atomic_write(STATUS_FILE, status)
    except Exception:
        pass


# Konfig buzilgan/yo'q bo'lsa ham dastur ISHGA TUSHISHI kerak - aks holda
# qurilma REPL'ga tushib, hech qanday buyruq qabul qilmay qoladi va uni faqat
# to'liq qayta flash qilib tiklash mumkin bo'lardi.
CONFIG_OK = True
try:
    MQTT_BROKER, MQTT_USER, MQTT_PASSWORD = load_mqtt_config()
except Exception as _e:
    CONFIG_OK = False
    MQTT_BROKER, MQTT_USER, MQTT_PASSWORD = "", "", ""
    print("[CONFIG] mqtt_config.json o'qilmadi:", _e)

MQTT_CLIENT_ID = "esp32-" + DEVICE + "-" + "".join("{:02x}".format(b) for b in machine.unique_id())
amp_threshold = load_threshold()

# ---------------------------------------------------------------------------
# WIFI
# ---------------------------------------------------------------------------

wdt = None  # main() ichida yoqiladi


def feed_wdt():
    if wdt is not None:
        wdt.feed()


def connect_wifi(timeout_s=WIFI_CONNECT_TIMEOUT_S):
    ssid, password = load_wifi_config()
    wlan = network.WLAN(network.STA_IF)
    wlan.active(True)
    if not wlan.isconnected():
        print("[WIFI] ulanmoqda:", ssid)
        try:
            wlan.disconnect()
        except Exception:
            pass
        wlan.connect(ssid, password)
        t0 = time.time()
        while not wlan.isconnected():
            feed_wdt()
            if time.time() - t0 > timeout_s:
                raise RuntimeError("WiFi ulanish vaqti tugadi")
            time.sleep(0.5)
    print("[WIFI] ulandi, ip =", wlan.ifconfig()[0])
    return wlan


def wifi_is_up():
    try:
        return network.WLAN(network.STA_IF).isconnected()
    except Exception:
        return False


time_synced = False


def time_is_valid():
    """NTP sozlanmagan bo'lsa, ESP32 vaqti 2000-yildan boshlanadi. Bunday
    vaqt bilan yozilgan voqealar Railway'da 26 yil xato sana bilan tushadi,
    hisobotlarni butunlay buzadi. Shu sabab vaqt haqiqiyligi tekshiriladi."""
    # 2025-01-01 (MicroPython epoch'ida) dan katta bo'lsa - haqiqiy vaqt
    return time.time() > 789000000


def sync_time():
    """NTP sinxronizatsiyasi. Vaqt SAKRAB ketishi mumkin (2000 -> 2026), shu
    sabab chaqirgan joy taymerlarni qayta hisoblashi shart (aks holda
    dt_hours ulkan chiqib, motosoatni butunlay buzib yuboradi)."""
    global time_synced
    try:
        import ntptime
        ntptime.settime()
        time_synced = True
        print("[TIME] NTP sozlandi (UTC):", time.localtime())
        return True
    except Exception as e:
        print("[TIME] NTP xato:", e)
        return False


def _decode_ssid(raw):
    if not raw:
        return ""
    try:
        return raw.decode("utf-8")
    except Exception:
        pass
    try:
        return raw.decode("latin-1")
    except Exception:
        return ""


def scan_wifi():
    wlan = network.WLAN(network.STA_IF)
    wlan.active(True)
    seen = []
    for n in wlan.scan():
        name = _decode_ssid(n[0])
        if name and name not in seen:
            seen.append(name)
    gc.collect()
    return seen


# ---------------------------------------------------------------------------
# PZEM-004T v3.0 / Modbus RTU (UART2)
# ---------------------------------------------------------------------------

uart = UART(2, baudrate=9600, tx=Pin(17), rx=Pin(16), bits=8, parity=None, stop=1, timeout=200)
pzem_fail_count = 0


def reinit_uart():
    global uart
    try:
        uart.deinit()
    except Exception:
        pass
    time.sleep_ms(100)
    uart = UART(2, baudrate=9600, tx=Pin(17), rx=Pin(16), bits=8, parity=None, stop=1, timeout=200)
    print("[PZEM] UART qayta ishga tushirildi")


def modbus_crc16(data):
    crc = 0xFFFF
    for b in data:
        crc ^= b
        for _ in range(8):
            if crc & 0x0001:
                crc = (crc >> 1) ^ 0xA001
            else:
                crc >>= 1
    return crc & 0xFFFF


def build_read_request(slave_addr, start_reg, num_regs):
    frame = bytearray([slave_addr, 0x04,
                       (start_reg >> 8) & 0xFF, start_reg & 0xFF,
                       (num_regs >> 8) & 0xFF, num_regs & 0xFF])
    crc = modbus_crc16(frame)
    frame.append(crc & 0xFF)
    frame.append((crc >> 8) & 0xFF)
    return frame


def pzem_read():
    """Muvaffaqiyatli bo'lsa dict, aks holda None qaytaradi."""
    req = build_read_request(PZEM_SLAVE_ADDR, 0x0000, 10)
    uart.read()          # eski baytlarni tozalash
    uart.write(req)
    time.sleep_ms(150)
    resp = uart.read()

    if not resp or len(resp) < 25:
        if PZEM_DEBUG:
            print("[PZEM] javob yo'q/qisqa:", resp)
        return None

    if resp[0] != PZEM_SLAVE_ADDR or resp[1] != 0x04:
        if PZEM_DEBUG:
            print("[PZEM] kutilmagan javob:", resp)
        return None

    if modbus_crc16(resp[:-2]) != (resp[-2] | (resp[-1] << 8)):
        return None

    p = resp[3:-2]
    voltage = ((p[0] << 8) | p[1]) * 0.1
    current = (((p[2] << 8) | p[3]) | ((p[4] << 8) | p[5]) << 16) * 0.001
    power = (((p[6] << 8) | p[7]) | ((p[8] << 8) | p[9]) << 16) * 0.1
    energy = (((p[10] << 8) | p[11]) | ((p[12] << 8) | p[13]) << 16)
    freq = ((p[14] << 8) | p[15]) * 0.1
    pf = ((p[16] << 8) | p[17]) * 0.01

    if not (80.0 <= voltage <= 300.0) or not (0.0 <= current <= 100.0) or not (40.0 <= freq <= 65.0):
        if PZEM_DEBUG:
            print("[PZEM] chegaradan tashqari: V={} A={} Hz={}".format(voltage, current, freq))
        return None

    return {"voltage": voltage, "current": current, "power": power,
            "energy": energy, "freq": freq, "pf": pf}


# ---------------------------------------------------------------------------
# OTA - GitHub'dan yangilash (XOTIRAGA XAVFSIZ)
# ---------------------------------------------------------------------------
# Eski versiya butun yangi kodni VA butun joriy kodni bir vaqtda RAM'ga olardi
# (~50-60 KB) - ochiq SSL ulanish ustiga. Aynan shu MBEDTLS_ERR_MPI_ALLOC_FAILED
# xatosini bergan. Endi fayl bo'lakma-bo'lak (512 bayt) diskka yoziladi va
# solishtirish ham bo'laklab qilinadi - RAM'da hech qachon 1 KB'dan ortiq turmaydi.

def _files_identical(path_a, path_b):
    try:
        if os.stat(path_a)[6] != os.stat(path_b)[6]:
            return False
    except OSError:
        return False
    try:
        with open(path_a, "rb") as fa, open(path_b, "rb") as fb:
            while True:
                feed_wdt()
                ca = fa.read(512)
                cb = fb.read(512)
                if ca != cb:
                    return False
                if not ca:
                    return True
    except OSError:
        return False


def check_for_update():
    if not OTA_ENABLED:
        return False
    if not wifi_is_up():
        print("[OTA] WiFi yo'q, o'tkazib yuborildi")
        return False

    try:
        import urequests
    except ImportError:
        print("[OTA] urequests yo'q - mip.install('urequests')")
        return False

    tmp_path = "ota_tmp.py"
    gc.collect()
    r = None
    total = 0
    try:
        r = urequests.get(OTA_URL)
        if r.status_code != 200:
            print("[OTA] HTTP status:", r.status_code)
            return False
        with open(tmp_path, "wb") as f:
            while True:
                feed_wdt()
                try:
                    chunk = r.raw.read(512)
                except Exception:
                    break          # timeout yoki ulanish yopildi = oqim tugadi
                if not chunk:
                    break
                f.write(chunk)
                total += len(chunk)
        print("[OTA] yuklandi:", total, "bayt")
    except Exception as e:
        print("[OTA] yuklab olishda xato:", e)
    finally:
        try:
            if r is not None:
                r.close()
        except Exception:
            pass
        gc.collect()

    # Yaxlitlik tekshiruvi. Fayl OXIRIDAGI maxsus belgi tekshiriladi - agar
    # yuklash yarmida uzilgan bo'lsa, bu belgi bo'lmaydi va main.py TEGILMAYDI.
    # (Yarim fayl yozilsa qurilma butunlay ishlamay qolardi.)
    valid = False
    if total > 1000:
        try:
            size = os.stat(tmp_path)[6]
            with open(tmp_path) as f:
                f.seek(max(0, size - 200))
                tail = f.read()
            valid = OTA_END_MARKER in tail
        except Exception:
            valid = False

    if not valid:
        print("[OTA] fayl to'liq emas yoki noto'g'ri - main.py tegilmadi")
        try:
            os.remove(tmp_path)
        except OSError:
            pass
        return False

    if _files_identical(tmp_path, "main.py"):
        try:
            os.remove(tmp_path)
        except OSError:
            pass
        print("[OTA] o'zgarish yo'q")
        return False

    _replace(tmp_path, "main.py")
    print("[OTA] yangi kod o'rnatildi, qayta yuklanmoqda...")
    time.sleep(1)
    machine.reset()


# ---------------------------------------------------------------------------
# MQTT (SSL) - HiveMQ Cloud
# ---------------------------------------------------------------------------

def mqtt_message_callback(topic, msg):
    global amp_threshold
    try:
        t = topic.decode() if isinstance(topic, bytes) else topic
        cmd = (msg.decode("utf-8", "ignore") if isinstance(msg, bytes) else str(msg)).strip()
    except Exception:
        return
    if t != MQTT_COMMAND_TOPIC:
        return

    print("[MQTT] buyruq:", cmd)
    if cmd == "CHECK_UPDATE":
        try:
            check_for_update()
        except Exception as e:
            print("[OTA] xato:", e)
    elif cmd == "REBOOT":
        print("[MQTT] masofadan qayta yuklash")
        time.sleep(1)
        machine.reset()
    elif cmd.startswith("SET_THRESHOLD|"):
        try:
            value = float(cmd.split("|", 1)[1])
            amp_threshold = value
            save_threshold(value)
            print("[THRESHOLD] yangi chegara:", value)
        except Exception as e:
            print("[THRESHOLD] xato:", e)


def mqtt_connect():
    if not CONFIG_OK:
        raise RuntimeError("mqtt_config.json yo'q/buzuq - MQTT ishlamaydi")
    gc.collect()  # SSL handshake uchun maksimal bo'sh xotira
    client = MQTTClient(
        client_id=MQTT_CLIENT_ID,
        server=MQTT_BROKER,
        port=MQTT_PORT,
        user=MQTT_USER,
        password=MQTT_PASSWORD,
        keepalive=MQTT_KEEPALIVE,
        ssl=True,
        ssl_params={"server_hostname": MQTT_BROKER, "cert_reqs": ssl.CERT_NONE},
    )
    client.set_callback(mqtt_message_callback)
    client.connect()
    client.subscribe(MQTT_COMMAND_TOPIC)
    restore_socket_timeout(client)
    print("[MQTT] ulandi:", MQTT_BROKER)
    return client


def mqtt_close(client):
    if client is None:
        return None
    try:
        client.disconnect()
    except Exception:
        pass
    try:
        client.sock.close()
    except Exception:
        pass
    return None


def mqtt_publish(client, topic, payload):
    """Bitta urinish. Muvaffaqiyatsiz bo'lsa ulanish yopiladi va None qaytadi -
    asosiy tsikl keyingi safar o'zi qayta ulanadi (bu yerda kutib turmaydi)."""
    if client is None:
        return None
    try:
        client.publish(topic, payload)
        restore_socket_timeout(client)
        return client
    except Exception as e:
        print("[MQTT] yuborishda xato:", e)
        return mqtt_close(client)


# ---------------------------------------------------------------------------
# VOQEALAR NAVBATI (internet yo'q paytda ham saqlanadi)
# ---------------------------------------------------------------------------

def _queue_line_count():
    n = 0
    try:
        with open(EVENTS_QUEUE_FILE) as f:
            for _ in f:
                n += 1
    except OSError:
        pass
    return n


_boot_ticks = time.ticks_ms()


def uptime_s():
    return time.ticks_diff(time.ticks_ms(), _boot_ticks) // 1000


def _next_boot_id():
    """Har yoqilishda oshib boradigan raqam. Oflayn voqea QAYSI yoqilishda
    yozilganini bilish uchun kerak: qurilma qayta yuklangan bo'lsa, 'necha
    soniya oldin' hisobi ma'nosini yo'qotadi (uptime noldan boshlanadi)."""
    try:
        with open(BOOT_ID_FILE) as f:
            n = int(f.read().strip())
    except (OSError, ValueError):
        n = 0
    n = (n + 1) % 1000000
    try:
        _atomic_write(BOOT_ID_FILE, str(n))
    except Exception:
        pass
    return n


BOOT_ID = _next_boot_id()


def queue_event(event_type, motosoat):
    # Agar NTP hali sozlanmagan bo'lsa (internetsiz yonganda), haqiqiy sana
    # ma'lum emas. Bunday paytda vaqt o'rniga MANFIY "uptime belgisi" yoziladi.
    # Yuborish paytida (vaqt allaqachon sozlangan bo'ladi) u haqiqiy sanaga
    # aylantiriladi - shunday qilib oflayn voqealar ham to'g'ri vaqtga tushadi.
    if time_is_valid():
        line = "EVENT|{}|{}|{:.4f}".format(
            int(time.time() + UNIX_EPOCH_OFFSET), event_type, motosoat)
    else:
        # 5-maydon: yoqilish raqami. Yuborishda shu raqam joriy yoqilishga
        # mos kelsa, aniq vaqt hisoblanadi; mos kelmasa (orada qayta yuklangan)
        # aniq vaqtni bilib bo'lmaydi - internet qaytgan payt taxminan olinadi.
        line = "EVENT|{}|{}|{:.4f}|{}".format(-uptime_s(), event_type, motosoat, BOOT_ID)
    try:
        if _queue_line_count() >= EVENTS_QUEUE_MAX_LINES:
            print("[EVENT] navbat to'ldi, eng eskilari tashlab yuborildi")
            _trim_queue(EVENTS_QUEUE_MAX_LINES // 2)
        with open(EVENTS_QUEUE_FILE, "a") as f:
            f.write(line + "\n")
        print("[EVENT] navbatga qo'shildi:", line)
    except Exception as e:
        print("[EVENT] navbatga yozishda xato:", e)


def _trim_queue(keep_last):
    """Eng oxirgi keep_last qatorni qoldiradi (RAM'ni to'ldirmasdan)."""
    total = _queue_line_count()
    skip = max(0, total - keep_last)
    tmp = EVENTS_QUEUE_FILE + ".tmp"
    try:
        with open(EVENTS_QUEUE_FILE) as src, open(tmp, "w") as dst:
            i = 0
            for line in src:
                if i >= skip:
                    dst.write(line)
                i += 1
        _replace(tmp, EVENTS_QUEUE_FILE)
    except Exception:
        pass


def _resolve_event_time(line):
    """Manfiy vaqtli (oflayn yozilgan) voqeani haqiqiy sanaga aylantiradi.
    Vaqt hali sozlanmagan bo'lsa None qaytaradi - voqea navbatda qoladi."""
    try:
        parts = line.split("|")
        ts = int(parts[1])
    except Exception:
        return line          # tushunarsiz qator - o'z holicha yuboriladi
    if ts >= 0:
        return line
    if not time_is_valid():
        return None          # hali aniqlab bo'lmaydi

    same_boot = False
    if len(parts) >= 5:
        try:
            same_boot = (int(parts[4]) == BOOT_ID)
        except Exception:
            same_boot = False

    if same_boot:
        age = uptime_s() + ts        # ts manfiy: yozilganidan beri o'tgan vaqt
        if age < 0:
            age = 0
    else:
        # Orada qayta yuklangan: qurilmada soat yo'q, u o'chiq turgan vaqtni
        # bilolmaydi. Eng halol taxmin - "aloqa tiklangan payt".
        age = 0

    parts[1] = str(int(time.time() + UNIX_EPOCH_OFFSET) - age)
    return "|".join(parts[:4])       # 5-maydon (yoqilish raqami) yuborilmaydi


def flush_event_queue(client):
    """Navbatdagi voqealarni yuboradi. Fayl RAM'ga TO'LIQ o'qilmaydi -
    qatorma-qator o'qiladi (eski readlines() versiyasi MemoryError berardi)."""
    try:
        os.stat(EVENTS_QUEUE_FILE)
    except OSError:
        return client  # navbat bo'sh

    if client is None:
        return client  # ulanish yo'q - keyingi safar

    sent = 0
    failed = False
    tmp = EVENTS_QUEUE_FILE + ".tmp"
    try:
        with open(EVENTS_QUEUE_FILE) as src, open(tmp, "w") as dst:
            for line in src:
                line = line.strip()
                if not line:
                    continue
                if failed:
                    dst.write(line + "\n")   # qolganlari saqlanadi
                    continue
                feed_wdt()
                out = _resolve_event_time(line)
                if out is None:
                    dst.write(line + "\n")   # vaqt hali noma'lum - keyinroq
                    continue
                try:
                    client.publish(MQTT_EVENTS_TOPIC, out)
                    restore_socket_timeout(client)
                    sent += 1
                except Exception as e:
                    print("[EVENT] yuborishda xato:", e)
                    client = mqtt_close(client)
                    failed = True
                    dst.write(line + "\n")
        _replace(tmp, EVENTS_QUEUE_FILE)
        if not failed:
            try:
                os.remove(EVENTS_QUEUE_FILE)
            except OSError:
                pass
    except Exception as e:
        print("[EVENT] navbatni qayta yozishda xato:", e)

    if sent:
        print("[EVENT] {} ta voqea yuborildi".format(sent))
    return client


# ---------------------------------------------------------------------------
# SERIAL BUYRUQLAR
# ---------------------------------------------------------------------------
# Poller BIR MARTA yaratiladi. Eski versiya buni sekundiga 20 marta yaratardi -
# bu heap'ni parchalab, vaqt o'tishi bilan xotira muammosiga olib kelardi.

_poller = select.poll()
_poller.register(sys.stdin, select.POLLIN)


def check_serial_commands():
    global amp_threshold
    if not _poller.poll(0):
        return

    line = sys.stdin.readline()
    if not line:
        return
    line = line.strip()
    if not line:
        return

    if line == "SCAN_WIFI":
        try:
            print("WIFI_LIST|" + ",".join(scan_wifi()))
        except Exception as e:
            print("WIFI_LIST_ERROR|" + str(e))

    elif line.startswith("SET_WIFI|"):
        try:
            _, ssid, password = line.split("|", 2)
            set_wifi(ssid, password)
            print("WIFI_SET_OK")
            time.sleep(1)
            machine.reset()
        except Exception as e:
            print("WIFI_SET_ERROR|" + str(e))

    elif line == "CHECK_UPDATE":
        try:
            if not check_for_update():
                print("[OTA] yangilanish kerak emas")
        except Exception as e:
            print("[OTA] xato:", e)

    elif line.startswith("SET_THRESHOLD|"):
        try:
            value = float(line.split("|", 1)[1])
            amp_threshold = value
            save_threshold(value)
            print("THRESHOLD_SET_OK|" + str(value))
        except Exception as e:
            print("THRESHOLD_SET_ERROR|" + str(e))

    elif line == "STATUS":
        print("STATUS|{}|thr={}|mem={}|wifi={}|config={}|vaqt={}".format(
            DEVICE, amp_threshold, gc.mem_free(), wifi_is_up(),
            "OK" if CONFIG_OK else "XATO", "OK" if time_is_valid() else "SOZLANMAGAN"))

    elif line == "REBOOT":
        print("REBOOTING")
        time.sleep(1)
        machine.reset()


# ---------------------------------------------------------------------------
# BOOT: QUTQARUV OYNASI
# ---------------------------------------------------------------------------
# MUHIM: ESP32'da WDT bir marta yoqilsa, uni HECH QACHON o'chirib bo'lmaydi.
# Agar kodda xato bo'lsa, qurilma har 60 soniyada qayta yuklanaveradi va
# Thonny hech qachon barqaror ulana olmaydi ("Device is busy" tuzog'i).
# Shu sabab WDT yoqilishidan OLDIN qisqa oyna beriladi: shu vaqt ichida
# serial orqali "SAFE" yozilsa, dastur to'xtaydi va REPL ochiq qoladi.

def boot_safe_window():
    print("[BOOT] {} soniya qutqaruv oynasi - to'xtatish uchun 'SAFE' yozing".format(BOOT_SAFE_WINDOW_S))
    t0 = time.time()
    while time.time() - t0 < BOOT_SAFE_WINDOW_S:
        if _poller.poll(0):
            line = sys.stdin.readline()
            if line and line.strip().upper() == "SAFE":
                print("[BOOT] XAVFSIZ REJIM - dastur to'xtatildi, REPL ochiq")
                return False
        time.sleep(0.1)
    return True


# ---------------------------------------------------------------------------
# MAIN
# ---------------------------------------------------------------------------

def main():
    global wdt, pzem_fail_count

    if not boot_safe_window():
        return  # xavfsiz rejim: WDT yoqilmaydi, REPL bilan ishlash mumkin

    wdt = WDT(timeout=WDT_TIMEOUT_MS)
    print("[BOOT] WDT yoqildi ({} ms)".format(WDT_TIMEOUT_MS))

    # WiFi'ga bir marta urinamiz, LEKIN ulanmasa ham monitoring boshlanadi.
    # MUHIM: eski kod WiFi ulanmaguncha kutib turardi - ya'ni zavodda internet
    # butunlay yo'q bo'lsa, motosoat UMUMAN sanalmasdi va ON/OFF voqealari
    # yozilmasdi. Holbuki oflayn hisoblash tizimning asosiy vazifasi.
    # Endi tarmoq bo'lmasa ham o'lchash ishlaydi, WiFi fonda tiklanadi.
    try:
        connect_wifi()
    except Exception as e:
        print("[WIFI] boshlang'ich ulanish bo'lmadi:", e, "- OFLAYN davom etadi")

    # NTP: vaqt sozlanmasa voqealar noto'g'ri sanaga tushadi. Ulanmasa,
    # tsikl ichida qayta urinib turiladi.
    if wifi_is_up():
        sync_time()

    motohours = load_motohours()
    last_status = load_last_status()   # reboot'dan keyin soxta voqea yozilmaydi
    now = time.time()
    last_poll = now
    last_publish = 0
    last_save = now
    last_check_msg = now
    last_ota_check = now
    last_wifi_check = now
    last_ntp_retry = now
    saved_motohours = motohours        # flesh'ga keraksiz yozmaslik uchun

    client = None
    try:
        client = mqtt_connect()
    except Exception as e:
        print("[MQTT] boshlang'ich ulanish xatosi:", e)

    while True:
        feed_wdt()
        check_serial_commands()
        now = time.time()

        # --- WiFi salomatligi ---
        # MUHIM: ishlash paytida WiFi uzilib qolsa (router qayta yuklandi,
        # signal yo'qoldi), eski kod uni HECH QACHON qayta ulamas edi - qurilma
        # abadiy "ko'r" holatda ishlab yuraverardi. Endi davriy tekshiriladi.
        if now - last_wifi_check >= WIFI_HEALTH_INTERVAL_S:
            last_wifi_check = now
            if not wifi_is_up():
                print("[WIFI] uzilgan - qayta ulanmoqda")
                client = mqtt_close(client)
                try:
                    connect_wifi()
                    if not time_is_valid():
                        sync_time()
                        now = time.time()          # NTP vaqtni sakratgan bo'lishi mumkin
                        last_poll = now            # taymerlar qayta hisoblanadi
                        last_publish = last_save = last_check_msg = now
                        last_wifi_check = last_ntp_retry = now
                except Exception as e:
                    print("[WIFI] qayta ulanmadi:", e)

        # --- NTP hali sozlanmagan bo'lsa, qayta urinish ---
        if (not time_is_valid()) and wifi_is_up() and now - last_ntp_retry >= NTP_RETRY_INTERVAL_S:
            last_ntp_retry = now
            if sync_time():
                now = time.time()                  # vaqt sakradi - taymerlarni tiklash
                last_poll = now
                last_publish = last_save = last_check_msg = now
                last_wifi_check = last_ntp_retry = now

        # --- MQTT buyruqlarni tekshirish (sekundiga bir marta yetarli) ---
        if client is not None and now - last_check_msg >= CHECK_MSG_INTERVAL_S:
            last_check_msg = now
            try:
                client.check_msg()
                restore_socket_timeout(client)   # <-- eng muhim tuzatish
            except Exception as e:
                print("[MQTT] xabar tekshirishda xato:", e)
                client = mqtt_close(client)

        # --- PZEM o'qish ---
        if now - last_poll >= POLL_INTERVAL_S:
            dt_raw = now - last_poll
            last_poll = now
            # HIMOYA: vaqt sakrashi (NTP, soat nosozligi) dt'ni ulkan qilib,
            # motosoatni butunlay buzib yuborishi mumkin - u esa flesh'ga
            # saqlanadi, ya'ni xato ABADIY qolardi. Mantiqsiz qiymat rad etiladi.
            if dt_raw < 0 or dt_raw > MAX_SANE_DT_S:
                print("[TIME] mantiqsiz vaqt farqi ({}s) - motosoatga qo'shilmadi".format(dt_raw))
                dt_hours = 0.0
            else:
                dt_hours = dt_raw / 3600.0

            reading = pzem_read()

            if reading is None:
                pzem_fail_count += 1
                # Uzoq javobsizlik NORMAL (tunda svet o'chganda) - reboot QILINMAYDI,
                # faqat UART davriy tozalanadi.
                if pzem_fail_count % PZEM_REINIT_AFTER_FAILS == 0:
                    reinit_uart()
                status = "OFF"
                data_line = "DATA|OFF|0.0|0.000|0.0|{:.4f}|0|0.0|0.00".format(motohours)
            else:
                pzem_fail_count = 0
                status = "ON" if reading["current"] > amp_threshold else "OFF"
                if status == "ON":
                    motohours += dt_hours
                data_line = "DATA|{}|{:.1f}|{:.3f}|{:.1f}|{:.4f}|{}|{:.1f}|{:.2f}".format(
                    status, reading["voltage"], reading["current"], reading["power"],
                    motohours, reading["energy"], reading["freq"], reading["pf"])

            if PRINT_DATA_LINES:
                print(data_line)

            # --- ON/OFF o'zgarishi ---
            if status != last_status:
                queue_event(status, motohours)
                last_status = status
                save_last_status(status)
                # Sikl tugagan payt - motosoatni darhol saqlash (quvvat shu
                # zahoti uzilsa ham oxirgi ish vaqti yo'qolmasin)
                if abs(motohours - saved_motohours) > 1e-6:
                    save_motohours(motohours)
                    saved_motohours = motohours
                client = flush_event_queue(client)

            # --- MQTT'ga yuborish ---
            if now - last_publish >= PUBLISH_INTERVAL_S:
                last_publish = now
                if client is None and wifi_is_up():
                    try:
                        client = mqtt_connect()
                    except Exception as e:
                        print("[MQTT] qayta ulanish xatosi:", e)
                client = mqtt_publish(client, MQTT_TOPIC, data_line)

            # --- Motosoatni saqlash + navbatni yuborishga urinish ---
            # Faqat QIYMAT O'ZGARGANDA yoziladi. Drobilka o'chiq turganda
            # motosoat o'zgarmaydi - eski kod baribir har daqiqada yozardi
            # (kuniga 1440 marta), bu yillar davomida flesh xotirani eskirtiradi.
            if now - last_save >= MOTOHOURS_SAVE_INTERVAL_S:
                last_save = now
                if abs(motohours - saved_motohours) > 1e-6:
                    save_motohours(motohours)
                    saved_motohours = motohours
                client = flush_event_queue(client)

            # --- OTA (kuniga bir marta) ---
            if now - last_ota_check >= OTA_CHECK_INTERVAL_S:
                last_ota_check = now
                try:
                    check_for_update()
                except Exception as e:
                    print("[OTA] xato:", e)

            gc.collect()

        time.sleep(0.05)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        pass
    except Exception as e:
        # Kutilmagan xato: WDT baribir qayta yuklaydi, lekin sabab ko'rinib qolsin
        print("[FATAL]", e)
        try:
            sys.print_exception(e)
        except Exception:
            pass
        time.sleep(5)
        machine.reset()

# OTA-FAYL-OXIRI  <- bu qatorsiz OTA yangilanishni qabul qilmaydi (yarim fayl himoyasi)
