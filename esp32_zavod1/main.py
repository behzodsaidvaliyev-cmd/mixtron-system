"""
ZAVOD 1 - Drobilka runtime & energy monitor
ESP32 / MicroPython v1.28.0
PZEM-004T (UART2, TX=17, RX=16) -> moto-hours calc -> HiveMQ Cloud (MQTT/SSL)
"""

import ujson
import machine
import network
import time
import ssl
import gc
from machine import UART, Pin, WDT

try:
    from umqtt.simple import MQTTClient
except ImportError:
    raise RuntimeError("umqtt.simple not found - install with: mip.install('umqtt.simple')")

# ---------------------------------------------------------------------------
# CONFIG
# ---------------------------------------------------------------------------

WIFI_CONFIG_FILE = "wifi_config.json"
MQTT_CONFIG_FILE = "mqtt_config.json"  # faqat qurilmada turadi, GitHub'ga hech qachon yuklanmaydi
MOTOHOURS_FILE = "motohours.txt"
EVENTS_QUEUE_FILE = "events_queue.txt"  # internet yo'q paytda ON/OFF voqealari shu yerda kutadi


def load_mqtt_config():
    with open(MQTT_CONFIG_FILE) as f:
        cfg = ujson.load(f)
    return cfg["broker"], cfg["user"], cfg["password"]


MQTT_BROKER, MQTT_USER, MQTT_PASSWORD = load_mqtt_config()
MQTT_PORT = 8883
MQTT_CLIENT_ID = "esp32-zavod1-" + "".join("{:02x}".format(b) for b in machine.unique_id())
MQTT_TOPIC = b"zavod1/data"
MQTT_EVENTS_TOPIC = b"zavod1/events"
MQTT_COMMAND_TOPIC = b"zavod1/command"
MQTT_KEEPALIVE = 60

AMP_THRESHOLD = 1.5          # Amps - above this, machine counted as "running"
POLL_INTERVAL_S = 2          # seconds between PZEM reads
PUBLISH_INTERVAL_S = 5       # seconds between MQTT publishes
MOTOHOURS_SAVE_INTERVAL_S = 60  # seconds between flash writes

PZEM_SLAVE_ADDR = 0xF8       # PZEM factory default address (change if you customized it)
PZEM_DEBUG = True            # prints raw bytes on read failure - set False once working

WDT_TIMEOUT_MS = 60000       # if main loop hangs this long (power-glitch freeze), force reboot

# --- OTA (masofadan yangilash, GitHub'dan) ---
OTA_ENABLED = True
OTA_URL = "https://raw.githubusercontent.com/behzodsaidvaliyev-cmd/mixtron-system/main/esp32_zavod1/main.py"
OTA_CHECK_INTERVAL_S = 86400  # kuniga bir marta tekshiradi (kod kamdan-kam o'zgargani uchun yetarli)

# ---------------------------------------------------------------------------
# WIFI
# ---------------------------------------------------------------------------

def load_wifi_config():
    with open(WIFI_CONFIG_FILE) as f:
        cfg = ujson.load(f)
    return cfg["ssid"].strip(), cfg["password"].strip()


def connect_wifi(timeout_s=10):
    ssid, password = load_wifi_config()
    wlan = network.WLAN(network.STA_IF)
    wlan.active(True)
    if not wlan.isconnected():
        print("[WIFI] connecting to", ssid)
        wlan.connect(ssid, password)
        t0 = time.time()
        while not wlan.isconnected():
            feed_wdt()
            if time.time() - t0 > timeout_s:
                raise RuntimeError("WiFi connect timeout")
            time.sleep(0.5)
    print("[WIFI] connected, ip =", wlan.ifconfig()[0])
    return wlan


def sync_time():
    """Haqiqiy sana-vaqtni internetdan olib qo'yadi (voqealarga aniq vaqt yozish uchun)."""
    try:
        import ntptime
        ntptime.settime()
        print("[TIME] NTP orqali sozlandi (UTC):", time.localtime())
    except Exception as e:
        print("[TIME] NTP sozlashda xato:", e)


def _decode_ssid(raw):
    if not raw:
        return ""
    try:
        return raw.decode("utf-8")
    except Exception:
        pass
    try:
        return raw.decode("latin-1")  # never fails - keeps non-UTF8 (e.g. Cyrillic) names visible
    except Exception:
        return ""


def scan_wifi():
    wlan = network.WLAN(network.STA_IF)
    wlan.active(True)
    nets = wlan.scan()
    print("[WIFI] raw scan results:", [n[0] for n in nets])  # debug: see exact bytes per SSID
    seen = set()
    names = []
    for n in nets:
        name = _decode_ssid(n[0])
        if name and name not in seen:
            seen.add(name)
            names.append(name)
    return names


def set_wifi(ssid, password):
    with open(WIFI_CONFIG_FILE, "w") as f:
        ujson.dump({"ssid": ssid, "password": password}, f)


# ---------------------------------------------------------------------------
# PZEM-004T v3.0 over Modbus RTU (UART2)
# ---------------------------------------------------------------------------

uart = UART(2, baudrate=9600, tx=Pin(17), rx=Pin(16), bits=8, parity=None, stop=1, timeout=200)

wdt = None  # set in main() once WDT is started


def feed_wdt():
    if wdt is not None:
        wdt.feed()


def modbus_crc16(data):
    crc = 0xFFFF
    for b in data:
        crc ^= b
        for _ in range(8):
            if crc & 0x0001:
                crc >>= 1
                crc ^= 0xA001
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
    """Returns dict with voltage, current, power, energy, freq, pf or None on failure."""
    req = build_read_request(PZEM_SLAVE_ADDR, 0x0000, 10)
    uart.read()  # flush stale bytes
    uart.write(req)
    time.sleep_ms(150)
    resp = uart.read()

    if not resp or len(resp) < 25:
        if PZEM_DEBUG:
            print("[PZEM] no/short response:", resp)
        return None

    if resp[0] != PZEM_SLAVE_ADDR or resp[1] != 0x04:
        if PZEM_DEBUG:
            print("[PZEM] unexpected response:", resp)
        return None

    payload = resp[3:-2]
    recv_crc = resp[-2] | (resp[-1] << 8)
    if modbus_crc16(resp[:-2]) != recv_crc:
        return None

    voltage = ((payload[0] << 8) | payload[1]) * 0.1
    current = (((payload[2] << 8) | payload[3]) | ((payload[4] << 8) | payload[5]) << 16) * 0.001
    power = (((payload[6] << 8) | payload[7]) | ((payload[8] << 8) | payload[9]) << 16) * 0.1
    energy = (((payload[10] << 8) | payload[11]) | ((payload[12] << 8) | payload[13]) << 16)
    freq = ((payload[14] << 8) | payload[15]) * 0.1
    pf = ((payload[16] << 8) | payload[17]) * 0.01

    if not (80.0 <= voltage <= 300.0) or not (0.0 <= current <= 100.0) or not (40.0 <= freq <= 65.0):
        if PZEM_DEBUG:
            print("[PZEM] out-of-range reading rejected: V={} A={} Hz={}".format(voltage, current, freq))
        return None

    return {
        "voltage": voltage,
        "current": current,
        "power": power,
        "energy": energy,
        "freq": freq,
        "pf": pf,
    }


# ---------------------------------------------------------------------------
# MOTO-HOURS PERSISTENCE
# ---------------------------------------------------------------------------

def load_motohours():
    try:
        with open(MOTOHOURS_FILE) as f:
            return float(f.read().strip())
    except (OSError, ValueError):
        return 0.0


def save_motohours(hours):
    tmp_file = MOTOHOURS_FILE + ".tmp"
    with open(tmp_file, "w") as f:
        f.write("{:.6f}".format(hours))
    import os
    os.rename(tmp_file, MOTOHOURS_FILE)


# ---------------------------------------------------------------------------
# OTA - GitHub'dan masofadan kod yangilash
# ---------------------------------------------------------------------------

def check_for_update():
    if not OTA_ENABLED:
        return False
    try:
        import urequests
    except ImportError:
        print("[OTA] urequests topilmadi - o'rnating: mip.install('urequests')")
        return False

    gc.collect()
    try:
        r = urequests.get(OTA_URL)
        new_code = r.text
        r.close()
    except Exception as e:
        print("[OTA] yuklab olishda xato:", e)
        return False
    finally:
        gc.collect()

    if not new_code or "def main()" not in new_code:
        print("[OTA] noto'g'ri fayl keldi, bekor qilindi")
        return False

    try:
        with open("main.py") as f:
            current_code = f.read()
    except OSError:
        current_code = ""

    if new_code == current_code:
        return False  # o'zgarish yo'q

    with open("main.py.new", "w") as f:
        f.write(new_code)
    import os
    os.rename("main.py.new", "main.py")
    print("[OTA] yangi kod o'rnatildi, qayta yuklanmoqda...")
    time.sleep(1)
    machine.reset()


# ---------------------------------------------------------------------------
# MQTT (SSL) - HiveMQ Cloud
# ---------------------------------------------------------------------------

def mqtt_message_callback(topic, msg):
    if topic == MQTT_COMMAND_TOPIC:
        cmd = msg.decode("utf-8", "ignore").strip()
        print("[MQTT] buyruq keldi:", cmd)
        if cmd == "CHECK_UPDATE":
            try:
                check_for_update()
            except Exception as e:
                print("[OTA] masofadan buyruq bilan tekshirishda xato:", e)


def mqtt_connect():
    gc.collect()  # SSL handshake uchun maksimal bo'sh xotira kerak
    client = MQTTClient(
        client_id=MQTT_CLIENT_ID,
        server=MQTT_BROKER,
        port=MQTT_PORT,
        user=MQTT_USER,
        password=MQTT_PASSWORD,
        keepalive=MQTT_KEEPALIVE,
        ssl=True,
        ssl_params={
            "server_hostname": MQTT_BROKER,
            "cert_reqs": ssl.CERT_NONE,
        },
    )
    client.set_callback(mqtt_message_callback)
    client.connect()
    client.subscribe(MQTT_COMMAND_TOPIC)
    print("[MQTT] connected to", MQTT_BROKER)
    return client


UNIX_EPOCH_OFFSET = 946684800  # MicroPython vaqti 2000-yildan, Unix vaqti 1970-yildan boshlanadi


def queue_event(event_type, motosoat):
    """Drobilka ON/OFF holatini o'zgarganda mahalliy faylga yozib qo'yadi (internet bo'lmasa ham)."""
    unix_ts = time.time() + UNIX_EPOCH_OFFSET
    line = "EVENT|{ts}|{event}|{motosoat:.4f}".format(
        ts=unix_ts, event=event_type, motosoat=motosoat
    )
    with open(EVENTS_QUEUE_FILE, "a") as f:
        f.write(line + "\n")
    print("[EVENT] queued:", line)


def flush_event_queue(client):
    """Navbatdagi voqealarni Railway'ga (MQTT orqali) yuborishga harakat qiladi."""
    try:
        with open(EVENTS_QUEUE_FILE) as f:
            lines = [l.strip() for l in f.readlines() if l.strip()]
    except OSError:
        return client

    if not lines:
        return client

    if client is None:
        try:
            client = mqtt_connect()
        except Exception as e:
            print("[EVENT] flush uchun ulanib bo'lmadi:", e)
            return client

    sent_count = 0
    for line in lines:
        try:
            client.publish(MQTT_EVENTS_TOPIC, line)
            sent_count += 1
        except Exception as e:
            print("[EVENT] yuborishda xato, qolganlari keyinroq:", e)
            try:
                client.disconnect()
            except Exception:
                pass
            try:
                client = mqtt_connect()
            except Exception as e2:
                print("[EVENT] qayta ulanib bo'lmadi:", e2)
            break

    remaining = lines[sent_count:]
    import os
    if remaining:
        tmp_file = EVENTS_QUEUE_FILE + ".tmp"
        with open(tmp_file, "w") as f:
            for l in remaining:
                f.write(l + "\n")
        os.rename(tmp_file, EVENTS_QUEUE_FILE)
    else:
        try:
            os.remove(EVENTS_QUEUE_FILE)
        except OSError:
            pass

    print("[EVENT] {} ta voqea yuborildi, {} ta navbatda qoldi".format(sent_count, len(remaining)))
    return client


def mqtt_publish_with_retry(client, payload, max_retries=3):
    for attempt in range(max_retries):
        feed_wdt()
        try:
            client.publish(MQTT_TOPIC, payload)
            return client
        except Exception as e:
            print("[MQTT] publish failed ({}): {}".format(attempt + 1, e))
            try:
                client.disconnect()
            except Exception:
                pass
            time.sleep(1 + attempt * 2)
            try:
                client = mqtt_connect()
            except Exception as e2:
                print("[MQTT] reconnect failed:", e2)
    return client


# ---------------------------------------------------------------------------
# SERIAL COMMAND HANDLING (SCAN_WIFI / SET_WIFI)
# ---------------------------------------------------------------------------

def check_serial_commands():
    import sys
    import select

    poller = select.poll()
    poller.register(sys.stdin, select.POLLIN)
    if not poller.poll(0):
        return

    line = sys.stdin.readline().strip()
    if not line:
        return

    if line == "SCAN_WIFI":
        try:
            names = scan_wifi()
            print("WIFI_LIST|" + ",".join(names))
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
        print("[OTA] qo'lda tekshirish boshlandi...")
        try:
            updated = check_for_update()
            if not updated:
                print("[OTA] yangilanish topilmadi yoki kerak emas")
        except Exception as e:
            print("[OTA] qo'lda tekshirishda xato:", e)


# ---------------------------------------------------------------------------
# MAIN LOOP
# ---------------------------------------------------------------------------

def main():
    global wdt
    wdt = WDT(timeout=WDT_TIMEOUT_MS)

    try:
        connect_wifi()
        sync_time()
    except Exception as e:
        print("[WIFI] connect failed:", e)
        try:
            network.WLAN(network.STA_IF).disconnect()  # radio bo'shatiladi - scan toza ishlashi uchun
        except Exception:
            pass
        print("[WIFI] offline recovery mode - kutmoqda: SCAN_WIFI / SET_WIFI (serial)")
        while True:
            feed_wdt()
            check_serial_commands()
            time.sleep(0.05)

    motohours = load_motohours()
    last_poll = time.time()
    last_publish = 0
    last_save = time.time()
    last_ota_check = time.time()  # birinchi OTA tekshiruvi MQTT barqarorlashgandan keyin, navbatdagi intervalda bo'ladi
    last_status = None

    client = None
    try:
        client = mqtt_connect()
    except Exception as e:
        print("[MQTT] initial connect failed:", e)

    while True:
        feed_wdt()
        check_serial_commands()

        if client is not None:
            try:
                client.check_msg()  # kelgan MQTT buyruqlarni tekshiradi (masalan CHECK_UPDATE)
            except Exception as e:
                print("[MQTT] xabar tekshirishda xato:", e)
                client = None

        now = time.time()
        if now - last_poll >= POLL_INTERVAL_S:
            dt_hours = (now - last_poll) / 3600.0
            last_poll = now

            reading = pzem_read()

            if reading is not None:
                status = "ON" if reading["current"] > AMP_THRESHOLD else "OFF"
                if status == "ON":
                    motohours += dt_hours

                if status != last_status:
                    queue_event(status, motohours)
                    last_status = status
                    client = flush_event_queue(client)

                data_line = "DATA|{status}|{volt:.1f}|{amp:.3f}|{watt:.1f}|{motosoat:.4f}|{energy}|{freq:.1f}|{pf:.2f}".format(
                    status=status,
                    volt=reading["voltage"],
                    amp=reading["current"],
                    watt=reading["power"],
                    motosoat=motohours,
                    energy=reading["energy"],
                    freq=reading["freq"],
                    pf=reading["pf"],
                )
                print(data_line)

                if now - last_publish >= PUBLISH_INTERVAL_S:
                    last_publish = now
                    if client is None:
                        try:
                            client = mqtt_connect()
                        except Exception as e:
                            print("[MQTT] reconnect failed:", e)
                    if client is not None:
                        client = mqtt_publish_with_retry(client, data_line)
            else:
                if last_status != "OFF":
                    queue_event("OFF", motohours)
                    last_status = "OFF"
                    client = flush_event_queue(client)

                data_line = "DATA|OFF|0.0|0.000|0.0|{motosoat:.4f}|0|0.0|0.00".format(motosoat=motohours)
                print(data_line + "  (PZEM javob bermayapti - quvvat yo'q bo'lishi mumkin)")

                if now - last_publish >= PUBLISH_INTERVAL_S:
                    last_publish = now
                    if client is None:
                        try:
                            client = mqtt_connect()
                        except Exception as e:
                            print("[MQTT] reconnect failed:", e)
                    if client is not None:
                        client = mqtt_publish_with_retry(client, data_line)

            if now - last_save >= MOTOHOURS_SAVE_INTERVAL_S:
                last_save = now
                save_motohours(motohours)
                client = flush_event_queue(client)  # navbatda qolgan voqealar bo'lsa, qayta urinadi

            if now - last_ota_check >= OTA_CHECK_INTERVAL_S:
                last_ota_check = now
                try:
                    check_for_update()
                except Exception as e:
                    print("[OTA] tekshirishda xato:", e)

            gc.collect()

        time.sleep(0.05)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        pass
