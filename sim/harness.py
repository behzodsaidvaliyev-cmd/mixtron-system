"""
ESP32 firmware simulyatori.

MicroPython modullarini (machine, network, umqtt, ...) taqlid qiladi va
main.py ning HAQIQIY kodini turli falokat senariylarida ishlatadi:
svet ketishi, WiFi uzilishi, broker qotib qolishi, NTP yo'qligi va h.k.

Virtual soat ishlatiladi - soatlab davom etadigan senariy bir necha
sekundda tekshiriladi.
"""
import sys
import types
import os


# --- Simulyatsiyani to'xtatuvchi signallar (BaseException - kod ularni ushlamasin) ---
class SimEnd(BaseException):
    pass


class DeviceReset(BaseException):
    pass


class WDTFired(BaseException):
    pass


class World:
    """Simulyatsiya holati: soat, WiFi, broker, PZEM, WDT."""

    def __init__(self):
        self.clock = 800000000.0      # MicroPython epoch (2000-dan), ~2025-yil
        self.budget = None
        self.mono = 0.0
        self.wifi_up = True
        self.wifi_can_connect = True
        self.ntp_works = True
        self.broker = "ok"            # ok | down | hang
        self.pzem = "ok"              # ok | dead
        self.pzem_current = 9.0
        self.pzem_voltage = 220.0

        self.log = []
        self.published = []           # (topic, payload)
        self.wdt_timeout_s = None
        self.wdt_last_feed = 0.0
        self.wdt_max_gap = 0.0
        self.mqtt_connects = 0
        self.wifi_connects = 0
        self.uart_reinits = 0
        self.flash_writes = 0
        self.led_writes = 0
        self.led_state = 0

    def advance(self, sec):
        self.clock += sec
        # HAQIQIY WDT o'tgan vaqtni sanaydi, devor soatini emas - NTP soatni
        # sakratganda u ishga tushmaydi. Shu sabab alohida monoton hisoblagich.
        self.mono += sec
        if self.wdt_timeout_s is not None:
            gap = self.mono - self.wdt_last_feed
            if gap > self.wdt_max_gap:
                self.wdt_max_gap = gap
            if gap > self.wdt_timeout_s:
                raise WDTFired("WDT ishga tushdi: {:.0f}s oziqlantirilmadi".format(gap))
        # Byudjet o'tgan vaqtga qarab kamayadi. NTP soatni sakratganda
        # byudjet sarflanmasligi kerak - shu sabab absolyut vaqt bilan
        # solishtirilmaydi.
        if self.budget is not None:
            self.budget -= sec
            if self.budget <= 0:
                raise SimEnd()

    def feed(self):
        self.wdt_last_feed = self.mono


W = World()


# --------------------------------------------------------------------------
# time
# --------------------------------------------------------------------------
def _install_time():
    m = types.ModuleType("time")
    m.time = lambda: W.clock
    m.sleep = lambda s: W.advance(s)
    m.sleep_ms = lambda ms: W.advance(ms / 1000.0)
    # ticks_ms HAQIQIY qurilmada monoton (yoqilgandan beri), NTP uni sakratmaydi
    m.ticks_ms = lambda: int(W.mono * 1000)
    m.ticks_diff = lambda a, b: a - b
    m.localtime = lambda *a: (2026, 8, 11, 12, 0, 0, 0, 0)
    sys.modules["time"] = m
    return m


# --------------------------------------------------------------------------
# machine
# --------------------------------------------------------------------------
def _pzem_frame(voltage, current, power, energy, freq, pf, addr=0xF8):
    def crc16(data):
        crc = 0xFFFF
        for b in data:
            crc ^= b
            for _ in range(8):
                crc = (crc >> 1) ^ 0xA001 if crc & 1 else crc >> 1
        return crc & 0xFFFF

    def w(x):                                      # 16-bit, big-endian registr
        x &= 0xFFFF
        return bytes([(x >> 8) & 0xFF, x & 0xFF])

    v = int(voltage * 10)
    c = int(current * 1000)
    p = int(power * 10)
    e = int(energy)
    # PZEM tartibi: voltaj, tok(past,yuqori), quvvat(past,yuqori),
    # energiya(past,yuqori), chastota, cos-fi, alarm = 10 registr
    payload = (w(v) + w(c) + w(c >> 16) + w(p) + w(p >> 16)
               + w(e) + w(e >> 16) + w(int(freq * 10)) + w(int(pf * 100)) + w(0))
    frame = bytes([addr, 0x04, len(payload)]) + bytes(payload)
    c16 = crc16(frame)
    return frame + bytes([c16 & 0xFF, c16 >> 8])


class FakeUART:
    def __init__(self, *a, **kw):
        pass

    def read(self, *a):
        if W.pzem == "dead":
            return None
        return _pzem_frame(W.pzem_voltage, W.pzem_current,
                           W.pzem_voltage * W.pzem_current * 0.8, 12345, 50.0, 0.8)

    def write(self, data):
        return len(data)

    def deinit(self):
        W.uart_reinits += 1


class FakeWDT:
    def __init__(self, timeout=0):
        W.wdt_timeout_s = timeout / 1000.0
        W.wdt_last_feed = W.mono

    def feed(self):
        W.feed()


class FakePin:
    OUT = 3
    IN = 1

    def __init__(self, pin, mode=None, *a, **kw):
        self.pin = pin
        self._v = 0

    def value(self, v=None):
        if v is None:
            return self._v
        self._v = v
        W.led_writes += 1
        W.led_state = v
        return None


def _install_machine():
    m = types.ModuleType("machine")
    m.UART = FakeUART
    m.Pin = FakePin
    m.WDT = FakeWDT
    m.unique_id = lambda: b"\xaa\xbb\xcc\xdd\xee\xff"
    m.reset = lambda: (_ for _ in ()).throw(DeviceReset("machine.reset()"))
    m.reset_cause = lambda: 1
    m.PWRON_RESET = 1
    m.WDT_RESET = 3
    sys.modules["machine"] = m
    return m


# --------------------------------------------------------------------------
# network
# --------------------------------------------------------------------------
class FakeWLAN:
    def __init__(self, *a):
        pass

    def active(self, *a):
        return True

    def isconnected(self):
        return W.wifi_up

    def connect(self, ssid, pwd):
        W.wifi_connects += 1
        if W.wifi_can_connect:
            W.advance(1.5)
            W.wifi_up = True
        else:
            W.advance(1.0)

    def disconnect(self):
        W.wifi_up = False

    def ifconfig(self):
        return ("192.168.1.50", "255.255.255.0", "192.168.1.1", "8.8.8.8")

    def scan(self):
        return [(b"TestWiFi", b"\x00" * 6, 6, -50, 3, False)]

    def status(self, *a):
        return -50


def _install_network():
    m = types.ModuleType("network")
    m.WLAN = FakeWLAN
    m.STA_IF = 0
    sys.modules["network"] = m
    return m


# --------------------------------------------------------------------------
# socket / ssl / select / ntptime / umqtt
# --------------------------------------------------------------------------
class FakeSock:
    def __init__(self, *a, **kw):
        self.timeout = None

    def settimeout(self, t):
        self.timeout = t

    def setblocking(self, flag):
        # MicroPython'dagi kabi: blocking rejim timeout'ni BEKOR QILADI
        self.timeout = None

    def close(self):
        pass

    def connect(self, *a):
        pass

    def read(self, *a):
        return b""

    def write(self, d):
        return len(d)


def _install_socket():
    m = types.ModuleType("socket")
    m.socket = FakeSock
    m.getaddrinfo = lambda h, p: [(2, 1, 0, "", (h, p))]
    m.AF_INET = 2
    m.SOCK_STREAM = 1
    m.SOCK_DGRAM = 2
    sys.modules["socket"] = m
    sys.modules["usocket"] = m          # bir xil modul obyekti
    return m


def _install_ssl():
    m = types.ModuleType("ssl")
    m.CERT_NONE = 0
    m.wrap_socket = lambda s, **kw: s
    sys.modules["ssl"] = m
    return m


class FakePoll:
    def register(self, *a):
        pass

    def poll(self, *a):
        return []                        # serial'dan hech narsa kelmayapti


def _install_select():
    m = types.ModuleType("select")
    m.poll = FakePoll
    m.POLLIN = 1
    sys.modules["select"] = m
    return m


def _install_ntptime():
    m = types.ModuleType("ntptime")

    def settime():
        if not (W.wifi_up and W.ntp_works):
            raise OSError("NTP yetib bo'lmadi")
        W.advance(0.3)
        if W.clock < 789000000:
            W.clock = 800000000.0        # 2000 -> haqiqiy vaqtga sakrash
    m.settime = settime
    sys.modules["ntptime"] = m
    return m


class FakeMQTTClient:
    def __init__(self, **kw):
        self.sock = FakeSock()
        self.cb = None

    def set_callback(self, cb):
        self.cb = cb

    def connect(self, *a, **kw):
        if not W.wifi_up:
            W.advance(0.3)
            raise OSError("tarmoq yo'q")
        if W.broker == "down":
            W.advance(0.5)
            raise OSError("broker mavjud emas")
        if W.broker == "hang":
            W.advance(15.0)              # socket timeout ishga tushdi
            raise OSError("ETIMEDOUT")
        W.advance(1.2)                   # SSL handshake
        W.mqtt_connects += 1
        self.sock = FakeSock()
        self.sock.settimeout(15)

    def disconnect(self):
        pass

    def subscribe(self, t, *a):
        pass

    def publish(self, topic, msg, *a, **kw):
        if not W.wifi_up:
            raise OSError("tarmoq yo'q")
        if W.broker == "down":
            raise OSError("ulanish yo'q")
        if W.broker == "hang":
            # Timeout BO'LMASA bu abadiy qotib qolardi. Timeout bor bo'lsa
            # 15 soniyada xato qaytaradi.
            if self.sock.timeout is None:
                W.advance(10000.0)       # ABADIY QOTISH (WDT ishga tushadi)
            W.advance(self.sock.timeout)
            raise OSError("ETIMEDOUT")
        W.advance(0.05)
        t = topic.decode() if isinstance(topic, bytes) else topic
        p = msg.decode() if isinstance(msg, bytes) else msg
        W.published.append((t, p))

    def check_msg(self):
        self.sock.setblocking(False)
        if W.broker in ("down", "hang"):
            raise OSError("ulanish uzilgan")
        self.sock.setblocking(True)      # <-- timeout shu yerda YO'QOLADI
        return None


def _install_umqtt():
    pkg = types.ModuleType("umqtt")
    pkg.__path__ = []
    sim = types.ModuleType("umqtt.simple")
    sim.MQTTClient = FakeMQTTClient
    sys.modules["umqtt"] = pkg
    sys.modules["umqtt.simple"] = sim
    return sim


def _install_misc():
    import json
    import gc as _gc
    sys.modules["ujson"] = json                   # MicroPython'da ujson == json
    if not hasattr(_gc, "mem_free"):
        _gc.mem_free = lambda: 90000              # CPython'da yo'q

    # urequests - OTA uchun (testlarda tarmoqqa chiqmaydi)
    ur = types.ModuleType("urequests")

    class _Resp:
        status_code = 599
        raw = None

        def close(self):
            pass

    ur.get = lambda url, **kw: _Resp()
    sys.modules["urequests"] = ur


def install_all():
    _install_misc()
    _install_time()
    _install_machine()
    _install_network()
    _install_socket()
    _install_ssl()
    _install_select()
    _install_ntptime()
    _install_umqtt()
