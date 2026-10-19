"""Proshivkadagi WS-BLOCK ni CPython'da REAL mixtron-broker'ga qarshi sinaydi.

Ishlatilishi (login/parol muhit o'zgaruvchilaridan):
    MQTT_USER=... MQTT_PASSWORD=... python sim/ws_real_test.py esp32_zavod3/main.py <umqtt papkasi> 120

<umqtt papkasi> - micropython-lib'dagi umqtt/simple.py turgan "umqtt" papkasi.
Proshivkadagi WS-BLOCK aynan o'zi (nusxa emas) ishga tushiriladi; umqtt ham
qurilmadagi bilan bir xil asl kod. Firmware kabi tsikl: har 5 s publish,
har 1 s check_msg, vaqti-vaqti bilan tashqi mijoz "buyruq" yuboradi.
"""
import io
import os
import select as _select
import socket
import ssl as _ssl
import struct
import binascii
import sys
import time

FW, UMQTT_DIR, SECONDS = sys.argv[1], sys.argv[2], int(sys.argv[3])
USER, PASSWORD = os.environ["MQTT_USER"], os.environ["MQTT_PASSWORD"]

sys.path.insert(0, os.path.dirname(os.path.abspath(UMQTT_DIR)))
from umqtt.simple import MQTTClient  # noqa: E402


class SslShim:
    """MicroPython ssl.wrap_socket o'rnida (CERT_NONE, SNI bilan)."""
    CERT_NONE = _ssl.CERT_NONE

    @staticmethod
    def wrap_socket(sock, server_hostname=None, **kw):
        ctx = _ssl.create_default_context()
        ctx.check_hostname = False
        ctx.verify_mode = _ssl.CERT_NONE
        return ctx.wrap_socket(sock, server_hostname=server_hostname)


class _Poll:
    """Windows'da select.poll yo'q - select.select bilan taqlid."""
    def __init__(self):
        self.r = []

    def register(self, s, ev):
        self.r.append((s, ev))

    def poll(self, timeout_ms):
        rs = [s for s, ev in self.r if ev & 1]
        ws = [s for s, ev in self.r if ev & 4]
        pend = [s for s in rs if hasattr(s, "pending") and s.pending()]
        if pend:
            return [(s, 1) for s in pend]
        r, w, _ = _select.select(rs, ws, [], timeout_ms / 1000)
        return [(s, 1) for s in r] + [(s, 4) for s in w]


class SelectShim:
    POLLIN, POLLOUT = 1, 4
    poll = _Poll


src = io.open(FW, encoding="utf-8").read()
block = src[src.index("# MQTT WebSocket orqali (443) - WS-BLOCK-BEGIN"):src.index("# WS-BLOCK-END")]
ns = {"os": os, "struct": struct, "binascii": binascii, "socket": socket, "ssl": SslShim,
      "select": SelectShim, "MQTTClient": MQTTClient, "SOCKET_TIMEOUT_S": 15}
exec(compile(block, "WS-BLOCK", "exec"), ns)
WSMQTTClient = ns["WSMQTTClient"]

HOST = "mixtron-broker-production.up.railway.app"
got = []
c = WSMQTTClient(client_id="diag-ws-fw-test", server=HOST, port=443,
                 user=USER, password=PASSWORD, keepalive=60)
c.set_callback(lambda t, m: got.append((t, m)))
t0 = time.time()
c.connect(timeout=15)
print("ulandi %.2fs" % (time.time() - t0))
c.subscribe(b"diag/wsfw")
print("subscribe ok")
print("bo'sh check_msg ->", c.check_msg(), "(None kutiladi)")

import paho.mqtt.client as mqtt  # noqa: E402
ext = mqtt.Client(callback_api_version=mqtt.CallbackAPIVersion.VERSION2, client_id="diag-ws-ext",
                  transport="websockets")
ext.username_pw_set(USER, PASSWORD)
ext.tls_set()
ext.connect(HOST, 443, 30)
ext.loop_start()

sent = ext_sent = errors = 0
start = time.time()
last_pub = 0
while time.time() - start < SECONDS:
    now = time.time()
    try:
        if now - last_pub >= 5:
            last_pub = now
            c.publish(b"diag/wsfw", b"DATA|ON|220.0|9.0|1584.0|%d" % sent)
            sent += 1
            if sent % 4 == 0:
                ext.publish("diag/wsfw", "CMD|%d" % ext_sent)
                ext_sent += 1
        c.check_msg()
    except Exception as e:
        errors += 1
        print("XATO:", repr(e))
        c.connect(timeout=15)
        c.subscribe(b"diag/wsfw")
    time.sleep(1)
time.sleep(2)
for _ in range(5):
    c.check_msg()
    time.sleep(0.3)
ext.loop_stop()
ext.disconnect()
own_back = sum(1 for t, m in got if m.startswith(b"DATA"))
cmd_back = sum(1 for t, m in got if m.startswith(b"CMD"))
c.disconnect()
print("davomiylik %ds | yuborildi %d, qaytdi %d | tashqi buyruq %d, yetib keldi %d | xatolar %d" %
      (SECONDS, sent, own_back, ext_sent, cmd_back, errors))
