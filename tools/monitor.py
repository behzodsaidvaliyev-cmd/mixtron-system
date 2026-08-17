"""Qurilmani qayta ishga tushirib, chiqishini kuzatadi.
   python monitor.py [soniya]"""
import sys
import time
import serial

PORT = "COM3"
secs = int(sys.argv[1]) if len(sys.argv) > 1 else 45

s = serial.Serial(PORT, 115200, timeout=1)
time.sleep(0.2)
s.write(b"\r\x03\x03")          # ishlayotgan dasturni to'xtatish
time.sleep(0.3)
s.reset_input_buffer()
s.write(b"\x02")                # normal REPL
time.sleep(0.2)
s.write(b"\r\x04")              # soft reset -> main.py qaytadan ishga tushadi
print("--- qayta ishga tushirildi, {} soniya kuzatilmoqda ---".format(secs))

t0 = time.time()
buf = b""
while time.time() - t0 < secs:
    data = s.read(s.in_waiting or 1)
    if data:
        buf += data
        while b"\n" in buf:
            line, buf = buf.split(b"\n", 1)
            txt = line.decode("utf-8", "replace").rstrip()
            if txt:
                print("{:5.1f}s | {}".format(time.time() - t0, txt))
    else:
        time.sleep(0.02)
s.close()
print("--- kuzatuv tugadi ---")
