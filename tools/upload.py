"""
ESP32'ga (MicroPython) fayl yuklash vositasi - Thonny'siz, to'g'ridan-to'g'ri
serial orqali "raw REPL" protokoli bilan ishlaydi.

Ishlatilishi:
    python upload.py info                       - qurilmadagi fayllar ro'yxati
    python upload.py disarm                     - main.py ni vaqtincha o'chirib qo'yish
                                                  (WDT to'xtaydi, xotirjam ishlash mumkin)
    python upload.py put <mahalliy> <qurilmada> - fayl yuklash
    python upload.py write <nom> <matn>         - kichik fayl yozish
    python upload.py reset                      - qayta ishga tushirish
"""
import sys
import time
import serial

PORT = "COM3"
BAUD = 115200


class Device:
    def __init__(self, port=PORT):
        self.s = serial.Serial(port, BAUD, timeout=2)
        time.sleep(0.2)

    def close(self):
        try:
            self.s.write(b"\x02")      # normal REPL'ga qaytish
            time.sleep(0.1)
        except Exception:
            pass
        self.s.close()

    def enter_raw(self, attempts=25):
        """Ishlab turgan dasturni to'xtatib, raw REPL'ga kiradi."""
        for i in range(attempts):
            self.s.write(b"\r\x03\x03")     # Ctrl-C ikki marta
            time.sleep(0.08)
            self.s.reset_input_buffer()
            self.s.write(b"\r\x01")         # Ctrl-A -> raw REPL
            time.sleep(0.25)
            data = self.s.read(self.s.in_waiting or 1)
            if b"raw REPL" in data:
                return True
        return False

    def exec(self, code, timeout=15):
        """Kodni qurilmada bajaradi, natijani qaytaradi."""
        self.s.reset_input_buffer()
        self.s.write(code.encode("utf-8") + b"\x04")
        # javob: "OK" <chiqish> \x04 <xato> \x04 ">"
        buf = b""
        t0 = time.time()
        while time.time() - t0 < timeout:
            chunk = self.s.read(self.s.in_waiting or 1)
            if chunk:
                buf += chunk
                if buf.count(b"\x04") >= 2:
                    break
            else:
                time.sleep(0.01)
        if not buf.startswith(b"OK"):
            raise RuntimeError("qurilma javobi kutilmagan: {!r}".format(buf[:200]))
        body = buf[2:]
        parts = body.split(b"\x04")
        out = parts[0].decode("utf-8", "replace")
        err = parts[1].decode("utf-8", "replace") if len(parts) > 1 else ""
        if err.strip():
            raise RuntimeError("qurilmada xato:\n" + err.strip())
        return out.strip()

    def put_file(self, local_path, remote_name, chunk=192):
        with open(local_path, "rb") as f:
            data = f.read()
        self.exec("f=open({!r},'wb')".format(remote_name))
        total = len(data)
        for i in range(0, total, chunk):
            self.exec("f.write({!r})".format(data[i:i + chunk]))
            done = min(i + chunk, total)
            pct = done * 100 // total
            sys.stdout.write("\r    {} : {}% ({}/{} bayt)".format(remote_name, pct, done, total))
            sys.stdout.flush()
        self.exec("f.close()")
        print()
        size = self.exec("import os; print(os.stat({!r})[6])".format(remote_name))
        return int(size), total


def main():
    if len(sys.argv) < 2:
        print(__doc__)
        return 1
    cmd = sys.argv[1]
    d = Device()
    try:
        if not d.enter_raw():
            print("XATO: qurilma javob bermadi. Thonny ochiq bo'lmasin, "
                  "kabelni qayta ulab ko'ring.")
            return 1

        if cmd == "info":
            print(d.exec(
                "import os,sys\n"
                "print('MicroPython:', sys.implementation)\n"
                "print('fayllar:', os.listdir())\n"
                "import gc; print('bo\\'sh xotira:', gc.mem_free())"))

        elif cmd == "disarm":
            out = d.exec(
                "import os\n"
                "try:\n"
                "    os.rename('main.py','main_old.py')\n"
                "    print('main.py -> main_old.py (vaqtincha o\\'chirildi)')\n"
                "except Exception as e:\n"
                "    print('e:', e)")
            print(out)

        elif cmd == "put":
            local, remote = sys.argv[2], sys.argv[3]
            got, sent = d.put_file(local, remote)
            print("    yuklandi: {} bayt (kutilgan {}) - {}".format(
                got, sent, "TO'G'RI" if got == sent else "HAJM MOS EMAS!"))

        elif cmd == "write":
            name, text = sys.argv[2], sys.argv[3]
            d.exec("f=open({!r},'w'); f.write({!r}); f.close()".format(name, text))
            back = d.exec("print(open({!r}).read())".format(name))
            print("    {} yozildi: {}".format(name, back))

        elif cmd == "reset":
            d.s.write(b"\x02")
            time.sleep(0.2)
            d.s.write(b"\r\x04")       # soft reset
            print("    qayta ishga tushirildi")

        else:
            print("noma'lum buyruq:", cmd)
            return 1
        return 0
    finally:
        d.close()


if __name__ == "__main__":
    sys.exit(main())
