"""
Uch zavod uchun qurilmaga yuklanadigan fayllarni tayyorlaydi.

Har bir zavod uchun:
    main.py (izohli, o'qishga qulay manba)
        -> izohlar olib tashlanadi
        -> mpy-cross bilan app.mpy ga kompilyatsiya qilinadi

NIMA UCHUN SHUNDAY: ESP32 katta .py faylni o'zi kompilyatsiya qilganda
MicroPython uyumi kengayadi va bu xotira TIZIMGA QAYTMAYDI. Qurilmada
o'lchandi: 59 KB yo'qolardi va TLS (MQTT) uchun yaxlit bo'lak qolmasdi -
"MBEDTLS_ERR_PK_ALLOC_FAILED". Oldindan kompilyatsiya qilingan .mpy esa
tizim xotirasidan deyarli hech narsa olmaydi.

Qurilmaga yuklanadi:
    main.py   <- device_main.py (himoyali yo'llovchi)
    app.mpy   <- asosiy kod

Ishlatilishi:  python build_all.py
"""
import io
import os
import subprocess
import sys

ROOT = os.path.dirname(os.path.abspath(__file__))       # .../mixtron-system/tools
REPO = os.path.dirname(ROOT)                            # .../mixtron-system


def build_one(zavod):
    src = os.path.join(REPO, "esp32_" + zavod, "main.py")
    slim = os.path.join(ROOT, "_slim_" + zavod + ".py")
    out = os.path.join(REPO, "esp32_" + zavod, "app.mpy")

    subprocess.check_call([sys.executable, os.path.join(ROOT, "build_device.py"), src, slim])
    subprocess.check_call([sys.executable, "-m", "mpy_cross", "-O2", "-o", out, slim])
    os.remove(slim)
    print("    {} -> {} bayt".format(os.path.basename(out), os.stat(out)[6]))


def main():
    # device_main.py qo'lda yoziladi - unda app.mpy buzilgan holat uchun
    # himoya mantig'i bor, shuning uchun bu skript unga TEGMAYDI.
    print("yo'llovchi: device_main.py (o'zgarmaydi)")

    for z in ("zavod1", "zavod2", "zavod3"):
        print(z + ":")
        build_one(z)
    return 0


if __name__ == "__main__":
    sys.exit(main())
