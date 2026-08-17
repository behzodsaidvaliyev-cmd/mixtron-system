# Qurilmadagi main.py - asosiy kod app.mpy ichida.
#
# NIMA UCHUN AJRATILGAN: ESP32 katta .py faylni o'zi kompilyatsiya qilganda
# MicroPython uyumi kengayadi va bu xotira tizimga qaytmaydi. Natijada TLS
# (MQTT) uchun yaxlit bo'lak qolmaydi. Oldindan kompilyatsiya qilingan .mpy
# bu muammoni butunlay yo'q qiladi.
#
# BU FAYL QISQA BO'LSA HAM MUHIM: oddiy "import app" yozilsa va app.mpy
# buzilgan yoki yo'q bo'lsa, qurilma JIMGINA o'lik qolardi - dastur ishga
# tushmagani uchun WDT ham yoqilmaydi va hech kim buni bilmaydi.
try:
    import app
    app.run()
except Exception as e:
    print("[BOOT] app.mpy ishga tushmadi:", e)
    try:
        import app_prev                    # OTA'dan oldingi ISHLAYDIGAN nusxa
        print("[BOOT] zaxira nusxa (app_prev.mpy) ishlatilmoqda")
        try:
            src = open("app_prev.mpy", "rb")
            dst = open("app.mpy", "wb")
            while True:
                b = src.read(256)
                if not b:
                    break
                dst.write(b)
            src.close()
            dst.close()
            print("[BOOT] app.mpy zaxiradan tiklandi")
        except Exception as e2:
            print("[BOOT] tiklab bo'lmadi:", e2)
        app_prev.run()
    except Exception as e3:
        # Ikkalasi ham ishlamadi. Qayta yuklaymiz, lekin oldin uzoq kutamiz -
        # shunda USB orqali Ctrl-C bilan to'xtatib qo'lda tuzatish mumkin.
        print("[BOOT] zaxira ham ishlamadi:", e3)
        print("[BOOT] 60 soniyadan keyin qayta yuklanadi (to'xtatish: Ctrl-C)")
        import time
        import machine
        time.sleep(60)
        machine.reset()
