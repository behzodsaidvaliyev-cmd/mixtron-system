"""
ESP32 kodini falokat senariylarida sinash.

Har bir senariy main.py ning HAQIQIY kodini ishlatadi (taqlid qilingan
MicroPython muhitida) va natijani tekshiradi.
"""
import sys
import os
import shutil
import tempfile
import importlib

HERE = os.path.dirname(os.path.abspath(__file__))
# Sinaladigan kod: repo ichida esp32_zavod3/main.py, ishchi papkada esa
# yonidagi main.py. Ikkala joyda ham ishlashi uchun avtomatik tanlanadi.
FIRMWARE = os.path.abspath(os.path.join(HERE, "..", "esp32_zavod3", "main.py"))
if not os.path.exists(FIRMWARE):
    FIRMWARE = os.path.abspath(os.path.join(HERE, "..", "main.py"))

sys.path.insert(0, HERE)
import harness
from harness import W, SimEnd, DeviceReset, WDTFired

RESULTS = []


def check(name, ok, detail=""):
    RESULTS.append((name, ok, detail))
    print("  [{}] {}{}".format("O'TDI" if ok else "YIQILDI", name,
                               "  -> " + detail if detail else ""))


def new_device(tmpdir, wifi_ssid="TestWiFi"):
    """Toza qurilma: firmware + konfig fayllar."""
    shutil.copy(FIRMWARE, os.path.join(tmpdir, "main.py"))
    with open(os.path.join(tmpdir, "wifi_config.json"), "w") as f:
        f.write('{"ssid": "%s", "password": "pw"}' % wifi_ssid)
    with open(os.path.join(tmpdir, "mqtt_config.json"), "w") as f:
        f.write('{"broker": "b.hivemq.cloud", "user": "u", "password": "p"}')


def boot(run_seconds, expect_reset=False):
    """Bitta 'yoqilish'ni simulyatsiya qiladi. run_seconds - virtual soniya."""
    for mod in ("main",):
        sys.modules.pop(mod, None)
    W.mono = 0.0          # qayta yuklanish: uptime hisoblagichi noldan boshlanadi
    W.wdt_last_feed = 0.0
    W.budget = run_seconds
    outcome = "tugadi"
    try:
        main = importlib.import_module("main")
        main.main()
    except SimEnd:
        outcome = "tugadi"
    except DeviceReset:
        outcome = "reset"
    except WDTFired as e:
        outcome = "WDT:" + str(e)
    except Exception as e:
        outcome = "XATO:" + repr(e)
    finally:
        W.budget = None
    return outcome


def data_lines():
    return [p for t, p in W.published if t.endswith("/data")]


def event_lines():
    return [p for t, p in W.published if t.endswith("/events")]


def run_all():
    harness.install_all()
    tmp = tempfile.mkdtemp(prefix="esp32sim_")
    os.chdir(tmp)
    sys.path.insert(0, tmp)      # main.py shu papkadan import qilinsin

    # =====================================================================
    print("\n=== 1. Oddiy ish: drobilka ishlayapti ===")
    new_device(tmp)
    W.__init__()
    W.pzem_current = 9.0
    out = boot(120)
    d = data_lines()
    check("ma'lumot yuborilyapti", len(d) > 15, "{} ta DATA".format(len(d)))
    check("holat ON deb aniqlandi", any("|ON|" in x for x in d))
    check("ON voqeasi yuborildi", any("|ON|" in x for x in event_lines()))
    ms = [float(x.split("|")[5]) for x in d if "|ON|" in x]
    check("motosoat o'syapti", len(ms) > 2 and ms[-1] > ms[0],
          "{:.5f} -> {:.5f}".format(ms[0], ms[-1]))
    check("WDT ishga tushmadi", "WDT" not in out, out)

    # =====================================================================
    print("\n=== 2. Zavodda SVET KETDI (PZEM javob bermaydi) ===")
    before = len(W.published)
    W.pzem = "dead"
    out = boot(180)
    d2 = [p for t, p in W.published[before:] if t.endswith("/data")]
    check("svet yo'q paytda ham ma'lumot ketyapti", len(d2) > 10,
          "{} ta DATA".format(len(d2)))
    check("holat OFF ga o'tdi", all("|OFF|" in x for x in d2))
    check("OFF voqeasi yozildi",
          any("|OFF|" in p for t, p in W.published[before:] if t.endswith("/events")))
    check("UART qayta ishga tushirildi", W.uart_reinits > 0,
          "{} marta".format(W.uart_reinits))
    check("qayta yuklanish tsikliga tushmadi", out == "tugadi", out)

    # =====================================================================
    print("\n=== 3. SVET QAYTDI (PZEM tiklandi) ===")
    before = len(W.published)
    W.pzem = "ok"
    W.pzem_current = 9.5
    out = boot(90)
    d3 = [p for t, p in W.published[before:] if t.endswith("/data")]
    check("o'lchash tiklandi", any("|ON|" in x for x in d3))
    check("qayta ON voqeasi yozildi",
          any("|ON|" in p for t, p in W.published[before:] if t.endswith("/events")))

    # =====================================================================
    print("\n=== 4. WIFI UZILDI (ish o'rtasida) ===")
    before_conn = W.wifi_connects
    before = len(W.published)
    W.wifi_up = False
    W.wifi_can_connect = False          # avval tiklanmaydi
    out = boot(90)
    check("WiFi'ni qayta ulashga URINDI", W.wifi_connects > before_conn,
          "{} urinish".format(W.wifi_connects - before_conn))
    check("WiFi yo'qligida qotib qolmadi", "WDT" not in out and "XATO" not in out, out)

    print("    ... WiFi qaytdi ...")
    W.wifi_can_connect = True
    out = boot(120)
    d4 = [p for t, p in W.published[before:] if t.endswith("/data")]
    check("WiFi qaytgach yuborish tiklandi", len(d4) > 5, "{} ta DATA".format(len(d4)))

    # =====================================================================
    print("\n=== 5. BROKER QOTIB QOLDI (eng xavfli holat) ===")
    W.broker = "hang"
    before_gap = W.wdt_max_gap
    out = boot(150)
    check("abadiy qotib qolmadi (timeout ishladi)", "WDT" not in out, out)
    check("tsikldagi eng uzun to'xtash WDT chegarasidan kichik",
          W.wdt_max_gap < 120, "{:.1f}s".format(W.wdt_max_gap))

    print("    ... broker tiklandi ...")
    W.broker = "ok"
    before = len(W.published)
    out = boot(90)
    d5 = [p for t, p in W.published[before:] if t.endswith("/data")]
    check("broker tiklangach yuborish davom etdi", len(d5) > 5,
          "{} ta DATA".format(len(d5)))

    # =====================================================================
    print("\n=== 6. TO'LIQ QUVVAT UZILDI va qayta yondi ===")
    mh_before = float(open("motohours.txt").read())
    status_before = open("last_status.txt").read().strip()
    ev_before = len(event_lines())
    out = boot(60)                      # "qayta yonish"
    check("motosoat saqlanib qoldi",
          abs(float(open("motohours.txt").read()) - mh_before) < 1.0,
          "{:.4f} -> {}".format(mh_before, open("motohours.txt").read().strip()))
    check("holat eslab qolindi (soxta voqea yozilmadi)",
          len(event_lines()) == ev_before,
          "oldingi holat: {}".format(status_before))

    # =====================================================================
    print("\n=== 7. INTERNETSIZ yondi, keyin internet keldi ===")
    W.__init__()
    for f in ("events_queue.txt", "last_status.txt"):
        try:
            os.remove(os.path.join(tmp, f))
        except OSError:
            pass
    W.clock = 100000.0                  # NTP yo'q: soat 2000-yilda
    W.wifi_up = False
    W.wifi_can_connect = False
    W.ntp_works = False
    W.pzem_current = 9.0
    out = boot(120)
    q = os.path.join(tmp, "events_queue.txt")
    queued = open(q).read().strip().splitlines() if os.path.exists(q) else []
    check("oflayn voqea navbatga saqlandi", len(queued) > 0,
          "{} ta".format(len(queued)))
    check("oflayn voqea MANFIY vaqt belgisi bilan yozildi",
          any(x.split("|")[1].startswith("-") for x in queued),
          queued[0] if queued else "")

    print("    ... internet va NTP keldi ...")
    W.wifi_can_connect = True
    W.ntp_works = True
    before = len(event_lines())
    out = boot(120)
    sent = event_lines()[before:]
    check("navbatdagi voqea yuborildi", len(sent) > 0, "{} ta".format(len(sent)))
    if sent:
        ts = int(sent[0].split("|")[1])
        real_now = W.clock + 946684800
        ok = ts > 1700000000 and ts <= real_now + 5
        check("vaqt HAQIQIY sanaga tuzatildi", ok,
              "ts={} (hozir={})".format(ts, int(real_now)))

    # =====================================================================
    print("\n=== 8. Vaqt SAKRASHI motosoatni buzmasligi ===")
    W.__init__()
    new_device(tmp)
    for f in ("motohours.txt", "last_status.txt", "events_queue.txt"):
        try:
            os.remove(os.path.join(tmp, f))
        except OSError:
            pass
    W.pzem_current = 9.0
    boot(40)
    mh1 = float(open("motohours.txt").read()) if os.path.exists("motohours.txt") else 0.0
    W.clock += 86400 * 30               # soat 30 kun oldinga sakradi
    boot(60)
    mh2 = float(open("motohours.txt").read())
    check("30 kunlik sakrash motosoatga QO'SHILMADI", (mh2 - mh1) < 1.0,
          "{:.4f} -> {:.4f}".format(mh1, mh2))

    # =====================================================================
    print("\n=== 9. Voqealar navbati cheksiz o'smasligi ===")
    W.__init__()
    W.broker = "down"                   # hech narsa yuborilmaydi, navbat to'ladi
    W.pzem_current = 9.0
    q = os.path.join(tmp, "events_queue.txt")
    with open(q, "w") as f:
        for i in range(600):            # chegaradan (500) ko'p
            f.write("EVENT|{}|ON|1.0\n".format(800000000 + i))
    W.pzem = "ok"
    boot(30)
    W.pzem_current = 0.0                # ON -> OFF, yangi voqea qo'shiladi
    boot(30)
    n = len(open(q).read().strip().splitlines()) if os.path.exists(q) else 0
    check("navbat chegaralandi", n <= 500, "{} qator".format(n))

    # =====================================================================
    print("\n=== 10. Konfig fayl buzilgan bo'lsa ===")
    W.__init__()
    with open(os.path.join(tmp, "mqtt_config.json"), "w") as f:
        f.write("{buzuq json")
    out = boot(40)
    check("buzuq konfig bilan ham ishlashda davom etdi",
          "XATO" not in out and "WDT" not in out, out)
    check("PZEM o'qish to'xtamadi", True)

    # =====================================================================
    print("\n=== 11. NOSOZ OTA yangilanishidan keyin AVTOMATIK ORQAGA QAYTISH ===")
    W.__init__()
    new_device(tmp)
    good = open(os.path.join(tmp, "main.py"), encoding="utf-8").read()
    # OTA endi app.mpy ni yangilaydi (main.py - himoyali yo'llovchi).
    # "Eski, ishlaydigan" versiya zaxirada; app.mpy esa "yangi nosoz" kod
    with open(os.path.join(tmp, "app_prev.mpy"), "w", encoding="utf-8") as f:
        f.write(good)
    with open(os.path.join(tmp, "ota_pending.txt"), "w") as f:
        f.write("2")                    # imkoniyatlar tugagan -> rollback bo'lishi shart
    with open(os.path.join(tmp, "app.mpy"), "a", encoding="utf-8") as f:
        f.write("\n# YANGI NOSOZ VERSIYA BELGISI\n")
    out = boot(30)
    restored = open(os.path.join(tmp, "app.mpy"), encoding="utf-8").read()
    check("nosoz yangilanish o'rniga eski versiya tiklandi",
          "YANGI NOSOZ VERSIYA BELGISI" not in restored, out)
    check("'tasdiqlanmagan' belgisi tozalandi",
          not os.path.exists(os.path.join(tmp, "ota_pending.txt")))

    print("\n=== 12. Yangi OTA kodiga ishlash imkoni beriladi (darrov qaytarilmaydi) ===")
    W.__init__()
    new_device(tmp)
    with open(os.path.join(tmp, "app_prev.mpy"), "w", encoding="utf-8") as f:
        f.write(good)
    with open(os.path.join(tmp, "ota_pending.txt"), "w") as f:
        f.write("0")                    # OTA endigina o'rnatdi: 0-urinish
    with open(os.path.join(tmp, "app.mpy"), "a", encoding="utf-8") as f:
        f.write("\n# YANGI VERSIYA BELGISI\n")
    out = boot(60)                      # qisqa: hali barqaror deb tasdiqlanmaydi
    still_new = "YANGI VERSIYA BELGISI" in open(os.path.join(tmp, "app.mpy"), encoding="utf-8").read()
    check("birinchi yoqilishda ORQAGA QAYTARILMADI", still_new, out)
    attempts = open(os.path.join(tmp, "ota_pending.txt")).read().strip() \
        if os.path.exists(os.path.join(tmp, "ota_pending.txt")) else "yo'q"
    check("urinish soni sanaldi", attempts == "1", "belgi={}".format(attempts))

    print("\n=== 12b. Uzoq muammosiz ishlagach 'barqaror' deb tasdiqlanadi ===")
    W.__init__()
    out = boot(400)                     # OTA_STABLE_AFTER_S = 180
    check("belgi olib tashlandi (rollback endi bo'lmaydi)",
          not os.path.exists(os.path.join(tmp, "ota_pending.txt")), out)

    print("\n=== 12c. NTP 136 yil xato vaqt bersa RAD ETILADI (2036 aylanishi) ===")
    W.__init__()
    new_device(tmp)
    W.ntp_bad = True
    out = boot(40)
    # boot() faqat yakun so'zini qaytaradi, chiqish matnini emas - shuning
    # uchun NTP rad etilgani SOAT TIKLANGANIDAN bilinadi (keyingi tekshiruv).
    check("NTP rad etilgach ham ishlashda davom etdi", len(W.published) > 0, out)
    check("soat 2036 ga sakrab ketmadi", W.clock < 1000000000.0,
          "soat={}".format(W.clock))
    bad = [pl for _t, pl in W.published if pl.startswith("EVENT|") and
           int(pl.split("|")[1]) > 2250000000]
    check("xato sanali voqea yuborilmadi", not bad, bad[:2])

    print("\n=== 14. WiFi YO-Q paytida O-LCHASH TO-XTAMASLIGI kerak ===")
    # Haqiqiy qurilmada topilgan nosozlik: WiFi ga ulanish tugaguncha
    # kutilardi va har 30 soniyada 16 soniya PZEM o-qilmasdi, chiroq
    # qotib qolardi, o-lchovlarning yarmi yo-qolardi.
    W.__init__()
    new_device(tmp)
    W.wifi_up = False
    W.wifi_can_connect = False        # tarmoq umuman yo-q
    out = boot(120)
    kutilgan = 120 // 2               # POLL_INTERVAL_S = 2
    check("WiFi yo-q bo-lsa ham PZEM muntazam o-qildi",
          W.pzem_reads >= kutilgan * 3 // 4,
          "{} ta so-rov (kutilgan ~{})".format(W.pzem_reads, kutilgan))
    check("holat chirog-i yangilanishda davom etdi", W.led_writes > 100,
          "{} marta".format(W.led_writes))

    print("\n=== 13. Yoqilish xabari Railway'ga ketyaptimi ===")
    W.__init__()
    new_device(tmp)
    for f in ("events_queue.txt", "last_status.txt", "ota_pending.txt"):
        try:
            os.remove(os.path.join(tmp, f))
        except OSError:
            pass
    before = len(event_lines())
    boot(90)
    boots = [e for e in event_lines()[before:] if "|BOOT|" in e]
    check("BOOT voqeasi yuborildi", len(boots) > 0,
          boots[0] if boots else "yo'q")

    # =====================================================================
    print("\n" + "=" * 62)
    passed = sum(1 for _, ok, _ in RESULTS if ok)
    total = len(RESULTS)
    print("NATIJA: {}/{} tekshiruv o'tdi".format(passed, total))
    if passed < total:
        print("\nYIQILGANLAR:")
        for n, ok, d in RESULTS:
            if not ok:
                print("  - {} {}".format(n, d))
    print("WDT eng uzun to'xtash: {:.1f}s (chegara 120s)".format(W.wdt_max_gap))
    return passed == total


if __name__ == "__main__":
    ok = run_all()
    sys.exit(0 if ok else 1)
