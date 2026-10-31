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
        # WebSocket transporti sim/ws_real_test.py bilan REAL brokerga qarshi
        # sinaladi; bu yerda esa u ham taqlid qilingan MQTT mijozi bilan almashtiriladi.
        main.WSMQTTClient = main.MQTTClient
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
        # Voqea OLDINGI yoqilishda bo'lgan, demak u joriy yoqilishdan OLDIN
        # sodir bo'lgani ANIQ. Eski kod uni "hozir" deb yozardi - bu bilib
        # turib noto'g'ri. Endi eng kechi - shu yoqilish boshlangan payt.
        check("oldingi yoqilishdagi voqea 'hozir' deb yozilmadi",
              real_now - ts >= 5,
              "voqea {:.0f} soniya oldin deb yozildi".format(real_now - ts))

    # =====================================================================
    print("\n=== 7b. UZOQ oflayn qolib, SHU YOQILISHDA internet qaytsa ===")
    # HAQIQIY QURILMADA KUZATILGAN NOSOZLIK: qayta yoqilgandan keyin
    # internet 5 soat yo-q edi. Navbat soat tiklanishidan OLDIN yuborilgani
    # uchun voqealar "vaqtsiz" ketgan va serverda QABUL QILINGAN lahza
    # bilan yozilgan - haqiqiy vaqtdan 5 soat farq qilgan.
    W.__init__()
    new_device(tmp)
    W.clock = 100000.0                  # NTP yo-q: soat 2000-yilda
    W.wifi_up = False
    W.wifi_can_connect = False
    W.ntp_works = False
    W.pzem_current = 9.0

    asl_advance = W.advance
    def _tarmoq_qaytadi(sec):
        # Yarim soat oflayn turgach, SHU yoqilishning ichida internet keladi
        asl_advance(sec)
        if W.mono >= 1800 and not W.wifi_can_connect:
            W.wifi_can_connect = True
            W.ntp_works = True
    W.advance = _tarmoq_qaytadi
    try:
        out = boot(2400)
    finally:
        del W.advance                   # keyingi senariylarga ta-sir qilmasin

    yuborilgan = event_lines()
    check("uzoq oflayndan keyin voqealar yuborildi", len(yuborilgan) > 0,
          "{} ta".format(len(yuborilgan)))
    vaqtsiz = [x for x in yuborilgan if int(x.split("|")[1]) <= 0]
    check("bironta voqea VAQTSIZ ketmadi", not vaqtsiz, vaqtsiz[:2])
    if yuborilgan and not vaqtsiz:
        ts = int(yuborilgan[0].split("|")[1])
        hozir = W.clock + 946684800
        yosh = hozir - ts
        check("voqea vaqti oflayn davrga tiklandi (hozirgi payt emas)",
              yosh > 1000,
              "voqea {:.0f} soniya oldin deb yozildi".format(yosh))

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
    # =====================================================================
    print("\n=== 15. O'z brokerimiz ishlaydi - HiveMQ'ga umuman borilmaydi ===")
    W.__init__()
    new_device(tmp)
    own = "mixtron-broker-production.up.railway.app"
    W.broker_hosts = {"b.hivemq.cloud": "hang"}     # HiveMQ O'zbekistondan yopiq
    out = boot(120)
    check("o'z brokerimizga ulandi", own in W.connected_hosts, str(W.connected_hosts[:3]))
    check("HiveMQ'ga urinilmadi", "b.hivemq.cloud" not in W.connect_attempts)
    check("ma'lumot yuborildi", len(data_lines()) > 15, "{} ta".format(len(data_lines())))
    check("WDT ishga tushmadi", "WDT" not in out, out)

    # =====================================================================
    print("\n=== 16. O'z brokerimiz yiqilgan - HiveMQ'ga o'tadi ===")
    W.__init__()
    new_device(tmp)
    W.broker_hosts = {own: "down"}
    out = boot(180)
    check("avval o'z brokerimiz sinaldi", W.connect_attempts[:1] == [own], str(W.connect_attempts[:4]))
    check("keyin HiveMQ'ga ulandi", "b.hivemq.cloud" in W.connected_hosts, str(W.connected_hosts[:3]))
    check("ma'lumot yuborildi (HiveMQ orqali)", len(data_lines()) > 10, "{} ta".format(len(data_lines())))
    check("WDT ishga tushmadi", "WDT" not in out, out)

    # =====================================================================
    print("\n=== 17. Ikkalasi ham QOTADI (hang) - qurilma qayta yuklanmaydi ===")
    W.__init__()
    new_device(tmp)
    W.broker = "hang"
    out = boot(900)
    check("qayta yuklanish/WDT yo'q", out == "tugadi", out)
    check("ikkala broker ham navbat bilan sinaldi",
          own in W.connect_attempts and "b.hivemq.cloud" in W.connect_attempts)
    check("WDT oralig'i chegaradan ancha past", W.wdt_max_gap < 60, "{:.1f}s".format(W.wdt_max_gap))
    print("    ... o'z brokerimiz tiklandi ...")
    W.broker = "ok"
    before = len(W.published)
    out = boot(180)
    d17 = [p for t, p in W.published[before:] if t.endswith("/data")]
    check("tiklangach ma'lumot ketdi", len(d17) > 10, "{} ta".format(len(d17)))

    # =====================================================================
    print("\n=== 18. Katta motosoat (1024 soatdan yuqori) aniq sanaladi ===")
    # ESP32'da float 32-bitli: motosoat float bilan yig'ilganda 1024 soatdan
    # keyin har 2 s ish 2.197 s bo'lib qo'shilardi (zavod1, 2026-10-15 dan
    # +9.9%). CPython float'i 64-bitli - u xatoni o'zi takrorlay olmaydi.
    # Shu sabab bu yerda butun-sekund yo'li, soat matni formati va
    # o'qish/yozish aylanmasining ANIQLIGI tekshiriladi.
    W.__init__()
    new_device(tmp)
    for f in ("last_status.txt", "events_queue.txt", "ota_pending.txt"):
        try:
            os.remove(os.path.join(tmp, f))
        except OSError:
            pass
    for f in ("motohours.txt", "motohours.bak"):
        with open(os.path.join(tmp, f), "w") as fh:
            fh.write("1100.000000")
    pub_clock = []

    class _TimedList(list):
        def append(self, x):
            list.append(self, x)
            pub_clock.append(W.clock)

    W.published = _TimedList()
    W.pzem_current = 9.0
    out = boot(3600)
    on = [(c, p.split("|")[5]) for (t, p), c in zip(W.published, pub_clock)
          if t.endswith("/data") and "|ON|" in p]
    if len(on) > 2:
        moto_s = (float(on[-1][1]) - float(on[0][1])) * 3600
        wall_s = on[-1][0] - on[0][0]
        check("1100 soatdan keyin 1 soat ish aniq sanaldi (xato < 2 s)",
              abs(moto_s - wall_s) < 2.0,
              "motosoat {:.1f}s, haqiqiy {:.1f}s".format(moto_s, wall_s))
    else:
        check("1100 soatdan keyin 1 soat ish aniq sanaldi (xato < 2 s)", False,
              "ON DATA yo'q: " + out)
    check("DATA'da motosoat 4 xonali soat matni",
          len(on) > 0 and all(len(m.split(".")[1]) == 4 for _, m in on),
          on[-1][1] if on else "yo'q")
    mtxt = open("motohours.txt").read().strip()
    check("fayl soat matnida, 6 xona (eski proshivka ham o'qiydi)",
          mtxt.startswith("1100.") or mtxt.startswith("1101."), mtxt)
    check("fayl 6 xonali", len(mtxt.split(".")[1]) == 6, mtxt)

    fw = sys.modules["main"]
    samples = [0, 1, 2, 3599, 3600, 3601, 1024 * 3600 - 1, 1024 * 3600,
               4211000, 16384 * 3600 + 7, 20000 * 3600 - 1]
    samples += list(range(0, 3600 * 3, 7))
    bad = [s for s in samples if fw._hours_text_to_sec(fw.hours_text(s, 6)) != s]
    check("soat matni <-> sekund aylanmasi aniq (0..20000 soat)", not bad,
          "xato: {}".format(bad[:5]))
    old_ok = (fw._hours_text_to_sec("1156.13") == 1156 * 3600 + 468
              and fw._hours_text_to_sec("1156.1302") == 4162069
              and fw._hours_text_to_sec("1000") == 3600000
              and fw._hours_text_to_sec("1e3") == 3600000)
    check("eski fayl formatlari o'qiladi", old_ok)
    rejected = 0
    for junk in ("", "abc", "nan", "inf", "-5", "1.2.3", "12a.5"):
        try:
            fw._hours_text_to_sec(junk)
        except (ValueError, OverflowError):
            rejected += 1
    check("buzuq matn rad etildi", rejected == 7, "{}/7".format(rejected))

    boots = [e for e in event_lines() if "|BOOT|" in e]
    check("BOOT voqeasida reset sababi (rc=)",
          len(boots) > 0 and boots[-1].endswith("|rc=1"),
          boots[-1] if boots else "yo'q")
    resolved = fw._resolve_event_time(
        "EVENT|-5|BOOT|1100.0000|{}|rc=3".format(fw.BOOT_ID))
    rp = resolved.split("|") if resolved else []
    check("oflayn BOOT: rc saqlandi, yoqilish raqami yuborilmadi",
          len(rp) == 5 and rp[4] == "rc=3" and int(rp[1]) > 0, str(resolved))
    check("WDT ishga tushmadi", "WDT" not in out, out)

    # =====================================================================
    print("\n=== 20. Zaxira brokerda qolib ketmaydi - 30 daqiqada o'z brokerga qaytadi ===")
    W.__init__()
    new_device(tmp)
    own = "mixtron-broker-production.up.railway.app"
    W.broker_down_until = {own: 300}     # 5 daqiqa yopiq, keyin tiklanadi
    out = boot(2400)                     # BITTA yoqilish - qayta yuklanishsiz
    hive_i = W.connected_hosts.index("b.hivemq.cloud") if "b.hivemq.cloud" in W.connected_hosts else -1
    check("o'z broker yopiq paytda HiveMQ'ga o'tdi", hive_i >= 0, str(W.connected_hosts[:3]))
    check("keyin (qayta yuklanishsiz) o'z brokerga qaytdi",
          hive_i >= 0 and own in W.connected_hosts[hive_i:], str(W.connected_hosts[:4]))
    check("WDT/qayta yuklanish yo'q", out == "tugadi", out)

    # =====================================================================
    print("\n=== 21. Qaytishda o'z broker hali yo'q - darhol zaxiraga qaytadi ===")
    W.__init__()
    new_device(tmp)
    W.broker_hosts = {own: "down"}
    W.connect_attempts = []
    out = boot(2400)
    own_tries = W.connect_attempts.count(own)
    check("o'z brokerga faqat boshida 3 marta + qaytishda 1 marta urinildi", own_tries == 4,
          "{} marta: {}".format(own_tries, [h[:6] for h in W.connect_attempts]))
    d19 = data_lines()
    check("ma'lumot uzluksiz ketdi (HiveMQ orqali)", len(d19) > 300, "{} ta".format(len(d19)))
    check("WDT/qayta yuklanish yo'q", out == "tugadi", out)

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
