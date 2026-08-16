"""
QATTIQ SINOV (stress / chaos).

Oddiy senariylardan farqli - bu yerda falokatlar TASODIFIY va USTMA-UST
keladi: svet o'chadi, WiFi yo'qoladi, broker qotadi, quvvat kutilmagan
paytda uziladi, fayllar buziladi, disk to'ladi, xotira tugaydi.

Asosiy savol: shundan keyin ham MOTOSOAT TO'G'RI qoladimi va qurilma
o'zini tiklaydimi?
"""
import sys
import os
import shutil
import tempfile
import importlib
import random
import builtins

HERE = os.path.dirname(os.path.abspath(__file__))
FIRMWARE = os.path.abspath(os.path.join(HERE, "..", "main.py"))
sys.path.insert(0, HERE)

import harness
from harness import W, SimEnd, DeviceReset, WDTFired

RESULTS = []


def check(name, ok, detail=""):
    RESULTS.append((name, ok, detail))
    print("  [{}] {}{}".format("O'TDI" if ok else "YIQILDI", name,
                               "  -> " + detail if detail else ""))


# ---------------------------------------------------------------------------
# Haqiqiy qiymatni (ground truth) kuzatish va disk/xotira nosozliklari
# ---------------------------------------------------------------------------
_orig_advance = harness.World.advance
_orig_open = builtins.open


def _patched_advance(self, sec):
    # Qurilma YONIQ va motor ishlayotgan vaqt - haqiqiy motosoat
    if getattr(self, "powered", True) and self.pzem == "ok" and self.pzem_current > self.threshold:
        self.truth_on += sec
    _orig_advance(self, sec)


def _patched_open(path, mode="r", *a, **kw):
    # Disk to'lganini taqlid qilish
    if getattr(W, "disk_full", False) and ("w" in mode or "a" in mode):
        raise OSError(28, "ENOSPC")
    return _orig_open(path, mode, *a, **kw)


harness.World.advance = _patched_advance
builtins.open = _patched_open


def boot(run_seconds):
    sys.modules.pop("main", None)
    W.mono = 0.0
    W.wdt_last_feed = 0.0
    W.budget = run_seconds
    W.powered = True
    out = "tugadi"
    try:
        importlib.import_module("main").main()
    except SimEnd:
        out = "tugadi"
    except DeviceReset:
        out = "reset"
    except WDTFired as e:
        out = "WDT"
    except Exception as e:
        out = "XATO:" + repr(e)
    finally:
        W.budget = None
        W.powered = False          # quvvat uzildi: haqiqiy hisob to'xtaydi
    return out


def read_motohours(tmp):
    """Qurilma o'zi qanday o'qisa, sinov ham shunday o'qiydi: ikkala
    nusxadan kattasi (motosoat faqat o'sadi)."""
    best = None
    for name in ("motohours.txt", "motohours.bak"):
        p = os.path.join(tmp, name)
        if not os.path.exists(p):
            continue
        try:
            v = float(_orig_open(p).read().strip())
        except Exception:
            continue
        best = v if best is None else max(best, v)
    if best is None:
        # ikkala nusxa ham o'qib bo'lmadi
        return None if (os.path.exists(os.path.join(tmp, "motohours.txt"))
                        or os.path.exists(os.path.join(tmp, "motohours.bak"))) else 0.0
    return best


def corrupt_random_file(tmp, rnd):
    """Yozish yarmida quvvat uzilganini taqlid qiladi: faylni kesib yoki
    axlat bilan to'ldirib qo'yadi."""
    cands = [f for f in ("motohours.txt", "motohours.bak", "last_status.txt",
                         "threshold.txt", "events_queue.txt", "boot_id.txt")
             if os.path.exists(os.path.join(tmp, f))]
    if not cands:
        return None
    name = rnd.choice(cands)
    p = os.path.join(tmp, name)
    how = rnd.choice(["kesish", "axlat", "bo'sh"])
    data = _orig_open(p, "rb").read()
    with _orig_open(p, "wb") as f:
        if how == "kesish" and len(data) > 2:
            f.write(data[:len(data) // 2])
        elif how == "axlat":
            f.write(b"\x00\xff!!buzuq!!\n")
        # "bo'sh" - hech narsa yozilmaydi
    return "{} ({})".format(name, how)


def run():
    harness.install_all()
    tmp = tempfile.mkdtemp(prefix="esp32stress_")
    os.chdir(tmp)
    sys.path.insert(0, tmp)
    shutil.copy(FIRMWARE, os.path.join(tmp, "main.py"))
    with _orig_open(os.path.join(tmp, "wifi_config.json"), "w") as f:
        f.write('{"ssid": "T", "password": "p"}')
    with _orig_open(os.path.join(tmp, "mqtt_config.json"), "w") as f:
        f.write('{"broker": "b.hivemq.cloud", "user": "u", "password": "p"}')

    W.__init__()
    W.truth_on = 0.0
    W.threshold = 1.5
    W.powered = False
    W.disk_full = False

    seed = int(os.environ.get("SEED", "12345"))
    rnd = random.Random(seed)           # takrorlanadigan tasodif
    print("    tasodif urug'i (seed):", seed)

    # =====================================================================
    print("\n=== A. XAOS: 60 ta tasodifiy yoqilish, ustma-ust falokatlar ===")
    outcomes = {}
    corruptions = 0
    mh_prev = 0.0
    motohours_decreased = 0
    bad_file_reads = 0

    for i in range(60):
        # Tasodifiy dunyo holati
        W.pzem = "ok" if rnd.random() > 0.25 else "dead"
        W.pzem_current = rnd.choice([0.0, 0.05, 1.4, 1.6, 9.0, 13.5])
        W.wifi_up = rnd.random() > 0.35
        W.wifi_can_connect = rnd.random() > 0.3
        W.ntp_works = rnd.random() > 0.3
        W.broker = rnd.choice(["ok", "ok", "down", "hang"])
        W.disk_full = rnd.random() < 0.08

        # Ba'zan soat sakraydi (NTP/RTC nosozligi)
        if rnd.random() < 0.15:
            W.clock += rnd.choice([-3600, 86400, 30 * 86400])

        out = boot(rnd.randint(20, 400))     # ixtiyoriy paytda quvvat uziladi
        outcomes[out] = outcomes.get(out, 0) + 1

        W.disk_full = False
        mh = read_motohours(tmp)
        if mh is None:
            bad_file_reads += 1
        else:
            if mh + 1e-9 < mh_prev:
                motohours_decreased += 1
            mh_prev = max(mh_prev, mh)

        # Ba'zan quvvat yozish paytida uzilgan - fayl buziladi
        if rnd.random() < 0.25:
            c = corrupt_random_file(tmp, rnd)
            if c:
                corruptions += 1

    print("    yoqilish natijalari:", outcomes)
    print("    buzilgan fayl soni:", corruptions)

    crashes = sum(v for k, v in outcomes.items() if k.startswith("XATO"))
    wdts = outcomes.get("WDT", 0)
    check("hech qachon ushlanmagan xato bilan qulamadi", crashes == 0,
          "{} ta qulash".format(crashes))
    check("WDT hech qachon ishga tushmadi (qotib qolmadi)", wdts == 0,
          "{} marta".format(wdts))
    check("motosoat hech qachon ORQAGA ketmadi", motohours_decreased == 0,
          "{} marta kamaydi".format(motohours_decreased))
    check("motosoat fayli hech qachon o'qib bo'lmas holga kelmadi",
          bad_file_reads == 0, "{} marta".format(bad_file_reads))

    # =====================================================================
    print("\n=== B. Tiklanish: xaosdan keyin hammasi joyiga tushadimi ===")
    W.pzem = "ok"
    W.pzem_current = 9.0
    W.wifi_up = True
    W.wifi_can_connect = True
    W.ntp_works = True
    W.broker = "ok"
    W.disk_full = False
    before = len(W.published)
    out = boot(300)
    d = [p for t, p in W.published[before:] if t.endswith("/data")]
    e = [p for t, p in W.published[before:] if t.endswith("/events")]
    check("xaosdan keyin ma'lumot yuborish tiklandi", len(d) > 20,
          "{} ta DATA".format(len(d)))
    check("xaosdan keyin voqealar ham ketyapti", len(e) > 0, "{} ta".format(len(e)))
    check("holat to'g'ri aniqlandi (ON)", any("|ON|" in x for x in d))

    # =====================================================================
    print("\n=== C. MOTOSOAT ANIQLIGI (uzoq, tinch ish) ===")
    # Toza qurilma, aniq o'lchash
    for f in os.listdir(tmp):
        if f not in ("main.py", "wifi_config.json", "mqtt_config.json"):
            try:
                os.remove(os.path.join(tmp, f))
            except OSError:
                pass
    W.__init__()
    W.truth_on = 0.0
    W.threshold = 1.5
    W.powered = False
    W.disk_full = False
    W.pzem_current = 9.0

    total_truth = 0.0
    for i in range(6):                  # 6 ta yoqilish, orasida quvvat uzilishi
        W.truth_on = 0.0
        # motor goh ishlaydi, goh o'chadi
        W.pzem_current = 9.0 if i % 2 == 0 else 0.0
        boot(1800)                      # yarim soatlik ish
        total_truth += W.truth_on

    mh = read_motohours(tmp)
    truth_h = total_truth / 3600.0
    err = abs(mh - truth_h) / truth_h * 100 if truth_h else 0
    check("motosoat haqiqiy ish vaqtiga mos (xato < 5%)", err < 5.0,
          "hisoblangan={:.4f}s, haqiqiy={:.4f}s, xato={:.2f}%".format(mh, truth_h, err))

    # =====================================================================
    print("\n=== D. Tez-tez ON/OFF (chegara atrofida tebranish) ===")
    W.__init__()
    W.threshold = 1.5
    W.powered = False
    W.broker = "down"                   # yuborilmaydi - navbat to'ladi
    W.pzem = "ok"
    q = os.path.join(tmp, "events_queue.txt")
    try:
        os.remove(q)
    except OSError:
        pass

    class Flapper:
        """Har o'qishda chegaraning u yoq-bu yog'iga sakraydigan tok."""
        def __init__(self):
            self.n = 0

        def __call__(self):
            self.n += 1
            return 9.0 if self.n % 2 else 0.0

    flap = Flapper()
    orig_read = harness.FakeUART.read

    def flapping_read(self, *a):
        W.pzem_current = flap()
        return orig_read(self, *a)

    harness.FakeUART.read = flapping_read
    out = boot(1200)
    harness.FakeUART.read = orig_read

    n = 0
    if os.path.exists(q):
        n = len(_orig_open(q).read().strip().splitlines())
    check("tebranishda ham navbat chegaradan oshmadi", n <= 500,
          "{} qator".format(n))
    check("tebranish qulatmadi", out in ("tugadi", "reset"), out)

    # =====================================================================
    print("\n=== E. PZEM buzuq ma'lumot yuborsa ===")
    W.__init__()
    W.powered = False
    W.broker = "ok"
    W.wifi_up = True

    def garbage_read(self, *a):
        kind = rnd.choice(["short", "badcrc", "wrongaddr", "insane", "empty"])
        if kind == "short":
            return b"\xf8\x04\x14\x00"
        if kind == "empty":
            return b""
        frame = bytearray(harness._pzem_frame(220.0, 9.0, 1500.0, 100, 50.0, 0.8))
        if kind == "badcrc":
            frame[-1] ^= 0xFF
        elif kind == "wrongaddr":
            frame[0] = 0x01
        elif kind == "insane":
            frame[3] = 0xFF          # voltajni mantiqsiz qilish
            frame[4] = 0xFF
        return bytes(frame)

    harness.FakeUART.read = garbage_read
    before = len(W.published)
    out = boot(200)
    harness.FakeUART.read = orig_read
    d = [p for t, p in W.published[before:] if t.endswith("/data")]
    bad = [x for x in d if "|ON|" in x]
    check("buzuq PZEM ma'lumoti qulatmadi", out == "tugadi", out)
    check("buzuq ma'lumot ISHLAYAPTI deb qabul qilinmadi", len(bad) == 0,
          "{} ta soxta ON".format(len(bad)))

    # =====================================================================
    print("\n=== F. Uzoq OFLAYN (2 hafta), keyin aloqa tiklandi ===")
    W.__init__()
    W.powered = False
    W.wifi_up = False
    W.wifi_can_connect = False
    W.ntp_works = False
    W.pzem = "ok"
    for f in ("events_queue.txt", "last_status.txt"):
        try:
            os.remove(os.path.join(tmp, f))
        except OSError:
            pass

    for i in range(14):                 # 14 kun oflayn
        W.pzem_current = 9.0 if i % 2 == 0 else 0.0
        boot(600)

    qn = 0
    if os.path.exists(q):
        qn = len(_orig_open(q).read().strip().splitlines())
    check("oflayn davrda voqealar saqlandi", qn > 0, "{} ta navbatda".format(qn))

    W.wifi_up = True
    W.wifi_can_connect = True
    W.ntp_works = True
    W.broker = "ok"
    before = len(W.published)
    out = boot(400)
    sent = [p for t, p in W.published[before:] if t.endswith("/events")]
    check("aloqa tiklangach navbat yuborildi", len(sent) > 0,
          "{} ta yuborildi".format(len(sent)))
    bad_ts = [s for s in sent if int(s.split("|")[1]) < 1700000000]
    check("yuborilgan voqealarda 2000-yil sanasi qolmadi", len(bad_ts) == 0,
          "{} ta xato sana".format(len(bad_ts)))

    # =====================================================================
    print("\n=== G. Tarmoq NTP'ni BLOKLAGAN (internet bor, vaqt yo'q) ===")
    # Zavod tarmog'i UDP 123 ni yopib qo'ysa: MQTT ishlaydi, lekin soat
    # hech qachon to'g'rilanmaydi. Voqealar navbatda abadiy qolib
    # ketmasligi kerak.
    W.__init__()
    W.powered = False
    W.wifi_up = True
    W.wifi_can_connect = True
    W.ntp_works = False                 # NTP bloklangan
    W.broker = "ok"
    W.clock = 100000.0                  # soat 2000-yilda
    W.pzem = "ok"
    W.pzem_current = 9.0
    for f in ("events_queue.txt", "last_status.txt"):
        try:
            os.remove(os.path.join(tmp, f))
        except OSError:
            pass

    before = len(W.published)
    out = boot(900)                     # NTP_GIVEUP_S = 600 dan uzoq
    sent = [p for t, p in W.published[before:] if t.endswith("/events")]
    check("NTP bloklangan bo'lsa ham voqealar yuborildi", len(sent) > 0,
          "{} ta".format(len(sent)))
    if sent:
        check("vaqtsiz voqea 0 belgisi bilan ketdi (server to'ldiradi)",
              any(s.split("|")[1] == "0" for s in sent),
              sent[-1])
    qn = 0
    if os.path.exists(q):
        qn = len(_orig_open(q).read().strip().splitlines())
    check("navbat to'planib qolmadi", qn < 5, "{} qator qoldi".format(qn))

    # =====================================================================
    print("\n" + "=" * 64)
    passed = sum(1 for _, ok, _ in RESULTS if ok)
    total = len(RESULTS)
    print("QATTIQ SINOV NATIJASI: {}/{}".format(passed, total))
    if passed < total:
        print("\nYIQILGANLAR:")
        for n_, ok, d_ in RESULTS:
            if not ok:
                print("  - {}  {}".format(n_, d_))
    print("WDT eng uzun to'xtash: {:.1f}s (chegara 120s)".format(W.wdt_max_gap))
    return passed == total


if __name__ == "__main__":
    sys.exit(0 if run() else 1)
