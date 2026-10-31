"""mixtron-nazorat - logger (mixtron-datchik) va datchiklarni kuzatuvchi servis.

Har NAZORAT_ORALIQ_S (5 daqiqa) da loggerning /nazorat jamlanmasini o'qiydi
va g'alati holatlarni shaxsiy chatga (TELEGRAM_CHAT_ID) yozadi. Logger o'zi
yiqilsa o'zi haqida xabar bera olmaydi - shuning uchun bu alohida servis va
Telegram'ga to'g'ridan-to'g'ri yuboradi.

Bir muammo bir marta aytiladi, davom etsa har TAKROR_S (6 soat) da eslatiladi,
tugaganda "tugadi" xabari boradi. Har kuni XULOSA_VAQTI (09:00) da qisqa
kunlik xulosa yuboriladi. Faqat standart kutubxona.
"""
import json
import os
import time
import urllib.request
from urllib.parse import urlencode

LOGGER_URL = os.environ.get("LOGGER_URL", "http://mixtron-datchik.railway.internal:8080").rstrip("/")
TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN", "")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "")
NAZORAT_ORALIQ_S = int(os.environ.get("NAZORAT_ORALIQ_S", "300"))
XULOSA_VAQTI = os.environ.get("XULOSA_VAQTI", "09:00")
TAKROR_S = 6 * 3600
SOATLIK_CHEGARA = 10                 # nazoratning o'zi ham toshqin qilmasin
UZ_OFFSET = 5 * 3600

JIM_DAQ = 10                         # logger bilan bir xil chegara
HIVEMQ_CHEGARA_DAQ = 60              # proshivka 30 daqiqada o'z brokerga qaytishi kerak
BOOT_CHEGARA = 4                     # 2 soatda shuncha qayta yuklanish - g'alati
SOAT_FARQ_S = 600                    # qurilma soati shuncha oldinda bo'lsa
TG_XATO_CHEGARA = 3
MOTOSOAT_ORQAGA_SOAT = 0.05              # 3 daqiqa


def mahalliy(ts=None, fmt="%Y-%m-%d %H:%M"):
    return time.strftime(fmt, time.gmtime((time.time() if ts is None else ts) + UZ_OFFSET))


def vaqt_ts(matn):
    """'YYYY-MM-DD HH:MM[:SS]' (mahalliy) -> UTC ts."""
    import calendar
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M"):
        try:
            return calendar.timegm(time.strptime(matn, fmt)) - UZ_OFFSET
        except (ValueError, TypeError):
            pass
    return None


_yuborilgan = []


def telegram(matn):
    print("[NAZORAT]", matn.replace(chr(10), " | "))
    hozir = time.time()
    _yuborilgan[:] = [t for t in _yuborilgan if hozir - t < 3600]
    if len(_yuborilgan) >= SOATLIK_CHEGARA:
        print("[NAZORAT] soatlik chegara - yuborilmadi")
        return False
    if not (TELEGRAM_TOKEN and TELEGRAM_CHAT_ID):
        return False
    try:
        data = urlencode({"chat_id": TELEGRAM_CHAT_ID, "text": matn[:4000]}).encode("utf-8")
        with urllib.request.urlopen("https://api.telegram.org/bot{}/sendMessage".format(TELEGRAM_TOKEN),
                                    data=data, timeout=20) as r:
            r.read()
        _yuborilgan.append(hozir)
        return True
    except Exception as e:
        print("[NAZORAT] Telegramga yuborilmadi:", e)
        return False


def ol(yol, timeout=60):
    with urllib.request.urlopen(LOGGER_URL + yol, timeout=timeout) as r:
        return json.loads(r.read())


def muammolar(j, hozir):
    """{kalit: (matn, doimiy)} - doimiy=True bo'lsa tugaganda "tugadi" aytiladi;
    False - bir martalik hodisa (kalitida vaqti bor, qayta aytilmaydi)."""
    m = {}
    for broker, h in (j.get("mqtt") or {}).items():
        dan = vaqt_ts(h.get("dan"))
        if not h.get("ulangan") and dan and hozir - dan > JIM_DAQ * 60:
            m["mqtt:" + broker] = ("Logger {} brokeriga {} dan beri ulanmagan".format(broker, h["dan"]), True)

    zavodlar = j.get("zavodlar") or []
    jimlar = [z for z in zavodlar if z.get("jim_daqiqa") is not None and z["jim_daqiqa"] >= JIM_DAQ]
    if zavodlar and len(jimlar) == len(zavodlar):
        m["hamma_jim"] = ("Hamma zavod jim ({} daqiqadan ko'p) - broker yoki logger MQTT muammosi bo'lishi mumkin"
                          .format(min(z["jim_daqiqa"] for z in jimlar)), True)

    for z in zavodlar:
        nom = z["zavod"]
        jim = z.get("jim_daqiqa")
        tirik = jim is not None and jim < JIM_DAQ
        dan = vaqt_ts(z.get("broker_dan"))
        if tirik and z.get("broker") == "hivemq" and dan and hozir - dan > HIVEMQ_CHEGARA_DAQ * 60:
            m[nom + ":hivemq"] = ("{} {} dan beri zaxira HiveMQ'da - o'z brokerga qaytmayapti"
                                  .format(nom, z["broker_dan"]), True)
        if z.get("boot_soni", 0) >= BOOT_CHEGARA:
            m[nom + ":boot"] = ("{} oxirgi {} soatda {} marta qayta yuklandi".format(
                nom, j.get("oraliq_soat"), z["boot_soni"]), True)
        for o in z.get("motosoat_orqaga") or []:
            # qayta yuklanishda 20-30 s orqaga qaytish odatiy (motosoat xotiraga
            # kechikib saqlanadi) - faqat MOTOSOAT_ORQAGA_SOAT dan ko'pi aytiladi
            if (o["oldin"] or 0) - (o["keyin"] or 0) < MOTOSOAT_ORQAGA_SOAT:
                continue
            m["{}:orqaga:{}".format(nom, o["vaqt"])] = (
                "{} motosoat ORQAGA ketdi {}: {} -> {}".format(nom, o["vaqt"], o["oldin"], o["keyin"]), False)
        for s in z.get("motosoat_sakrash") or []:
            m["{}:sakrash:{}".format(nom, s["vaqt"])] = (
                "{} motosoat sakradi {}: {} daqiqada {} soat qo'shildi".format(
                    nom, s["vaqt"], s["oraliq_daq"], s["motosoat_osdi_soat"]), False)
        for v in z.get("voqealar") or []:
            farq = v.get("qabul_minus_voqea_s")
            if farq is not None and farq < -SOAT_FARQ_S:
                m["{}:soat:{}".format(nom, v["vaqt"])] = (
                    "{} soati {} oldinda: {} voqeasi {} vaqt bilan keldi (qabul {})".format(
                        nom, soat_daq(-farq), v["holat"], v["vaqt"], v.get("qabul_qilingan")), False)

    tg = j.get("telegram") or []
    xatolar = [t for t in tg if t.get("natija") == "xato" and (vaqt_ts(t["vaqt"]) or 0) > hozir - 3600]
    if len(xatolar) >= TG_XATO_CHEGARA:
        m["tg_xato"] = ("Logger oxirgi soatda Telegramga {} ta xabarni yubora olmadi".format(len(xatolar)), True)

    # Hisobotlar o'z vaqtida chiqdimi (kunlik 00:02, smena 08:01; 30 daq kechikishga ruxsat)
    kecha = mahalliy(hozir - 86400, "%Y-%m-%d")
    soat_daqiqa = mahalliy(hozir, "%H:%M")
    bor = {h["kalit"] for h in j.get("hisobotlar_yuborilgan") or []} | \
          {h["kalit"] for h in j.get("hisobotlar_ushlangan") or []}
    for kalit, boshi, oxiri in ((kecha, "00:30", "06:00"), ("smena:" + kecha, "08:30", "12:00")):
        if boshi <= soat_daqiqa < oxiri and kalit not in bor:
            m["hisobot:" + kalit] = ("{} hisoboti hali yuborilmagan".format(kalit), True)
    for h in j.get("hisobotlar_ushlangan") or []:
        m["ushlangan:" + h["kalit"]] = ("{} hisoboti shubhali deb ushlab qolingan: {}".format(
            h["kalit"], h.get("sabab")), False)
    return m


def soat_daq(s):
    s = int(abs(s))
    return "{}:{:02d}".format(s // 3600, s % 3600 // 60)


def kunlik_xulosa():
    kecha = mahalliy(time.time() - 86400, "%Y-%m-%d")
    q = ["📋 Nazorat xulosasi - " + mahalliy()]
    try:
        tl = ol("/timeline?sana=" + kecha, timeout=120)
        q.append("Kecha ({}) ishlagan:".format(kecha))
        for z in tl.get("zavodlar") or []:
            q.append("  {}: {}{}".format(z["zavod"], z["ishlagan"],
                                         " (aloqa yo'q " + z["aloqa_yoq_dan"] + " dan)" if z.get("aloqa_yoq_dan") else ""))
    except Exception as e:
        q.append("Kechagi vaqt chizig'i olinmadi: {}".format(e))
    try:
        j = ol("/nazorat?soat=24", timeout=120)
        q.append("Hozir:")
        for z in j.get("zavodlar") or []:
            q.append("  {}: {} daq oldin, {}, 24 soatda {} qayta yuklanish".format(
                z["zavod"], z.get("jim_daqiqa"), z.get("broker") or "broker noma'lum", z.get("boot_soni", 0)))
        tg = j.get("telegram") or []
        q.append("Telegram 24 soatda: kanal {}, shaxsiy {}, xato {}, to'xtatilgan {}".format(
            sum(1 for t in tg if t["tur"] == "kanal" and t["natija"] == "ok"),
            sum(1 for t in tg if t["tur"] == "ogoh" and t["natija"] == "ok"),
            sum(1 for t in tg if t["natija"] == "xato"),
            sum(1 for t in tg if t["natija"] == "chegara")))
        ush = j.get("hisobotlar_ushlangan") or []
        if ush:
            q.append("Ushlangan hisobotlar: " + ", ".join(h["kalit"] for h in ush[:4]))
    except Exception as e:
        q.append("Nazorat jamlanmasi olinmadi: {}".format(e))
    return chr(10).join(q)


def main():
    print("[NAZORAT] yoqildi: {} har {} s, xulosa {} da, Telegram {}".format(
        LOGGER_URL, NAZORAT_ORALIQ_S, XULOSA_VAQTI,
        "sozlangan" if TELEGRAM_TOKEN and TELEGRAM_CHAT_ID else "SOZLANMAGAN"))
    faol = {}                 # kalit -> (matn, oxirgi_aytilgan)
    aytilgan_bir_martalik = {}  # kalit -> ts (2 kun saqlanadi)
    xatolar_ketma = 0
    birinchi = True
    xulosa_kuni = mahalliy(fmt="%Y-%m-%d") if mahalliy(fmt="%H:%M") >= XULOSA_VAQTI else None
    while True:
        hozir = time.time()
        try:
            j = ol("/nazorat?soat=2")
            if xatolar_ketma >= 2:
                telegram("✅ Logger yana javob beryapti")
            xatolar_ketma = 0
            yangi = muammolar(j, hozir)
        except Exception as e:
            xatolar_ketma += 1
            print("[NAZORAT] logger javob bermadi:", e)
            if xatolar_ketma == 2:
                telegram("🚨 Logger (mixtron-datchik) javob bermayapti: {}".format(e))
            elif xatolar_ketma > 2 and xatolar_ketma % (TAKROR_S // NAZORAT_ORALIQ_S) == 0:
                telegram("🚨 Logger hali ham javob bermayapti ({} daqiqa)".format(xatolar_ketma * NAZORAT_ORALIQ_S // 60))
            yangi = None

        if yangi is not None:
            for kalit, (matn, doimiy) in yangi.items():
                if not doimiy:
                    # birinchi aylanishda (deploy/qayta ishga tushish) eski
                    # hodisalar qayta aytilmaydi - faqat eslab qolinadi
                    if kalit not in aytilgan_bir_martalik:
                        aytilgan_bir_martalik[kalit] = hozir
                        if not birinchi:
                            telegram("⚠ " + matn)
                    continue
                oldin = faol.get(kalit)
                if oldin is None:
                    faol[kalit] = (matn, hozir)
                    telegram("⚠ " + matn)
                elif hozir - oldin[1] >= TAKROR_S:
                    faol[kalit] = (matn, hozir)
                    telegram("⏰ Hali ham: " + matn)
            for kalit in [k for k in faol if k not in yangi]:
                telegram("✅ Tugadi: " + faol.pop(kalit)[0])
            for kalit in [k for k, t in aytilgan_bir_martalik.items() if hozir - t > 2 * 86400]:
                del aytilgan_bir_martalik[kalit]
            birinchi = False

        bugun = mahalliy(fmt="%Y-%m-%d")
        if mahalliy(fmt="%H:%M") >= XULOSA_VAQTI and xulosa_kuni != bugun:
            xulosa_kuni = bugun
            telegram(kunlik_xulosa())

        time.sleep(NAZORAT_ORALIQ_S)


if __name__ == "__main__":
    main()
