"""Kunlik infografika hisobot: uch zavodning bir kunlik ishi bitta rasmda.

Har kuni 00:02 da (Toshkent) o'tgan kun uchun "MIXTRON ZAVOD HOLATI"
kanaliga yuboriladi. Rasmni /hisobot.png?sana=YYYY-MM-DD orqali ham ko'rish
mumkin - yuborishdan oldin tekshirish uchun.

Ma'lumot manbalari:
- ishlagan soat - motosoat farqi (boshqa hisobotlar bilan bir xil son);
- vaqt chizig'i va undagi yonish/o'chish vaqtlari - readings'dagi jonli
  Status (~6 soniyada bir o'lchov), daqiqalik bo'laklarga yig'ib; o'lchov
  kelmagan oraliqlar motosoat o'sishi bo'yicha to'ldiriladi.
"""
import io
import os
import time

from PIL import Image, ImageDraw, ImageFont

UZ_OFFSET = 5 * 3600

_FONT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "fonts")


def _font(size, bold=False):
    return ImageFont.truetype(
        os.path.join(_FONT_DIR, "DejaVuSans-Bold.ttf" if bold else "DejaVuSans.ttf"), size)


# --- ranglar ---
FON = (24, 30, 39)
KARTA = (34, 42, 54)
MATN = (236, 240, 244)
XIRA = (140, 154, 170)
YASHIL = (46, 204, 113)
QIZIL = (231, 76, 60)
KULRANG = (78, 90, 106)
CHIZIQ = (55, 66, 82)


def kun_statistikasi(conn, lock, device, kun_boshi, hours_fn):
    """kun_boshi - mahalliy yarim tunning UTC timestamp'i."""
    kun_oxiri = kun_boshi + 86400
    with lock:
        rows = conn.execute(
            "SELECT received_ts, status, motosoat FROM readings "
            "WHERE device = ? AND received_ts >= ? AND received_ts < ? ORDER BY received_ts",
            (device, kun_boshi, kun_oxiri),
        ).fetchall()
        oldingi_row = conn.execute(
            "SELECT motosoat FROM readings WHERE device = ? AND received_ts < ? "
            "ORDER BY received_ts DESC LIMIT 1",
            (device, kun_boshi),
        ).fetchone()

    # Daqiqalik holat: 1 = ishladi, 0 = to'xtagan, None = o'lchov yo'q.
    # mot_oxiri[i] - shu daqiqadagi oxirgi motosoat, mot_boshi[i] - birinchisi.
    daq = [None] * 1440
    mot_boshi = [None] * 1440
    mot_oxiri = [None] * 1440
    for ts, st, mot in rows:
        i = int((ts - kun_boshi) // 60)
        if 0 <= i < 1440:
            if st == "ON":
                daq[i] = 1
            elif daq[i] is None:
                daq[i] = 0
            if mot_boshi[i] is None:
                mot_boshi[i] = mot
            mot_oxiri[i] = mot

    # O'lchov kelmagan daqiqalar (aloqa uzilishi, qayta ulanish) MOTOSOAT
    # bo'yicha to'ldiriladi: ESP32 motosoatni aloqasiz ham hisoblab turadi.
    # Bo'shliq davomida motosoat bo'shliq uzunligining kamida yarmicha
    # o'sgan bo'lsa - zavod ishlagan, aks holda to'xtagan. Chegaralardan
    # biri yo'q bo'lsa (kun boshi/oxiri) - qo'shni holat olinadi.
    hozir_daq = int((time.time() - kun_boshi) // 60)
    oxirgi = min(1440, max(hozir_daq, 0))       # bugungi kunning kelajagi bo'yalmaydi
    holat = list(daq)
    i = 0
    while i < oxirgi:
        if holat[i] is not None:
            i += 1
            continue
        j = i
        while j < oxirgi and holat[j] is None:
            j += 1
        oldin = mot_oxiri[i - 1] if i > 0 else (oldingi_row[0] if oldingi_row else None)
        keyin = mot_boshi[j] if j < 1440 else None
        if oldin is not None and keyin is not None:
            qiymat = 1 if (keyin - oldin) * 60 >= 0.5 * (j - i) else 0
        elif i > 0:
            qiymat = holat[i - 1]
        else:
            qiymat = holat[j] if j < 1440 and holat[j] is not None else 0
        for k in range(i, j):
            holat[k] = qiymat
        i = j

    return {
        "zavod": device,
        "soat": hours_fn(device, kun_boshi, kun_oxiri),
        "belgilar": _belgilar(holat),
        "holat": holat,
    }


QISQA_TOXTASH_DAQ = 5       # bundan qisqa to'xtash chiziqda ko'rinadi, lekin vaqti yozilmaydi


def _belgilar(holat):
    """Chiziqqa yoziladigan yonish/o'chish vaqtlari: [(daqiqa, "ON"/"OFF")].

    Vaqt chizig'idan olinadi, cycle_events dan emas: qurilma qayta
    yuklanganda (BOOT) soniyalik soxta OFF/ON voqealari yoziladi -
    2026-10-21 zavod1 da "o'chish 18:38" aslida qayta yuklanish edi.
    QISQA_TOXTASH_DAQ dan qisqa to'xtashning ikkala chegarasi ham yozilmaydi."""
    bolaklar = []                      # (boshi, oxiri, holat)
    i = 0
    while i < 1440:
        j = i
        while j < 1440 and holat[j] == holat[i]:
            j += 1
        bolaklar.append((i, j, holat[i]))
        i = j
    natija = []
    for k in range(1, len(bolaklar)):
        oldin, hozir = bolaklar[k - 1], bolaklar[k]
        if oldin[2] == 0 and hozir[2] == 1:
            toxtash = oldin[1] - oldin[0]
            if toxtash >= QISQA_TOXTASH_DAQ or oldin[0] == 0:
                natija.append((hozir[0], "ON"))
        elif oldin[2] == 1 and hozir[2] == 0:
            toxtash = hozir[1] - hozir[0]
            # kun oxirigacha (yoki hozirgacha) davom etayotgan to'xtash ham yoziladi
            if toxtash >= QISQA_TOXTASH_DAQ or hozir[1] == 1440 or (k + 1 < len(bolaklar) and bolaklar[k + 1][2] is None):
                natija.append((hozir[0], "OFF"))
    return natija


def soat_daqiqa(soat):
    """22.2 -> '22:12' (ishlagan vaqt hamma Telegram xabarlarida shu formatda)."""
    jami_daq = int(round(soat * 60))
    return "{}:{:02d}".format(jami_daq // 60, jami_daq % 60)


def _hm(ts):
    return time.strftime("%H:%M", time.gmtime(ts + UZ_OFFSET))


def _zavod_nomi(device):
    return "Zavod " + device[5:] if device.startswith("zavod") else device


QATORLAR = 3                # vaqt yozuvlari uchun qatorlar soni (chiziq ustida)


def _belgi_joylash(d, belgilar, x_of, font, chap, ong):
    """Vaqt yozuvlarini qatorlarga taqsimlaydi: yaqin turgan yozuvlar
    ustma-ust tushmasin. [(x_markaz, matn, rang, qator, x_chiziq)] qaytaradi.
    Har yozuv eng pastki bo'sh qatorga tushadi (chiziqqa eng yaqin)."""
    joy = []
    band = [-1e9] * QATORLAR            # har qatorda oxirgi yozuvning o'ng cheti
    for daq, tur in belgilar:
        matn = "{:02d}:{:02d}".format(daq // 60, daq % 60)
        w = d.textlength(matn, font=font)
        x = min(max(x_of(daq), chap + w / 2), ong - w / 2)
        qator = next((q for q in range(QATORLAR) if x - w / 2 > band[q] + 10),
                     min(range(QATORLAR), key=lambda q: band[q]))
        band[qator] = x + w / 2
        joy.append((x, matn, YASHIL if tur == "ON" else QIZIL, qator, x_of(daq)))
    return joy


def rasm_chizish(statlar, sana_matn):
    """statlar - kun_statistikasi() natijalari ro'yxati. PNG bayt qaytaradi."""
    W = 1080
    pad = 48
    karta_h = 270
    H = 230 + len(statlar) * (karta_h + 26) + 20
    img = Image.new("RGB", (W, H), FON)
    d = ImageDraw.Draw(img)

    f_sarlavha = _font(46, True)
    f_kichik = _font(26)
    f_katta = _font(56, True)
    f_nom = _font(34, True)
    f_qiymat = _font(26, True)
    f_belgi = _font(20)
    f_vaqt = _font(21, True)

    d.text((pad, 40), "MIXTRON", font=f_sarlavha, fill=YASHIL)
    d.text((pad + d.textlength("MIXTRON ", font=f_sarlavha), 40), "kunlik hisobot",
           font=f_sarlavha, fill=MATN)
    d.text((pad, 104), sana_matn, font=f_kichik, fill=XIRA)

    jt = "Jami: " + soat_daqiqa(sum(s["soat"] for s in statlar))
    d.text((W - pad - d.textlength(jt, font=f_qiymat), 108), jt, font=f_qiymat, fill=MATN)

    # rang izohi
    lx, ly = pad, 156
    for rang, nom in ((YASHIL, "ishladi"), (KULRANG, "to'xtagan")):
        d.rounded_rectangle((lx, ly + 4, lx + 22, ly + 22), radius=4, fill=rang)
        d.text((lx + 32, ly), nom, font=f_belgi, fill=XIRA)
        lx += 50 + d.textlength(nom, font=f_belgi)
    for rang, nom in ((YASHIL, "yondi"), (QIZIL, "o'chdi")):
        d.text((lx, ly - 1), nom, font=f_vaqt, fill=rang)
        lx += 30 + d.textlength(nom, font=f_vaqt)

    y = 206
    for s in statlar:
        x0, x1 = pad, W - pad
        d.rounded_rectangle((x0, y, x1, y + karta_h), radius=20, fill=KARTA)

        d.text((x0 + 28, y + 24), _zavod_nomi(s["zavod"]), font=f_nom, fill=MATN)
        st = soat_daqiqa(s["soat"])
        d.text((x1 - 28 - d.textlength(st, font=f_katta), y + 14), st, font=f_katta, fill=YASHIL)

        # 24 soatlik vaqt chizig'i
        tx0, tx1 = x0 + 28, x1 - 28
        ty0, ty1 = y + 170, y + 212
        tw = tx1 - tx0

        def x_of(daq):
            return tx0 + tw * daq / 1440

        d.rounded_rectangle((tx0, ty0, tx1, ty1), radius=8, fill=CHIZIQ)
        holat = s["holat"]
        i = 0
        while i < 1440:
            h = holat[i]
            j = i
            while j < 1440 and holat[j] == h:
                j += 1
            rang = {1: YASHIL, 0: KULRANG}.get(h)
            if rang:
                d.rectangle((x_of(i), ty0, x_of(j), ty1), fill=rang)
            i = j

        # yonish/o'chish vaqtlari chiziq ustida. Avval hamma ko'rsatkich
        # chiziqlar, keyin fonli yozuvlar - chiziq yozuv ustidan o'tmasin.
        joy = _belgi_joylash(d, s["belgilar"], x_of, f_vaqt, tx0, tx1)
        qator_y = [ty0 - 34 - 26 * q for q in range(QATORLAR)]
        for x, matn, rang, qator, xm in joy:
            d.line((xm, qator_y[qator] + 22, xm, ty0), fill=rang, width=2)
        for x, matn, rang, qator, xm in joy:
            w = d.textlength(matn, font=f_vaqt)
            ty = qator_y[qator]
            d.rectangle((x - w / 2 - 3, ty, x + w / 2 + 3, ty + 24), fill=KARTA)
            d.text((x - w / 2, ty), matn, font=f_vaqt, fill=rang)

        # soat shkalasi chiziq ostida
        for soat in (0, 6, 12, 18, 24):
            sx = tx0 + tw * soat / 24
            d.line((sx, ty1, sx, ty1 + 6), fill=XIRA, width=2)
            lb = "{:02d}:00".format(soat)
            lw = d.textlength(lb, font=f_belgi)
            d.text((min(max(sx - lw / 2, tx0), tx1 - lw), ty1 + 8), lb, font=f_belgi, fill=XIRA)

        y += karta_h + 26

    buf = io.BytesIO()
    img.save(buf, "PNG", optimize=True)
    return buf.getvalue()


def izoh_matni(statlar, sana_matn):
    """Rasm ostidagi qisqa matn (Telegram caption)."""
    q = ["📊 Kunlik hisobot — " + sana_matn]
    for s in statlar:
        q.append("{}: {}".format(_zavod_nomi(s["zavod"]), soat_daqiqa(s["soat"])))
    q.append("Jami: " + soat_daqiqa(sum(s["soat"] for s in statlar)))
    return "\n".join(q)
