"""Kunlik infografika hisobot: uch zavodning bir kunlik ishi bitta rasmda.

Har kuni 00:02 da (Toshkent) o'tgan kun uchun "MIXTRON ZAVOD HOLATI"
kanaliga yuboriladi. Rasmni /hisobot.png?sana=YYYY-MM-DD orqali ham ko'rish
mumkin - yuborishdan oldin tekshirish uchun.

Ma'lumot manbalari:
- ishlagan soat - motosoat farqi (boshqa hisobotlar bilan bir xil son);
- yondi soni, birinchi yonish, oxirgi o'chish, vaqt chizig'i, eng uzun
  uzluksiz ish, aloqa uzilishi - readings'dagi
  jonli Status (~6 soniyada bir o'lchov), daqiqalik bo'laklarga yig'ib.
"""
import io
import os
import time

from PIL import Image, ImageDraw, ImageFont

UZ_OFFSET = 5 * 3600
JIM_CHEGARA_DAQ = 10        # shundan uzun bo'shliq = aloqa uzilishi (Telegram ogohlantirishi bilan bir xil)

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
KULRANG = (78, 90, 106)
SARIQ = (241, 196, 15)
CHIZIQ = (55, 66, 82)


def kun_statistikasi(conn, lock, device, kun_boshi, hours_fn):
    """kun_boshi - mahalliy yarim tunning UTC timestamp'i."""
    kun_oxiri = kun_boshi + 86400
    with lock:
        rows = conn.execute(
            "SELECT received_ts, status FROM readings "
            "WHERE device = ? AND received_ts >= ? AND received_ts < ? ORDER BY received_ts",
            (device, kun_boshi, kun_oxiri),
        ).fetchall()

    # Daqiqalik holat: 1 = ishladi, 0 = to'xtagan, None = o'lchov yo'q
    daq = [None] * 1440
    for ts, st in rows:
        i = int((ts - kun_boshi) // 60)
        if 0 <= i < 1440:
            if st == "ON":
                daq[i] = 1
            elif daq[i] is None:
                daq[i] = 0

    # Qisqa bo'shliqlar (qayta ulanish, bir necha soniya) oldingi holat bilan
    # to'ldiriladi; JIM_CHEGARA_DAQ dan uzunlari - aloqa uzilishi.
    holat = list(daq)
    jim_daq = 0
    i = 0
    while i < 1440:
        if holat[i] is None:
            j = i
            while j < 1440 and holat[j] is None:
                j += 1
            uzunlik = j - i
            if uzunlik >= JIM_CHEGARA_DAQ:
                for k in range(i, j):
                    holat[k] = "jim"
                jim_daq += uzunlik
            else:
                oldingi = holat[i - 1] if i > 0 and holat[i - 1] in (0, 1) else 0
                for k in range(i, j):
                    holat[k] = oldingi
            i = j
        else:
            i += 1

    # Bugungi kun hali tugamagan bo'lsa, kelajak daqiqalari "jim" emas
    hozir_daq = int((time.time() - kun_boshi) // 60)
    if hozir_daq < 1440:
        for k in range(max(hozir_daq, 0), 1440):
            if holat[k] == "jim":
                holat[k] = None
                jim_daq -= 1

    eng_uzun = joriy = 0
    for h in holat:
        joriy = joriy + 1 if h == 1 else 0
        eng_uzun = max(eng_uzun, joriy)

    # Yonish/o'chish vaqt chizig'idan olinadi, cycle_events dan emas: qurilma
    # qayta yuklanganda (BOOT) soniyalik soxta OFF/ON voqealari yoziladi -
    # 2026-10-21 zavod1 da "oxirgi o'chish 18:38" aslida qayta yuklanish edi.
    on_lar, off_lar = [], []
    for k in range(1, 1440):
        # aloqa uzilishi ("jim") chegarasi yonish/o'chish hisoblanmaydi -
        # u paytda zavod nima qilgani noma'lum
        if holat[k] == 1 and holat[k - 1] == 0:
            on_lar.append(kun_boshi + k * 60)
        elif holat[k] == 0 and holat[k - 1] == 1:
            off_lar.append(kun_boshi + k * 60)
    return {
        "zavod": device,
        "soat": hours_fn(device, kun_boshi, kun_oxiri),
        "yonish_soni": len(on_lar),
        "birinchi_yonish": _hm(on_lar[0]) if on_lar else "—",
        "oxirgi_ochish": _hm(off_lar[-1]) if off_lar else "—",
        "eng_uzun_daq": eng_uzun,
        "jim_daq": max(jim_daq, 0),
        "holat": holat,
    }


def _hm(ts):
    return time.strftime("%H:%M", time.gmtime(ts + UZ_OFFSET))


def _davomiylik(daq):
    s, m = divmod(int(daq), 60)
    if s and m:
        return "{} soat {} daq".format(s, m)
    if s:
        return "{} soat".format(s)
    return "{} daq".format(m)


def _zavod_nomi(device):
    return "Zavod " + device[5:] if device.startswith("zavod") else device


def rasm_chizish(statlar, sana_matn):
    """statlar - kun_statistikasi() natijalari ro'yxati. PNG bayt qaytaradi."""
    W = 1080
    pad = 48
    karta_h = 300
    H = 250 + len(statlar) * (karta_h + 28) + 90
    img = Image.new("RGB", (W, H), FON)
    d = ImageDraw.Draw(img)

    f_sarlavha = _font(46, True)
    f_kichik = _font(26)
    f_katta = _font(56, True)
    f_nom = _font(34, True)
    f_qiymat = _font(26, True)
    f_belgi = _font(20)

    d.text((pad, 40), "MIXTRON", font=f_sarlavha, fill=YASHIL)
    d.text((pad + d.textlength("MIXTRON ", font=f_sarlavha), 40), "kunlik hisobot",
           font=f_sarlavha, fill=MATN)
    d.text((pad, 104), sana_matn, font=f_kichik, fill=XIRA)

    jami = sum(s["soat"] for s in statlar)
    jt = "Jami: {:.1f} soat".format(jami)
    d.text((W - pad - d.textlength(jt, font=f_qiymat), 108), jt, font=f_qiymat, fill=MATN)

    # rang izohi
    lx, ly = pad, 160
    for rang, nom in ((YASHIL, "ishladi"), (KULRANG, "to'xtagan"), (SARIQ, "aloqa yo'q")):
        d.rounded_rectangle((lx, ly + 4, lx + 22, ly + 22), radius=4, fill=rang)
        d.text((lx + 32, ly), nom, font=f_belgi, fill=XIRA)
        lx += 50 + d.textlength(nom, font=f_belgi)

    y = 210
    for s in statlar:
        x0, x1 = pad, W - pad
        d.rounded_rectangle((x0, y, x1, y + karta_h), radius=20, fill=KARTA)

        d.text((x0 + 28, y + 24), _zavod_nomi(s["zavod"]), font=f_nom, fill=MATN)
        st = "{:.1f}".format(s["soat"])
        sw = d.textlength(st, font=f_katta)
        d.text((x1 - 28 - sw - d.textlength(" soat", font=f_kichik), y + 14), st,
               font=f_katta, fill=YASHIL)
        d.text((x1 - 28 - d.textlength(" soat", font=f_kichik), y + 42), " soat",
               font=f_kichik, fill=XIRA)

        # 24 soatlik vaqt chizig'i
        tx0, tx1, ty0, ty1 = x0 + 28, x1 - 28, y + 96, y + 140
        tw = tx1 - tx0
        d.rounded_rectangle((tx0, ty0, tx1, ty1), radius=8, fill=CHIZIQ)
        i = 0
        holat = s["holat"]
        while i < 1440:
            h = holat[i]
            j = i
            while j < 1440 and holat[j] == h:
                j += 1
            rang = {1: YASHIL, 0: KULRANG, "jim": SARIQ}.get(h)
            if rang:
                d.rectangle((tx0 + tw * i / 1440, ty0, tx0 + tw * j / 1440, ty1), fill=rang)
            i = j
        for soat in (0, 6, 12, 18, 24):
            sx = tx0 + tw * soat / 24
            d.line((sx, ty1, sx, ty1 + 8), fill=XIRA, width=2)
            lb = "{:02d}:00".format(soat % 24) if soat < 24 else "24:00"
            lw = d.textlength(lb, font=f_belgi)
            d.text((min(max(sx - lw / 2, tx0), tx1 - lw), ty1 + 10), lb, font=f_belgi, fill=XIRA)

        # ko'rsatkichlar - 4 ustun
        ustunlar = [
            ("Yondi", "{} marta".format(s["yonish_soni"])),
            ("Birinchi yonish", s["birinchi_yonish"]),
            ("Oxirgi o'chish", s["oxirgi_ochish"]),
            ("Eng uzun ish", _davomiylik(s["eng_uzun_daq"])),
        ]
        cw = (x1 - x0 - 56) / 4
        for k, (nom, qiymat) in enumerate(ustunlar):
            cx = x0 + 28 + k * cw
            d.text((cx, y + 196), nom, font=f_belgi, fill=XIRA)
            d.text((cx, y + 224), qiymat, font=f_qiymat, fill=MATN)
        if s["jim_daq"]:
            jm = "Aloqa yo'q: " + _davomiylik(s["jim_daq"])
            d.text((x0 + 28, y + karta_h - 34), jm, font=f_belgi, fill=SARIQ)

        y += karta_h + 28

    d.text((pad, H - 60), "Ishlagan soat - motosoat bo'yicha. Vaqt - Toshkent.",
           font=f_belgi, fill=XIRA)

    buf = io.BytesIO()
    img.save(buf, "PNG", optimize=True)
    return buf.getvalue()


def izoh_matni(statlar, sana_matn):
    """Rasm ostidagi qisqa matn (Telegram caption)."""
    q = ["📊 Kunlik hisobot — " + sana_matn]
    for s in statlar:
        q.append("{}: {:.1f} soat, {} marta yondi".format(
            _zavod_nomi(s["zavod"]), s["soat"], s["yonish_soni"]))
    q.append("Jami: {:.1f} soat".format(sum(s["soat"] for s in statlar)))
    return "\n".join(q)
