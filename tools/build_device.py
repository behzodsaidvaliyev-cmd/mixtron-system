"""
Qurilma uchun "yengil" nusxa tayyorlaydi.

NIMA UCHUN: ESP32 .py faylni O'ZI kompilyatsiya qiladi. Fayl qancha katta
bo'lsa, kompilyatsiya paytida xotira shuncha parchalanadi. Natijada bo'sh
xotira ko'p bo'lsa ham, SSL uchun kerakli YAXLIT bo'lak topilmaydi
(MBEDTLS_ERR_PK_ALLOC_FAILED).

Izohlar bytecode'ga tushmaydi - ular faqat kompilyatsiya bosqichida yuk
beradi. Shu sabab qurilmaga izohsiz nusxa yuboriladi, repo'da esa to'liq
izohli, o'qishga qulay nusxa qoladi.

Ishlatilishi:  python build_device.py <kirish.py> <chiqish.py>
"""
import io
import sys
import tokenize


def strip_source(src):
    """Izoh va ortiqcha bo'sh qatorlarni olib tashlaydi.
    Satr ichidagi '#' belgisiga TEGMAYDI (tokenize ishlatiladi)."""
    out = []
    prev_end = (1, 0)
    prev_type = None
    tokens = tokenize.generate_tokens(io.StringIO(src).readline)
    for tok_type, tok_str, start, end, _line in tokens:
        if tok_type == tokenize.COMMENT:
            prev_end = end
            continue
        if tok_type == tokenize.NL and prev_type in (tokenize.NL, tokenize.NEWLINE, None):
            prev_end = end
            continue
        # qatorlar orasidagi joyni tiklash
        if start[0] > prev_end[0]:
            out.append("\n" * (start[0] - prev_end[0]))
            out.append(" " * start[1])
        elif start[1] > prev_end[1]:
            out.append(" " * (start[1] - prev_end[1]))
        out.append(tok_str)
        prev_end = end
        prev_type = tok_type
    text = "".join(out)
    # ketma-ket bo'sh qatorlarni siqish
    lines = [l.rstrip() for l in text.split("\n")]
    result = []
    blank = 0
    for l in lines:
        if l.strip() == "":
            blank += 1
            if blank > 1:
                continue
        else:
            blank = 0
        result.append(l)
    return "\n".join(result).strip() + "\n"


def main():
    if len(sys.argv) != 3:
        print(__doc__)
        return 1
    src_path, dst_path = sys.argv[1], sys.argv[2]
    with io.open(src_path, encoding="utf-8") as f:
        src = f.read()

    stripped = strip_source(src)

    # OTA to'liqlik belgisi saqlanishi SHART - u izoh qatorida turadi,
    # shuning uchun qayta qo'shiladi.
    marker = "# OTA-FAYL-OXIRI"
    if marker not in stripped:
        stripped = stripped.rstrip() + "\n\n" + marker + "\n"

    with io.open(dst_path, "w", encoding="utf-8", newline="\n") as f:
        f.write(stripped)

    # Sinov: kod hali ham to'g'ri kompilyatsiya bo'lyaptimi
    compile(stripped, dst_path, "exec")

    a, b = len(src.encode("utf-8")), len(stripped.encode("utf-8"))
    print("{}: {} -> {} bayt ({}% kichraydi)".format(
        dst_path.split("/")[-1], a, b, 100 - b * 100 // a))
    return 0


if __name__ == "__main__":
    sys.exit(main())
