# mixtron-datchik (logger) - Docker bilan yig'iladi.
#
# NIMA UCHUN: 2026-10-26 da Railway'ning Railpack builder'i (v0.43.0) uch marta
# ketma-ket "frontend grpc server closed unexpectedly" bilan yiqildi - kod
# emas, builder ichidagi xato. Dockerfile bilan Railway Railpack'ni
# ishlatmaydi. Natija avvalgidek: Python 3.13, pip install, python logger.py.
FROM python:3.13-slim

ENV PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

WORKDIR /app
COPY requirements.txt .
RUN pip install -r requirements.txt

COPY . .
CMD ["python", "logger.py"]
