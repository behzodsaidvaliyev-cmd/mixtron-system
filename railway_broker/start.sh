#!/bin/sh
# Login/parol Railway o'zgaruvchilaridan olinadi - repo ochiq, parol bu yerda
# bo'lmasligi shart. Qiymatlar qurilmalardagi mqtt_config.json bilan bir xil.
set -e

RUN_DIR=/mosquitto/run
mkdir -p "$RUN_DIR"

if [ -z "$MQTT_USER" ] || [ -z "$MQTT_PASSWORD" ]; then
    echo "[BROKER] MQTT_USER va MQTT_PASSWORD o'rnatilmagan" >&2
    exit 1
fi

# Sertifikat har ishga tushishda yangidan yaratiladi - ESP32 uni tekshirmaydi,
# shuning uchun saqlab qo'yish uchun volume kerak emas.
openssl req -x509 -newkey rsa:2048 -nodes -days 3650 \
    -subj "/CN=mixtron-broker" \
    -keyout "$RUN_DIR/server.key" -out "$RUN_DIR/server.crt" 2>/dev/null

rm -f "$RUN_DIR/passwd"
touch "$RUN_DIR/passwd"
chmod 0600 "$RUN_DIR/passwd"
mosquitto_passwd -b "$RUN_DIR/passwd" "$MQTT_USER" "$MQTT_PASSWORD"

chown -R mosquitto:mosquitto "$RUN_DIR"
chmod 0700 "$RUN_DIR"
chmod 0600 "$RUN_DIR/passwd" "$RUN_DIR/server.key"

echo "[BROKER] ishga tushmoqda"
exec mosquitto -c /mosquitto/config/mosquitto.conf
