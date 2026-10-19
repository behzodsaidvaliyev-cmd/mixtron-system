# Mixtron o'z MQTT brokeri (Railway, mixtron loyihasi).
#
# NIMA UCHUN: 2026-10-19 da HiveMQ Cloud'ga O'zbekistondan ulanish yopildi
# (TCP ochiladi, TLS handshake javobsiz qotadi). Railway TCP proxy esa
# ochiq. Bundan tashqari HiveMQ Serverless 2026-12-31 da yopiladi.
FROM eclipse-mosquitto:2.0.20

RUN apk add --no-cache openssl

COPY mosquitto.conf /mosquitto/config/mosquitto.conf
COPY openssl.cnf /mosquitto/config/openssl.cnf
ENV OPENSSL_CONF=/mosquitto/config/openssl.cnf
COPY start.sh /start.sh
RUN chmod +x /start.sh

EXPOSE 1883 8883 8080
CMD ["/start.sh"]
