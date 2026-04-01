FROM python:3.14-slim

LABEL org.opencontainers.image.title="arris-exporter" \
      org.opencontainers.image.description="Prometheus exporter for Arris Touchstone cable modems" \
      org.opencontainers.image.source="https://github.com/slother/arris-exporter" \
      org.opencontainers.image.licenses="MIT"

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt && \
    rm -rf /root/.cache

COPY arris_exporter.py .

RUN groupadd --system exporter && \
    useradd --system --gid exporter --no-create-home exporter && \
    chown -R exporter:exporter /app

USER exporter

EXPOSE 9120

HEALTHCHECK --interval=30s --timeout=5s --start-period=5s --retries=3 \
    CMD ["python", "-c", "import urllib.request; urllib.request.urlopen('http://localhost:9120/')"]

ENTRYPOINT ["python", "arris_exporter.py"]
