ARG PYTHON_VERSION=3.11
FROM python:${PYTHON_VERSION}-slim-bookworm

# ffmpeg: the remuxer. tini: reaps ffmpeg children and forwards signals.
# curl: HEALTHCHECK only.
RUN apt-get update \
 && apt-get install -y --no-install-recommends ffmpeg curl tini \
 && rm -rf /var/lib/apt/lists/*

RUN useradd --system --uid 10001 --no-create-home --shell /usr/sbin/nologin app

WORKDIR /app

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY mpegts-proxy.py .

USER app
EXPOSE 8000

HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
  CMD curl -fsS "http://127.0.0.1:${MPEGTS_PORT:-8000}/health" || exit 1

ENTRYPOINT ["/usr/bin/tini", "--"]
CMD ["python", "mpegts-proxy.py"]