# Use Debian Slim for glibc performance and stability
FROM python:3.11-slim-bookworm

# Install ffmpeg and tini
RUN apt-get update && apt-get install -y \
    ffmpeg \
    curl \
    tini \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Enable Python optimizations
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONOPTIMIZE=1

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY mpegts-proxy.py .

EXPOSE 8000

# tini is essential for reaping ffmpeg subprocesses
ENTRYPOINT ["/usr/bin/tini", "--"]

CMD ["python", "mpegts-proxy.py"]