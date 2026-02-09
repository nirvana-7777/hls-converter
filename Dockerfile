FROM python:3.11-alpine

# 1. Install tini for signal handling and curl for healthchecks
# 2. Ensure ffmpeg is present
RUN apk add --no-cache ffmpeg curl tini

WORKDIR /app

# Optimize Python environment
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONOPTIMIZE=1

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY mpegts-proxy.py .

EXPOSE 8000

# Use tini to manage FFmpeg subprocesses correctly
ENTRYPOINT ["/sbin/tini", "--"]

# Run with optimized Python execution
CMD ["python", "mpegts-proxy.py"]