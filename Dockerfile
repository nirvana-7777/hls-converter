FROM python:3.11-alpine

RUN apk add --no-cache ffmpeg curl

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY mpegts-proxy.py .

EXPOSE 8000
CMD ["python", "mpegts-proxy.py"]