FROM python:3.9-slim

# Install ffmpeg and dependencies
RUN apt-get update && apt-get install -y \
    ffmpeg \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Create app directory
WORKDIR /app
COPY . .

# Create temp directory for segments
RUN mkdir -p /tmp/hls_segments

EXPOSE 8000

CMD ["python", "hls_converter.py"]