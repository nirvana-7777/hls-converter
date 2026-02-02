FROM python:3.9-slim

# Install ffmpeg and dependencies
RUN apt-get update && apt-get install -y \
    ffmpeg \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Create app directory and set as working directory
WORKDIR /app

# Copy and install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code (note: file has HYPHEN, not underscore)
COPY hls-converter.py .

# Create temp directory for segments
RUN mkdir -p /tmp/hls_segments

# Expose port
EXPOSE 8000

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=40s --retries=3 \
    CMD curl -f http://localhost:8000/health || exit 1

# Run the application (note: filename has HYPHEN, not underscore)
CMD ["python", "hls-converter.py"]