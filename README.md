# MPEG-TS Stream Proxy

Convert DASH (MPD) streams to MPEG-TS format on-the-fly for direct streaming.

[![Docker Build](https://github.com/YOUR_USERNAME/YOUR_REPO/actions/workflows/docker-build.yml/badge.svg)](https://github.com/YOUR_USERNAME/YOUR_REPO/actions/workflows/docker-build.yml)
[![Docker Pulls](https://img.shields.io/docker/pulls/nirvana777/hls-converter)](https://hub.docker.com/r/nirvana777/hls-converter)

## Features

- 🎥 Real-time DASH to MPEG-TS conversion
- 🚀 FFmpeg-based streaming (copy mode - no re-encoding)
- 🧹 Automatic cleanup of dead/stale streams
- 📊 Health monitoring with stream statistics
- 🔒 Resource limits (max streams, max age)
- 🔄 Smart stream reuse (same URL shares FFmpeg process)
- 🐳 Docker ready (Alpine-based, small footprint)

## Quick Start

### Using Docker

```bash
docker pull nirvana777/hls-converter:latest

docker run -d \
  --name mpegts-proxy \
  -p 8000:8000 \
  nirvana777/hls-converter:latest
```

### Using Docker Compose

```yaml
version: '3.8'

services:
  mpegts-proxy:
    image: nirvana777/hls-converter:latest
    container_name: mpegts-proxy
    ports:
      - "8000:8000"
    restart: unless-stopped
    healthcheck:
      test: ["CMD", "wget", "--no-verbose", "--tries=1", "--spider", "http://localhost:8000/health"]
      interval: 30s
      timeout: 10s
      retries: 3
      start_period: 10s
    deploy:
      resources:
        limits:
          cpus: '2.0'
          memory: 2G
        reservations:
          cpus: '0.5'
          memory: 512M
```

### Local Development

```bash
# Clone repository
git clone https://github.com/YOUR_USERNAME/YOUR_REPO.git
cd YOUR_REPO

# Install dependencies
pip install -r requirements.txt

# Install FFmpeg (Ubuntu/Debian)
sudo apt-get install ffmpeg

# Run service
python mpegts-proxy.py
```

## API Usage

### Stream a DASH URL

```bash
# Direct streaming (outputs MPEG-TS)
curl "http://localhost:8000/stream?url=https://example.com/stream.mpd&name=MyStream" > output.ts

# Use in media player (VLC, ffplay, etc.)
vlc "http://localhost:8000/stream?url=https://example.com/stream.mpd"
ffplay "http://localhost:8000/stream?url=https://example.com/stream.mpd"
```

### Parameters

| Parameter | Required | Description |
|-----------|----------|-------------|
| `url` | Yes | DASH manifest URL (.mpd) |
| `name` | No | Stream name for metadata (default: "Stream") |

### Health Check

```bash
curl http://localhost:8000/health
```

Response:
```json
{
  "status": "healthy",
  "streams": 2,
  "max_streams": 10,
  "stream_details": [
    {
      "id": "a1b2c3d4",
      "age": 120,
      "clients": 2,
      "alive": true
    }
  ]
}
```

## Configuration

Configuration is done via code constants in `mpegts-proxy.py`:

```python
manager = StreamManager(
    max_streams=10,      # Maximum concurrent streams
    max_stream_age=3600  # Maximum stream lifetime (1 hour)
)
```

Cleanup intervals (in `StreamManager._cleanup_loop`):
- **Cleanup frequency**: 30 seconds
- **Idle timeout**: 300 seconds (5 minutes)

## Architecture

```
┌─────────┐                                    ┌──────────┐
│ Client  │───── HTTP GET /stream?url=... ────▶│  aiohttp │
└─────────┘                                    │   API    │
     │                                         └──────────┘
     │                                              │
     │  ◄─── MPEG-TS Stream ───                    │
     │                                              ▼
     │                                         ┌──────────┐
     └─────────────────────────────────────────│  FFmpeg  │
                                               │ Process  │
                                               └──────────┘
                                                    │
                                                    ▼
                                            ┌───────────────┐
                                            │  DASH Source  │
                                            │  (.mpd URL)   │
                                            └───────────────┘

Key Features:
• Stream Reuse: Same URL = same FFmpeg process (multiple clients)
• Auto Cleanup: Dead/idle/old streams removed every 30s
• Process Monitoring: Health checks on FFmpeg processes
• Resource Limits: Max 10 concurrent streams (configurable)
```

## How It Works

1. **Client requests stream** → `/stream?url=<mpd_url>`
2. **Proxy checks** if stream already exists for this URL
3. **If exists**: Reuses existing FFmpeg process (increments client count)
4. **If new**: Creates FFmpeg process with MPEG-TS output to stdout
5. **Streams data** → Reads from FFmpeg stdout, writes to HTTP response
6. **Background cleanup** → Every 30s, removes:
   - Dead FFmpeg processes
   - Streams older than 1 hour
   - Streams idle for 5+ minutes (no clients)
7. **Client disconnect** → Decrements client count
8. **No clients** → Stream marked for cleanup after 5 min idle

## Performance Tips

1. **Memory**: Each stream uses ~50-100MB. Monitor with `/health`
2. **Concurrent streams**: Default 10, adjust `max_streams` for your hardware
3. **Stream reuse**: Multiple clients can watch the same URL efficiently
4. **CPU**: FFmpeg uses copy mode (no transcoding), minimal CPU usage
5. **Network**: Outbound bandwidth = (stream bitrate × active clients)

## Monitoring

### Check service health
```bash
curl http://localhost:8000/health | jq
```

### Watch FFmpeg logs
```bash
# Container logs show stream lifecycle
docker logs -f mpegts-proxy
```

### Resource usage
```bash
# Inside container
docker exec mpegts-proxy ps aux
docker stats mpegts-proxy
```

## Troubleshooting

### Stream won't start
- **Check URL**: Verify DASH manifest is accessible
- **Check logs**: `docker logs mpegts-proxy` for FFmpeg errors
- **Network**: Ensure container can reach external URLs

### "Max streams reached" (503 error)
- Increase `max_streams` in code
- Check `/health` to see if streams are stuck
- Wait for cleanup (runs every 30s)

### Stream stops unexpectedly
- FFmpeg process may have crashed (check logs)
- Source stream may have ended
- Stream exceeded max age (1 hour default)

### High memory usage
- Reduce `max_streams`
- Check for stuck streams in `/health`
- Restart container to clear all streams

### No cleanup happening
- Background cleanup task runs every 30s automatically
- Check logs for "Started stream cleanup task"
- Verify app is running (not just FFmpeg processes)

## Differences from HLS Converter

This is **not** an HLS converter. Key differences:

| Feature | MPEG-TS Proxy | HLS Converter |
|---------|---------------|---------------|
| Output format | MPEG-TS stream | HLS playlist + segments |
| Storage | No disk storage | Requires disk for segments |
| Volumes | None needed | `/tmp/hls_segments` required |
| API | Simple GET endpoint | POST to convert, GET playlist |
| Use case | Direct streaming | CDN distribution |
| Latency | Real-time | Segment duration delay |

## Use Cases

✅ **Good for:**
- Direct streaming to media players (VLC, ffplay)
- Low-latency proxy for DASH sources
- Simple DASH → MPEG-TS conversion
- Temporary stream access (no recording)

❌ **Not for:**
- HLS playlist generation
- CDN distribution
- DVR/recording functionality
- Multiple quality levels

## GitHub Actions

Automated CI/CD pipeline:

- ✅ Testing with FFmpeg validation
- 🐳 Multi-platform builds (amd64, arm64)
- 🔒 Security scanning with Trivy
- 📦 Auto-push to Docker Hub
- 🏷️ Semantic versioning

### Required Secrets

Set in GitHub repository settings:

- `DOCKERHUB_USERNAME`: Docker Hub username
- `DOCKERHUB_TOKEN`: Docker Hub access token

## Roadmap

Potential improvements:

- [ ] Authentication/API keys
- [ ] Rate limiting per IP
- [ ] Configuration via environment variables
- [ ] Prometheus metrics endpoint
- [ ] Stream quality/bitrate selection
- [ ] Input URL validation (prevent SSRF)
- [ ] Configurable timeout on FFmpeg startup

## License

MIT

## Contributing

Pull requests welcome! Ensure:
- Code follows existing async patterns
- Add logging for new features
- Test with actual DASH streams
- Docker build succeeds

## Support

- 🐛 [Report Issues](https://github.com/YOUR_USERNAME/YOUR_REPO/issues)
- 💬 [Discussions](https://github.com/YOUR_USERNAME/YOUR_REPO/discussions)