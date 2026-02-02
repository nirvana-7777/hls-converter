# HLS Converter Service

Convert DASH (MPD) streams to HLS (M3U8/TS) format on-the-fly.

[![Docker Build](https://github.com/YOUR_USERNAME/YOUR_REPO/actions/workflows/docker-build.yml/badge.svg)](https://github.com/YOUR_USERNAME/YOUR_REPO/actions/workflows/docker-build.yml)
[![Docker Pulls](https://img.shields.io/docker/pulls/nirvana777/hls-converter)](https://hub.docker.com/r/nirvana777/hls-converter)

## Features

- 🎥 Real-time DASH to HLS conversion
- 🚀 FFmpeg-based transcoding (copy mode - no re-encoding)
- 🧹 Automatic cleanup of old streams
- 📊 Health monitoring and stream management API
- 🔧 RESTful API for easy integration
- 🐳 Docker ready

## Quick Start

### Using Docker

```bash
docker pull nirvana777/hls-converter:latest

docker run -d \
  --name hls-converter \
  -p 8000:8000 \
  -v /tmp/hls_segments:/tmp/hls_segments \
  nirvana777/hls-converter:latest
```

### Using Docker Compose

```yaml
version: '3.8'

services:
  hls-converter:
    image: nirvana777/hls-converter:latest
    ports:
      - "8000:8000"
    volumes:
      - hls_segments:/tmp/hls_segments
    restart: unless-stopped
    environment:
      - HLS_SEGMENT_DURATION=2
      - HLS_LIST_SIZE=5
      - CLEANUP_INTERVAL=300

volumes:
  hls_segments:
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
python hls-converter.py
```

## API Usage

### Convert a DASH Stream

```bash
curl -X POST http://localhost:8000/convert \
  -H "Content-Type: application/json" \
  -d '{
    "dash_url": "https://example.com/stream.mpd"
  }'
```

Response:
```json
{
  "stream_id": "a1b2c3d4",
  "hls_url": "/hls/a1b2c3d4/index.m3u8",
  "status": "converting"
}
```

### Play the HLS Stream

```bash
# Get HLS playlist
curl http://localhost:8000/hls/a1b2c3d4/index.m3u8

# Use in video player
# http://localhost:8000/hls/a1b2c3d4/index.m3u8
```

### List Active Streams

```bash
curl http://localhost:8000/streams
```

### Stop a Stream

```bash
curl -X DELETE http://localhost:8000/streams/a1b2c3d4
```

### Health Check

```bash
curl http://localhost:8000/health
```

## Configuration

Environment variables:

| Variable | Default | Description |
|----------|---------|-------------|
| `HLS_SEGMENT_DURATION` | `2` | Segment duration in seconds |
| `HLS_LIST_SIZE` | `5` | Number of segments in playlist |
| `CLEANUP_INTERVAL` | `300` | Cleanup interval in seconds |
| `MAX_STREAMS` | `10` | Maximum concurrent streams |
| `TEMP_DIR` | `/tmp/hls_segments` | Directory for HLS segments |

## Architecture

```
┌─────────┐
│ Client  │
└────┬────┘
     │
     ▼
┌──────────────┐     ┌──────────┐
│ Flask API    │────▶│  FFmpeg  │
│ (Port 8000)  │     │ Process  │
└──────────────┘     └──────────┘
     │                     │
     ▼                     ▼
┌──────────────────────────────┐
│   /tmp/hls_segments/         │
│   ├── stream1/               │
│   │   ├── index.m3u8         │
│   │   ├── segment_000.ts     │
│   │   └── segment_001.ts     │
│   └── stream2/               │
└──────────────────────────────┘
```

## Performance Tips

1. **Memory Management**: Each stream uses ~50-200MB. Monitor with `/health` endpoint
2. **Concurrent Streams**: Default limit is 10. Adjust `MAX_STREAMS` based on your resources
3. **Storage**: Segments are automatically cleaned up. Configure `CLEANUP_INTERVAL` as needed
4. **Network**: Use CDN or Nginx for production to offload static file serving

## Monitoring

Check active streams and resource usage:

```bash
# Health endpoint shows active streams and disk usage
curl http://localhost:8000/health

# List all active conversions
curl http://localhost:8000/streams
```

## Troubleshooting

### FFmpeg not found
```bash
# Install FFmpeg in container
docker exec -it hls-converter apt-get update && apt-get install -y ffmpeg
```

### High memory usage
- Reduce `MAX_STREAMS`
- Decrease `HLS_LIST_SIZE`
- Increase `CLEANUP_INTERVAL`

### Segments not playing
- Check FFmpeg logs in container: `docker logs hls-converter`
- Verify DASH URL is accessible
- Ensure correct MIME types are set

## GitHub Actions

This repository includes automated CI/CD:

- ✅ Automated testing on push
- 🐳 Multi-platform Docker builds (amd64, arm64)
- 🔒 Security scanning with Trivy
- 📦 Automatic push to Docker Hub
- 🏷️ Semantic versioning support

### Required Secrets

Set these in your GitHub repository settings:

- `DOCKERHUB_USERNAME`: Your Docker Hub username
- `DOCKERHUB_TOKEN`: Docker Hub access token

## License

MIT

## Contributing

Pull requests welcome! Please ensure:
- Tests pass
- Code follows existing style
- Docker build succeeds

## Support

- 🐛 [Report Issues](https://github.com/YOUR_USERNAME/YOUR_REPO/issues)
- 💬 [Discussions](https://github.com/YOUR_USERNAME/YOUR_REPO/discussions)