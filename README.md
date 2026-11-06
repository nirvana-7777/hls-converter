# MPEG-TS Stream Proxy

Convert DASH (MPD), HLS (m3u8), RTSP and other FFmpeg-supported sources to MPEG-TS format on-the-fly for direct streaming. Stream copy only — no re-encoding, no disk storage.

[![Docker Build](https://github.com/YOUR_USERNAME/YOUR_REPO/actions/workflows/docker-build.yml/badge.svg)](https://github.com/YOUR_USERNAME/YOUR_REPO/actions/workflows/docker-build.yml)
[![Docker Pulls](https://img.shields.io/docker/pulls/nirvana777/hls-converter)](https://hub.docker.com/r/nirvana777/hls-converter)

## Features

- 🎥 Real-time DASH / HLS / RTSP → MPEG-TS conversion (copy mode — no re-encoding)
- 🚀 FFmpeg-based streaming with pre-flight first-byte check
- 🔄 Smart stream reuse (same URL shares one FFmpeg process, fanned out to all viewers)
- 🧹 Automatic cleanup of dead/stale streams (streams with active viewers are never evicted)
- 📊 Health monitoring with stream statistics (`/health`) + Prometheus metrics (`/metrics`)
- 🔒 Bearer-token authentication on `/stream`
- 🛡️ SSRF defense-in-depth: scheme allow-list, private/link-local/metadata IP rejection, credential redaction in logs and error bodies
- 🐢 Slow-client management: bounded per-client buffers (~4 MB); a lagging client is skipped ahead to the live edge in one packet-aligned batch that resumes on a PES frame start — every skip is counted and logged
- ⚙️ Full configuration via environment variables — no code edits needed
- 🐳 Docker ready (Alpine-based, small footprint)

## Security Model

Read this before exposing the service beyond a trusted LAN:

- **Auth**: every `/stream` request requires a bearer token (`Authorization: Bearer <token>`, set via `MPEGTS_AUTH_TOKEN`). `/health` and `/metrics` are unauthenticated by design (for orchestration and Prometheus scraping). Without a configured token the service refuses all `/stream` requests with `503` rather than running open — set `MPEGTS_AUTH_DISABLED=1` only for LAN development.
- **`pipe://` is disabled by default.** When enabled (`MPEGTS_ALLOW_PIPE=1`) it executes arbitrary commands — treat it as a remote-code-execution surface. It stays behind auth either way.
- **SSRF checks (http/https only)**: private, loopback, link-local, reserved, multicast and cloud-metadata IPs are rejected. `rtsp/rtp/udp/rtmp/mms` may target LAN hosts legitimately (cameras, encoders) and are exempt. `MPEGTS_ALLOW_PRIVATE_URLS=1` disables the check for all schemes (adds http/https SSRF risk).
- **URL validation is defense-in-depth, NOT a security boundary**: FFmpeg re-resolves DNS on its own and follows HTTP redirects, so DNS-rebinding and redirect-based SSRF cannot be fully prevented at the application layer. Internet-facing deployments MUST also enforce egress filtering at the network layer (block RFC1918, 169.254.0.0/16 and cloud metadata endpoints at the firewall/NAT).
- **URLs are never rewritten** (no DNS pinning): pinning breaks HTTPS (SNI/certificate mismatch) and vhosted HTTP, and breaks stream dedup on round-robin DNS. Streams are keyed by the original URL; the short ID in logs/health is display-only.
- **Credentials never leak**: tokens/api-keys/passwords in query strings and `user:pass@host` userinfo are redacted from logs, `/health` and 502 bodies; `/health` shows sanitized URLs (no query, no credentials); `pipe://` commands are never displayed.
- **FFmpeg hardening**: `-nostdin`, protocol whitelist without `file` (a remote playlist cannot pull local files), each process runs in its own process group for clean teardown on shutdown.

## Quick Start

### Using Docker

```bash
docker pull nirvana777/hls-converter:latest

docker run -d \
  --name mpegts-proxy \
  -p 8000:8000 \
  -e MPEGTS_AUTH_TOKEN="change-me" \
  nirvana777/hls-converter:latest
```

> Without `MPEGTS_AUTH_TOKEN` (or `MPEGTS_AUTH_DISABLED=1` for LAN-only dev), every `/stream` request is refused with `503` by design.

### Using Docker Compose

```yaml
services:
  mpegts-proxy:
    image: nirvana777/hls-converter:latest
    container_name: mpegts-proxy
    ports:
      - "8000:8000"
    environment:
      MPEGTS_AUTH_TOKEN: "change-me"
      # MPEGTS_MAX_STREAMS: "15"
      # MPEGTS_MAX_STREAM_AGE: "3600"
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

`/health` is unauthenticated, so the healthcheck works with auth enabled.

### Local Development

```bash
# Clone repository
git clone https://github.com/YOUR_USERNAME/YOUR_REPO.git
cd YOUR_REPO

# Install dependencies (Python >= 3.10, aiohttp)
pip install -r requirements.txt   # or: pip install aiohttp

# Install FFmpeg (Ubuntu/Debian)
sudo apt-get install ffmpeg

# Run service (set a token, or disable auth for LAN dev)
MPEGTS_AUTH_TOKEN="dev-token" python mpegts_proxy.py
```

## API Usage

### Stream a source

```bash
# Direct streaming with auth (outputs MPEG-TS)
curl -H "Authorization: Bearer dev-token" \
  "http://localhost:8000/stream?url=https://example.com/stream.mpd&name=MyStream" \
  -o output.ts

# Use in a media player
ffplay -headers $'Authorization: Bearer dev-token\r\n' \
  "http://localhost:8000/stream?url=https://example.com/stream.mpd"

# With auth disabled (MPEGTS_AUTH_DISABLED=1, trusted LAN only)
vlc "http://localhost:8000/stream?url=https://example.com/stream.mpd"
```

Supported `url` schemes: `http`, `https` (DASH `.mpd`, HLS `.m3u8`, direct TS/FLV...), `rtsp`, `rtmp`, `udp`, `rtp`, `mms` — anything FFmpeg can demux. `pipe://` only when explicitly enabled.

### Parameters

| Parameter | Required | Description |
|-----------|----------|-------------|
| `url`     | Yes | Source URL (DASH manifest, HLS playlist, RTSP camera, ...) |
| `name`    | No  | Stream name written as MPEG-TS `service_name` metadata (default: `"Stream"`, max 64 chars). Does **not** affect stream sharing — reuse is keyed by `url` only. |

### Response Codes

| Code | Meaning |
|------|---------|
| `200` | Success — `video/mp2t` byte stream |
| `400` | Missing `url`, or URL failed validation (scheme not allowed, private IP rejected, `pipe://` disabled, DNS failure) — reason in JSON body |
| `401` | Missing bearer token (`WWW-Authenticate: Bearer` header included) |
| `403` | Invalid bearer token |
| `502` | FFmpeg not found / spawn failed, or source exited before producing output — body carries the redacted FFmpeg stderr tail for diagnosis |
| `503` | Auth token not configured; max streams reached; per-stream client cap; stream terminated during subscribe |
| `504` | Spawn did not complete within `MPEGTS_SPAWN_WAIT_TIMEOUT` |

Slow or dead sources fail **before headers are committed**: the first subscriber waits up to `MPEGTS_PREFLIGHT_TIMEOUT` (default 5 s) for the first byte, so a dead URL yields a real `502` with diagnostics instead of a `200` with an empty body.

### Health Check

```bash
curl http://localhost:8000/health
```

Response (URLs sanitized, stderr redacted):

```json
{
  "status": "healthy",
  "streams": 1,
  "alive_streams": 1,
  "max_streams": 15,
  "totals": {
    "bytes_read": 123456789,
    "dropped_chunks": 0,
    "dropped_bytes": 0,
    "stalls": 2,
    "streams_created": 42
  },
  "stream_details": [
    {
      "id": "a1b2c3d4e5f6",
      "name": "MyStream",
      "url": "https://example.com/stream.mpd",
      "age": 120,
      "clients": 2,
      "alive": true,
      "bytes_read": 9876543,
      "dropped_chunks": 0,
      "dropped_bytes": 0,
      "stall_events": 0,
      "last_stderr": []
    }
  ]
}
```

### Prometheus Metrics

```bash
curl http://localhost:8000/metrics
```

Open (unauthenticated) endpoint in Prometheus text exposition format:

| Metric | Type | Description |
|--------|------|-------------|
| `mpegts_proxy_active_streams` | gauge | Tracked streams |
| `mpegts_proxy_alive_streams` | gauge | Streams whose source process is alive |
| `mpegts_proxy_active_clients` | gauge | Connected clients across all streams |
| `mpegts_proxy_max_streams` | gauge | Configured stream cap |
| `mpegts_proxy_bytes_read_total` | counter | Cumulative bytes read from sources (pre fan-out) |
| `mpegts_proxy_stream_stalls_total` | counter | Cumulative per-client read stalls |
| `mpegts_proxy_streams_created_total` | counter | Streams ever created |
| `mpegts_proxy_dropped_chunks_total` | counter | Chunks skipped by live-edge evictions (clients that fell behind) |
| `mpegts_proxy_dropped_bytes_total` | counter | Bytes skipped by live-edge evictions (always whole 188-byte packets, re-anchored on PES starts) |

```yaml
# prometheus.yml scrape config
scrape_configs:
  - job_name: mpegts-proxy
    static_configs:
      - targets: ["your-host:8000"]
```

## Configuration

All configuration is via environment variables — no code edits required:

| Variable | Default | Description |
|----------|---------|-------------|
| `MPEGTS_AUTH_TOKEN` | *(unset)* | Bearer token required on `/stream`. Unset + auth enabled = all requests refused |
| `MPEGTS_AUTH_DISABLED` | `false` | Disable authentication (trusted LAN development only) |
| `MPEGTS_ALLOW_PIPE` | `false` | Enable `pipe://` sources (executes arbitrary commands — RCE surface) |
| `MPEGTS_ALLOW_PRIVATE_URLS` | `false` | Allow private/LAN IPs for http/https too (SSRF risk) |
| `MPEGTS_HOST` | `0.0.0.0` | HTTP listen address |
| `MPEGTS_PORT` | `8000` | HTTP listen port |
| `MPEGTS_MAX_STREAMS` | `15` | Maximum concurrent source streams |
| `MPEGTS_MAX_STREAM_AGE` | `3600` | Max stream age in seconds (only evicts streams with no viewers) |
| `MPEGTS_IDLE_TIMEOUT` | `300` | Seconds with zero viewers before eviction |
| `MPEGTS_CLEANUP_INTERVAL` | `30` | Cleanup loop period in seconds |
| `MPEGTS_MAX_CLIENTS_PER_STREAM` | `50` | Per-stream client cap (503 beyond) |
| `MPEGTS_CLIENT_QUEUE_MAXSIZE` | `64` | Per-client chunk buffer (64 × ~64 KB ≈ 4 MB; should comfortably exceed one DASH segment burst). Overflow skips the client ahead to the live edge in ONE batch, re-anchored on a PES start; skips are counted in `/health` |
| `MPEGTS_READ_SIZE` | `65800` | Bytes per chunk read from FFmpeg (350 × 188-byte TS packets) |
| `MPEGTS_SPAWN_WAIT_TIMEOUT` | `30` | Seconds a concurrent request waits for an in-flight spawn (504 beyond) |
| `MPEGTS_PREFLIGHT_TIMEOUT` | `5` | Seconds the first subscriber waits for the first byte before headers |
| `MPEGTS_USER_AGENT` | *(unset)* | Custom User-Agent passed to FFmpeg (`-user_agent`) |
| `MPEGTS_FFMPEG_EXTRA_IN` | *(unset)* | Extra FFmpeg args injected before `-i` (shlex-split) — e.g. `-headers` for token-gated origins |
| `MPEGTS_FFMPEG_EXTRA_OUT` | *(unset)* | Extra FFmpeg args injected before the output (shlex-split) — e.g. `-mpegts_flags latm` for HE-AAC sources, `-an` to drop audio |
| `MPEGTS_LOG_LEVEL` | `INFO` | Logging level |

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
                                            │  DASH / HLS /  │
                                            │  RTSP Source   │
                                            └───────────────┘
```

Key properties:

- **Stream reuse**: same URL = one FFmpeg process; each client gets its own bounded buffer fed by a single reader (fan-out).
- **Spawn dedup**: concurrent requests for the same URL wait on one in-flight spawn instead of starting duplicates.
- **Pre-flight check**: dead sources fail fast with a real 502 + stderr before any headers are sent.
- **Packet-grid fan-out**: reads from FFmpeg are re-anchored on the 188-byte TS packet grid, so every chunk a client can ever receive is a whole number of packets — client demuxers never have to resync the byte stream.
- **Slow clients**: bounded buffer with a batched live-edge policy — a client that falls behind is skipped ahead to the newest PES frame start in ONE batch (never spliced mid-packet); skips are counted (`dropped_chunks` / `dropped_bytes`), and repeated read stalls disconnect the client.
- **Auto cleanup**: dead processes, idle and over-age streams removed every `MPEGTS_CLEANUP_INTERVAL`; dead entries are also pruned on demand at request time so the cap never lags.
- **Process monitoring**: health/metrics expose process liveness, bytes read and stall counters.
- **Resource limits**: max 15 concurrent streams by default (`MPEGTS_MAX_STREAMS`), 50 clients per stream.
- **Graceful shutdown**: SIGTERM → SIGKILL to the whole FFmpeg process group — no orphaned processes.

## How It Works

1. **Auth middleware** validates the bearer token (`/health` and `/metrics` are exempt).
2. **URL validation**: scheme allow-list + IP checks; rejected URLs return `400` with the reason.
3. **Get-or-create**: an alive stream for the exact URL is reused; a dead entry is pruned immediately; otherwise one FFmpeg process spawns (concurrent requests for the same URL wait on it, up to `MPEGTS_SPAWN_WAIT_TIMEOUT`).
4. **Pre-flight** (first subscriber only): wait up to `MPEGTS_PREFLIGHT_TIMEOUT` for the first byte — a source that dies first returns `502` with its redacted stderr tail.
5. **Subscribe**: the handler registers its own bounded buffer (a late joiner starts at the next PES frame start, never mid-PES) and the response begins (`video/mp2t`, no proxy buffering via `X-Accel-Buffering: no`).
6. **Fan-out loop**: chunks read from FFmpeg stdout are re-anchored on the 188-byte TS packet grid and written to every subscriber. A client that falls behind has its buffer trimmed to half in one batch, re-anchored on the next PES start, and the skip is counted (`dropped_chunks` / `dropped_bytes`) — visible in `/health`, `/metrics` and the logs.
7. **Client disconnect / stall**: subscriber removed; repeated read stalls (10 s timeouts for the first 10 chunks, then 5 s; 5 then 3 consecutive) disconnect the client.
8. **EOF or termination**: every subscriber is woken with a sentinel and closes cleanly.
9. **Background cleanup** every `MPEGTS_CLEANUP_INTERVAL`: dead processes reaped; streams idle past `MPEGTS_IDLE_TIMEOUT` or older than `MPEGTS_MAX_STREAM_AGE` with no viewers removed. **Streams with active viewers are never evicted.**
10. **Shutdown**: cleanup task stopped, every stream's process group killed (SIGTERM → SIGKILL), pumps and handlers closed cleanly.

## Performance Tips

1. **Memory**: each FFmpeg process uses ~50–100 MB; add up to ~4 MB per connected client (bounded queue, allocated only when a client lags). Monitor with `/health`.
2. **Concurrent streams**: default 15 — adjust `MPEGTS_MAX_STREAMS` for your hardware.
3. **Stream reuse**: N viewers of one URL cost one source fetch — inbound bandwidth is (bitrate × unique streams), outbound is (bitrate × clients).
4. **CPU**: copy mode means no transcoding; near-zero proxy CPU — the work is I/O.
5. **Bursty sources**: if clients stutter or `dropped_chunks` in `/health` grows, raise `MPEGTS_CLIENT_QUEUE_MAXSIZE` before touching anything else.

## Monitoring

### Check service health
```bash
curl http://localhost:8000/health | jq
```

Drop watch: `curl -s http://localhost:8000/health | jq '.stream_details[] | {id, clients, bytes_read, dropped_chunks, dropped_bytes}'` — growing counters mean clients are falling behind (see Troubleshooting).

### Scrape metrics
```bash
curl http://localhost:8000/metrics | head
```

### Watch FFmpeg logs
```bash
# Container logs show stream lifecycle + redacted FFmpeg errors
docker logs -f mpegts-proxy
```

### Resource usage
```bash
docker exec mpegts-proxy ps aux
docker stats mpegts-proxy
```

## Troubleshooting

### Stream won't start (502)
- **Read the response body** — it contains FFmpeg's stderr (credentials redacted) for sources that died before producing output
- **Check URL**: verify the manifest is reachable *from the container* (geo-blocks and expired signed URLs are common)
- **Check logs**: `docker logs mpegts-proxy` for FFmpeg errors

### Corrupted playback / decode errors on the client

Symptoms: the player logs floods of `aac: channel element ... is not allocated`, `Prediction is not allowed in AAC-LC`, `h264: error while decoding MB`, `mpegts: PES packet size mismatch`, `Packet corrupt`, or `Invalid video timestamp: X -> Y` jumps.

The proxy counts every live-edge skip (`dropped_chunks` / `dropped_bytes` per stream in `/health`, `mpegts_proxy_dropped_chunks_total` / `mpegts_proxy_dropped_bytes_total` in `/metrics`, plus a rate-limited `skipping a lagging client ahead to the live edge` warning in the logs). Use it to triage:

1. **`dropped_chunks` grows while errors occur** → the client fell behind and was skipped ahead to the live edge. Each skip is packet-aligned and resumes on a PES frame start, so the player sees a clean jump: a brief continuity-counter notice (`mpegts: Packet corrupt` once or twice per jump — unavoidable in raw TS) and an `Invalid video timestamp: X -> Y` jump, then normal playback. If it happens often, raise `MPEGTS_CLIENT_QUEUE_MAXSIZE` — each chunk is ~64 KB, and the buffer should comfortably exceed one DASH segment burst (a 6 s segment at 3 Mbps is already ~34 chunks).
2. **`dropped_chunks` stays 0 but the player floods `PES packet size mismatch` / `Packet corrupt`** → data reached the client intact from this proxy; the corruption is upstream (a dirty source) or a remux-format issue:
   - Compare with direct playback of the source: `ffplay "<source-url>"` — the same errors without the proxy mean a dirty source (common with IPTV restreams: splices, missing IDRs, discontinuities).
   - `ffprobe <source-url>` and check the audio profile: **HE-AAC** (SBR/PS) copied into MPEG-TS as ADTS can decode as garbage — look for `SBR was found before the first channel element` in the player log. Try LATM encapsulation: `MPEGTS_FFMPEG_EXTRA_OUT="-mpegts_flags latm"`, or drop audio as a stopgap: `MPEGTS_FFMPEG_EXTRA_OUT="-an"`.
   - `Invalid video timestamp` jumps at otherwise-clean times are splice/discontinuity markers from the source.

### 401 / 403 / 503 "refusing all requests"
- Send `Authorization: Bearer <token>`; the token comes from `MPEGTS_AUTH_TOKEN`
- `503 ... not configured` means auth is enabled but no token is set

### URL rejected with 400
- `scheme ... not allowed`: only `http/https/rtsp/rtp/udp/rtmp/mms` are accepted (`file://`, `ftp://` are not)
- `ip ... is disallowed`: private/loopback/metadata IP over http/https — use the `rtsp://`/`udp://` form for LAN cameras, or `MPEGTS_ALLOW_PRIVATE_URLS=1` (SSRF risk)
- `pipe:// is disabled`: set `MPEGTS_ALLOW_PIPE=1` — understand it executes commands
- `DNS resolution failed`: hostname doesn't resolve from the container

### "Max streams reached" (503)
- Raise `MPEGTS_MAX_STREAMS` (env var — no code edit)
- Check `/health` for stuck streams; dead ones are pruned on demand at the next request, everything else within one cleanup interval (30 s default)

### Stream stops unexpectedly
- FFmpeg crashed or the source ended — `last_stderr` in `/health` shows the final lines
- Idle eviction: with zero viewers the stream is removed after `MPEGTS_IDLE_TIMEOUT` (5 min default) — active viewers are never evicted
- Max age: 1 h default (`MPEGTS_MAX_STREAM_AGE`), also only with no viewers

### High memory usage
- Reduce `MPEGTS_MAX_STREAMS`; each stream is an FFmpeg process (~50–100 MB) plus ~2 MB per client
- Check for stuck streams in `/health`; restart the container as a last resort

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
- Low-latency proxy for DASH/HLS sources
- Repackaging LAN RTSP cameras/encoders as MPEG-TS over HTTP
- Simple source → MPEG-TS conversion
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

Done:

- [x] Authentication/API keys — bearer-token auth (`MPEGTS_AUTH_TOKEN`)
- [x] Configuration via environment variables — full `MPEGTS_*` env config
- [x] Prometheus metrics endpoint — `GET /metrics`
- [x] Input URL validation (prevent SSRF) — scheme + IP checks (defense-in-depth; pair with network-layer egress filtering)
- [x] Configurable timeout on FFmpeg startup — `MPEGTS_SPAWN_WAIT_TIMEOUT`, `MPEGTS_PREFLIGHT_TIMEOUT`
- [x] Packet-aligned live-edge fan-out — 188-byte packet grid + PES-anchored batch eviction + PES-anchored late joins (no mid-packet splices, no demuxer resync cascades)

Potential improvements:

- [ ] Rate limiting per IP
- [ ] Stream quality/bitrate selection
- [ ] Per-client API keys / token rotation
- [ ] Keyframe-anchored live-edge joins (skip to the next IDR/GOP start instead of the next PES start, for instant clean video after a jump)
- [ ] OpenMetrics exposition (`application/openmetrics-text`)

## License

MIT

## Contributing

Pull requests welcome! Ensure:
- Code follows existing async patterns
- Add logging for new features
- Test with actual DASH streams (run the smoke tests: `python3 scripts/smoke_test.py`, requires `ffmpeg`)
- Docker build succeeds

## Support

- 🐛 [Report Issues](https://github.com/YOUR_USERNAME/YOUR_REPO/issues)
- 💬 [Discussions](https://github.com/YOUR_USERNAME/YOUR_REPO/discussions)
