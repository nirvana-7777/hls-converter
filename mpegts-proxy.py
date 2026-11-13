"""
MPEG-TS Stream Proxy

Optional HLS/DASH -> MPEG-TS converter for clients that only understand TS.
The Ultimate Backend keeps serving HLS/DASH exactly as before; this service
sits next to it and remuxes (never transcodes: `-c copy`) on request.

Endpoints
---------
  GET /api/providers/<provider>/{channels|events|recordings}/<id>/stream/index.ts
  GET /api/providers/<provider>/vod/<id>/stream/index.ts

These mirror the backend's own stream routes: the TS URL of a piece of
content is its backend stream URL with `.ts` as the extension. The proxy
builds the backend request itself (MPEGTS_BACKEND_URL) and always forces
`client_drm=false&highest_quality_only=true` — server-side decryption, one
video quality — whatever the caller passes. Accepted query parameters:

  country, drm_variant, no_proxy        passed through to the backend
  start_time, end_time, epg_id          channel catchup, passed through
  start=<seconds>                       on-demand only: seek via ffmpeg -ss
  fast=0                                on-demand only: no parallel segment
                                        fetching, ffmpeg fetches (debugging)
  name=<text>                           MPEG-TS service_name (default
                                        "<provider>/<id>")

Two playback modes, chosen by content type (X-TS-Mode response header):

  live       channels (no catchup) and events. One shared ffmpeg per stream;
             clients fan out from it and a client that falls behind is
             skipped ahead to the live edge (see ClientBuffer).
  ondemand   VOD, recordings and catchup. One ffmpeg PER REQUEST with real
             backpressure: nothing is ever dropped, ffmpeg simply blocks
             while the client is slower. TS has no seeking, so players get
             play-through only (use ?start= to begin later).

Static manifests (finished HLS playlists / DASH type="static", i.e. nearly
all VOD, recordings and past catchup) are downloaded as fast as origin and
client allow: the proxy fetches the segments itself, MPEGTS_PARALLEL_SEGMENTS
at a time per track over keep-alive connections, and pipes them in order
into ffmpeg, which only remuxes. ffmpeg's own demuxers fetch one segment
after the other. Backpressure still holds end to end, so a slow client only
ever holds a few segments per track in memory. Dynamic manifests, and
anything unusual in a static one (several periods, discontinuities, byte
ranges, HLS encryption, ?start=, MPEGTS_FFMPEG_EXTRA_IN), keep the plain
ffmpeg path, as does a parallel attempt that yields no first byte
(X-TS-Fetch: parallel | ffmpeg tells which one served a request). A segment
that cannot be fetched mid-download aborts the connection rather than
ending the body cleanly, so a downloader sees an incomplete file.
  MPEGTS_PARALLEL_SEGMENTS (4; 0 = off), MPEGTS_SEGMENT_TIMEOUT (60 s),
  MPEGTS_SEGMENT_RETRIES (3), MPEGTS_SEGMENT_MAX_BYTES (64 MiB).

Before spawning ffmpeg the proxy asks the backend once (no redirects
followed). Errors come back as JSON with the backend's message and a hint
(e.g. Widevine content cannot be converted) instead of a bare "HTTP 400"
from ffmpeg. A 302 to the upstream manifest is followed directly, so
one-time provider tokens are not resolved twice. The manifest is also read
to pick the highest-bandwidth HLS variant / DASH representation, because
the backend can only collapse variants on paths that rewrite the manifest.

The generic `/stream?url=...` endpoint (any URL, optionally pipe://) is OFF
unless MPEGTS_ALLOW_GENERIC_URL=1.

Live-edge fan-out: every chunk handed to a client is a whole number of
188-byte TS packets. A late joiner starts on PAT+PMT followed by a video
keyframe (a PES start with the random-access flag); a client that falls
behind is skipped ahead in ONE batch that resumes on a keyframe — the byte
stream is never spliced mid-packet (mid-packet splices are what showed up as
'Packet corrupt' / 'PES packet size mismatch' spam in client demuxers).

Security model:
  - pipe:// is DISABLED unless MPEGTS_ALLOW_PIPE=1 (and only exists on the
    generic endpoint). It executes arbitrary commands, so treat it as a
    remote-code-execution surface (it remains behind bearer auth either way).
  - private/loopback/link-local/metadata IPs are rejected for http/https
    unless MPEGTS_ALLOW_PRIVATE_URLS=1. rtsp/rtp/udp/rtmp/mms may target
    LAN hosts legitimately (cameras, encoders) and are exempt from the
    private-IP check — but only reachable via the generic endpoint; in
    backend mode ffmpeg's protocol whitelist is http/https only.
  - every request requires a bearer token (MPEGTS_AUTH_TOKEN) unless
    MPEGTS_AUTH_DISABLED=1 (LAN development only). /health returns status
    only without a valid token.
  - the aiohttp access log is OFF: it would print full request lines,
    including `url=` and any credentials. The proxy logs its own lines.
  - URL validation is defense-in-depth, NOT a security boundary: ffmpeg
    re-resolves DNS on its own and follows HTTP redirects, so DNS
    rebinding and redirect-based SSRF cannot be fully prevented at the
    application layer. Internet-facing deployments MUST also enforce
    egress filtering at the network layer (block RFC1918, 169.254.0.0/16
    and cloud metadata endpoints at the firewall/NAT).
  - URLs are never rewritten (no DNS pinning): pinning breaks HTTPS
    (SNI/certificate mismatch) and vhosted HTTP, and keying streams by a
    resolved IP breaks dedup on round-robin DNS. Streams are keyed by the
    backend URL (or the original URL on the generic endpoint); the hash is
    display-only.

Requires Python >= 3.10.
"""
from __future__ import annotations

import asyncio
import contextlib
import enum
import fcntl
import functools
import hashlib
import hmac
import ipaddress
import json
import logging
import os
import re
import shlex
import signal
import socket
import subprocess
import time
import xml.etree.ElementTree as ET
from collections import deque
from dataclasses import dataclass, field
from typing import Mapping
from urllib.parse import quote, urlencode, urljoin, urlparse

import aiohttp
from aiohttp import web


# ===== Configuration =====

def _env_int(name: str, default: int) -> int:
    try:
        return int(os.environ.get(name, default))
    except (TypeError, ValueError):
        return default


def _env_bool(name: str, default: bool) -> bool:
    v = os.environ.get(name)
    if v is None:
        return default
    return v.strip().lower() in ("1", "true", "yes", "on")



LOG_LEVEL = os.environ.get("MPEGTS_LOG_LEVEL", "INFO").upper()
MAX_STREAMS = _env_int("MPEGTS_MAX_STREAMS", 15)              # shared (live) ffmpeg processes
MAX_ONDEMAND = _env_int("MPEGTS_MAX_ONDEMAND", 10)            # per-request (VOD/recording/catchup) processes
MAX_STREAM_AGE = _env_int("MPEGTS_MAX_STREAM_AGE", 3600)      # s; only evicts streams with no viewers
IDLE_TIMEOUT = _env_int("MPEGTS_IDLE_TIMEOUT", 300)           # s
CLEANUP_INTERVAL = _env_int("MPEGTS_CLEANUP_INTERVAL", 30)    # s
WATCHDOG_INTERVAL = 5                                         # s between source-stall checks
STALL_TIMEOUT = _env_int("MPEGTS_STALL_TIMEOUT", 20)          # s without a source byte -> kill (flowing stream)
STARTUP_TIMEOUT = _env_int("MPEGTS_STARTUP_TIMEOUT", 45)      # s without the FIRST byte -> kill (probing is slow)
RW_TIMEOUT = _env_int("MPEGTS_RW_TIMEOUT", 15)                # s; ffmpeg -rw_timeout: prefer dying over hanging
# ffmpeg's own log level. 'error' hides the DASH demuxer's 404/skip warnings; set
# 'warning' (or 'verbose' to see every requested URL) to diagnose segment problems.
FFMPEG_LOGLEVEL = os.environ.get("MPEGTS_FFMPEG_LOGLEVEL", "error").strip().lower() or "error"
JOIN_KEYFRAME_TIMEOUT = _env_int("MPEGTS_JOIN_KEYFRAME_TIMEOUT", 10)  # s a joining client waits for a video keyframe before settling for a PES start
STREAM_READ_SIZE = _env_int("MPEGTS_READ_SIZE", 65800)        # 350 x 188-byte TS packets
# Per-client buffer BUDGET IN BYTES (not chunks: with -flush_packets 1 chunks are
# only ~10 KB, so a chunk limit of 64 held about one second of a 5 Mbit/s
# stream). 8 MiB is ~12 s at 5.4 Mbit/s and must outlast a DASH backlog burst.
# Overflow evicts to half the budget in ONE batch — see ClientBuffer.
CLIENT_QUEUE_BYTES = _env_int("MPEGTS_CLIENT_QUEUE_BYTES", 8 * 1024 * 1024)
MAX_CLIENTS_PER_STREAM = _env_int("MPEGTS_MAX_CLIENTS_PER_STREAM", 50)
SPAWN_WAIT_TIMEOUT = _env_int("MPEGTS_SPAWN_WAIT_TIMEOUT", 30)       # s; follower waits for leader
PREFLIGHT_TIMEOUT = _env_int("MPEGTS_PREFLIGHT_TIMEOUT", 15)         # s; first-byte wait pre-headers (live). HLS/DASH probing is slow.
BACKEND_TIMEOUT = _env_int("MPEGTS_BACKEND_TIMEOUT", 10)             # s; backend pre-resolution request
ONDEMAND_WRITE_TIMEOUT = _env_int("MPEGTS_ONDEMAND_WRITE_TIMEOUT", 900)  # s a client may block a write (a paused player is normal; a dead one is not)
RETRY_AFTER = _env_int("MPEGTS_RETRY_AFTER", 5)                      # s; Retry-After on capacity 503s
MANIFEST_MAX_BYTES = 2 * 1024 * 1024                                 # manifests are small; never buffer more
# Static manifests (VOD, recordings, finished catchup): the proxy fetches the
# segments itself, PARALLEL_SEGMENTS at a time over keep-alive connections, and
# pipes them into ffmpeg (see "Static manifests" below). 0 = always let ffmpeg
# fetch (sequentially, one segment after the other).
PARALLEL_SEGMENTS = max(0, _env_int("MPEGTS_PARALLEL_SEGMENTS", 4))
SEGMENT_TIMEOUT = _env_int("MPEGTS_SEGMENT_TIMEOUT", 60)             # s, whole request incl. body
SEGMENT_RETRIES = max(0, _env_int("MPEGTS_SEGMENT_RETRIES", 3))      # extra attempts per segment
SEGMENT_MAX_BYTES = _env_int("MPEGTS_SEGMENT_MAX_BYTES", 64 * 1024 * 1024)   # one segment, held in memory
MAX_SEGMENTS = 200_000                                               # sanity cap per track
ALLOW_PRIVATE_URLS = _env_bool("MPEGTS_ALLOW_PRIVATE_URLS", False)
ALLOW_PIPE = _env_bool("MPEGTS_ALLOW_PIPE", False)
ALLOW_GENERIC_URL = _env_bool("MPEGTS_ALLOW_GENERIC_URL", False)     # the open /stream?url=... endpoint
# How this proxy reaches the Ultimate Backend (e.g. http://backend:8080 on a
# Docker network). No trailing slash. Required for the mirrored /api/... routes.
BACKEND_URL = (os.environ.get("MPEGTS_BACKEND_URL") or "").strip().rstrip("/")
AUTH_TOKEN = os.environ.get("MPEGTS_AUTH_TOKEN")
AUTH_DISABLED = _env_bool("MPEGTS_AUTH_DISABLED", False)
USER_AGENT = os.environ.get("MPEGTS_USER_AGENT")              # optional; via ffmpeg -user_agent
# Escape hatch: extra ffmpeg args (shlex-split), injected verbatim.
# IN:  before -i (input side, e.g. -headers "Authorization: Bearer ...").
# OUT: before the output target (e.g. -mpegts_flags latm for HE-AAC sources,
#      -an to drop audio). Lets operators tune remuxing without code changes.
FFMPEG_EXTRA_IN = shlex.split(os.environ.get("MPEGTS_FFMPEG_EXTRA_IN", "") or "")
FFMPEG_EXTRA_OUT = shlex.split(os.environ.get("MPEGTS_FFMPEG_EXTRA_OUT", "") or "")

# rtsp/rtp/udp/rtmp/mms legitimately target LAN hosts (cameras, encoders).
SCHEMES_ALLOW_PRIVATE = {"rtsp", "rtp", "udp", "rtmp", "mms"}
ALLOWED_SCHEMES = {"http", "https"} | SCHEMES_ALLOW_PRIVATE

# ffmpeg -protocol_whitelist. 'file' is deliberately absent everywhere: the
# HLS/DASH demuxers check segment URLs from remote playlists against this
# whitelist, and allowing 'file' would let a remote playlist pull local files
# (e.g. file:///etc/passwd) and remux them to clients. 'crypto' is needed for
# AES-128 HLS segments; 'pipe' for the pipe:1 output. Backend mode needs
# nothing but HTTP(S) — no reason to let a manifest reach rtsp/udp/mms.
PROTOCOLS_BACKEND = "http,https,tcp,tls,crypto,pipe"
PROTOCOLS_GENERIC = "http,https,tcp,tls,crypto,rtmp,rtsp,udp,rtp,mms,pipe"

# Backend content-type path segments this proxy mirrors.
LIVE_KINDS = {"channels", "events"}
ONDEMAND_KINDS = {"vod", "recordings"}
# Query parameters forwarded to the backend. client_drm / highest_quality_only
# are NOT forwarded: this proxy forces them (see _backend_stream_url).
PASSTHROUGH_PARAMS = ("country", "drm_variant", "no_proxy")
CATCHUP_PARAMS = ("start_time", "end_time", "epg_id")     # channels only

logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("mpegts_proxy")



# ===== Auth =====

OPEN_PATHS = {"/health", "/metrics"}   # reachable without a token, for orchestration


def _bearer_valid(request: web.Request) -> bool:
    """True when the request carries the configured bearer token (or auth is
    disabled). Used by /health to decide how much detail to reveal."""
    if AUTH_DISABLED:
        return True
    if not AUTH_TOKEN:
        return False
    header = request.headers.get("Authorization", "")
    if not header.startswith("Bearer "):
        return False
    return hmac.compare_digest(header[7:].strip().encode(), AUTH_TOKEN.encode())


@web.middleware
async def auth_middleware(request: web.Request, handler):
    if request.path in OPEN_PATHS:
        return await handler(request)
    if AUTH_DISABLED:
        return await handler(request)
    if not AUTH_TOKEN:
        return web.json_response(
            {"error": "MPEGTS_AUTH_TOKEN is not configured; refusing all requests"},
            status=503,
        )
    header = request.headers.get("Authorization", "")
    if not header.startswith("Bearer "):
        return web.json_response(
            {"error": "missing bearer token"},
            status=401,
            headers={"WWW-Authenticate": 'Bearer realm="mpegts"'},
        )
    token = header[7:].strip()
    if not hmac.compare_digest(token.encode(), AUTH_TOKEN.encode()):
        return web.json_response({"error": "invalid token"}, status=403)
    return await handler(request)


# ===== SSRF guard (defense-in-depth — see module docstring) =====

def _is_disallowed_ip(ip_str: str) -> bool:
    try:
        ip = ipaddress.ip_address(ip_str)
    except ValueError:
        return True
    return (
        ip.is_loopback
        or ip.is_private
        or ip.is_link_local
        or ip.is_reserved
        or ip.is_multicast
        or ip.is_unspecified
    )


async def _resolve_host_ips(host: str) -> list[str]:
    loop = asyncio.get_running_loop()
    try:
        infos = await loop.getaddrinfo(host, None, type=socket.SOCK_STREAM)
    except (socket.gaierror, UnicodeError):
        return []
    return [info[4][0] for info in infos]


async def _validate_url(url: str) -> tuple[bool, str]:
    """Validate scheme and target. The URL itself is passed to ffmpeg unmodified."""
    if not url or not isinstance(url, str):
        return False, "empty url"

    if url.startswith("pipe://"):
        if not ALLOW_PIPE:
            return False, "pipe:// is disabled (set MPEGTS_ALLOW_PIPE=1 to enable)"
        return True, "ok"

    try:
        parsed = urlparse(url)
    except ValueError:
        return False, "unparseable url"

    if parsed.scheme not in ALLOWED_SCHEMES:
        return False, f"scheme {parsed.scheme!r} not allowed"

    host = parsed.hostname
    if not host:
        return False, "no host in url"

    strict = parsed.scheme not in SCHEMES_ALLOW_PRIVATE and not ALLOW_PRIVATE_URLS

    # Literal IP — check directly.
    try:
        ipaddress.ip_address(host)
    except ValueError:
        pass
    else:
        if strict and _is_disallowed_ip(host):
            return False, f"ip {host} is disallowed"
        return True, "ok"

    # Hostname — resolve and check (non-blocking).
    ips = await _resolve_host_ips(host)
    if not ips:
        return False, f"DNS resolution failed for {host}"
    if strict:
        for ip in ips:
            if _is_disallowed_ip(ip):
                return False, f"{host} resolves to disallowed ip {ip}"
    return True, "ok"


# ===== Redaction (never leak credentials into logs / /health / 502 bodies) =====

_CRED_QUERY_RE = re.compile(
    r"(?i)\b((?:api[_-]?key|token|password|passwd|pass|secret|signature|sig"
    r"|session|jwt|auth|credential|key)s?=)[^\s&'\"]+"
)
_URL_USERINFO_RE = re.compile(r"//[^\s/'\"@]+:[^\s/'\"@]+@")


def _redact(text: str) -> str:
    """Mask credential-looking material: query params and user:pass@host."""
    text = _URL_USERINFO_RE.sub("//<redacted>@", text)
    return _CRED_QUERY_RE.sub(r"\1<redacted>", text)


def safe_url(url: str) -> str:
    """Strip credentials, query and fragment — for /health display."""
    if url.startswith("pipe://"):
        return "pipe://<command>"
    try:
        p = urlparse(url)
    except ValueError:
        return "<invalid>"
    host = p.hostname or ""
    if ":" in host:  # bare IPv6 literal
        host = f"[{host}]"
    port = f":{p.port}" if p.port else ""
    return f"{p.scheme}://{host}{port}{p.path}"


def loggable_url(url: str) -> str:
    """Redacted, length-capped URL for log lines."""
    if url.startswith("pipe://"):
        return "pipe://<command>"
    return _redact(url)[:160]

# ===== Stream state =====

class SpawnResult(enum.Enum):
    REUSED = "reused"
    SPAWNED = "spawned"
    MAX_STREAMS = "max streams reached"
    SPAWN_FAILED = "spawn failed"
    SPAWN_TIMEOUT = "spawn timed out"


# ===== TS packet grid / PSI tracking / keyframe anchoring =====

# Every chunk fanned out to clients is a whole number of 188-byte MPEG-TS
# packets (we generate the TS ourselves with '-f mpegts', which always
# writes 188-byte packets). Keeping the grid across fan-out, live-edge
# evictions and late joins is what prevents client-side demuxers from
# spiraling into 'Packet corrupt' / 'PES packet size mismatch' spam: an
# eviction that splices the stream mid-packet shifts the sync-byte phase
# and every following packet is misparsed until the demuxer rescans.
TS_PACKET_SIZE = 188

# PMT stream_type values that carry video (MPEG-1/2, MPEG-4 part 2, H.264, HEVC).
_VIDEO_STREAM_TYPES = {0x01, 0x02, 0x10, 0x1B, 0x24}


def _psi_section(pkt: bytes) -> bytes | None:
    """The PSI section carried by a payload-unit-start TS packet, or None
    when it does not fit in this single packet (PAT/PMT of a normal
    single-program stream always do)."""
    afc = (pkt[3] >> 4) & 0x3
    if not afc & 1:
        return None                      # no payload
    off = 4
    if afc & 2:
        off += 1 + pkt[4]                # skip adaptation field
    if off >= TS_PACKET_SIZE:
        return None
    off += 1 + pkt[off]                  # pointer_field
    if off + 3 > TS_PACKET_SIZE:
        return None
    section_length = ((pkt[off + 1] & 0x0F) << 8) | pkt[off + 2]
    end = off + 3 + section_length
    if end > TS_PACKET_SIZE:
        return None
    return bytes(pkt[off:end])


def _parse_pat(section: bytes) -> int | None:
    """PMT PID of the first real program, or None."""
    if len(section) < 12 or section[0] != 0x00:
        return None
    i = 8
    end = len(section) - 4               # CRC32
    while i + 4 <= end:
        program = (section[i] << 8) | section[i + 1]
        pid = ((section[i + 2] & 0x1F) << 8) | section[i + 3]
        if program != 0:
            return pid
        i += 4
    return None


def _parse_pmt_video_pid(section: bytes) -> tuple[bool, int | None]:
    """(parsed_ok, video_pid). video_pid is None for audio-only programs."""
    if len(section) < 16 or section[0] != 0x02:
        return False, None
    program_info_length = ((section[10] & 0x0F) << 8) | section[11]
    i = 12 + program_info_length
    end = len(section) - 4
    while i + 5 <= end:
        stream_type = section[i]
        pid = ((section[i + 1] & 0x1F) << 8) | section[i + 2]
        es_info_length = ((section[i + 3] & 0x0F) << 8) | section[i + 4]
        if stream_type in _VIDEO_STREAM_TYPES:
            return True, pid
        i += 5 + es_info_length
    return True, None


class PsiTracker:
    """Learns the PAT/PMT of a stream from the pump, so that
      * the video PID is known (late joiners can start on a video keyframe),
      * a late joiner can be handed PAT+PMT first, before any media.

    Keeps the FIRST complete PAT/PMT pair. Replaying a stored copy gives
    those packets a repeated continuity counter on the client side; like the
    counter jump of a live-edge skip this is harmless for demuxers (raw TS
    has no way to signal it)."""

    def __init__(self) -> None:
        self.pmt_pid: int | None = None
        self.pat_pkt: bytes | None = None
        self.pmt_pkt: bytes | None = None
        self.video_pid: int | None = None
        self.complete = False

    def feed(self, data: bytes) -> None:
        if self.complete:
            return
        for i in range(0, len(data) - TS_PACKET_SIZE + 1, TS_PACKET_SIZE):
            if data[i] != 0x47 or not (data[i + 1] & 0x40):
                continue
            pid = ((data[i + 1] & 0x1F) << 8) | data[i + 2]
            if pid == 0 and self.pat_pkt is None:
                section = _psi_section(data[i:i + TS_PACKET_SIZE])
                pmt_pid = _parse_pat(section) if section else None
                if pmt_pid is not None:
                    self.pmt_pid = pmt_pid
                    self.pat_pkt = bytes(data[i:i + TS_PACKET_SIZE])
            elif self.pmt_pid is not None and pid == self.pmt_pid and self.pmt_pkt is None:
                section = _psi_section(data[i:i + TS_PACKET_SIZE])
                ok, video_pid = _parse_pmt_video_pid(section) if section else (False, None)
                if ok and self.pat_pkt is not None:
                    self.pmt_pkt = bytes(data[i:i + TS_PACKET_SIZE])
                    self.video_pid = video_pid
                    self.complete = True
                    return

    def prefix(self) -> bytes:
        return self.pat_pkt + self.pmt_pkt if self.complete else b""


def _is_random_access(data: bytes, i: int) -> bool:
    """Adaptation-field random_access_indicator of the packet at offset i.
    ffmpeg's mpegts muxer sets it on keyframes."""
    afc = (data[i + 3] >> 4) & 0x3
    return bool(afc & 2) and data[i + 4] > 0 and bool(data[i + 5] & 0x40)


def _first_anchor_offset(data: bytes, video_pid: int | None) -> int:
    """Offset of the first packet a client may START on, scanning the
    188-byte grid; -1 when none.

    video_pid known: a video PES start with the random-access flag (a
    keyframe — playback starts clean instead of smearing until the next
    GOP). video_pid None (audio-only / unknown / fallback): any packet with
    payload_unit_start_indicator set.

    `data` must start on a packet boundary — guaranteed for every chunk,
    because the pump only emits whole packets."""
    last = len(data) - TS_PACKET_SIZE
    i = 0
    while i <= last:
        if data[i] == 0x47 and (data[i + 1] & 0x40):
            if video_pid is None:
                return i
            pid = ((data[i + 1] & 0x1F) << 8) | data[i + 2]
            if pid == video_pid and _is_random_access(data, i):
                return i
        i += TS_PACKET_SIZE
    return -1


def _trim_head_to_anchor(
    chunks: deque[bytes], video_pid: int | None
) -> tuple[bool, int, int]:
    """Drop leading chunks/bytes so the head starts exactly on an anchor
    (see _first_anchor_offset). Returns (found, chunks_dropped,
    bytes_dropped). Leaves the buffer untouched when there is no anchor
    anywhere."""
    drop_chunks = 0
    drop_bytes = 0
    partial = -1
    found = False
    for head in chunks:
        off = _first_anchor_offset(head, video_pid)
        if off == 0:
            found = True
            break
        if off < 0:
            drop_chunks += 1
            drop_bytes += len(head)
        else:
            found = True
            partial = off
            drop_bytes += off
            break
    if not found:
        return False, 0, 0
    for _ in range(drop_chunks):
        chunks.popleft()
    if partial >= 0 and chunks:
        chunks[0] = chunks[0][partial:]
    return True, drop_chunks, drop_bytes


class ClientBuffer:
    """Bounded per-client FIFO of packet-aligned TS chunks.

    Owns the live-edge policy:

      * a late joiner is HELD until the buffer contains a video keyframe,
        then released starting with PAT+PMT and that keyframe. The wait is
        bounded by TIME (JOIN_KEYFRAME_TIMEOUT), not by queue size: how much
        data a GOP spans depends on the bitrate and live DASH sources burst,
        so a size limit releases clients mid-GOP — no SPS/PPS, undecodable
        until the next keyframe. After the timeout it settles for a PES start.
      * while held, the buffer is capped by silently discarding the oldest
        chunks (nothing useful is lost: no client could start there, and it
        is not counted as a live-edge drop).
      * evictions are BATCHED — when the client falls behind, drop to half
        capacity in one go and re-anchor the head on the next keyframe (PES
        start as fallback), so an overflow episode costs a single clean
        seam instead of one mid-packet splice per chunk;
      * the end-of-stream sentinel rides the tail and never evicts data;
      * every skip is counted per client and per stream — never silent.

    Note: a live-edge jump still shows up client-side as a continuity
    counter discontinuity (raw TS has no drop signaling); players resync
    within a frame. This buffer guarantees they never have to resync the
    byte stream itself.
    """

    def __init__(self, max_bytes: int, stream: "StreamInfo | None" = None,
                 start_on_anchor: bool = False,
                 psi: PsiTracker | None = None) -> None:
        self._chunks: deque[bytes] = deque()
        self._max_bytes = max(4 * TS_PACKET_SIZE, int(max_bytes))
        self._bytes = 0               # bytes currently queued
        self._stream = stream
        self._psi = psi
        self._armed = start_on_anchor  # late join: hold until the first anchor
        self._armed_since = time.monotonic()
        self._closed = False
        self._event = asyncio.Event()
        self.dropped_chunks = 0       # evicted by live-edge trims
        self.dropped_bytes = 0
        self.join_skipped_chunks = 0  # bytes skipped to start on a keyframe
        self.join_skipped_bytes = 0

    # ----- producer side (pump task) -----

    def push(self, chunk: bytes) -> None:
        self._chunks.append(chunk)
        self._bytes += len(chunk)
        if self._armed:
            self._release_on_anchor()
        elif self._bytes > self._max_bytes:
            self._trim_to_live_edge()
        if not self._armed:
            self._event.set()

    def push_sentinel(self) -> None:
        """End of stream. Rides the tail: the writer drains queued data
        first, then observes the close (queued data is never evicted)."""
        self._closed = True
        self._armed = False           # nothing more will arrive; flush what we have
        self._event.set()

    # ----- live-edge policy -----

    def _release_on_anchor(self) -> None:
        """Late join: drop everything before the first anchor and release
        the client. Counted separately from evictions — a late join is
        normal operation, not a client falling behind."""
        video_pid = self._psi.video_pid if self._psi is not None else None
        timed_out = (time.monotonic() - self._armed_since) > JOIN_KEYFRAME_TIMEOUT
        if timed_out:
            video_pid = None          # no keyframe in time: settle for a PES start
        found, chunks, n_bytes = _trim_head_to_anchor(self._chunks, video_pid)
        self._bytes = sum(map(len, self._chunks))
        if found:
            if chunks or n_bytes:
                self.join_skipped_chunks += chunks
                self.join_skipped_bytes += n_bytes
                logger.debug(
                    f"client join skipped {chunks} chunks / {n_bytes} bytes "
                    f"to start on a keyframe"
                )
            # Always hand a joiner the tables first: when the anchor is the very
            # first packet nothing was skipped, yet no PAT/PMT precedes it.
            prefix = self._psi.prefix() if self._psi is not None else b""
            if prefix:
                self._chunks.appendleft(prefix)
                self._bytes += len(prefix)
            self._armed = False
        elif timed_out:
            self._armed = False       # not even a PES start: odd producer, flush what we have
        else:
            # Still waiting for a keyframe: bound the memory, silently.
            while self._bytes > self._max_bytes and len(self._chunks) > 1:
                dropped = self._chunks.popleft()
                self._bytes -= len(dropped)
                self.join_skipped_chunks += 1
                self.join_skipped_bytes += len(dropped)

    def _trim_to_live_edge(self) -> None:
        """One-batch eviction: down to half capacity, then re-anchor the
        head on the next keyframe (or PES start if none is buffered)."""
        target = self._max_bytes // 2
        popped = popped_bytes = 0
        while self._bytes > target and len(self._chunks) > 1:
            dropped = self._chunks.popleft()
            self._bytes -= len(dropped)
            popped_bytes += len(dropped)
            popped += 1
        video_pid = self._psi.video_pid if self._psi is not None else None
        found, anchored_chunks, anchored_bytes = _trim_head_to_anchor(self._chunks, video_pid)
        if not found and video_pid is not None:
            found, anchored_chunks, anchored_bytes = _trim_head_to_anchor(self._chunks, None)
        self._bytes = sum(map(len, self._chunks))
        dropped_chunks = popped + anchored_chunks
        dropped_bytes = popped_bytes + anchored_bytes
        if dropped_chunks or dropped_bytes:
            self.dropped_chunks += dropped_chunks
            self.dropped_bytes += dropped_bytes
            if self._stream is not None:
                self._stream.note_drop(dropped_chunks, dropped_bytes)

    # ----- consumer side (handler task) -----

    async def get(self, timeout: float) -> bytes | None:
        """Next chunk, or None once closed and fully drained. Raises
        asyncio.TimeoutError when nothing arrives within `timeout`."""
        while True:
            if self._chunks and not self._armed:
                chunk = self._chunks.popleft()
                self._bytes -= len(chunk)
                return chunk
            if self._closed:
                return None
            self._event.clear()
            # No await between the checks above and the wait below, so a
            # push cannot interleave and be lost (single event loop).
            await asyncio.wait_for(self._event.wait(), timeout)

    def fully_drained(self) -> bool:
        """True when no buffered chunks remain (a closed buffer drains
        through get() -> None)."""
        return not self._chunks


@dataclass(eq=False)      # identity hash: instances live in sets
class StreamInfo:
    """Per-stream state: process, fan-out buffers, diagnostics."""

    stream_id: str
    url: str                                   # canonical key (backend URL / original URL / pipe command)
    display_name: str
    process: asyncio.subprocess.Process
    mode: str = "live"                         # "live" (shared) | "ondemand" (one process per request)
    source_kind: str = "unknown"               # "hls" | "dash" | "unknown" (display / X-TS-Source)
    created: float = field(default_factory=time.time)
    started_mono: float = field(default_factory=time.monotonic)
    last_byte_mono: float = field(default_factory=time.monotonic)   # watchdog reference
    last_client_disconnect: float | None = None
    subscribers: set[ClientBuffer] = field(default_factory=set)
    psi: PsiTracker = field(default_factory=PsiTracker)
    pump_task: asyncio.Task | None = None
    stderr_task: asyncio.Task | None = None
    stderr_tail: deque[str] = field(default_factory=lambda: deque(maxlen=50))
    bytes_read: int = 0                        # bytes read from the source stdout (pre fan-out)
    dropped_chunks: int = 0                    # chunks evicted by live-edge trims (clients that fell behind)
    dropped_bytes: int = 0                     # bytes skipped by the same trims
    drop_sink: "callable | None" = None        # set by StreamManager: global-counter hook
    stall_events: int = 0                      # informational
    fetch_mode: str = "ffmpeg"                 # who fetches the segments: "ffmpeg" | "parallel" (X-TS-Fetch)
    feeders: list[asyncio.Task] = field(default_factory=list)   # parallel mode: one segment feeder per track
    feed_error: str | None = None              # parallel mode: why a feeder gave up

    def __post_init__(self) -> None:
        self._last_drop_log = 0.0

    def fail_feed(self, message: str) -> None:
        """A segment feeder gave up. Stop ffmpeg right away: the other tracks
        would otherwise keep flowing into a stream that is missing one of
        its parts. The handler notices feed_error and aborts the response."""
        if self.feed_error is None:
            self.feed_error = message
            if self.process.returncode is None:
                with contextlib.suppress(ProcessLookupError, PermissionError):
                    os.killpg(os.getpgid(self.process.pid), signal.SIGTERM)

    @property
    def client_count(self) -> int:
        return len(self.subscribers)

    def add_subscriber(self) -> ClientBuffer:
        # start_on_anchor: the client is held until the buffer holds a video
        # keyframe, then starts on PAT+PMT+keyframe — for a fresh stream and
        # for a late joiner alike, never mid-GOP and never mid-PES.
        buf = ClientBuffer(CLIENT_QUEUE_BYTES, stream=self,
                           start_on_anchor=True, psi=self.psi)
        self.subscribers.add(buf)
        self.last_client_disconnect = None
        return buf

    def remove_subscriber(self, buf: ClientBuffer) -> None:
        self.subscribers.discard(buf)
        if not self.subscribers:
            self.last_client_disconnect = time.time()

    def note_drop(self, chunks: int, n_bytes: int) -> None:
        """Account a live-edge eviction (per stream + global sink) and log
        rate-limited, so a stalling client never floods the log."""
        first = self.dropped_chunks == 0
        self.dropped_chunks += chunks
        self.dropped_bytes += n_bytes
        if self.drop_sink is not None:
            self.drop_sink(chunks, n_bytes)
        now = time.monotonic()
        if first or (now - self._last_drop_log) > 30.0:
            self._last_drop_log = now
            logger.warning(
                f"stream {self.stream_id}: skipping a lagging client ahead "
                f"to the live edge (stream total: {self.dropped_chunks} "
                f"chunks / {self.dropped_bytes} bytes skipped)"
            )


# ===== ffmpeg command =====

# Stream selection when the manifest gives no reason to be cleverer: the
# first video stream (optional: radio channels have none) and ALL audio
# tracks (channels commonly carry several languages, e.g. AAC + AC-3).
DEFAULT_MAP = ["-map", "0:v:0?", "-map", "0:a?"]


@dataclass
class PipeInput:
    """One ffmpeg input fed by the proxy through an inherited pipe fd."""
    fd: int
    fmt: str | None            # ffmpeg -f for the input; None = let ffmpeg probe


def _ffmpeg_cmd(source: str, name: str, *, map_args: list[str] | None = None,
                start: float | None = None, generic: bool = False,
                pipes: list[PipeInput] | None = None,
                low_latency: bool = True) -> list[str]:
    """ffmpeg remux command.

    pipes: the inputs are pipe:<fd> (static manifests, segments fetched by
      this proxy) instead of `source`; the HTTP-only input options
      (-reconnect, -rw_timeout, -user_agent, protocol whitelist) are left out
      because ffmpeg rejects options no protocol consumed.
    low_latency: flush every packet (live fan-out). On-demand playback has no
      latency to protect, and one write() per packet is what limits its
      throughput, so it passes False and lets the muxer batch its writes."""
    cmd: list[str] = [
        "ffmpeg",
        "-nostdin",                # never touch our stdin
        "-loglevel", FFMPEG_LOGLEVEL,
    ]
    if pipes:
        if len(pipes) > 1:
            # Separate audio/video inputs: keep their original relative
            # timestamps (otherwise each input is re-based to its own start);
            # -avoid_negative_ts below then shifts them all together.
            cmd += ["-copyts"]
        for p in pipes:
            cmd += [
                "-fflags", "+genpts+igndts+discardcorrupt",
                "-probesize", "5M",
                "-analyzeduration", "2M",
                "-thread_queue_size", "2048",
            ]
            if p.fmt:
                cmd += ["-f", p.fmt]
            cmd += ["-i", f"pipe:{p.fd}"]
    else:
        if USER_AGENT:
            cmd += ["-user_agent", USER_AGENT]
        cmd += [
            "-protocol_whitelist", PROTOCOLS_GENERIC if generic else PROTOCOLS_BACKEND,
            "-fflags", "+genpts+igndts+discardcorrupt",
            "-probesize", "5M",
            "-analyzeduration", "2M",
            "-reconnect", "1",
            "-reconnect_streamed", "1",
            "-reconnect_delay_max", "2",
            # A stalled TCP connection that never errors out would hang ffmpeg
            # forever while clients see a "running" stream without data. A read
            # timeout makes ffmpeg exit instead; the watchdog covers the rest.
            "-rw_timeout", str(RW_TIMEOUT * 1_000_000),
            "-thread_queue_size", "2048",
            # Escape hatch (MPEGTS_FFMPEG_EXTRA_IN): injected verbatim before -i,
            # e.g. -headers "Authorization: Bearer ..." for token-gated origins.
            *FFMPEG_EXTRA_IN,
        ]
        if start:
            cmd += ["-ss", f"{start:g}"]       # input seek (on-demand only)
        cmd += ["-i", source]
    cmd += [
        *(map_args or DEFAULT_MAP),
        "-c", "copy",
        "-avoid_negative_ts", "make_zero",
        "-max_muxing_queue_size", "8192",  # multi-audio timescales
        "-f", "mpegts",
        "-muxdelay", "0",
        "-muxpreload", "0",
        "-mpegts_flags", "resend_headers",
        "-mpegts_copyts", "1",
        "-metadata", f"service_name={name}",
        "-flush_packets", "1" if low_latency else "0",
        # Escape hatch (MPEGTS_FFMPEG_EXTRA_OUT): injected verbatim before the
        # output target, e.g. -mpegts_flags latm (HE-AAC sources), -an.
        *FFMPEG_EXTRA_OUT,
        "pipe:1",
    ]
    return cmd


# ===== Stream planning: pick the best variant from the manifest =====
#
# The backend collapses variants (highest_quality_only) only on paths that
# rewrite the manifest; a plain redirect to the upstream manifest — and the
# HLS passthrough — leave every variant in. ffmpeg then maps "0:v:0", the
# FIRST variant, which is often the lowest. So the proxy reads the manifest
# it is about to hand to ffmpeg and tells ffmpeg which variant to map.
#   HLS : ffmpeg exposes every #EXT-X-STREAM-INF as program N (in playlist
#         order) holding that variant's video plus its audio group.
#   DASH: every Representation becomes a stream tagged variant_bitrate.

def _hls_best_program(text: str) -> int | None:
    """Program index of the highest-BANDWIDTH variant of a master playlist;
    None when it has fewer than two variants (nothing to choose)."""
    best_idx: int | None = None
    best_bw = -1
    count = 0
    pending: int | None = None
    for raw in text.splitlines():
        line = raw.strip()
        if line.startswith("#EXT-X-STREAM-INF:"):
            m = re.search(r"(?:^|[:,])BANDWIDTH=(\d+)", line)
            pending = int(m.group(1)) if m else 0
        elif pending is not None and line and not line.startswith("#"):
            if pending > best_bw:
                best_bw = pending
                best_idx = count
            count += 1
            pending = None
    return best_idx if count > 1 else None


def _dash_map_args(text: str) -> list[str] | None:
    """-map arguments selecting the highest-bandwidth VIDEO representation;
    None when the MPD has nothing to choose between (or cannot be parsed).

    Audio is deliberately NOT filtered: every audio track is kept
    (-map 0:a?), like the backend's own ffmpeg variant does. An earlier
    version also picked one audio representation per language through the
    variant_bitrate tag; on a real live stream its output carried several
    times the audio data of a plain '-map 0:a?' run with otherwise identical
    options, and for the usual case (several languages/codecs) it selected the
    same streams anyway."""
    try:
        root = ET.fromstring(text)
    except ET.ParseError:
        return None

    def local(tag: str) -> str:
        return tag.rsplit("}", 1)[-1]

    video: list[int] = []
    for aset in root.iter():
        if local(aset.tag) != "AdaptationSet":
            continue
        for rep in aset:
            if local(rep.tag) != "Representation":
                continue
            ctype = (aset.get("contentType") or rep.get("contentType") or "").lower()
            if not ctype:
                ctype = (aset.get("mimeType") or rep.get("mimeType") or "").lower().split("/")[0]
            if ctype != "video":
                continue
            try:
                video.append(int(rep.get("bandwidth", "0")))
            except ValueError:
                continue
    if len(set(video)) > 1:
        return ["-map", f"0:v:m:variant_bitrate:{max(video)}?", "-map", "0:a?"]
    return None


def _plan_streams(text: str) -> tuple[str, list[str]]:
    """(kind, map_args) for a manifest body. kind: hls | dash | unknown."""
    stripped = text.lstrip("\ufeff \t\r\n")
    if stripped.startswith("#EXTM3U"):
        idx = _hls_best_program(stripped)
        if idx is None:
            return "hls", list(DEFAULT_MAP)
        return "hls", ["-map", f"0:p:{idx}:v:0?", "-map", f"0:p:{idx}:a?"]
    if "<MPD" in stripped[:4096]:
        args = _dash_map_args(stripped)
        return "dash", args if args else list(DEFAULT_MAP)
    return "unknown", list(DEFAULT_MAP)


# ===== Stream manager =====

class StreamManager:
    def __init__(self, max_streams: int = MAX_STREAMS, max_stream_age: int = MAX_STREAM_AGE):
        self.streams: dict[str, StreamInfo] = {}      # key -> shared (live) StreamInfo
        self.ondemand: set[StreamInfo] = set()        # one per on-demand request
        self.max_streams = max_streams
        self.max_stream_age = max_stream_age
        self._cleanup_task: asyncio.Task | None = None
        self._watchdog_task: asyncio.Task | None = None
        self._spawn_lock = asyncio.Lock()
        self._spawning: dict[str, asyncio.Future] = {}  # key -> future resolved by the leader
        self._ondemand_slots = 0                       # reserved before the (slow) backend lookup
        # Cumulative counters (monotonic — safe for Prometheus rate()).
        self._total_bytes_read = 0
        self._total_dropped_chunks = 0
        self._total_dropped_bytes = 0
        self._total_stalls = 0
        self._total_streams_created = 0
        self._total_ondemand_created = 0
        self._total_watchdog_kills = 0
        self._ondemand_seq = 0

    # ----- lifecycle (cleanup_ctx: startup and shutdown are guaranteed paired) -----

    async def lifecycle(self, app: web.Application):
        self._cleanup_task = asyncio.create_task(self._cleanup_loop())
        self._watchdog_task = asyncio.create_task(self._watchdog_loop())
        logger.info("StreamManager started")
        try:
            yield
        finally:
            await self.shutdown()

    async def shutdown(self) -> None:
        """Idempotent. Registered on app.on_shutdown as well: aiohttp waits
        for open handlers BEFORE it runs cleanup contexts, and our streaming
        handlers only end once their sentinel is pushed / their ffmpeg dies —
        so without this hook `docker stop` would wait out the whole grace
        period and then SIGKILL."""
        logger.info("StreamManager shutting down")
        for attr in ("_cleanup_task", "_watchdog_task"):
            task = getattr(self, attr)
            if task is not None and not task.done():
                task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await task
            setattr(self, attr, None)
        for key in list(self.streams.keys()):
            await self._terminate_stream(key, reason="shutdown")
        # On-demand handlers see EOF on stdout once their process is gone and
        # clean up after themselves.
        for info in list(self.ondemand):
            await self._kill_process(info.process)
        # If a spawn is still in flight, unblock its followers.
        for fut in list(self._spawning.values()):
            if not fut.done():
                fut.set_result(None)
        self._spawning.clear()

    # ----- background cleanup -----

    async def _cleanup_loop(self) -> None:
        while True:
            await asyncio.sleep(CLEANUP_INTERVAL)
            try:
                await self._cleanup_stale_streams()
            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.error(f"cleanup loop error: {e}")

    async def _cleanup_stale_streams(self) -> None:
        now = time.time()
        to_remove: list[tuple[str, str]] = []
        for key, info in list(self.streams.items()):
            if info.process.returncode is not None:
                to_remove.append((key, "process exited"))
                continue
            if info.client_count > 0:
                continue  # active viewers — never evicted for age/idle
            idle_since = info.last_client_disconnect or info.created
            if (now - idle_since) > IDLE_TIMEOUT:
                to_remove.append((key, f"idle {IDLE_TIMEOUT}s"))
            elif (now - info.created) > self.max_stream_age:
                to_remove.append((key, f"max age {self.max_stream_age}s with no viewers"))
        for key, reason in to_remove:
            logger.info(f"removing stream {safe_url(key)} ({reason})")
            await self._terminate_stream(key, reason=reason)

    async def _watchdog_loop(self) -> None:
        """Kill shared streams whose SOURCE went silent. A hung connection
        keeps ffmpeg alive without producing bytes; clients get dropped by
        their own stall limit and would reconnect to the same dead process
        (a stream with viewers is never evicted by the cleanup loop). Killing
        it here means the next request respawns a fresh ffmpeg."""
        while True:
            await asyncio.sleep(WATCHDOG_INTERVAL)
            try:
                now = time.monotonic()
                for key, info in list(self.streams.items()):
                    if info.process.returncode is not None:
                        continue
                    if info.bytes_read > 0:
                        silent, limit = now - info.last_byte_mono, STALL_TIMEOUT
                    else:
                        silent, limit = now - info.started_mono, STARTUP_TIMEOUT
                    if silent > limit:
                        self._total_watchdog_kills += 1
                        logger.warning(
                            f"stream {info.stream_id}: source silent for {int(silent)}s "
                            f"(limit {limit}s) — killing so the next request respawns it"
                        )
                        await self._terminate_stream(key, reason="source stalled")
            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.error(f"watchdog loop error: {e}")

    # ----- process helpers -----

    async def _start_process(self, cmd: list[str],
                             pass_fds: tuple[int, ...] = ()) -> asyncio.subprocess.Process | None:
        try:
            proc = await asyncio.create_subprocess_exec(
                *cmd,
                pass_fds=pass_fds,
                stdin=subprocess.DEVNULL,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                limit=2 * 1024 * 1024,
                start_new_session=True,  # own process group -> clean kill of ffmpeg + children
            )
        except FileNotFoundError:
            logger.error(f"binary not found on PATH: {cmd[0]}")
            return None
        except Exception as e:
            logger.error(f"failed to spawn {cmd[0]}: {e}")
            return None

        # Best-effort pipe buffer growth (F_SETPIPE_SZ = 1031). Unprivileged
        # processes are capped by /proc/sys/fs/pipe-max-size (1 MB default)
        # — failure here is expected on most hosts and harmless.
        # NOTE: proc.stdout is an asyncio StreamReader (no .fileno());
        # the raw fd lives on the underlying pipe transport. Catch broadly:
        # this must NEVER break a spawn.
        try:
            transport = getattr(proc.stdout, "transport", None) or getattr(
                proc.stdout, "_transport", None
            )
            pipe_fd = transport.get_extra_info("pipe") if transport else None
            if pipe_fd is not None:
                fcntl.fcntl(pipe_fd.fileno(), 1031, 2 * 1048576)
        except Exception:
            pass
        return proc

    @staticmethod
    async def _kill_process(proc: asyncio.subprocess.Process) -> None:
        """SIGTERM the whole process group, SIGKILL after 2 s. Never signals
        an already-reaped pid (the pid — and its group — may have been reused)."""
        if proc.returncode is None:
            try:
                os.killpg(os.getpgid(proc.pid), signal.SIGTERM)
            except (ProcessLookupError, PermissionError):
                pass
        try:
            await asyncio.wait_for(proc.wait(), timeout=2.0)
        except asyncio.TimeoutError:
            if proc.returncode is None:
                try:
                    os.killpg(os.getpgid(proc.pid), signal.SIGKILL)
                except (ProcessLookupError, PermissionError):
                    with contextlib.suppress(ProcessLookupError):
                        proc.kill()
            with contextlib.suppress(asyncio.TimeoutError):
                await asyncio.wait_for(proc.wait(), timeout=2.0)

    async def _terminate_stream(self, key: str, reason: str = "unknown") -> None:
        info = self.streams.pop(key, None)
        if info is None:
            return
        # Wake every waiting handler. The sentinel rides the buffer tail, so
        # any still-queued data is delivered before the clean close.
        for buf in list(info.subscribers):
            buf.push_sentinel()
        info.subscribers.clear()

        await self._kill_process(info.process)

        for t in (info.pump_task, info.stderr_task):
            if t is not None and not t.done():
                t.cancel()
                with contextlib.suppress(Exception, asyncio.CancelledError):
                    await asyncio.wait_for(t, timeout=1.0)
        logger.info(f"stream {info.stream_id} terminated ({reason})")

    # ----- stderr drain -----

    async def _drain_stderr(self, info: StreamInfo) -> None:
        try:
            while True:
                line = await info.process.stderr.readline()
                if not line:
                    break
                msg = line.decode(errors="replace").strip()
                if not msg:
                    continue
                # ffmpeg echoes source URLs (including credentials) in errors.
                msg = _redact(msg)
                info.stderr_tail.append(msg)
                if any(n in msg for n in (
                    "Non-monotonic DTS", "Past duration",
                    "Application provided invalid", "Last message repeated",
                )):
                    continue
                if any(e in msg for e in ("Error", "Invalid", "Failed", "Cannot", "404", "fragment")):
                    logger.error(f"ffmpeg[{info.stream_id}]: {msg}")
                else:
                    logger.debug(f"ffmpeg[{info.stream_id}]: {msg}")
        except asyncio.CancelledError:
            raise
        except Exception as e:
            logger.debug(f"stderr drain for {info.stream_id} ended: {e}")

    # ----- pump (live mode) -----

    async def _pump(self, info: StreamInfo) -> None:
        """Single reader -> bounded fan-out.

        Reads are re-anchored on the 188-byte TS packet grid before
        fan-out, so every chunk a client can ever receive is a whole
        number of packets and live-edge evictions can never splice the
        stream mid-packet (that splice is what showed up as 'Packet
        corrupt' / 'PES packet size mismatch' in client demuxers). A
        client that falls behind is trimmed to the live edge in one batch
        by its ClientBuffer; the end sentinel rides the tail."""
        proc = info.process
        carry = bytearray()
        try:
            while True:
                try:
                    chunk = await proc.stdout.read(STREAM_READ_SIZE)
                except asyncio.CancelledError:
                    raise
                except Exception as e:
                    logger.warning(f"read error on {info.stream_id}: {e}")
                    break
                if not chunk:
                    logger.info(f"stream {info.stream_id} EOF")
                    break

                info.last_byte_mono = time.monotonic()
                info.bytes_read += len(chunk)
                self._total_bytes_read += len(chunk)

                carry += chunk
                emit = len(carry) - len(carry) % TS_PACKET_SIZE
                if emit:
                    payload = bytes(carry[:emit])
                    del carry[:emit]
                    info.psi.feed(payload)     # before fan-out: joiners need the tables
                    for buf in list(info.subscribers):
                        buf.push(payload)

            if carry:
                # EOF with a trailing partial packet: only possible for a
                # truncated source or a non-TS pipe:// producer. Forward it
                # rather than silently losing bytes.
                payload = bytes(carry)
                for buf in list(info.subscribers):
                    buf.push(payload)
        finally:
            # EOF/error/exit: wake all handlers so they close cleanly. The
            # sentinel rides the tail; queued data is drained first.
            for buf in list(info.subscribers):
                buf.push_sentinel()

    # ----- spawn / get (live mode) -----

    async def get_or_create(
        self, key: str, name: str, *, source: str | None = None,
        map_args: list[str] | None = None, kind: str = "unknown",
        generic: bool = False,
    ) -> tuple[StreamInfo | None, SpawnResult]:
        """Shared stream for `key`. `source` is what ffmpeg opens (defaults
        to the key itself); a reused stream ignores source/map_args."""

        # Fast path: alive -> reuse; dead -> prune now so it stops counting
        # against the cap (the cleanup loop otherwise lags up to 30 s).
        existing = self.streams.get(key)
        if existing is not None:
            if existing.process.returncode is None:
                return existing, SpawnResult.REUSED
            await self._terminate_stream(key, reason="process exited (pruned on request)")

        async with self._spawn_lock:
            existing = self.streams.get(key)
            if existing is not None and existing.process.returncode is None:
                return existing, SpawnResult.REUSED

            fut = self._spawning.get(key)
            is_leader = fut is None
            if is_leader:
                if len(self.streams) + len(self._spawning) >= self.max_streams:
                    return None, SpawnResult.MAX_STREAMS
                fut = asyncio.get_running_loop().create_future()
                self._spawning[key] = fut

        if not is_leader:
            # Follower: wait for the leader's result. shield() so a follower
            # being cancelled (its client left) cannot cancel the shared
            # future and poison the leader and other followers.
            try:
                info = await asyncio.wait_for(
                    asyncio.shield(fut), timeout=SPAWN_WAIT_TIMEOUT
                )
            except asyncio.TimeoutError:
                return None, SpawnResult.SPAWN_TIMEOUT
            except asyncio.CancelledError:
                raise
            if info is None:
                return None, SpawnResult.SPAWN_FAILED
            return info, SpawnResult.REUSED

        # Leader. The finally block resolves the future on EVERY exit path —
        # including cancellation of the leader — so followers can never hang.
        info: StreamInfo | None = None
        try:
            info = await self._spawn(key, name, source or key, map_args, kind, generic)
        except asyncio.CancelledError:
            raise
        except Exception as e:
            logger.error(f"spawn failed for {loggable_url(key)}: {e}")
        finally:
            self._spawning.pop(key, None)
            if not fut.done():
                fut.set_result(info)
        if info is None:
            return None, SpawnResult.SPAWN_FAILED
        return info, SpawnResult.SPAWNED

    async def _spawn(self, key: str, name: str, source: str,
                     map_args: list[str] | None, kind: str,
                     generic: bool) -> StreamInfo | None:
        stream_id = self._stream_id_from_key(key)

        if key.startswith("pipe://"):
            cmd = shlex.split(key[7:])
        else:
            cmd = _ffmpeg_cmd(source, name, map_args=map_args, generic=generic)

        proc = await self._start_process(cmd)
        if proc is None:
            return None

        # NB: there is no await between _start_process() returning and the
        # try block below, so task cancellation cannot be delivered in that
        # gap — the process cannot leak past the BaseException guard.
        try:
            info = StreamInfo(
                stream_id=stream_id,
                url=key,
                display_name=name,
                process=proc,
                source_kind=kind,
                drop_sink=self._account_drops,
            )
            self.streams[key] = info
            self._total_streams_created += 1
            info.pump_task = asyncio.create_task(self._pump(info))
            info.stderr_task = asyncio.create_task(self._drain_stderr(info))
            logger.info(f"stream {stream_id} created ({loggable_url(key)}, {kind})")
            return info
        except BaseException:
            # Cancelled (or otherwise failed) between process creation and
            # registration — kill the group so we don't leak a process.
            with contextlib.suppress(ProcessLookupError, PermissionError):
                os.killpg(os.getpgid(proc.pid), signal.SIGKILL)
            with contextlib.suppress(Exception):
                await asyncio.wait_for(proc.wait(), timeout=1.0)
            raise

    # ----- on-demand sessions (one ffmpeg per request) -----

    def acquire_ondemand(self) -> bool:
        """Reserve a slot BEFORE the backend lookup, so concurrent requests
        cannot overshoot the cap while they await it."""
        if self._ondemand_slots >= MAX_ONDEMAND:
            return False
        self._ondemand_slots += 1
        return True

    def release_ondemand(self) -> None:
        self._ondemand_slots = max(0, self._ondemand_slots - 1)

    async def start_ondemand(self, key: str, name: str, source: str,
                             map_args: list[str] | None, kind: str,
                             start: float | None) -> StreamInfo | None:
        cmd = _ffmpeg_cmd(source, name, map_args=map_args, start=start)
        proc = await self._start_process(cmd)
        if proc is None:
            return None
        self._ondemand_seq += 1
        self._total_ondemand_created += 1
        info = StreamInfo(
            stream_id=f"{self._stream_id_from_key(key)}-{self._ondemand_seq}",
            url=key, display_name=name, process=proc,
            mode="ondemand", source_kind=kind,
        )
        info.stderr_task = asyncio.create_task(self._drain_stderr(info))
        self.ondemand.add(info)
        logger.info(f"on-demand {info.stream_id} started ({loggable_url(key)}, {kind})")
        return info

    async def start_ondemand_fast(self, key: str, name: str, plan: "StaticPlan",
                                  session: aiohttp.ClientSession) -> StreamInfo | None:
        """On-demand playback of a static manifest: this proxy downloads the
        segments (several at a time) and writes them into one pipe per track;
        ffmpeg only remuxes. Returns None when ffmpeg cannot be started."""
        read_fds: list[int] = []
        write_fds: list[int] = []
        proc = None
        try:
            for _ in plan.tracks:
                r, w = os.pipe()
                read_fds.append(r)
                write_fds.append(w)
            maps: list[str] = []
            for i, track in enumerate(plan.tracks):
                maps += track.maps(i)
            cmd = _ffmpeg_cmd(
                "", name, map_args=maps, low_latency=False,
                pipes=[PipeInput(fd, t.fmt) for fd, t in zip(read_fds, plan.tracks)],
            )
            proc = await self._start_process(cmd, pass_fds=tuple(read_fds))
        finally:
            for fd in read_fds:                 # the child owns its copies now
                with contextlib.suppress(OSError):
                    os.close(fd)
        if proc is None:
            for fd in write_fds:
                with contextlib.suppress(OSError):
                    os.close(fd)
            return None

        self._ondemand_seq += 1
        self._total_ondemand_created += 1
        info = StreamInfo(
            stream_id=f"{self._stream_id_from_key(key)}-{self._ondemand_seq}",
            url=key, display_name=name, process=proc,
            mode="ondemand", source_kind=plan.kind, fetch_mode="parallel",
        )
        info.stderr_task = asyncio.create_task(self._drain_stderr(info))
        self.ondemand.add(info)
        try:
            for i, (track, wfd) in enumerate(zip(plan.tracks, write_fds)):
                writer = await _open_write_pipe(wfd)
                write_fds[i] = -1               # now owned by the writer
                info.feeders.append(asyncio.create_task(_feed_track(session, track, writer, info)))
        except BaseException as e:  # CancelledError too: the handler's
            # `info` is still unassigned there, so nobody else will clean up.
            for fd in write_fds:
                if fd >= 0:
                    with contextlib.suppress(OSError):
                        os.close(fd)
            info.fail_feed(f"could not open pipes: {e}")
            with contextlib.suppress(ProcessLookupError):
                os.killpg(os.getpgid(info.process.pid), signal.SIGKILL)
            self.ondemand.discard(info)
            raise
        logger.info(
            f"on-demand {info.stream_id} started ({loggable_url(key)}, {plan.kind}, "
            f"{len(plan.tracks)} track(s), {PARALLEL_SEGMENTS} segments in flight each)"
        )
        return info

    async def finish_ondemand(self, info: StreamInfo) -> None:
        self.ondemand.discard(info)
        for t in info.feeders:
            t.cancel()
        for t in info.feeders:
            with contextlib.suppress(Exception, asyncio.CancelledError):
                await asyncio.wait_for(t, timeout=1.0)
        await self._kill_process(info.process)
        t = info.stderr_task
        if t is not None and not t.done():
            t.cancel()
            with contextlib.suppress(Exception, asyncio.CancelledError):
                await asyncio.wait_for(t, timeout=1.0)
        logger.info(f"on-demand {info.stream_id} finished ({info.bytes_read / 1048576:.1f} MB)")

    @staticmethod
    def _stream_id_from_key(key: str) -> str:
        # Display/label only — never used for matching (the dict is keyed by URL).
        return hashlib.md5(key.encode()).hexdigest()[:12]

    def note_stall(self, info: StreamInfo) -> None:
        info.stall_events += 1
        self._total_stalls += 1

    def _account_drops(self, chunks: int, n_bytes: int) -> None:
        """drop_sink target: global live-edge counters for /health + /metrics."""
        self._total_dropped_chunks += chunks
        self._total_dropped_bytes += n_bytes

    # ----- introspection -----

    def health_snapshot(self, detailed: bool = True) -> dict:
        now = time.time()
        infos = list(self.streams.values())
        alive = sum(1 for i in infos if i.process.returncode is None)
        status = "healthy"
        if infos and alive == 0:
            status = "degraded"
        snap = {
            "status": status,
            "streams": len(infos),
            "alive_streams": alive,
            "max_streams": self.max_streams,
            "ondemand_streams": len(self.ondemand),
            "max_ondemand": MAX_ONDEMAND,
        }
        if not detailed:
            return snap       # no stream names / ids / viewer counts without a token
        snap["totals"] = {
            "bytes_read": self._total_bytes_read,
            "dropped_chunks": self._total_dropped_chunks,
            "dropped_bytes": self._total_dropped_bytes,
            "stalls": self._total_stalls,
            "streams_created": self._total_streams_created,
            "ondemand_created": self._total_ondemand_created,
            "watchdog_kills": self._total_watchdog_kills,
        }
        snap["stream_details"] = [
            {
                "id": i.stream_id,
                "mode": i.mode,
                "source": i.source_kind,
                "name": i.display_name,
                "url": safe_url(i.url),
                "age": int(now - i.created),
                "clients": i.client_count if i.mode == "live" else 1,
                "alive": i.process.returncode is None,
                "bytes_read": i.bytes_read,
                "dropped_chunks": i.dropped_chunks,
                "dropped_bytes": i.dropped_bytes,
                "stall_events": i.stall_events,
                "last_stderr": list(i.stderr_tail)[-3:],
            }
            for i in infos + list(self.ondemand)
        ]
        return snap

    def metrics_snapshot(self) -> str:
        alive = sum(1 for i in self.streams.values() if i.process.returncode is None)
        clients = sum(i.client_count for i in self.streams.values())
        lines = [
            "# HELP mpegts_proxy_active_streams Tracked shared (live) streams",
            "# TYPE mpegts_proxy_active_streams gauge",
            f"mpegts_proxy_active_streams {len(self.streams)}",
            "# HELP mpegts_proxy_alive_streams Streams whose source process is alive",
            "# TYPE mpegts_proxy_alive_streams gauge",
            f"mpegts_proxy_alive_streams {alive}",
            "# HELP mpegts_proxy_active_clients Connected clients across all live streams",
            "# TYPE mpegts_proxy_active_clients gauge",
            f"mpegts_proxy_active_clients {clients}",
            "# HELP mpegts_proxy_active_ondemand Running per-request (on-demand) ffmpeg processes",
            "# TYPE mpegts_proxy_active_ondemand gauge",
            f"mpegts_proxy_active_ondemand {len(self.ondemand)}",
            "# HELP mpegts_proxy_max_streams Configured shared-stream cap",
            "# TYPE mpegts_proxy_max_streams gauge",
            f"mpegts_proxy_max_streams {self.max_streams}",
            "# HELP mpegts_proxy_max_ondemand Configured on-demand cap",
            "# TYPE mpegts_proxy_max_ondemand gauge",
            f"mpegts_proxy_max_ondemand {MAX_ONDEMAND}",
            "# HELP mpegts_proxy_bytes_read_total Cumulative bytes read from live source processes (pre fan-out)",
            "# TYPE mpegts_proxy_bytes_read_total counter",
            f"mpegts_proxy_bytes_read_total {self._total_bytes_read}",
            "# HELP mpegts_proxy_stream_stalls_total Cumulative per-client read stalls",
            "# TYPE mpegts_proxy_stream_stalls_total counter",
            f"mpegts_proxy_stream_stalls_total {self._total_stalls}",
            "# HELP mpegts_proxy_streams_created_total Shared streams ever created",
            "# TYPE mpegts_proxy_streams_created_total counter",
            f"mpegts_proxy_streams_created_total {self._total_streams_created}",
            "# HELP mpegts_proxy_ondemand_created_total On-demand sessions ever started",
            "# TYPE mpegts_proxy_ondemand_created_total counter",
            f"mpegts_proxy_ondemand_created_total {self._total_ondemand_created}",
            "# HELP mpegts_proxy_watchdog_kills_total Streams killed because the source went silent",
            "# TYPE mpegts_proxy_watchdog_kills_total counter",
            f"mpegts_proxy_watchdog_kills_total {self._total_watchdog_kills}",
            "# HELP mpegts_proxy_dropped_chunks_total Chunks evicted from full client queues (live-edge drops)",
            "# TYPE mpegts_proxy_dropped_chunks_total counter",
            f"mpegts_proxy_dropped_chunks_total {self._total_dropped_chunks}",
            "# HELP mpegts_proxy_dropped_bytes_total Bytes skipped by live-edge evictions (packet-aligned, keyframe-anchored)",
            "# TYPE mpegts_proxy_dropped_bytes_total counter",
            f"mpegts_proxy_dropped_bytes_total {self._total_dropped_bytes}",
        ]
        return "\n".join(lines) + "\n"



# ===== Backend resolution =====

@dataclass
class Resolved:
    source: str            # what ffmpeg should open
    map_args: list[str]    # which streams to map (see _plan_streams)
    kind: str              # hls | dash | unknown
    text: str | None = None    # the manifest as fetched (None: unreadable)
    base_url: str = ""         # where relative URIs in `text` resolve against


class BackendError(Exception):
    """The backend (or the redirect it handed us) cannot be used. Carries the
    HTTP status to answer with, plus a human hint."""

    def __init__(self, status: int, message: str, hint: str | None = None) -> None:
        super().__init__(message)
        self.status = status
        self.message = message
        self.hint = hint


def _json_error(status: int, message: str, *, hint: str | None = None,
                details: list[str] | None = None,
                headers: dict[str, str] | None = None) -> web.Response:
    body: dict = {"error": message}
    if hint:
        body["hint"] = hint
    if details:
        body["details"] = details
    return web.json_response(body, status=status, headers=headers)


def _backend_error_response(e: BackendError) -> web.Response:
    # Visible in the log: a request that fails before ffmpeg is spawned would
    # otherwise leave no trace there at all.
    logger.warning(f"backend lookup failed ({e.status}): {e.message}")
    headers = {"Retry-After": str(RETRY_AFTER)} if e.status == 503 else None
    return _json_error(e.status, e.message, hint=e.hint, headers=headers)


def _hint_for_backend_status(status: int) -> str | None:
    if status == 400:
        return ("The backend refused the request. A TS conversion needs clear "
                "(unencrypted or ClearKey) content, decrypted server-side; "
                "Widevine/PlayReady content cannot be converted.")
    if status == 404:
        return "Unknown provider or content id, or no manifest is available for it."
    if status == 503:
        return ("The backend is not ready. ClearKey content also needs "
                "MEDIA_PROXY_URL on the backend (server-side decryption).")
    return None


_STDERR_HINTS = (
    (("Server returned 401", "Server returned 403"),
     "The upstream refused access (HTTP 401/403): expired token, geo-block or missing headers."),
    (("Server returned 404",), "The upstream manifest or segment was not found (HTTP 404)."),
    (("codec not currently supported in container", "Could not find tag for codec"),
     "The source codec cannot be carried in MPEG-TS (e.g. VP9/AV1)."),
    (("Invalid data found when processing input",),
     "The upstream did not return a valid HLS/DASH stream (DRM-protected, or an error page)."),
    (("Connection refused", "Failed to resolve", "Name or service not known", "Connection timed out"),
     "Cannot reach the upstream host."),
    (("matches no streams", "does not contain any stream"),
     "The manifest has no playable audio/video stream."),
)


def _hint_from_stderr(lines) -> str | None:
    text = "\n".join(lines)
    for needles, hint in _STDERR_HINTS:
        if any(n in text for n in needles):
            return hint
    return None


def _error_from_backend(status: int, body: bytes) -> BackendError:
    message = f"backend returned HTTP {status}"
    try:
        data = json.loads(body.decode("utf-8", "replace"))
        if isinstance(data, dict) and data.get("error"):
            message = str(data["error"])[:500]
    except ValueError:
        pass
    if 400 <= status < 500:
        out = status
    elif status in (502, 503, 504):
        out = status
    else:
        out = 502
    return BackendError(out, message, _hint_for_backend_status(status))


def _backend_stream_url(kind: str, provider: str, content_id: str,
                        query: Mapping[str, str]) -> str:
    """The backend URL this proxy asks for. Deterministic (stable parameter
    order), so it doubles as the dedup key of a shared stream.

    client_drm=false: the server decrypts ClearKey, the converter gets plain
    segments. highest_quality_only=true: one video quality. Both are forced,
    whatever the caller sent."""
    params = [(k, query[k]) for k in PASSTHROUGH_PARAMS if query.get(k)]
    if kind == "channels":
        params += [(k, query[k]) for k in CATCHUP_PARAMS if query.get(k)]
    params += [("client_drm", "false"), ("highest_quality_only", "true")]
    path = (
        f"/api/providers/{quote(provider, safe='')}/{kind}/"
        f"{quote(content_id, safe='')}/stream/index.mpd"
    )
    return f"{BACKEND_URL}{path}?{urlencode(params)}"


def _is_ondemand(kind: str, query: Mapping[str, str]) -> bool:
    """On-demand: VOD, recordings and catchup. Mirrors the backend, which
    treats a catchup start_time in the future as 'serve the live stream'."""
    if kind in ONDEMAND_KINDS:
        return True
    if kind == "channels":
        start, end = query.get("start_time"), query.get("end_time")
        if start and end:
            try:
                return int(start) <= time.time()
            except ValueError:
                return False     # the backend answers 400 for it
    return False


def _parse_start(raw: str | None) -> float | None:
    if raw is None or raw == "":
        return None
    v = float(raw)               # ValueError -> caller answers 400
    if not (0 <= v < 1e7):       # also rejects NaN
        raise ValueError("start out of range")
    return v


async def _read_capped(resp: aiohttp.ClientResponse, cap: int = MANIFEST_MAX_BYTES) -> bytes:
    buf = bytearray()
    async for chunk in resp.content.iter_chunked(65536):
        buf += chunk
        if len(buf) > cap:
            break
    return bytes(buf[:cap])


async def _fetch_text(session: aiohttp.ClientSession, url: str) -> str | None:
    """Best effort: the manifest is only read to choose a variant, so any
    failure just means 'use the default stream selection'."""
    try:
        async with session.get(url, max_redirects=3) as r:
            if r.status != 200:
                return None
            return (await _read_capped(r)).decode("utf-8", "replace")
    except Exception as e:
        logger.debug(f"manifest fetch for variant selection failed: {e}")
        return None


async def _resolve_source(session: aiohttp.ClientSession, backend_url: str) -> Resolved:
    """Ask the backend once, WITHOUT following redirects.

      3xx -> ffmpeg opens the redirect target directly (the backend's work is
             done; provider tokens are not resolved a second time).
      200 -> the backend serves the manifest itself (rewritten for the media
             proxy / header context): ffmpeg opens the same backend URL again.
      4xx/5xx -> relayed with the backend's message and a hint.
    """
    text: str | None = None
    redirect_target: str | None = None
    try:
        async with session.get(backend_url, allow_redirects=False) as r:
            if r.status in (301, 302, 303, 307, 308):
                location = r.headers.get("Location")
                if not location:
                    raise BackendError(502, "backend redirected without a Location header")
                redirect_target = urljoin(backend_url, location)
            elif r.status == 200:
                text = (await _read_capped(r)).decode("utf-8", "replace")
            else:
                raise _error_from_backend(r.status, await _read_capped(r, 8192))
    except BackendError:
        raise
    except (aiohttp.ClientError, asyncio.TimeoutError) as e:
        raise BackendError(
            502, "backend unreachable",
            hint=f"Check MPEGTS_BACKEND_URL ({safe_url(BACKEND_URL)}) and the network between proxy and backend.",
        ) from e

    if redirect_target is not None:
        if urlparse(redirect_target).scheme not in ("http", "https"):
            raise BackendError(502, "backend redirected to a non-HTTP target")
        ok, reason = await _validate_url(redirect_target)
        if not ok:
            raise BackendError(502, f"backend redirected to a disallowed target ({reason})")
        text = await _fetch_text(session, redirect_target)
        source = redirect_target
    else:
        source = backend_url

    kind, map_args = _plan_streams(text or "")
    return Resolved(source=source, map_args=map_args, kind=kind, text=text, base_url=source)


# ===== Static manifests: parallel segment fetching =====
#
# A finished manifest (VOD, recording, past catchup: DASH type="static", HLS
# with #EXT-X-ENDLIST) lists every segment up front, so nothing forces them to
# be downloaded one after the other. ffmpeg's demuxers do exactly that, one
# short-lived connection per segment, and a slow origin (backend + media-proxy
# decrypt) makes the whole download latency-bound.
#
# The fast path therefore reads the manifest itself and keeps up to
# PARALLEL_SEGMENTS requests in flight per track over keep-alive connections.
# The segments are written, in order, into one pipe per track; ffmpeg
# (-c copy) remuxes them to TS. Backpressure runs the whole way back:
#   client socket -> ffmpeg stdout -> ffmpeg pipe reads -> pipe write -> window
# so the download is as fast as origin AND client allow, and a slow client
# only ever holds PARALLEL_SEGMENTS segments per track in memory.
#
# Only the clean, common cases take this path. Anything unusual (live/dynamic,
# several periods, discontinuities, byte ranges, HLS encryption, unreadable
# manifests, ...) returns None from the planner and ffmpeg fetches the stream
# the way it always did.

@dataclass
class TrackPlan:
    kind: str                  # "muxed" (audio+video) | "video" | "audio"
    fmt: str | None            # ffmpeg input format: "mp4" | "mpegts" | None (probe)
    init: str | None           # initialization segment, written first
    segments: list[str]
    label: str = ""

    def maps(self, idx: int) -> list[str]:
        if self.kind == "video":
            return ["-map", f"{idx}:v:0?"]
        if self.kind == "audio":
            return ["-map", f"{idx}:a?"]
        return ["-map", f"{idx}:v:0?", "-map", f"{idx}:a?"]

    def urls(self) -> list[str]:
        return ([self.init] if self.init else []) + self.segments


@dataclass
class StaticPlan:
    kind: str                  # hls | dash
    tracks: list[TrackPlan]


def _origin(url: str) -> tuple[str, str | None, int | None]:
    p = urlparse(url)
    return (p.scheme, p.hostname, p.port)


# -- HLS ------------------------------------------------------------------------

_HLS_ATTR_RE = re.compile(r'([A-Z0-9-]+)=("[^"]*"|[^,]*)')


def _hls_attrs(line: str) -> dict[str, str]:
    body = line.split(":", 1)[1] if ":" in line else ""
    return {k: v.strip().strip('"') for k, v in _HLS_ATTR_RE.findall(body)}


def _hls_parse_media(text: str, base: str) -> tuple[str | None, str | None, list[str]] | None:
    """(init, ffmpeg input format, segment URLs) of a FINISHED, clean media
    playlist; None when the fast path does not apply."""
    init: str | None = None
    segs: list[str] = []
    ended = False
    for raw in text.splitlines():
        line = raw.strip()
        if not line:
            continue
        if line.startswith("#"):
            tag = line.split(":", 1)[0]
            if tag == "#EXT-X-ENDLIST":
                ended = True
            elif tag == "#EXT-X-KEY":
                if _hls_attrs(line).get("METHOD", "NONE") != "NONE":
                    return None                    # AES-128 / SAMPLE-AES: ffmpeg decrypts
            elif tag == "#EXT-X-MAP":
                a = _hls_attrs(line)
                if init is not None or "BYTERANGE" in a or "URI" not in a:
                    return None
                init = urljoin(base, a["URI"])
            elif tag in ("#EXT-X-BYTERANGE", "#EXT-X-DISCONTINUITY", "#EXT-X-GAP",
                         "#EXT-X-I-FRAMES-ONLY", "#EXT-X-DEFINE"):
                return None                        # timestamp resets / sub-ranges: ffmpeg's job
            continue
        segs.append(urljoin(base, line))
        if len(segs) > MAX_SEGMENTS:
            return None
    if not ended or not segs:
        return None
    if init:
        fmt: str | None = "mp4"
    elif urlparse(segs[0]).path.lower().endswith(".ts"):
        fmt = "mpegts"
    else:
        fmt = None
    return init, fmt, segs


async def _plan_hls(session: aiohttp.ClientSession, text: str, base: str) -> StaticPlan | None:
    if "#EXT-X-STREAM-INF" not in text:             # a media playlist was served directly
        media = _hls_parse_media(text, base)
        if media is None:
            return None
        init, fmt, segs = media
        return StaticPlan("hls", [TrackPlan("muxed", fmt, init, segs, "media")])

    variants: list[tuple[int, str, str | None]] = []
    audio: dict[str, list[dict[str, str]]] = {}
    pending: dict[str, str] | None = None
    for raw in text.splitlines():
        line = raw.strip()
        if line.startswith("#EXT-X-STREAM-INF:"):
            pending = _hls_attrs(line)
        elif line.startswith("#EXT-X-MEDIA:"):
            a = _hls_attrs(line)
            if a.get("TYPE") == "AUDIO":
                audio.setdefault(a.get("GROUP-ID", ""), []).append(a)
        elif pending is not None and line and not line.startswith("#"):
            try:
                bw = int(pending.get("BANDWIDTH", "0") or 0)
            except ValueError:
                bw = 0
            variants.append((bw, urljoin(base, line), pending.get("AUDIO")))
            pending = None
    if not variants:
        return None
    _, v_url, group = max(variants, key=lambda v: v[0])      # first of equals, like _plan_streams
    renditions = audio.get(group, []) if group else []
    with_uri = [a for a in renditions if a.get("URI")]
    if with_uri and len(with_uri) != len(renditions):
        return None                                           # half muxed, half separate: ffmpeg's job
    with_uri.sort(key=lambda a: a.get("DEFAULT") != "YES")   # stable: default language first

    urls = [v_url] + [urljoin(base, a["URI"]) for a in with_uri]
    texts = await asyncio.gather(*(_fetch_text(session, u) for u in urls))
    tracks: list[TrackPlan] = []
    for i, (u, t) in enumerate(zip(urls, texts)):
        if t is None:
            return None
        media = _hls_parse_media(t, u)
        if media is None:
            return None
        init, fmt, segs = media
        if i == 0:
            tracks.append(TrackPlan("video" if with_uri else "muxed", fmt, init, segs, "video"))
        else:
            tracks.append(TrackPlan("audio", fmt, init, segs, with_uri[i - 1].get("NAME", "audio")))
    return StaticPlan("hls", tracks)


# -- DASH -----------------------------------------------------------------------

_ISO_DURATION_RE = re.compile(
    r"^P(?:(\d+)Y)?(?:(\d+)M)?(?:(\d+)D)?(?:T(?:(\d+)H)?(?:(\d+)M)?(?:(\d+(?:\.\d+)?)S)?)?$")
_DASH_TEMPLATE_RE = re.compile(r"\$(RepresentationID|Number|Bandwidth|Time)(?:%0(\d+)d)?\$|\$\$")


def _iso_seconds(value: str | None) -> float | None:
    m = _ISO_DURATION_RE.match((value or "").strip())
    if not m or m.group(1) or m.group(2):                    # years/months: not a fixed length
        return None
    d, h, mi, sec = (float(g) if g else 0.0 for g in m.groups()[2:])
    return d * 86400 + h * 3600 + mi * 60 + sec


def _dash_expand(template: str, rep_id: str, bandwidth: int, number: int | None, time_: int | None) -> str:
    def sub(m: re.Match[str]) -> str:
        if m.group(0) == "$$":
            return "$"
        name, width = m.group(1), m.group(2)
        value = {"RepresentationID": rep_id, "Bandwidth": bandwidth,
                 "Number": number, "Time": time_}[name]
        text = str(value)
        return text.zfill(int(width)) if width and name != "RepresentationID" else text
    return _DASH_TEMPLATE_RE.sub(sub, template)


def _xml_local(tag: str) -> str:
    return tag.rsplit("}", 1)[-1]


def _xml_children(el: ET.Element, name: str) -> list[ET.Element]:
    return [c for c in el if _xml_local(c.tag) == name]


def _xml_child(el: ET.Element, name: str) -> ET.Element | None:
    kids = _xml_children(el, name)
    return kids[0] if kids else None


def _dash_base(base: str, *elements: ET.Element) -> str:
    for el in elements:
        bu = _xml_child(el, "BaseURL")
        if bu is not None and (bu.text or "").strip():
            base = urljoin(base, bu.text.strip())
    return base


def _dash_rep_segments(period: ET.Element, aset: ET.Element, rep: ET.Element, rep_base: str,
                       duration_s: float | None) -> tuple[str | None, list[str]] | None:
    """(init, media URLs) of one Representation; None = not plain enough."""
    rep_id = rep.get("id", "")
    bandwidth = int(rep.get("bandwidth", "0") or 0)

    templates = [t for t in (_xml_child(period, "SegmentTemplate"),
                             _xml_child(aset, "SegmentTemplate"),
                             _xml_child(rep, "SegmentTemplate")) if t is not None]
    if templates:
        attrs: dict[str, str] = {}
        timeline: ET.Element | None = None
        for t in templates:                        # inner levels override outer ones
            attrs.update(t.attrib)
            found = _xml_child(t, "SegmentTimeline")
            if found is not None:
                timeline = found
        media = attrs.get("media")
        if not media:
            return None
        timescale = int(attrs.get("timescale", "1") or 1)
        start_number = int(attrs.get("startNumber", "1") or 1)
        entries: list[tuple[int, int]] = []       # (number, time)
        if timeline is not None:
            number, t_cursor = start_number, 0
            for s_el in _xml_children(timeline, "S"):
                if s_el.get("t") is not None:
                    t_cursor = int(s_el.get("t"))
                d = int(s_el.get("d"))
                r = int(s_el.get("r", "0"))
                if r < 0 or d <= 0 or len(entries) + r + 1 > MAX_SEGMENTS:
                    return None                    # open-ended repeat: needs the period length
                for _ in range(r + 1):
                    entries.append((number, t_cursor))
                    number += 1
                    t_cursor += d
        else:
            seg_len = float(attrs.get("duration", "0") or 0)
            if seg_len <= 0 or duration_s is None:
                return None
            count = -(-int(duration_s * timescale) // int(seg_len))   # ceil
            if count <= 0 or count > MAX_SEGMENTS:
                return None
            entries = [(start_number + i, int(round(i * seg_len))) for i in range(count)]
        init_t = attrs.get("initialization")
        init = urljoin(rep_base, _dash_expand(init_t, rep_id, bandwidth, None, None)) if init_t else None
        return init, [urljoin(rep_base, _dash_expand(media, rep_id, bandwidth, n, t)) for n, t in entries]

    for holder in (rep, aset, period):
        sl = _xml_child(holder, "SegmentList")
        if sl is None:
            continue
        init_el = _xml_child(sl, "Initialization")
        if init_el is not None and (init_el.get("range") or not init_el.get("sourceURL")):
            return None
        urls = []
        for su in _xml_children(sl, "SegmentURL"):
            if su.get("mediaRange") or not su.get("media"):
                return None
            urls.append(urljoin(rep_base, su.get("media")))
        if not urls or len(urls) > MAX_SEGMENTS:
            return None
        return (urljoin(rep_base, init_el.get("sourceURL")) if init_el is not None else None), urls
    return None                                    # SegmentBase / single file: nothing to parallelise


def _plan_dash(text: str, base: str) -> StaticPlan | None:
    try:
        root = ET.fromstring(text.lstrip("\ufeff").strip())
    except ET.ParseError:
        return None
    if _xml_local(root.tag) != "MPD" or root.get("type", "static") != "static":
        return None
    periods = _xml_children(root, "Period")
    if len(periods) != 1:
        return None                                # new init segments mid-stream: ffmpeg's job
    period = periods[0]
    duration_s = _iso_seconds(period.get("duration")) or _iso_seconds(root.get("mediaPresentationDuration"))
    mpd_base = _dash_base(base, root)
    period_base = _dash_base(mpd_base, period)

    best_video: tuple[int, ET.Element, ET.Element] | None = None
    audio_pick: list[tuple[ET.Element, ET.Element]] = []
    for aset in _xml_children(period, "AdaptationSet"):
        reps = _xml_children(aset, "Representation")
        if not reps:
            continue
        mime = aset.get("mimeType") or reps[0].get("mimeType") or ""
        ctype = aset.get("contentType") or mime.split("/")[0]
        bw = lambda r: int(r.get("bandwidth", "0") or 0)
        top = max(reps, key=bw)
        if ctype == "video":
            if best_video is None or bw(top) > best_video[0]:
                best_video = (bw(top), aset, top)
        elif ctype == "audio":
            audio_pick.append((aset, top))

    tracks: list[TrackPlan] = []
    picks: list[tuple[str, ET.Element, ET.Element]] = []
    if best_video is not None:
        picks.append(("video", best_video[1], best_video[2]))
    picks += [("audio", a, r) for a, r in audio_pick]
    for kind, aset, rep in picks:
        segs = _dash_rep_segments(period, aset, rep, _dash_base(period_base, aset, rep), duration_s)
        if segs is None:
            return None
        init, urls = segs
        mime = rep.get("mimeType") or aset.get("mimeType") or ""
        tracks.append(TrackPlan(kind, "mpegts" if "mp2t" in mime else "mp4", init, urls,
                                aset.get("lang") or kind))
    return StaticPlan("dash", tracks) if tracks else None


# -- planning entry point ---------------------------------------------------------

async def _vet_plan_hosts(plan: StaticPlan, manifest_url: str) -> str | None:
    """The proxy itself now requests every segment URL the manifest names, so
    apply the same SSRF policy ffmpeg's targets get: the manifest's own origin
    (the backend / the redirect target that was already vetted) is fine, any
    other origin must pass _validate_url."""
    seen = {_origin(manifest_url)}
    for track in plan.tracks:
        for url in track.urls():
            o = _origin(url)
            if o in seen:
                continue
            if o[0] not in ("http", "https"):
                return f"segment scheme {o[0]!r} not allowed"
            ok, reason = await _validate_url(url)
            if not ok:
                return reason
            seen.add(o)
    return None


async def _plan_static(session: aiohttp.ClientSession, res: Resolved) -> StaticPlan | None:
    """The fast-path plan for a resolved manifest, or None = let ffmpeg fetch."""
    if PARALLEL_SEGMENTS <= 0 or FFMPEG_EXTRA_IN or res.text is None:
        return None            # FFMPEG_EXTRA_IN (e.g. -headers) can't be replayed on our own requests
    try:
        if res.kind == "hls":
            plan = await _plan_hls(session, res.text, res.base_url)
        elif res.kind == "dash":
            plan = _plan_dash(res.text, res.base_url)
        else:
            return None
        if plan is None:
            logger.info("manifest is not a plain static one; ffmpeg fetches the segments")
            return None
        reason = await _vet_plan_hosts(plan, res.base_url)
        if reason:
            logger.warning(f"static fast path off ({reason}); ffmpeg fetches the segments")
            return None
        return plan
    except Exception as e:                         # a planner bug must never break playback
        logger.warning(f"static manifest planning failed ({e!r}); ffmpeg fetches the segments")
        return None


# -- fetching + feeding -----------------------------------------------------------

class SegmentError(Exception):
    pass


_RETRY_STATUS = frozenset({408, 425, 429, 500, 502, 503, 504})


async def _fetch_segment(session: aiohttp.ClientSession, url: str) -> bytes:
    delay = 0.5
    last = "no attempt"
    headers = {"User-Agent": USER_AGENT} if USER_AGENT else None
    for attempt in range(SEGMENT_RETRIES + 1):
        try:
            async with session.get(url, headers=headers,
                                   timeout=aiohttp.ClientTimeout(total=SEGMENT_TIMEOUT)) as r:
                if r.status == 200:
                    buf = bytearray()
                    async for chunk in r.content.iter_chunked(256 * 1024):
                        buf += chunk
                        if len(buf) > SEGMENT_MAX_BYTES:
                            raise SegmentError(f"segment larger than {SEGMENT_MAX_BYTES} bytes")
                    return bytes(buf)
                last = f"HTTP {r.status}"
                if r.status not in _RETRY_STATUS:
                    break
        except SegmentError:
            raise
        except (aiohttp.ClientError, asyncio.TimeoutError) as e:
            last = type(e).__name__
        if attempt < SEGMENT_RETRIES:
            await asyncio.sleep(delay)
            delay *= 2
    raise SegmentError(f"{last} for {loggable_url(url)}")


async def _open_write_pipe(wfd: int) -> asyncio.StreamWriter:
    """Async writer (with drain() backpressure) for the parent's end of a pipe."""
    loop = asyncio.get_running_loop()
    protocol = asyncio.streams.FlowControlMixin(loop=loop)
    transport, _ = await loop.connect_write_pipe(lambda: protocol, os.fdopen(wfd, "wb", buffering=0))
    return asyncio.StreamWriter(transport, protocol, None, loop)


async def _feed_track(session: aiohttp.ClientSession, track: TrackPlan,
                      writer: asyncio.StreamWriter, info: StreamInfo) -> None:
    """Download track.urls() with up to PARALLEL_SEGMENTS requests in flight
    and write the bodies into the pipe strictly in order."""
    urls = iter(track.urls())
    pending: deque[asyncio.Task] = deque()

    def top_up() -> None:
        while len(pending) < PARALLEL_SEGMENTS:
            url = next(urls, None)
            if url is None:
                return
            pending.append(asyncio.create_task(_fetch_segment(session, url)))

    try:
        top_up()
        while pending:
            data = await pending.popleft()
            top_up()                       # refill BEFORE the (maybe slow) write: the window stays full
            writer.write(data)
            await writer.drain()           # blocks while ffmpeg / the client are slower
    except (BrokenPipeError, ConnectionResetError):
        pass                               # ffmpeg is gone; the handler sees that on its own
    except asyncio.CancelledError:
        raise
    except SegmentError as e:
        logger.error(f"on-demand {info.stream_id}: {track.label}: {e}")
        info.fail_feed(str(e))
    except Exception as e:
        logger.error(f"on-demand {info.stream_id}: {track.label} feeder crashed: {e!r}")
        info.fail_feed(repr(e))
    finally:
        for t in pending:
            t.cancel()
        for t in pending:
            with contextlib.suppress(Exception, asyncio.CancelledError):
                await t
        with contextlib.suppress(Exception):
            writer.close()                 # EOF for ffmpeg: this track is complete (or abandoned)


# ===== HTTP handlers =====

def _stream_headers(mode: str, kind: str) -> dict[str, str]:
    return {
        "Content-Type": "video/mp2t",
        "Cache-Control": "no-cache",
        "X-Accel-Buffering": "no",     # disable nginx proxy buffering
        "Accept-Ranges": "none",       # TS has no seeking; say so instead of ignoring Range silently
        "X-TS-Mode": mode,
        "X-TS-Source": kind,
    }


async def _await_stderr(info: StreamInfo) -> None:
    # returncode is set on reap; give the stderr drain task a moment to
    # capture the final lines before we read them. Also suppress
    # CancelledError: if _terminate_stream already cancelled the drain task
    # (cleanup loop, shutdown, or a concurrent request pruning this dead
    # stream), the shield propagates that cancellation here and would tear
    # down the handler instead of returning the error below. Swallowing a
    # genuine cancellation of *this* task is safe too: every exit after this
    # point returns immediately (no awaits left).
    if info.stderr_task is not None:
        with contextlib.suppress(asyncio.TimeoutError, asyncio.CancelledError):
            await asyncio.wait_for(asyncio.shield(info.stderr_task), timeout=0.25)


def _startup_failure(info: StreamInfo) -> web.Response:
    tail = list(info.stderr_tail)[-10:]
    rc = info.process.returncode
    logger.warning(f"stream {info.stream_id} failed before first byte (rc={rc})")
    return _json_error(
        502,
        f"source exited (rc={rc}) before producing output",
        hint=_hint_from_stderr(tail),
        details=tail or None,
    )


async def _serve_live(request: web.Request, mgr: StreamManager, key: str, name: str,
                      *, resolve=None, generic: bool = False) -> web.StreamResponse:
    """Shared-stream playback: one ffmpeg per key, fanned out to all clients."""
    source: str | None = None
    map_args: list[str] | None = None
    kind = "unknown"

    # Only a stream that has to be (re)spawned needs the backend lookup; a
    # running one is simply joined.
    existing = mgr.streams.get(key)
    if resolve is not None and (existing is None or existing.process.returncode is not None):
        try:
            res = await resolve()
        except BackendError as e:
            return _backend_error_response(e)
        source, map_args, kind = res.source, res.map_args, res.kind

    info, result = await mgr.get_or_create(
        key, name, source=source, map_args=map_args, kind=kind, generic=generic,
    )
    if info is None:
        retry = {"Retry-After": str(RETRY_AFTER)}
        if result is SpawnResult.MAX_STREAMS:
            return _json_error(503, result.value, headers=retry)
        if result is SpawnResult.SPAWN_TIMEOUT:
            return _json_error(504, result.value, headers=retry)
        return _json_error(502, result.value, hint="is ffmpeg installed and on PATH?")

    if info.client_count >= MAX_CLIENTS_PER_STREAM:
        return _json_error(
            503, f"too many clients on this stream (max {MAX_CLIENTS_PER_STREAM})",
            headers={"Retry-After": str(RETRY_AFTER)},
        )

    # Subscribe BEFORE waiting for the first byte. A client that attaches
    # only after the first chunk has arrived never sees the start of the
    # stream (the first keyframe is spread over several chunks), so it would
    # wait a whole extra GOP for the next one.
    client_buf = info.add_subscriber()

    # Pre-flight (fresh stream): wait for the first byte BEFORE committing
    # headers, so a dead source still yields a real 502 with diagnostics
    # instead of 200 + empty body.
    if info.bytes_read == 0:
        deadline = time.monotonic() + PREFLIGHT_TIMEOUT
        while time.monotonic() < deadline:
            if info.bytes_read > 0 or info.process.returncode is not None:
                break
            await asyncio.sleep(0.05)
        if info.bytes_read == 0 and info.process.returncode is not None:
            info.remove_subscriber(client_buf)
            await _await_stderr(info)
            return _startup_failure(info)

    # Race guard. The stream may have died or been torn down between
    # get_or_create() and now (cleanup loop, shutdown, watchdog, or a
    # concurrent request pruning a dead source): a buffer added that late
    # can miss the pump's sentinel fan-out entirely and end the response as
    # 200 with zero bytes delivered. A reaped source or a done pump can never
    # feed a fresh subscriber anyway (see the loop's terminal condition
    # below), and an info that is no longer the one registered under this
    # key has been terminated (or replaced by a respawn) — so unsubscribe
    # and fail fast instead.
    if (
        info.process.returncode is not None
        or (info.pump_task is not None and info.pump_task.done())
        or mgr.streams.get(key) is not info
    ):
        info.remove_subscriber(client_buf)
        return _json_error(503, "stream terminated", hint="retry; a fresh source will be started",
                           headers={"Retry-After": str(RETRY_AFTER)})

    response = web.StreamResponse(headers=_stream_headers("live", info.source_kind))
    response.headers["X-TS-Shared"] = "1"

    bytes_delivered = 0
    chunk_count = 0
    stall_count = 0
    last_log = time.monotonic()

    try:
        await response.prepare(request)

        while True:
            # No more data can ever arrive once the source is reaped or the
            # pump has exited; drain what's queued, then close.
            pump_done = info.pump_task is not None and info.pump_task.done()
            if (info.process.returncode is not None or pump_done) and client_buf.fully_drained():
                break

            # Longer timeout during startup, tighter once flowing.
            timeout = 10.0 if chunk_count < 10 else 5.0
            try:
                chunk = await client_buf.get(timeout)
            except asyncio.TimeoutError:
                stall_count += 1
                mgr.note_stall(info)
                max_stalls = 5 if chunk_count < 10 else 3
                if stall_count > max_stalls:
                    logger.error(
                        f"client on {info.stream_id} stalled out "
                        f"({stall_count} consecutive timeouts)"
                    )
                    break
                continue

            if chunk is None:  # sentinel: stream ended or was terminated
                break

            stall_count = 0
            chunk_count += 1

            try:
                await response.write(chunk)
            except (ConnectionResetError, BrokenPipeError):
                break
            except Exception as e:
                logger.warning(f"write failed on {info.stream_id}: {e}")
                break

            bytes_delivered += len(chunk)

            now = time.monotonic()
            if now - last_log > 30:
                skipped = ""
                if client_buf.dropped_chunks:
                    skipped = (f", {client_buf.dropped_chunks} chunks skipped "
                               f"(live edge)")
                logger.info(
                    f"stream {info.stream_id}: client at {chunk_count} chunks, "
                    f"{info.client_count} viewers, "
                    f"{bytes_delivered / 1048576:.1f} MB delivered{skipped}"
                )
                last_log = now

    except asyncio.CancelledError:
        raise  # client went away; let aiohttp handle it
    except Exception as e:
        logger.error(f"handler error on {info.stream_id}: {e}")
    finally:
        info.remove_subscriber(client_buf)
        if info.client_count == 0:
            logger.info(
                f"last client left {info.stream_id}; "
                f"idle eviction in {IDLE_TIMEOUT}s"
            )

    return response


async def _serve_ondemand(request: web.Request, mgr: StreamManager, key: str, name: str,
                          resolve, start: float | None, *, allow_fast: bool = True) -> web.StreamResponse:
    """VOD / recording / catchup playback: one ffmpeg per request, wired
    straight to the client. No fan-out buffer and therefore no live-edge
    skipping: when the client is slower than ffmpeg the socket write blocks,
    stdout fills up and ffmpeg waits. Nothing is ever dropped.

    A static manifest is downloaded by this proxy with several segments in
    flight (see "Static manifests"); if that cannot produce a first byte, the
    request is retried once with ffmpeg doing the fetching, as before."""
    if not mgr.acquire_ondemand():
        return _json_error(
            503, f"too many on-demand streams (max {MAX_ONDEMAND})",
            headers={"Retry-After": str(RETRY_AFTER)},
        )
    info: StreamInfo | None = None
    try:
        try:
            res = await resolve()
        except BackendError as e:
            return _backend_error_response(e)

        plan = None
        # -ss on a pipe can only discard data; a start offset keeps ffmpeg's own input seek.
        if allow_fast and start is None:
            plan = await _plan_static(request.app["client_session"], res)

        first = b""
        for attempt in ([plan] if plan is not None else []) + [None]:
            fast = attempt is not None
            if fast:
                info = await mgr.start_ondemand_fast(key, name, attempt, request.app["client_session"])
            else:
                info = await mgr.start_ondemand(key, name, res.source, res.map_args, res.kind, start)
            if info is None:
                if fast:
                    continue
                return _json_error(502, "failed to start ffmpeg", hint="is ffmpeg installed and on PATH?")

            # First byte BEFORE headers, so failures are real HTTP errors.
            try:
                first = await asyncio.wait_for(
                    info.process.stdout.read(STREAM_READ_SIZE), timeout=STARTUP_TIMEOUT
                )
            except asyncio.TimeoutError:
                if fast:
                    logger.warning(f"on-demand {info.stream_id}: no data from the parallel fetch within "
                                   f"{STARTUP_TIMEOUT}s; retrying with ffmpeg fetching")
                    await mgr.finish_ondemand(info)
                    info = None
                    continue
                return _json_error(
                    504, f"source produced no data within {STARTUP_TIMEOUT}s",
                    hint=_hint_from_stderr(info.stderr_tail),
                    details=list(info.stderr_tail)[-10:] or None,
                )
            if not first:
                await _await_stderr(info)
                if fast:
                    logger.warning(
                        f"on-demand {info.stream_id}: parallel fetch failed before the first byte "
                        f"({info.feed_error or 'ffmpeg exited'}); retrying with ffmpeg fetching")
                    await mgr.finish_ondemand(info)
                    info = None
                    continue
                return _startup_failure(info)
            break
        if info is None:
            return _json_error(502, "failed to start ffmpeg", hint="is ffmpeg installed and on PATH?")

        response = web.StreamResponse(headers=_stream_headers("ondemand", res.kind))
        response.headers["X-TS-Shared"] = "0"
        response.headers["X-TS-Fetch"] = info.fetch_mode
        await response.prepare(request)

        carry = bytearray()
        chunk = first
        try:
            while True:
                info.bytes_read += len(chunk)
                carry += chunk
                n = len(carry) - len(carry) % TS_PACKET_SIZE
                if n:
                    payload = bytes(carry[:n])
                    del carry[:n]
                    await asyncio.wait_for(response.write(payload), timeout=ONDEMAND_WRITE_TIMEOUT)
                transport = request.transport
                if transport is None or transport.is_closing():
                    break
                try:
                    chunk = await asyncio.wait_for(
                        info.process.stdout.read(STREAM_READ_SIZE), timeout=STALL_TIMEOUT
                    )
                except asyncio.TimeoutError:
                    logger.error(
                        f"on-demand {info.stream_id}: source silent for {STALL_TIMEOUT}s — closing"
                    )
                    break
                if not chunk:
                    break
            if info.feed_error:
                # A segment could not be fetched mid-download. A clean end of
                # the chunked body would make the file look complete; hang up
                # so the client sees an incomplete download instead.
                logger.error(f"on-demand {info.stream_id}: aborting the response ({info.feed_error})")
                if request.transport is not None:
                    request.transport.abort()
                return response
            if carry:
                await response.write(bytes(carry))
            await response.write_eof()
        except (ConnectionResetError, BrokenPipeError, asyncio.TimeoutError):
            pass                                  # client left / stopped reading
        except asyncio.CancelledError:
            raise
        except Exception as e:
            logger.warning(f"on-demand {info.stream_id} write failed: {e}")
        return response
    finally:
        # Hanging up on the client (transport.abort(), or the client leaving)
        # makes aiohttp cancel this handler; the cancellation is then delivered
        # at the first await below. Shield the cleanup and release the slot
        # whatever happens, or each such request would leak an on-demand slot.
        try:
            if info is not None:
                await asyncio.shield(mgr.finish_ondemand(info))
        finally:
            mgr.release_ondemand()


async def ts_handler(request: web.Request) -> web.StreamResponse:
    """GET /api/providers/<provider>/<kind>/<id>/stream/index.ts"""
    mgr: StreamManager = request.app["stream_manager"]
    if not BACKEND_URL:
        return _json_error(
            503, "MPEGTS_BACKEND_URL is not configured",
            hint="Set MPEGTS_BACKEND_URL to the Ultimate Backend base URL as reachable from this proxy.",
        )

    kind = request.match_info.get("kind", "vod")
    provider = request.match_info["provider"]
    content_id = request.match_info["content_id"]
    if kind == "vod":
        # The backend's VOD ids are hierarchical ("clip_1417600/stream");
        # only the first segment identifies the content.
        content_id = content_id.split("/")[0]
    if not provider or not content_id or len(provider) > 128 or len(content_id) > 256:
        return _json_error(400, "invalid provider or content id")

    ondemand = _is_ondemand(kind, request.query)
    mode = "ondemand" if ondemand else "live"

    # HEAD (players and probes love it) must not spawn ffmpeg.
    if request.method == "HEAD":
        return web.Response(headers=_stream_headers(mode, "unknown"))

    try:
        start = _parse_start(request.query.get("start"))
    except ValueError:
        return _json_error(400, "invalid start (seconds, >= 0)")
    if start is not None and not ondemand:
        return _json_error(400, "start is only supported for on-demand content (vod, recordings, catchup)")

    name = (request.query.get("name") or f"{provider}/{content_id}")[:64]
    backend_url = _backend_stream_url(kind, provider, content_id, request.query)
    resolve = functools.partial(_resolve_source, request.app["client_session"], backend_url)

    logger.info(f"{request.remote} -> {kind}/{provider}/{content_id} ({mode})")
    if ondemand:
        return await _serve_ondemand(request, mgr, backend_url, name, resolve, start,
                                     allow_fast=request.query.get("fast") != "0")
    return await _serve_live(request, mgr, backend_url, name, resolve=resolve)


async def generic_handler(request: web.Request) -> web.StreamResponse:
    """GET /stream?url=...&name=... — the open "convert any URL" endpoint.
    Off by default (MPEGTS_ALLOW_GENERIC_URL): it is the SSRF / pipe://
    surface, and backend mode does not need it."""
    if not ALLOW_GENERIC_URL:
        return _json_error(
            404, "the generic /stream?url=... endpoint is disabled",
            hint=("Use /api/providers/<provider>/<channels|events|vod|recordings>/<id>/stream/index.ts, "
                  "or set MPEGTS_ALLOW_GENERIC_URL=1."),
        )
    mgr: StreamManager = request.app["stream_manager"]
    if request.method == "HEAD":
        return web.Response(headers=_stream_headers("live", "unknown"))

    raw_url = request.query.get("url")
    name = (request.query.get("name") or "Stream")[:64]
    if not raw_url:
        return _json_error(400, "missing url")

    ok, reason = await _validate_url(raw_url)
    if not ok:
        logger.warning(f"rejected url ({reason}): {loggable_url(raw_url)}")
        return _json_error(400, reason)

    return await _serve_live(request, mgr, raw_url, name, generic=True)


async def health_handler(request: web.Request) -> web.Response:
    mgr: StreamManager = request.app["stream_manager"]
    return web.json_response(mgr.health_snapshot(detailed=_bearer_valid(request)))


async def metrics_handler(request: web.Request) -> web.Response:
    mgr: StreamManager = request.app["stream_manager"]
    return web.Response(
        body=mgr.metrics_snapshot().encode(),
        headers={"Content-Type": "text/plain; version=0.0.4; charset=utf-8"},
    )


# ===== App factory =====

async def _client_session_ctx(app: web.Application):
    timeout = aiohttp.ClientTimeout(total=BACKEND_TIMEOUT)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        app["client_session"] = session
        yield


async def _on_shutdown(app: web.Application) -> None:
    await app["stream_manager"].shutdown()


def create_app() -> web.Application:
    manager = StreamManager()
    app = web.Application(middlewares=[auth_middleware])
    app["stream_manager"] = manager
    app.cleanup_ctx.append(_client_session_ctx)
    app.cleanup_ctx.append(manager.lifecycle)  # startup/shutdown guaranteed paired
    app.on_shutdown.append(_on_shutdown)       # see StreamManager.shutdown
    # Mirrors of the backend's own stream routes (same path, .ts extension).
    app.router.add_get(
        "/api/providers/{provider}/{kind:channels|events|recordings}/{content_id}/stream/index.ts",
        ts_handler,
    )
    app.router.add_get(
        "/api/providers/{provider}/vod/{content_id:.+}/stream/index.ts",
        ts_handler,
    )
    app.router.add_get("/stream", generic_handler)
    app.router.add_get("/health", health_handler)
    app.router.add_get("/metrics", metrics_handler)
    return app


if __name__ == "__main__":
    host = os.environ.get("MPEGTS_HOST", "0.0.0.0")
    port = _env_int("MPEGTS_PORT", 8000)

    if not BACKEND_URL:
        logger.warning(
            "MPEGTS_BACKEND_URL is not set — the /api/providers/.../index.ts routes "
            "will answer 503. Set it to the Ultimate Backend base URL."
        )
    if not AUTH_TOKEN and not AUTH_DISABLED:
        logger.warning(
            "MPEGTS_AUTH_TOKEN is not set — every stream request will be refused. "
            "Set MPEGTS_AUTH_TOKEN (recommended) or MPEGTS_AUTH_DISABLED=1 for "
            "LAN-only development."
        )
    if AUTH_DISABLED:
        logger.warning(
            "MPEGTS_AUTH_DISABLED=1 — authentication is OFF; only safe on a trusted LAN"
        )
    if ALLOW_GENERIC_URL:
        logger.warning(
            "MPEGTS_ALLOW_GENERIC_URL=1 — /stream?url=... converts ANY url the caller names"
        )
    if ALLOW_PIPE:
        logger.warning(
            "MPEGTS_ALLOW_PIPE=1 — pipe:// executes arbitrary commands (RCE surface)"
        )
    if ALLOW_PRIVATE_URLS:
        logger.warning(
            "MPEGTS_ALLOW_PRIVATE_URLS=1 — private targets allowed for ALL schemes "
            "(includes http/https SSRF risk)"
        )
    logger.info(
        "note: URL validation is defense-in-depth; also enforce egress filtering "
        "(RFC1918, link-local, metadata IPs) at the network layer"
    )

    # access_log=None: aiohttp's access log prints whole request lines
    # (query strings included). shutdown_timeout: streaming handlers are also
    # ended by StreamManager.shutdown, this is only the backstop.
    web.run_app(create_app(), host=host, port=port, access_log=None, shutdown_timeout=10)