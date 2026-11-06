"""
MPEG-TS Stream Proxy

Convert DASH/HLS/other sources to MPEG-TS on the fly for direct streaming.

Security model:
  - pipe:// is DISABLED unless MPEGTS_ALLOW_PIPE=1. It executes arbitrary
    commands, so treat it as a remote-code-execution surface (it remains
    behind bearer auth either way).
  - private/loopback/link-local/metadata IPs are rejected for http/https
    unless MPEGTS_ALLOW_PRIVATE_URLS=1. rtsp/rtp/udp/rtmp/mms may target
    LAN hosts legitimately (cameras, encoders) and are exempt from the
    private-IP check.
  - every /stream request requires a bearer token (MPEGTS_AUTH_TOKEN)
    unless MPEGTS_AUTH_DISABLED=1 (LAN development only).
  - URL validation is defense-in-depth, NOT a security boundary: ffmpeg
    re-resolves DNS on its own and follows HTTP redirects, so DNS
    rebinding and redirect-based SSRF cannot be fully prevented at the
    application layer. Internet-facing deployments MUST also enforce
    egress filtering at the network layer (block RFC1918, 169.254.0.0/16
    and cloud metadata endpoints at the firewall/NAT).
  - URLs are never rewritten (no DNS pinning): pinning breaks HTTPS
    (SNI/certificate mismatch) and vhosted HTTP, and keying streams by a
    resolved IP breaks dedup on round-robin DNS. Streams are keyed by the
    original URL; the hash is display-only.

Requires Python >= 3.10.
"""
from __future__ import annotations

import asyncio
import contextlib
import enum
import fcntl
import hashlib
import hmac
import ipaddress
import logging
import os
import re
import shlex
import signal
import socket
import subprocess
import time
from collections import deque
from dataclasses import dataclass, field
from urllib.parse import urlparse

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
MAX_STREAMS = _env_int("MPEGTS_MAX_STREAMS", 15)
MAX_STREAM_AGE = _env_int("MPEGTS_MAX_STREAM_AGE", 3600)      # s; only evicts streams with no viewers
IDLE_TIMEOUT = _env_int("MPEGTS_IDLE_TIMEOUT", 300)           # s
CLEANUP_INTERVAL = _env_int("MPEGTS_CLEANUP_INTERVAL", 30)    # s
STREAM_READ_SIZE = _env_int("MPEGTS_READ_SIZE", 65800)        # 350 x 188-byte TS packets
CLIENT_QUEUE_MAXSIZE = _env_int("MPEGTS_CLIENT_QUEUE_MAXSIZE", 32)   # chunks/client (~2 MB)
MAX_CLIENTS_PER_STREAM = _env_int("MPEGTS_MAX_CLIENTS_PER_STREAM", 50)
SPAWN_WAIT_TIMEOUT = _env_int("MPEGTS_SPAWN_WAIT_TIMEOUT", 30)       # s; follower waits for leader
PREFLIGHT_TIMEOUT = _env_int("MPEGTS_PREFLIGHT_TIMEOUT", 5)          # s; first-byte wait pre-headers
ALLOW_PRIVATE_URLS = _env_bool("MPEGTS_ALLOW_PRIVATE_URLS", False)
ALLOW_PIPE = _env_bool("MPEGTS_ALLOW_PIPE", False)
AUTH_TOKEN = os.environ.get("MPEGTS_AUTH_TOKEN")
AUTH_DISABLED = _env_bool("MPEGTS_AUTH_DISABLED", False)
USER_AGENT = os.environ.get("MPEGTS_USER_AGENT")              # optional; via ffmpeg -user_agent

# rtsp/rtp/udp/rtmp/mms legitimately target LAN hosts (cameras, encoders).
SCHEMES_ALLOW_PRIVATE = {"rtsp", "rtp", "udp", "rtmp", "mms"}
ALLOWED_SCHEMES = {"http", "https"} | SCHEMES_ALLOW_PRIVATE

logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("mpegts_proxy")


# ===== Auth =====

OPEN_PATHS = {"/health", "/metrics"}   # unauthenticated, for orchestration


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


def _force_put(q: asyncio.Queue, item) -> None:
    """put_nowait with drop-oldest semantics; used for chunks and sentinels.

    Drop-oldest suits live TV: a client that falls behind skips ahead to the
    newest chunk instead of stalling behind a growing backlog. The None
    sentinel rides the same path, so on a full queue it evicts the oldest
    queued chunk (possibly the stream's last) to guarantee a clean close —
    losing a final chunk is invisible for live TV.
    """
    try:
        q.put_nowait(item)
    except asyncio.QueueFull:
        with contextlib.suppress(asyncio.QueueEmpty):
            q.get_nowait()
        with contextlib.suppress(asyncio.QueueFull):
            q.put_nowait(item)


@dataclass
class StreamInfo:
    """Per-stream state: process, fan-out queues, diagnostics."""

    stream_id: str
    url: str                                   # canonical key (original URL / pipe command)
    display_name: str
    process: asyncio.subprocess.Process
    created: float = field(default_factory=time.time)
    last_client_disconnect: float | None = None
    subscribers: set[asyncio.Queue] = field(default_factory=set)
    pump_task: asyncio.Task | None = None
    stderr_task: asyncio.Task | None = None
    stderr_tail: deque[str] = field(default_factory=lambda: deque(maxlen=50))
    bytes_read: int = 0                        # bytes read from the source stdout (pre fan-out)
    stall_events: int = 0                      # informational

    @property
    def client_count(self) -> int:
        return len(self.subscribers)

    def add_subscriber(self) -> asyncio.Queue:
        q: asyncio.Queue = asyncio.Queue(maxsize=CLIENT_QUEUE_MAXSIZE)
        self.subscribers.add(q)
        self.last_client_disconnect = None
        return q

    def remove_subscriber(self, q: asyncio.Queue) -> None:
        self.subscribers.discard(q)
        if not self.subscribers:
            self.last_client_disconnect = time.time()


def _ffmpeg_cmd(url: str, name: str) -> list[str]:
    cmd: list[str] = [
        "ffmpeg",
        "-nostdin",                # never touch our stdin
        "-loglevel", "error",
    ]
    if USER_AGENT:
        cmd += ["-user_agent", USER_AGENT]
    cmd += [
        # 'file' is deliberately absent: the HLS/DASH demuxers check segment
        # URLs from remote playlists against this whitelist, and allowing
        # 'file' would let a remote playlist pull local files (e.g.
        # file:///etc/passwd) and remux them to clients. 'crypto' is needed
        # for AES-128 HLS segments; 'pipe' for the pipe:1 output.
        "-protocol_whitelist", "http,https,tcp,tls,crypto,rtmp,rtsp,udp,rtp,mms,pipe",
        "-fflags", "+genpts+igndts+discardcorrupt",
        "-probesize", "5M",
        "-analyzeduration", "2M",
        "-reconnect", "1",
        "-reconnect_streamed", "1",
        "-reconnect_delay_max", "2",
        "-thread_queue_size", "512",
        "-i", url,
        "-map", "0:v:0",
        "-map", "0:a:0?",
        "-c", "copy",
        "-avoid_negative_ts", "make_zero",
        "-max_muxing_queue_size", "4096",
        "-f", "mpegts",
        "-muxdelay", "0",
        "-muxpreload", "0",
        "-mpegts_flags", "resend_headers",
        "-mpegts_copyts", "1",
        "-metadata", f"service_name={name}",
        "-flush_packets", "1",
        "pipe:1",
    ]
    return cmd


class StreamManager:
    def __init__(self, max_streams: int = MAX_STREAMS, max_stream_age: int = MAX_STREAM_AGE):
        self.streams: dict[str, StreamInfo] = {}      # canonical URL -> StreamInfo
        self.max_streams = max_streams
        self.max_stream_age = max_stream_age
        self._cleanup_task: asyncio.Task | None = None
        self._spawn_lock = asyncio.Lock()
        self._spawning: dict[str, asyncio.Future] = {}  # key -> future resolved by the leader
        # Cumulative counters (monotonic — safe for Prometheus rate()).
        self._total_bytes_read = 0
        self._total_stalls = 0
        self._total_streams_created = 0

    # ----- lifecycle (cleanup_ctx: startup and shutdown are guaranteed paired) -----

    async def lifecycle(self, app: web.Application):
        self._cleanup_task = asyncio.create_task(self._cleanup_loop())
        logger.info("StreamManager started")
        try:
            yield
        finally:
            await self.shutdown()

    async def shutdown(self) -> None:
        logger.info("StreamManager shutting down")
        if self._cleanup_task is not None and not self._cleanup_task.done():
            self._cleanup_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._cleanup_task
            self._cleanup_task = None
        for key in list(self.streams.keys()):
            await self._terminate_stream(key, reason="shutdown")
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

    async def _terminate_stream(self, key: str, reason: str = "unknown") -> None:
        info = self.streams.pop(key, None)
        if info is None:
            return
        # Wake every waiting handler. The sentinel may evict a subscriber's
        # last queued chunk on a full queue (see _force_put) — acceptable for
        # live TV; the client still gets a clean close.
        for q in list(info.subscribers):
            _force_put(q, None)
        info.subscribers.clear()

        proc = info.process
        try:
            os.killpg(os.getpgid(proc.pid), signal.SIGTERM)  # whole process group
        except (ProcessLookupError, PermissionError):
            pass
        try:
            await asyncio.wait_for(proc.wait(), timeout=2.0)
        except asyncio.TimeoutError:
            try:
                os.killpg(os.getpgid(proc.pid), signal.SIGKILL)
            except (ProcessLookupError, PermissionError):
                with contextlib.suppress(ProcessLookupError):
                    proc.kill()
            with contextlib.suppress(asyncio.TimeoutError):
                await asyncio.wait_for(proc.wait(), timeout=2.0)

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
                if any(e in msg for e in ("Error", "Invalid", "Failed", "Cannot")):
                    logger.error(f"ffmpeg[{info.stream_id}]: {msg}")
                else:
                    logger.debug(f"ffmpeg[{info.stream_id}]: {msg}")
        except asyncio.CancelledError:
            raise
        except Exception as e:
            logger.debug(f"stderr drain for {info.stream_id} ended: {e}")

    # ----- pump -----

    async def _pump(self, info: StreamInfo) -> None:
        """Single reader -> bounded fan-out. A slow client drops chunks and
        stays near the live edge instead of stalling the reader or being
        starved forever."""
        proc = info.process
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

                info.bytes_read += len(chunk)
                self._total_bytes_read += len(chunk)

                for q in list(info.subscribers):
                    _force_put(q, chunk)
        finally:
            # EOF/error/exit: wake all handlers so they close cleanly (the
            # sentinel may evict the final buffered chunk on a full queue;
            # see _force_put).
            for q in list(info.subscribers):
                _force_put(q, None)

    # ----- spawn / get -----

    async def get_or_create(
        self, url_or_command: str, name: str
    ) -> tuple[StreamInfo | None, SpawnResult]:
        key = url_or_command

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
            info = await self._spawn(url_or_command, name)
        except asyncio.CancelledError:
            raise
        except Exception as e:
            logger.error(f"spawn failed for {loggable_url(key)}: {e}")
        finally:
            if not fut.done():
                fut.set_result(info)
            # Synchronous bookkeeping: no awaits -> atomic under the event
            # loop and immune to interruption by cancellation.
            self._spawning.pop(key, None)

        if info is None:
            return None, SpawnResult.SPAWN_FAILED
        return info, SpawnResult.SPAWNED

    async def _spawn(self, url_or_command: str, name: str) -> StreamInfo | None:
        stream_id = self._stream_id_from_key(url_or_command)

        if url_or_command.startswith("pipe://"):
            cmd = shlex.split(url_or_command[7:])
        else:
            cmd = _ffmpeg_cmd(url_or_command, name)

        try:
            proc = await asyncio.create_subprocess_exec(
                *cmd,
                stdin=subprocess.DEVNULL,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                limit=2 * 1024 * 1024,
                start_new_session=True,  # own process group -> clean kill of ffmpeg + children
            )
        except FileNotFoundError:
            logger.error("ffmpeg binary not found on PATH")
            return None
        except Exception as e:
            logger.error(f"failed to spawn ffmpeg: {e}")
            return None

        # NB: there is no await between create_subprocess_exec() returning
        # and the try block below, so task cancellation cannot be delivered
        # in that gap — the process cannot leak past the BaseException guard.
        try:
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

            info = StreamInfo(
                stream_id=stream_id,
                url=url_or_command,
                display_name=name,
                process=proc,
            )
            self.streams[url_or_command] = info
            self._total_streams_created += 1
            info.pump_task = asyncio.create_task(self._pump(info))
            info.stderr_task = asyncio.create_task(self._drain_stderr(info))
            logger.info(f"stream {stream_id} created ({loggable_url(url_or_command)})")
            return info
        except BaseException:
            # Cancelled (or otherwise failed) between process creation and
            # registration — kill the group so we don't leak a process.
            with contextlib.suppress(ProcessLookupError, PermissionError):
                os.killpg(os.getpgid(proc.pid), signal.SIGKILL)
            with contextlib.suppress(Exception):
                await asyncio.wait_for(proc.wait(), timeout=1.0)
            raise

    @staticmethod
    def _stream_id_from_key(key: str) -> str:
        # Display/label only — never used for matching (the dict is keyed by URL).
        return hashlib.md5(key.encode()).hexdigest()[:12]

    def note_stall(self, info: StreamInfo) -> None:
        info.stall_events += 1
        self._total_stalls += 1

    # ----- introspection -----

    def health_snapshot(self) -> dict:
        now = time.time()
        infos = list(self.streams.values())
        alive = sum(1 for i in infos if i.process.returncode is None)
        status = "healthy"
        if infos and alive == 0:
            status = "degraded"
        return {
            "status": status,
            "streams": len(infos),
            "alive_streams": alive,
            "max_streams": self.max_streams,
            "totals": {
                "bytes_read": self._total_bytes_read,
                "stalls": self._total_stalls,
                "streams_created": self._total_streams_created,
            },
            "stream_details": [
                {
                    "id": i.stream_id,
                    "name": i.display_name,
                    "url": safe_url(i.url),
                    "age": int(now - i.created),
                    "clients": i.client_count,
                    "alive": i.process.returncode is None,
                    "bytes_read": i.bytes_read,
                    "stall_events": i.stall_events,
                    "last_stderr": list(i.stderr_tail)[-3:],
                }
                for i in infos
            ],
        }

    def metrics_snapshot(self) -> str:
        alive = sum(1 for i in self.streams.values() if i.process.returncode is None)
        clients = sum(i.client_count for i in self.streams.values())
        lines = [
            "# HELP mpegts_proxy_active_streams Tracked streams",
            "# TYPE mpegts_proxy_active_streams gauge",
            f"mpegts_proxy_active_streams {len(self.streams)}",
            "# HELP mpegts_proxy_alive_streams Streams whose source process is alive",
            "# TYPE mpegts_proxy_alive_streams gauge",
            f"mpegts_proxy_alive_streams {alive}",
            "# HELP mpegts_proxy_active_clients Connected clients across all streams",
            "# TYPE mpegts_proxy_active_clients gauge",
            f"mpegts_proxy_active_clients {clients}",
            "# HELP mpegts_proxy_max_streams Configured stream cap",
            "# TYPE mpegts_proxy_max_streams gauge",
            f"mpegts_proxy_max_streams {self.max_streams}",
            "# HELP mpegts_proxy_bytes_read_total Cumulative bytes read from source processes (pre fan-out)",
            "# TYPE mpegts_proxy_bytes_read_total counter",
            f"mpegts_proxy_bytes_read_total {self._total_bytes_read}",
            "# HELP mpegts_proxy_stream_stalls_total Cumulative per-client read stalls",
            "# TYPE mpegts_proxy_stream_stalls_total counter",
            f"mpegts_proxy_stream_stalls_total {self._total_stalls}",
            "# HELP mpegts_proxy_streams_created_total Streams ever created",
            "# TYPE mpegts_proxy_streams_created_total counter",
            f"mpegts_proxy_streams_created_total {self._total_streams_created}",
        ]
        return "\n".join(lines) + "\n"


# ===== HTTP handlers =====

async def stream_handler(request: web.Request) -> web.Response:
    mgr: StreamManager = request.app["stream_manager"]
    raw_url = request.query.get("url")
    name = (request.query.get("name") or "Stream")[:64]

    if not raw_url:
        return web.json_response({"error": "missing url"}, status=400)

    ok, reason = await _validate_url(raw_url)
    if not ok:
        logger.warning(f"rejected url ({reason}): {loggable_url(raw_url)}")
        return web.json_response({"error": reason}, status=400)

    info, result = await mgr.get_or_create(raw_url, name)
    if info is None:
        if result is SpawnResult.MAX_STREAMS:
            return web.json_response({"error": result.value}, status=503)
        if result is SpawnResult.SPAWN_TIMEOUT:
            return web.json_response({"error": result.value}, status=504)
        return web.json_response({"error": result.value}, status=502)

    # Pre-flight (first subscriber only): wait briefly for the first byte
    # BEFORE committing headers, so a dead source still yields a real 502
    # with diagnostics instead of 200 + empty body.
    if info.client_count == 0:
        deadline = time.monotonic() + PREFLIGHT_TIMEOUT
        while time.monotonic() < deadline:
            if info.bytes_read > 0 or info.process.returncode is not None:
                break
            await asyncio.sleep(0.05)
        if info.bytes_read == 0 and info.process.returncode is not None:
            # returncode is set on reap; give the stderr drain task a moment
            # to capture the final lines before we read them.
            if info.stderr_task is not None:
                # Also suppress CancelledError: if _terminate_stream already
                # cancelled the drain task (cleanup loop, shutdown, or a
                # concurrent request pruning this dead stream), the shield
                # propagates that cancellation here and would tear down the
                # handler instead of returning the 502 below. Swallowing a
                # genuine cancellation of *this* task is safe too: every
                # exit after this block returns immediately (no awaits left).
                with contextlib.suppress(asyncio.TimeoutError, asyncio.CancelledError):
                    await asyncio.wait_for(
                        asyncio.shield(info.stderr_task), timeout=0.25
                    )
            if info.stderr_tail:
                logger.warning(
                    f"stream {info.stream_id} failed before first byte "
                    f"(rc={info.process.returncode})"
                )
                return web.Response(
                    text="\n".join(info.stderr_tail),
                    status=502,
                    content_type="text/plain",
                )
            return web.json_response(
                {
                    "error": (
                        f"source exited (rc={info.process.returncode}) "
                        "before producing output"
                    )
                },
                status=502,
            )

    if info.client_count >= MAX_CLIENTS_PER_STREAM:
        return web.json_response(
            {"error": f"too many clients on this stream (max {MAX_CLIENTS_PER_STREAM})"},
            status=503,
        )

    client_q = info.add_subscriber()
    # Race guard. There is no await between add_subscriber() and this check,
    # so the pair is atomic under the event loop. The stream may have died
    # or been torn down between get_or_create() and this subscription
    # (cleanup loop, shutdown, or a concurrent request pruning a dead
    # source): a queue added that late can miss the pump's sentinel fan-out
    # entirely and end the response as 200 with zero bytes delivered. A
    # reaped source or a done pump can never feed a fresh subscriber anyway
    # (see the loop's terminal condition below), and an info that is no
    # longer the one registered under this key has been terminated (or
    # replaced by a respawn) — so unsubscribe and fail fast instead.
    if (
        info.process.returncode is not None
        or (info.pump_task is not None and info.pump_task.done())
        or mgr.streams.get(raw_url) is not info
    ):
        info.remove_subscriber(client_q)
        return web.json_response({"error": "stream terminated"}, status=503)

    response = web.StreamResponse(
        headers={
            "Content-Type": "video/mp2t",
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",  # disable nginx proxy buffering
        }
    )

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
            if (info.process.returncode is not None or pump_done) and client_q.empty():
                break

            # Longer timeout during startup, tighter once flowing.
            timeout = 10.0 if chunk_count < 10 else 5.0
            try:
                chunk = await asyncio.wait_for(client_q.get(), timeout=timeout)
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
                logger.info(
                    f"stream {info.stream_id}: client at {chunk_count} chunks, "
                    f"{info.client_count} viewers, "
                    f"{bytes_delivered / 1048576:.1f} MB delivered"
                )
                last_log = now

    except asyncio.CancelledError:
        raise  # client went away; let aiohttp handle it
    except Exception as e:
        logger.error(f"handler error on {info.stream_id}: {e}")
    finally:
        info.remove_subscriber(client_q)
        if info.client_count == 0:
            logger.info(
                f"last client left {info.stream_id}; "
                f"idle eviction in {IDLE_TIMEOUT}s"
            )

    return response


async def health_handler(request: web.Request) -> web.Response:
    mgr: StreamManager = request.app["stream_manager"]
    return web.json_response(mgr.health_snapshot())


async def metrics_handler(request: web.Request) -> web.Response:
    mgr: StreamManager = request.app["stream_manager"]
    return web.Response(
        body=mgr.metrics_snapshot().encode(),
        headers={"Content-Type": "text/plain; version=0.0.4; charset=utf-8"},
    )


# ===== App factory =====

def create_app() -> web.Application:
    manager = StreamManager()
    app = web.Application(middlewares=[auth_middleware])
    app["stream_manager"] = manager
    app.cleanup_ctx.append(manager.lifecycle)  # startup/shutdown guaranteed paired
    app.router.add_get("/stream", stream_handler)
    app.router.add_get("/health", health_handler)
    app.router.add_get("/metrics", metrics_handler)
    return app


if __name__ == "__main__":
    host = os.environ.get("MPEGTS_HOST", "0.0.0.0")
    port = _env_int("MPEGTS_PORT", 8000)

    if not AUTH_TOKEN and not AUTH_DISABLED:
        logger.warning(
            "MPEGTS_AUTH_TOKEN is not set — every /stream request will be refused. "
            "Set MPEGTS_AUTH_TOKEN (recommended) or MPEGTS_AUTH_DISABLED=1 for "
            "LAN-only development."
        )
    if AUTH_DISABLED:
        logger.warning(
            "MPEGTS_AUTH_DISABLED=1 — authentication is OFF; only safe on a trusted LAN"
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

    web.run_app(create_app(), host=host, port=port)
