import asyncio
import fcntl
import hashlib
import logging
import shlex
import subprocess
import time
import uuid

from aiohttp import web

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)
logger = logging.getLogger(__name__)


class StreamManager:
    def __init__(self, max_streams=15, max_stream_age=3600):
        self.streams = {}
        self.max_streams = max_streams
        self.max_stream_age = max_stream_age
        self._cleanup_task = None

    @staticmethod
    def get_stream_id(url):
        return hashlib.md5(url.encode()).hexdigest()[:8]

    async def start_cleanup_task(self):
        self._cleanup_task = asyncio.create_task(self._cleanup_loop())
        logger.info("Started stream cleanup task")

    async def _cleanup_loop(self):
        while True:
            await asyncio.sleep(30)
            await self._cleanup_stale_streams()

    async def _cleanup_dead_streams(self):
        streams_to_remove = [
            s_id
            for s_id, info in self.streams.items()
            if info["process"].returncode is not None
        ]
        for s_id in streams_to_remove:
            await self._terminate_stream(s_id)

    async def _cleanup_stale_streams(self):
        now = time.time()
        streams_to_remove = []
        for s_id, info in list(self.streams.items()):
            if info["process"].returncode is not None:
                streams_to_remove.append(s_id)
                continue
            if (now - info["created"]) > self.max_stream_age:
                streams_to_remove.append(s_id)
                continue
            if info.get("client_count", 0) == 0:
                idle_time = now - info.get("last_client_disconnect", info["created"])
                if (
                    idle_time > 60
                ):  # Reduced idle timeout for better resource management
                    streams_to_remove.append(s_id)

        for s_id in streams_to_remove:
            await self._terminate_stream(s_id)

    async def client_disconnected(self, stream_id):
        """Called when a client disconnects from a stream."""
        if stream_id not in self.streams:
            return

        info = self.streams[stream_id]
        info["client_count"] = max(0, info["client_count"] - 1)

        if info["client_count"] == 0:
            info["last_client_disconnect"] = time.time()
            logger.info(
                f"Last client disconnected from {stream_id}, will cleanup if idle for 60s"
            )
        else:
            logger.info(
                f"Client disconnected from {stream_id} ({info['client_count']} clients remaining)"
            )

    async def _terminate_stream(self, stream_id):
        if stream_id not in self.streams:
            return
        info = self.streams[stream_id]
        proc = info["process"]
        try:
            proc.terminate()
            await asyncio.wait_for(proc.wait(), timeout=2.0)
        except Exception:
            try:
                proc.kill()
            except:
                pass
        finally:
            if stream_id in self.streams:
                del self.streams[stream_id]
            logger.info(f"Terminated stream {stream_id}")

    async def _drain_stderr(self, proc, stream_id):
        """Monitor FFmpeg stderr for errors"""
        try:
            while True:
                line = await proc.stderr.readline()
                if not line:
                    break
                msg = line.decode().strip()
                # Filter out common noise, log actual problems
                if any(
                    noise in msg
                    for noise in [
                        "Non-monotonic DTS",
                        "Past duration",
                        "Application provided invalid",
                    ]
                ):
                    continue

                # Log actual errors
                if any(err in msg for err in ["Error", "Invalid", "Failed", "Cannot"]):
                    logger.error(f"FFmpeg [{stream_id}]: {msg}")
                else:
                    logger.debug(f"FFmpeg [{stream_id}]: {msg}")
        except Exception:
            pass

    async def get_or_create_stream(self, url_or_command, name="Stream"):
        """Get existing stream or create new one. Reuses streams for same URL."""
        await self._cleanup_dead_streams()

        stream_id = self.get_stream_id(url_or_command)

        # Check if we already have a running stream for this URL
        for existing_id, info in list(self.streams.items()):
            if existing_id.startswith(stream_id) and info["process"].returncode is None:
                # Reuse existing stream
                info["client_count"] += 1
                logger.info(
                    f"Reusing stream {existing_id} (clients: {info['client_count']})"
                )
                return existing_id

        # Need to create new stream
        if len(self.streams) >= self.max_streams:
            return None

        unique_id = f"{stream_id}_{uuid.uuid4().hex[:6]}"

        if url_or_command.startswith("pipe://"):
            cmd = shlex.split(url_or_command[7:])
        else:
            cmd = [
                "ffmpeg",
                "-loglevel",
                "error",
                "-fflags",
                "+genpts+igndts+discardcorrupt",  # Added discardcorrupt for resilience
                "-probesize",
                "5M",  # Reduced from 32M for faster startup
                "-analyzeduration",
                "2M",  # Reduced from 10M for faster startup
                "-reconnect",
                "1",
                "-reconnect_streamed",
                "1",
                "-reconnect_delay_max",
                "2",
                "-thread_queue_size",
                "512",  # Increase thread queue to prevent drops during startup
                "-i",
                url_or_command,
                "-map",
                "0:v:0",
                "-map",
                "0:a:0?",
                "-c",
                "copy",
                "-avoid_negative_ts",
                "make_zero",
                "-max_muxing_queue_size",
                "4096",  # Doubled for startup buffering
                "-f",
                "mpegts",
                "-muxdelay",
                "0",
                "-muxpreload",
                "0",
                "-mpegts_flags",
                "resend_headers",
                "-mpegts_copyts",
                "1",
                "-metadata",
                f"service_name={name}",
                "-flush_packets",
                "1",
                "pipe:1",
            ]

        try:
            proc = await asyncio.create_subprocess_exec(
                *cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                limit=2 * 1024 * 1024,  # 2MB internal buffer for smoother streaming
            )

            # Set Linux pipe size to 2MB for better buffering
            try:
                fcntl.fcntl(proc.stdout.fileno(), 1031, 2 * 1048576)
            except:
                pass

            self.streams[unique_id] = {
                "process": proc,
                "created": time.time(),
                "client_count": 1,
                "last_client_disconnect": None,
                "url": url_or_command,  # Store URL for logging
            }
            asyncio.create_task(self._drain_stderr(proc, unique_id))
            logger.info(f"Created new stream {unique_id} for {name}")
            return unique_id
        except Exception as e:
            logger.error(f"Failed to create stream: {e}")
            return None


manager = StreamManager()


async def stream_handler(request):
    url = request.query.get("url")
    name = request.query.get("name", "Stream")
    if not url:
        return web.Response(text="Missing URL", status=400)

    stream_id = await manager.get_or_create_stream(url, name)
    if not stream_id:
        return web.Response(text="Max streams reached", status=503)

    info = manager.streams[stream_id]
    proc = info["process"]

    response = web.StreamResponse(
        headers={
            "Content-Type": "video/mp2t",
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
        }
    )

    await response.prepare(request)

    try:
        bytes_sent = 0
        last_log = time.time()
        stall_count = 0
        chunk_count = 0

        while True:
            if proc.returncode is not None:
                logger.warning(
                    f"Stream {stream_id} process died (returncode: {proc.returncode})"
                )
                break

            # Adaptive timeout: longer during startup (first 10 chunks), shorter after
            timeout = 10.0 if chunk_count < 10 else 5.0

            try:
                # Read smaller chunks more frequently for smoother delivery
                # 188 bytes * 350 = 65,800 bytes (350 MPEG-TS packets per chunk)
                chunk = await asyncio.wait_for(proc.stdout.read(65800), timeout=timeout)
            except asyncio.TimeoutError:
                stall_count += 1
                # More lenient during startup
                max_stalls = 5 if chunk_count < 10 else 3
                if stall_count > max_stalls:
                    logger.error(
                        f"Stream {stream_id} stalled (no data for {stall_count * timeout:.0f}s)"
                    )
                    break
                if chunk_count < 10:
                    logger.debug(
                        f"Startup buffering on {stream_id} (stall {stall_count}/{max_stalls})"
                    )
                else:
                    logger.warning(
                        f"Read timeout on {stream_id} (stall {stall_count}/{max_stalls})"
                    )
                continue

            if not chunk:
                logger.info(f"Stream {stream_id} ended (EOF)")
                break

            stall_count = 0  # Reset stall counter on successful read
            chunk_count += 1

            try:
                await response.write(chunk)
            except Exception as write_err:
                logger.warning(f"Write failed on {stream_id}: {write_err}")
                break

            bytes_sent += len(chunk)

            # Log first successful chunk after startup
            if chunk_count == 1:
                logger.info(
                    f"Stream {stream_id} started successfully, first chunk delivered"
                )

            # Log progress every 30 seconds
            now = time.time()
            if now - last_log > 30:
                logger.info(
                    f"Stream {stream_id}: {bytes_sent / 1024 / 1024:.1f} MB sent, "
                    f"clients: {info['client_count']}"
                )
                last_log = now

    except (asyncio.CancelledError, ConnectionResetError):
        logger.info(f"Client connection closed for {stream_id}")
    except Exception as e:
        logger.error(f"Handler error on {stream_id}: {e}")
    finally:
        # Don't terminate - just decrement client count
        await manager.client_disconnected(stream_id)
        logger.info(
            f"Client session ended for {stream_id}, sent {bytes_sent / 1024 / 1024:.1f} MB"
        )

    return response


async def health_handler(request):
    return web.json_response(
        {"status": "healthy", "active_streams": len(manager.streams)}
    )


app = web.Application()
app.on_startup.append(lambda _: manager.start_cleanup_task())
app.router.add_get("/stream", stream_handler)
app.router.add_get("/health", health_handler)

if __name__ == "__main__":
    web.run_app(app, host="0.0.0.0", port=8000)
