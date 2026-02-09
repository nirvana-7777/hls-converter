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
        try:
            while True:
                line = await proc.stderr.readline()
                if not line:
                    break
                msg = line.decode().strip()
                if "Non-monotonic DTS" not in msg:  # Filter noise, keep real errors
                    logger.warning(f"FFmpeg [{stream_id}]: {msg}")
        except Exception:
            pass

    async def create_stream(self, url_or_command, name="Stream"):
        await self._cleanup_dead_streams()
        if len(self.streams) >= self.max_streams:
            return None

        stream_id = self.get_stream_id(url_or_command)
        unique_id = f"{stream_id}_{uuid.uuid4().hex[:6]}"

        if url_or_command.startswith("pipe://"):
            cmd = shlex.split(url_or_command[7:])
        else:
            cmd = [
                "ffmpeg",
                "-loglevel",
                "warning",
                "-probesize",
                "10M",
                "-analyzeduration",
                "10M",
                "-fflags",
                "+genpts+discardcorrupt+igndts+flush_packets",
                "-reconnect",
                "1",
                "-reconnect_streamed",
                "1",
                "-reconnect_delay_max",
                "1",
                "-i",
                url_or_command,
                "-map",
                "0:v",
                "-map",
                "0:a?",
                "-c",
                "copy",
                "-copyts",
                "-start_at_zero",
                "-avoid_negative_ts",
                "make_non_negative",
                "-max_interleave_delta",
                "0",
                "-f",
                "mpegts",
                "-muxdelay",
                "0.5",  # Critical: allows muxer to fix timeline jumps
                "-mpegts_flags",
                "resend_headers+initial_discontinuity",
                "-metadata",
                f"service_name={name}",
                "pipe:1",
            ]

        try:
            proc = await asyncio.create_subprocess_exec(
                *cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                limit=1024 * 1024,  # 1MB internal buffer for the reader
            )

            # Set Linux pipe size to 1MB (F_SETPIPE_SZ = 1031)
            try:
                fcntl.fcntl(proc.stdout.fileno(), 1031, 1048576)
            except:
                pass

            self.streams[unique_id] = {
                "process": proc,
                "created": time.time(),
                "client_count": 1,
                "last_client_disconnect": None,
            }
            asyncio.create_task(self._drain_stderr(proc, unique_id))
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

    stream_id = await manager.create_stream(url, name)
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
        while True:
            if proc.returncode is not None:
                break

            try:
                # Read whatever is available up to 256KB
                chunk = await asyncio.wait_for(proc.stdout.read(262144), timeout=20.0)
            except asyncio.TimeoutError:
                break

            if not chunk:
                break

            await response.write(chunk)
            bytes_sent += len(chunk)

    except (asyncio.CancelledError, ConnectionResetError):
        pass
    except Exception as e:
        logger.error(f"Handler error on {stream_id}: {e}")
    finally:
        await manager._terminate_stream(stream_id)

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
