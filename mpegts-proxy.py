#!/usr/bin/env python3
"""
Minimal MPEG-TS Stream Proxy
Direct DASH → MPEG-TS streaming with process monitoring and resource management
"""

import asyncio
import hashlib
import logging
import subprocess
import time

from aiohttp import web

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class StreamManager:
    def __init__(self, max_streams=10, max_stream_age=3600):
        self.streams = {}
        self.max_streams = max_streams
        self.max_stream_age = max_stream_age
        self._cleanup_task = None

    @staticmethod
    def get_stream_id(url):
        return hashlib.md5(url.encode()).hexdigest()[:8]

    async def start_cleanup_task(self):
        """Start background task to clean up stale/dead streams"""
        self._cleanup_task = asyncio.create_task(self._cleanup_loop())
        logger.info("Started stream cleanup task")

    async def _cleanup_loop(self):
        """Background task that runs every 30 seconds to clean up streams"""
        while True:
            await asyncio.sleep(30)
            await self._cleanup_stale_streams()

    async def _cleanup_dead_streams(self):
        """Immediately clean up streams with dead processes"""
        streams_to_remove = []

        for stream_id, info in list(self.streams.items()):
            proc = info["process"]
            if proc.returncode is not None:
                streams_to_remove.append(stream_id)

        for stream_id in streams_to_remove:
            await self._terminate_stream(stream_id)

        if streams_to_remove:
            logger.info(f"Cleaned up {len(streams_to_remove)} dead streams")

    async def _cleanup_stale_streams(self):
        """Remove streams that are dead or too old"""
        now = time.time()
        streams_to_remove = []

        for stream_id, info in list(self.streams.items()):
            proc = info["process"]

            # Check if process has died
            if proc.returncode is not None:
                logger.info(
                    f"Stream {stream_id} process died (exit code: {proc.returncode})"
                )
                streams_to_remove.append(stream_id)
                continue

            # Check if stream is too old
            age = now - info["created"]
            if age > self.max_stream_age:
                logger.info(f"Stream {stream_id} exceeded max age ({age:.0f}s)")
                streams_to_remove.append(stream_id)
                continue

            # Check if no clients are connected
            if info.get("client_count", 0) == 0:
                idle_time = now - info.get("last_client_disconnect", info["created"])
                if idle_time > 300:  # 5 minutes idle
                    logger.info(f"Stream {stream_id} idle for {idle_time:.0f}s")
                    streams_to_remove.append(stream_id)

        # Remove identified streams
        for stream_id in streams_to_remove:
            await self._terminate_stream(stream_id)

    async def _terminate_stream(self, stream_id):
        """Safely terminate and remove a stream"""
        if stream_id not in self.streams:
            return

        info = self.streams[stream_id]
        proc = info["process"]

        try:
            # Try graceful termination first
            proc.terminate()
            try:
                await asyncio.wait_for(proc.wait(), timeout=5.0)
                logger.info(f"Stream {stream_id} terminated gracefully")
            except asyncio.TimeoutError:
                # Force kill if termination times out
                proc.kill()
                await proc.wait()
                logger.warning(f"Stream {stream_id} force killed")
        except Exception as e:
            logger.error(f"Error terminating stream {stream_id}: {e}")
        finally:
            del self.streams[stream_id]

    async def _drain_stderr(self, proc, stream_id):
        """Continuously read stderr to prevent blocking and log errors"""
        try:
            while True:
                line = await proc.stderr.readline()
                if not line:
                    break
                msg = line.decode().strip()
                if msg:
                    # Log FFmpeg errors at warning level so we can see what's wrong
                    logger.warning(f"FFmpeg [{stream_id}]: {msg}")
        except Exception as e:
            logger.debug(f"stderr drain ended for {stream_id}: {e}")

    async def create_stream(self, url_or_command, name="Stream"):
        """Create a new stream - can accept URL or pipe:// FFmpeg command"""

        # Clean up any dead streams first to free up slots
        await self._cleanup_dead_streams()

        # Check resource limits
        if len(self.streams) >= self.max_streams:
            logger.warning(f"Max streams ({self.max_streams}) reached")
            return None

        # Generate unique stream ID
        import uuid

        stream_id = self.get_stream_id(url_or_command)
        unique_id = f"{stream_id}_{uuid.uuid4().hex[:6]}"

        # Check if this is a pipe:// FFmpeg command or a regular URL
        if url_or_command.startswith("pipe://"):
            # Extract the FFmpeg command (everything after "pipe://")
            ffmpeg_command = url_or_command[7:]  # Remove "pipe://"

            # Parse the command into arguments
            # Use shlex to properly handle quoted arguments
            import shlex

            try:
                cmd = shlex.split(ffmpeg_command)
                logger.info(
                    f"Using backend FFmpeg command: {cmd[0]} {cmd[1]} ... (total {len(cmd)} args)"
                )
            except Exception as e:
                logger.error(f"Failed to parse FFmpeg command: {e}")
                return None
        else:
            # Regular URL - build our own FFmpeg command
            cmd = [
                "ffmpeg",
                "-loglevel",
                "verbose",
                "-fflags",
                "+genpts+discardcorrupt",
                "-reconnect",
                "1",
                "-reconnect_streamed",
                "1",
                "-reconnect_delay_max",
                "2",
                "-i",
                url_or_command,
                "-map",
                "0:v",
                "-map",
                "0:a?",
                "-c",
                "copy",
                "-bsf:v",
                "h264_mp4toannexb",
                "-f",
                "mpegts",
                "-metadata",
                f"service_name={name}",
                "pipe:1",
            ]
            logger.info(f"Using proxy FFmpeg command for URL: {url_or_command[:50]}...")

        try:
            proc = await asyncio.create_subprocess_exec(
                *cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE
            )

            self.streams[unique_id] = {
                "process": proc,
                "url": url_or_command,
                "created": time.time(),
                "client_count": 1,
                "last_client_disconnect": None,
            }

            # Start draining stderr to prevent blocking
            asyncio.create_task(self._drain_stderr(proc, unique_id))

            logger.info(f"Created stream {unique_id} (total: {len(self.streams)})")
            return unique_id

        except Exception as e:
            logger.error(f"Failed to create stream: {e}")
            return None

    async def disconnect_client(self, stream_id):
        """Mark a client as disconnected from a stream"""
        if stream_id in self.streams:
            info = self.streams[stream_id]
            info["client_count"] = max(0, info.get("client_count", 1) - 1)
            if info["client_count"] == 0:
                info["last_client_disconnect"] = time.time()
                logger.info(f"Stream {stream_id} has no clients")

    async def shutdown(self):
        """Gracefully shutdown all streams"""
        logger.info("Shutting down all streams...")

        if self._cleanup_task:
            self._cleanup_task.cancel()
            try:
                await self._cleanup_task
            except asyncio.CancelledError:
                pass

        for stream_id in list(self.streams.keys()):
            await self._terminate_stream(stream_id)

        logger.info("All streams terminated")


manager = StreamManager(max_streams=10, max_stream_age=3600)


async def stream_handler(request):
    """Handle stream requests"""
    url = request.query.get("url")
    name = request.query.get("name", "Stream")

    if not url:
        return web.Response(text="Missing URL", status=400)

    stream_id = await manager.create_stream(url, name)
    if not stream_id:
        return web.Response(
            text="Stream creation failed or max streams reached", status=503
        )

    proc = manager.streams[stream_id]["process"]

    response = web.StreamResponse(
        headers={"Content-Type": "video/mp2t", "Cache-Control": "no-cache"}
    )
    await response.prepare(request)

    try:
        bytes_sent = 0
        first_chunk = True

        while True:
            # Check if process is still alive
            if proc.returncode is not None:
                logger.warning(
                    f"Stream {stream_id} process died (exit: {proc.returncode}, "
                    f"sent {bytes_sent} bytes)"
                )
                break

            try:
                # Add timeout on first chunk to detect slow startup
                if first_chunk:
                    chunk = await asyncio.wait_for(proc.stdout.read(65536), timeout=5.0)
                    if chunk:
                        logger.info(
                            f"Stream {stream_id} started, "
                            f"sending first chunk ({len(chunk)} bytes)"
                        )
                        first_chunk = False
                else:
                    chunk = await proc.stdout.read(65536)
            except asyncio.TimeoutError:
                # Read any stderr output to see why FFmpeg failed
                stderr_data = b""
                try:
                    while True:
                        line = await asyncio.wait_for(
                            proc.stderr.readline(), timeout=0.1
                        )
                        if not line:
                            break
                        stderr_data += line
                except asyncio.TimeoutError:
                    pass

                error_msg = (
                    stderr_data.decode().strip() if stderr_data else "no error output"
                )
                logger.error(
                    f"Stream {stream_id} timeout waiting for FFmpeg data (>5s), "
                    f"FFmpeg says: {error_msg}"
                )
                break

            if not chunk:
                break

            await response.write(chunk)
            bytes_sent += len(chunk)

        logger.info(f"Stream {stream_id} ended, sent {bytes_sent} bytes total")

        # If FFmpeg exited immediately with no data, clean up right away
        if bytes_sent == 0 and proc.returncode is not None:
            logger.info(f"Stream {stream_id} failed immediately, cleaning up")
            await manager._terminate_stream(stream_id)

    except asyncio.CancelledError:
        logger.info(f"Client cancelled stream {stream_id} (sent {bytes_sent} bytes)")
        raise
    except Exception as e:
        logger.error(f"Stream {stream_id} error: {e} (sent {bytes_sent} bytes)")
    finally:
        # Mark client as disconnected
        await manager.disconnect_client(stream_id)

    return response


async def health_handler(request):
    """Health check endpoint with stream statistics"""
    stream_info = []
    for stream_id, info in manager.streams.items():
        proc = info["process"]
        stream_info.append(
            {
                "id": stream_id,
                "age": int(time.time() - info["created"]),
                "clients": info.get("client_count", 0),
                "alive": proc.returncode is None,
            }
        )

    return web.json_response(
        {
            "status": "healthy",
            "streams": len(manager.streams),
            "max_streams": manager.max_streams,
            "stream_details": stream_info,
        }
    )


async def on_startup(app):
    """Start background tasks on app startup"""
    await manager.start_cleanup_task()


async def on_cleanup(app):
    """Cleanup on app shutdown"""
    await manager.shutdown()


app = web.Application()
app.router.add_get("/stream", stream_handler)
app.router.add_get("/health", health_handler)
app.on_startup.append(on_startup)
app.on_cleanup.append(on_cleanup)

if __name__ == "__main__":
    web.run_app(app, host="0.0.0.0", port=8000)
