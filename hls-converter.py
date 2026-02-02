#!/usr/bin/env python3
"""
HLS Converter Service
Converts DASH (MPD) to HLS (M3U8/TS) on-the-fly
Enhanced with:
- Stream deduplication
- Graceful shutdown
- Orphaned segment cleanup
- Async I/O
"""

import sys
import time
import uuid
import signal
import asyncio
import hashlib
from pathlib import Path
from typing import Optional, Dict, Set

from aiohttp import web
import logging

# Configuration
HLS_SEGMENT_DURATION = 2  # seconds
HLS_LIST_SIZE = 5  # segments in playlist
CLEANUP_INTERVAL = 60  # Check every 60 seconds
INACTIVITY_TIMEOUT = 30  # Stop stream after 30 seconds of no requests
MAX_STREAM_AGE = 3600  # Maximum stream age regardless of activity
TEMP_DIR = "/tmp/hls_segments"
MAX_STREAMS = 10  # Maximum concurrent streams

# Ensure temp directory exists
Path(TEMP_DIR).mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class StreamConverter:
    def __init__(self):
        self.active_streams: Dict[str, dict] = {}
        self.url_to_stream_id: Dict[str, str] = {}
        self.stream_subscribers: Dict[str, Set[str]] = {}
        # Don't create Lock or Event here!
        self.lock = None
        self.shutdown_event = None

    async def setup(self):
        """Initialize async primitives within the correct running loop"""
        if self.lock is None:
            self.lock = asyncio.Lock()
        if self.shutdown_event is None:
            self.shutdown_event = asyncio.Event()

    @staticmethod
    def _hash_url(url: str) -> str:
        """Create a hash of the URL for deduplication"""
        return hashlib.sha256(url.encode()).hexdigest()[:16]

    async def convert_to_hls(self, dash_url: str, client_id: Optional[str] = None) -> Optional[tuple]:
        """
        Convert DASH stream to HLS with deduplication and optimized startup.
        """
        url_hash = self._hash_url(dash_url)

        async with self.lock:
            # 1. Deduplication Logic
            if url_hash in self.url_to_stream_id:
                existing_id = self.url_to_stream_id[url_hash]
                if existing_id in self.active_streams:
                    logger.info(f"Reusing stream {existing_id} for {url_hash}")
                    if client_id:
                        self.stream_subscribers[existing_id].add(client_id)

                    self.active_streams[existing_id]["last_activity"] = time.time()
                    self.active_streams[existing_id]["subscriber_count"] = len(self.stream_subscribers[existing_id])
                    return existing_id, False
                else:
                    del self.url_to_stream_id[url_hash]

            # 2. Resource Guard
            if len(self.active_streams) >= MAX_STREAMS:
                logger.warning(f"Max streams ({MAX_STREAMS}) reached. Blocking request.")
                return None, False

            # 3. Setup Directory Structure
            stream_id = str(uuid.uuid4())[:8]
            stream_dir = Path(TEMP_DIR) / stream_id
            stream_dir.mkdir(exist_ok=True)

            playlist_filename = "index.m3u8"
            playlist_path = stream_dir / playlist_filename
            segment_pattern = "segment_%03d.ts"

            # 4. Optimized FFmpeg Command
            # -map 0: ensures all streams (video/audio) are copied
            # -hls_flags: omit_endlist is crucial for live streams so players don't stop
            cmd = [
                "ffmpeg", "-hide_banner", "-loglevel", "warning",
                "-i", dash_url,
                "-c", "copy",
                "-map", "0",
                "-f", "hls",
                "-hls_time", str(HLS_SEGMENT_DURATION),
                "-hls_list_size", str(HLS_LIST_SIZE),
                "-hls_flags", "delete_segments+independent_segments+omit_endlist",
                "-hls_segment_type", "mpegts",
                "-hls_segment_filename", segment_pattern,
                "-start_number", "0",
                playlist_filename
            ]

            try:
                # Start FFmpeg with cwd set to the stream directory
                process = await asyncio.create_subprocess_exec(
                    *cmd,
                    cwd=str(stream_dir),
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.PIPE
                )

                self.active_streams[stream_id] = {
                    "process": process,
                    "dir": stream_dir,
                    "start_time": time.time(),
                    "last_activity": time.time(),
                    "request_count": 0,
                    "dash_url": dash_url,
                    "url_hash": url_hash,
                    "subscriber_count": 1 if client_id else 0
                }

                self.url_to_stream_id[url_hash] = stream_id
                self.stream_subscribers[stream_id] = {client_id} if client_id else set()

                # Monitor the process for failures
                asyncio.create_task(self._monitor_process(stream_id, process))

                # 5. Non-blocking wait for initial segment/playlist
                try:
                    # Use our robust utility instead of a hard-coded loop
                    await asyncio.wait_for(wait_for_file(playlist_path), timeout=10.0)
                    logger.info(f"Stream {stream_id} started successfully.")
                except asyncio.TimeoutError:
                    logger.warning(f"Stream {stream_id} process started, but playlist took >10s to appear.")

                return stream_id, True

            except Exception as e:
                logger.error(f"Failed to launch FFmpeg for {stream_id}: {e}")
                if stream_dir.exists():
                    import shutil
                    shutil.rmtree(stream_dir)
                return None, False

    async def _monitor_process(self, stream_id: str, process: asyncio.subprocess.Process):
        """Monitor FFmpeg process"""
        try:
            stdout, stderr = await process.communicate()

            if process.returncode != 0:
                # Return code 255 often means terminated by signal, which is normal
                if process.returncode == 255:
                    logger.info(f"FFmpeg process {stream_id} terminated (code 255)")
                else:
                    logger.error(f"FFmpeg process {stream_id} failed with code {process.returncode}")

                if stderr:
                    stderr_text = stderr.decode()[:500]
                    # Only log non-monotonic DTS warnings at debug level
                    if "Non-monotonic DTS" in stderr_text:
                        logger.debug(f"FFmpeg stderr: {stderr_text}")
                    else:
                        logger.error(f"FFmpeg stderr: {stderr_text}")
        except Exception as e:
            logger.error(f"Error monitoring process {stream_id}: {e}")

        # Schedule cleanup in the main event loop to avoid "different loop" errors
        try:
            loop = asyncio.get_event_loop()
            loop.create_task(self._cleanup_stream(stream_id))
        except Exception as e:
            logger.error(f"Error scheduling cleanup for {stream_id}: {e}")

    async def _cleanup_stream(self, stream_id: str):
        """Cleanup stream data and directory"""

        async with self.lock:
            if stream_id in self.active_streams:
                url_hash = self.active_streams[stream_id]["url_hash"]

                # Remove URL mapping
                if url_hash and url_hash in self.url_to_stream_id:
                    del self.url_to_stream_id[url_hash]

                # Remove from active streams
                del self.active_streams[stream_id]

                # Remove subscribers
                if stream_id in self.stream_subscribers:
                    del self.stream_subscribers[stream_id]

        # Cleanup directory (outside lock)
        stream_dir = Path(TEMP_DIR) / stream_id
        if stream_dir.exists():
            try:
                import shutil
                shutil.rmtree(stream_dir)
                logger.info(f"Cleaned up directory for stream {stream_id}")
            except Exception as e:
                logger.warning(f"Could not cleanup {stream_dir}: {e}")

    async def update_activity(self, stream_id: str, client_id: Optional[str] = None):
        """Update last activity time for a stream"""
        async with self.lock:
            if stream_id in self.active_streams:
                self.active_streams[stream_id]["last_activity"] = time.time()
                self.active_streams[stream_id]["request_count"] += 1

                # Add client to subscribers if provided
                if client_id and stream_id in self.stream_subscribers:
                    self.stream_subscribers[stream_id].add(client_id)
                    self.active_streams[stream_id]["subscriber_count"] = len(
                        self.stream_subscribers[stream_id]
                    )

                logger.debug(f"Activity updated for stream {stream_id}")
                return True
        return False

    async def get_stream_info(self, stream_id: str) -> Optional[dict]:
        """Get info about active stream"""
        async with self.lock:
            return self.active_streams.get(stream_id)

    async def stop_stream(self, stream_id: str, reason: str = "manual"):
        """Stop a stream conversion"""
        async with self.lock:
            if stream_id not in self.active_streams:
                return False

            info = self.active_streams[stream_id]
            process = info["process"]
            url_hash = info["url_hash"]

            logger.info(f"Stopping stream {stream_id} (reason: {reason}, "
                        f"requests: {info['request_count']}, "
                        f"subscribers: {info.get('subscriber_count', 0)}, "
                        f"age: {time.time() - info['start_time']:.1f}s)")

            try:
                # Send SIGTERM
                process.terminate()

                # Wait for graceful shutdown
                try:
                    await asyncio.wait_for(process.wait(), timeout=5.0)
                except asyncio.TimeoutError:
                    logger.warning(f"Force killing stream {stream_id}")
                    process.kill()
                    await process.wait()
            except Exception as e:
                logger.error(f"Error stopping stream {stream_id}: {e}")

            # Remove URL mapping
            if url_hash in self.url_to_stream_id:
                del self.url_to_stream_id[url_hash]

            # Remove from active streams
            del self.active_streams[stream_id]

            # Remove subscribers
            if stream_id in self.stream_subscribers:
                del self.stream_subscribers[stream_id]

            return True

    async def cleanup_inactive_streams(self, inactivity_timeout: int = INACTIVITY_TIMEOUT,
                                       max_age: int = MAX_STREAM_AGE):
        """Clean up inactive and old streams"""
        to_remove = []

        async with self.lock:
            current_time = time.time()

            for stream_id, info in self.active_streams.items():
                age = current_time - info["start_time"]
                inactive_time = current_time - info["last_activity"]

                # Check if process is still alive
                if info["process"].returncode is not None:
                    to_remove.append((stream_id, "process_died"))
                    continue

                # Check max age
                if age > max_age:
                    to_remove.append((stream_id, f"max_age_exceeded ({age:.1f}s)"))
                    continue

                # Check inactivity
                if inactive_time > inactivity_timeout:
                    to_remove.append((stream_id, f"inactive ({inactive_time:.1f}s)"))
                    continue

        # Stop streams outside the lock to avoid deadlock
        for stream_id, reason in to_remove:
            await self.stop_stream(stream_id, reason)

    async def get_statistics(self) -> list:
        """Get statistics about all streams"""
        async with self.lock:
            stats = []
            current_time = time.time()

            for stream_id, info in self.active_streams.items():
                stats.append({
                    "stream_id": stream_id,
                    "age": current_time - info["start_time"],
                    "inactive_for": current_time - info["last_activity"],
                    "request_count": info["request_count"],
                    "subscriber_count": info.get("subscriber_count", 0),
                    "alive": info["process"].returncode is None,
                    "url_hash": info["url_hash"]
                })

            return stats

    async def cleanup_orphaned_directories(self):
        """Clean up orphaned directories from previous runs"""
        logger.info("Cleaning up orphaned directories...")

        temp_path = Path(TEMP_DIR)
        if not temp_path.exists():
            return

        cleaned = 0
        for item in temp_path.iterdir():
            if item.is_dir():
                stream_id = item.name

                # Check if this stream is active
                async with self.lock:
                    if stream_id not in self.active_streams:
                        try:
                            import shutil
                            shutil.rmtree(item)
                            cleaned += 1
                            logger.info(f"Removed orphaned directory: {stream_id}")
                        except Exception as e:
                            logger.warning(f"Could not remove orphaned directory {item}: {e}")

        if cleaned > 0:
            logger.info(f"Cleaned up {cleaned} orphaned directories")

    async def shutdown_all_streams(self):
        """Gracefully shutdown all active streams"""
        logger.info("Shutting down all streams...")

        stream_ids = list(self.active_streams.keys())

        for stream_id in stream_ids:
            await self.stop_stream(stream_id, reason="shutdown")

        logger.info(f"Shutdown complete. Stopped {len(stream_ids)} streams.")


# Global converter instance
converter = StreamConverter()


async def cleanup_task():
    """Background task to clean up inactive streams"""
    # Wait for converter to be initialized
    while converter.shutdown_event is None:
        await asyncio.sleep(0.1)

    while not converter.shutdown_event.is_set():
        try:
            await asyncio.sleep(CLEANUP_INTERVAL)
            logger.debug("Running cleanup check...")
            await converter.cleanup_inactive_streams()
        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.error(f"Error in cleanup task: {e}")


async def convert_stream(request):
    """Start converting a DASH stream to HLS"""
    try:
        data = await request.json()
    except Exception:
        return web.json_response({"error": "Invalid JSON"}, status=400)

    if not data or "dash_url" not in data:
        return web.json_response({"error": "Missing dash_url"}, status=400)

    dash_url = data["dash_url"]
    client_id = data.get("client_id")  # Optional client identifier

    # Start conversion (with deduplication)
    result = await converter.convert_to_hls(dash_url, client_id)

    if not result or result[0] is None:
        return web.json_response({"error": "Failed to start conversion"}, status=500)

    stream_id, is_new = result

    # Return HLS playlist URL
    hls_playlist_url = f"/hls/{stream_id}/index.m3u8"

    return web.json_response({
        "stream_id": stream_id,
        "hls_url": hls_playlist_url,
        "status": "converting",
        "is_new_stream": is_new
    })


async def get_hls_playlist(request):
    stream_id = request.match_info['stream_id']
    client_id = request.query.get('client_id')

    await converter.update_activity(stream_id, client_id)
    stream_info = await converter.get_stream_info(stream_id)

    if not stream_info:
        return web.json_response({"error": "Stream not active"}, status=404)

    playlist_path = stream_info["dir"] / "index.m3u8"

    # Wait for the first write if it's a brand new stream
    if not playlist_path.exists():
        try:
            # Efficient wait for up to 5 seconds
            await asyncio.wait_for(wait_for_file(playlist_path), timeout=5.0)
        except asyncio.TimeoutError:
            return web.json_response({"error": "Playlist timeout"}, status=404)

    # Use FileResponse for zero-copy transfer
    return web.FileResponse(
        playlist_path,
        headers={
            "Content-Type": "application/vnd.apple.mpegurl",
            "Cache-Control": "no-cache",
            "Access-Control-Allow-Origin": "*"
        }
    )


async def get_hls_segment(request):
    stream_id = request.match_info['stream_id']
    segment = request.match_info['segment']

    # Path traversal protection
    if not segment.endswith('.ts') or any(x in segment for x in ['/', '\\', '..']):
        return web.json_response({"error": "Invalid segment"}, status=400)

    segment_path = Path(TEMP_DIR) / stream_id / segment

    # If the player is faster than the converter, wait for the file to appear
    if not segment_path.exists():
        try:
            await asyncio.wait_for(wait_for_file(segment_path), timeout=3.0)
        except asyncio.TimeoutError:
            return web.json_response({"error": "Segment not ready"}, status=404)

    return web.FileResponse(
        segment_path,
        headers={
            "Content-Type": "video/MP2T",
            "Cache-Control": "max-age=3600",
            "Access-Control-Allow-Origin": "*"
        }
    )


async def wait_for_file(path: Path):
    """Non-blocking check for file existence using exponential backoff"""
    delay = 0.05
    while not path.exists():
        await asyncio.sleep(delay)
        delay = min(delay * 2, 0.5)  # Cap delay at 500ms


async def list_streams(request):
    """List active streams with detailed statistics"""
    stats = await converter.get_statistics()

    return web.json_response({
        "streams": stats,
        "total": len(stats),
        "config": {
            "inactivity_timeout": INACTIVITY_TIMEOUT,
            "max_stream_age": MAX_STREAM_AGE,
            "cleanup_interval": CLEANUP_INTERVAL,
            "max_streams": MAX_STREAMS
        }
    })


async def stop_stream_handler(request):
    """Stop a stream"""
    stream_id = request.match_info['stream_id']

    if await converter.stop_stream(stream_id, reason="api_request"):
        return web.json_response({"status": "stopped"})
    else:
        return web.json_response({"error": "Stream not found"}, status=404)


async def health(request):
    """Health check"""
    return web.json_response({
        "status": "healthy",
        "active_streams": len(converter.active_streams),
        "unique_sources": len(converter.url_to_stream_id),
        "temp_dir_size": sum(f.stat().st_size for f in Path(TEMP_DIR).rglob('*') if f.is_file())
    })


async def on_startup(app):
    logger.info("Starting HLS Converter Service...")

    # Initialize the converter's locks within the current loop
    await converter.setup()

    await converter.cleanup_orphaned_directories()
    app['cleanup_task'] = asyncio.create_task(cleanup_task())
    logger.info("HLS Converter Service started successfully")


async def on_shutdown(app):
    """Shutdown tasks"""
    logger.info("Shutting down HLS Converter Service...")

    # Signal shutdown
    converter.shutdown_event.set()

    # Cancel cleanup task
    if 'cleanup_task' in app:
        app['cleanup_task'].cancel()
        try:
            await app['cleanup_task']
        except asyncio.CancelledError:
            pass

    # Shutdown all streams
    await converter.shutdown_all_streams()

    logger.info("HLS Converter Service shutdown complete")


def handle_signal(signum, frame):
    """Handle shutdown signals"""
    logger.info(f"Received signal {signum}, initiating graceful shutdown...")
    sys.exit(0)


def create_app():
    """Create and configure the application"""
    app = web.Application()

    # Add routes
    app.router.add_post('/convert', convert_stream)
    app.router.add_get('/hls/{stream_id}/index.m3u8', get_hls_playlist)
    app.router.add_get('/hls/{stream_id}/{segment}', get_hls_segment)
    app.router.add_get('/streams', list_streams)
    app.router.add_delete('/streams/{stream_id}', stop_stream_handler)
    app.router.add_get('/health', health)

    # Add startup/shutdown handlers
    app.on_startup.append(on_startup)
    app.on_shutdown.append(on_shutdown)

    return app


if __name__ == "__main__":
    # Register signal handlers for graceful shutdown
    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)

    app = create_app()

    logger.info("Starting server on 0.0.0.0:8000")
    web.run_app(app, host="0.0.0.0", port=8000)