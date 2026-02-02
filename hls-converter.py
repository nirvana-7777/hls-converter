#!/usr/bin/env python3
"""
HLS Converter Service
Converts DASH (MPD) to HLS (M3U8/TS) on-the-fly
"""

import os
import time
import uuid
import signal
import threading
import subprocess
from datetime import datetime, timedelta
from pathlib import Path

from flask import Flask, Response, request, jsonify
import logging
import requests

app = Flask(__name__)

# Configuration
HLS_SEGMENT_DURATION = 2  # seconds
HLS_LIST_SIZE = 5  # segments in playlist
CLEANUP_INTERVAL = 300  # seconds
MAX_STREAMS = 10
TEMP_DIR = "/tmp/hls_segments"

# Ensure temp directory exists
Path(TEMP_DIR).mkdir(parents=True, exist_ok=True)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class StreamConverter:
    """Manages DASH to HLS conversion"""

    def __init__(self):
        self.active_streams = {}
        self.lock = threading.Lock()

    def convert_to_hls(self, stream_id, dash_url):
        """Convert DASH stream to HLS"""
        stream_dir = Path(TEMP_DIR) / str(stream_id)
        stream_dir.mkdir(exist_ok=True)

        playlist_path = stream_dir / "index.m3u8"
        segment_pattern = stream_dir / "segment_%03d.ts"

        # FFmpeg command for DASH to HLS conversion
        cmd = [
            "ffmpeg",
            "-i", dash_url,
            "-c:v", "copy",  # Copy video (no re-encode)
            "-c:a", "copy",  # Copy audio (no re-encode)
            "-f", "hls",
            "-hls_time", str(HLS_SEGMENT_DURATION),
            "-hls_list_size", str(HLS_LIST_SIZE),
            "-hls_flags", "delete_segments+append_list",
            "-hls_segment_filename", str(segment_pattern),
            str(playlist_path)
        ]

        logger.info(f"Starting FFmpeg conversion for stream {stream_id}")
        logger.debug(f"FFmpeg command: {' '.join(cmd)}")

        # Start FFmpeg process
        try:
            process = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True
            )

            with self.lock:
                self.active_streams[stream_id] = {
                    "process": process,
                    "dir": stream_dir,
                    "start_time": time.time(),
                    "dash_url": dash_url
                }

            # Monitor process in background
            threading.Thread(
                target=self._monitor_process,
                args=(stream_id, process),
                daemon=True
            ).start()

            return stream_dir

        except Exception as e:
            logger.error(f"FFmpeg failed for stream {stream_id}: {e}")
            return None

    def _monitor_process(self, stream_id, process):
        """Monitor FFmpeg process"""
        stdout, stderr = process.communicate()

        if process.returncode != 0:
            logger.error(f"FFmpeg process {stream_id} failed: {stderr}")

        with self.lock:
            if stream_id in self.active_streams:
                del self.active_streams[stream_id]

        # Cleanup directory
        stream_dir = Path(TEMP_DIR) / str(stream_id)
        if stream_dir.exists():
            try:
                import shutil
                shutil.rmtree(stream_dir)
                logger.info(f"Cleaned up directory for stream {stream_id}")
            except Exception as e:
                logger.warning(f"Could not cleanup {stream_dir}: {e}")

    def get_stream_info(self, stream_id):
        """Get info about active stream"""
        with self.lock:
            return self.active_streams.get(stream_id)

    def stop_stream(self, stream_id):
        """Stop a stream conversion"""
        with self.lock:
            if stream_id in self.active_streams:
                process = self.active_streams[stream_id]["process"]
                process.terminate()
                process.wait(timeout=5)
                del self.active_streams[stream_id]
                return True
        return False

    def cleanup_old_streams(self, max_age=600):
        """Clean up old streams"""
        with self.lock:
            current_time = time.time()
            to_remove = []

            for stream_id, info in self.active_streams.items():
                if current_time - info["start_time"] > max_age:
                    to_remove.append(stream_id)

            for stream_id in to_remove:
                self.stop_stream(stream_id)
                logger.info(f"Cleaned up old stream {stream_id}")


# Global converter instance
converter = StreamConverter()


# Start cleanup thread
def cleanup_thread():
    while True:
        time.sleep(CLEANUP_INTERVAL)
        converter.cleanup_old_streams()


threading.Thread(target=cleanup_thread, daemon=True).start()


@app.route("/convert", methods=["POST"])
def convert_stream():
    """Start converting a DASH stream to HLS"""
    data = request.json

    if not data or "dash_url" not in data:
        return jsonify({"error": "Missing dash_url"}), 400

    dash_url = data["dash_url"]
    stream_id = str(uuid.uuid4())[:8]

    # Start conversion
    stream_dir = converter.convert_to_hls(stream_id, dash_url)

    if not stream_dir:
        return jsonify({"error": "Failed to start conversion"}), 500

    # Return HLS playlist URL
    hls_playlist_url = f"/hls/{stream_id}/index.m3u8"

    return jsonify({
        "stream_id": stream_id,
        "hls_url": hls_playlist_url,
        "status": "converting"
    })


@app.route("/hls/<stream_id>/index.m3u8")
def get_hls_playlist(stream_id):
    """Serve HLS playlist"""
    stream_info = converter.get_stream_info(stream_id)

    if not stream_info:
        # Check if playlist exists (might be cached)
        playlist_path = Path(TEMP_DIR) / stream_id / "index.m3u8"

        if not playlist_path.exists():
            return jsonify({"error": "Stream not found or expired"}), 404

    try:
        with open(playlist_path, "r") as f:
            playlist_content = f.read()

        # Update URLs in playlist to point to our service
        playlist_content = playlist_content.replace(
            "segment_",
            f"/hls/{stream_id}/segment_"
        )

        response = Response(playlist_content, mimetype="application/vnd.apple.mpegurl")
        response.headers["Cache-Control"] = "no-cache"
        return response

    except Exception as e:
        logger.error(f"Error serving playlist for {stream_id}: {e}")
        return jsonify({"error": "Internal server error"}), 500


@app.route("/hls/<stream_id>/<segment>")
def get_hls_segment(stream_id, segment):
    """Serve HLS segment (.ts file)"""
    segment_path = Path(TEMP_DIR) / stream_id / segment

    if not segment_path.exists():
        return jsonify({"error": "Segment not found"}), 404

    try:
        with open(segment_path, "rb") as f:
            segment_data = f.read()

        response = Response(segment_data, mimetype="video/MP2T")
        response.headers["Cache-Control"] = "max-age=3600"
        return response

    except Exception as e:
        logger.error(f"Error serving segment {segment}: {e}")
        return jsonify({"error": "Internal server error"}), 500


@app.route("/streams")
def list_streams():
    """List active streams"""
    streams = []

    with converter.lock:
        for stream_id, info in converter.active_streams.items():
            streams.append({
                "stream_id": stream_id,
                "age": time.time() - info["start_time"],
                "dash_url": info["dash_url"],
                "alive": info["process"].poll() is None
            })

    return jsonify({"streams": streams, "total": len(streams)})


@app.route("/streams/<stream_id>", methods=["DELETE"])
def stop_stream(stream_id):
    """Stop a stream"""
    if converter.stop_stream(stream_id):
        return jsonify({"status": "stopped"})
    else:
        return jsonify({"error": "Stream not found"}), 404


@app.route("/health")
def health():
    """Health check"""
    return jsonify({
        "status": "healthy",
        "active_streams": len(converter.active_streams),
        "temp_dir_size": sum(f.stat().st_size for f in Path(TEMP_DIR).rglob('*') if f.is_file())
    })


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8000, debug=False)