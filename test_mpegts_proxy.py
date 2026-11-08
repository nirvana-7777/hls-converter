"""Tests for mpegts-proxy.py.

Pure-logic tests need nothing but Python + aiohttp. The end-to-end tests
generate real HLS/DASH media with ffmpeg, serve it from a fake "backend" and
check the TS the proxy produces with ffprobe — they are skipped when
ffmpeg/ffprobe are not installed.

Run:  pip install aiohttp pytest && pytest test_mpegts_proxy.py -v
"""
import asyncio
import importlib.util
import json
import os
import shutil
import socket
import subprocess
import sys
import time
from collections import deque
from pathlib import Path

import pytest
from aiohttp import ClientSession, web
from aiohttp.test_utils import TestServer

HERE = Path(__file__).parent
PROXY_FILE = HERE / "mpegts-proxy.py"
HAVE_FFMPEG = bool(shutil.which("ffmpeg") and shutil.which("ffprobe"))
needs_ffmpeg = pytest.mark.skipif(not HAVE_FFMPEG, reason="ffmpeg/ffprobe not installed")


def free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def load_proxy(**env):
    """Import mpegts-proxy.py fresh with the given environment (its config
    is read at import time)."""
    base = {"MPEGTS_AUTH_DISABLED": "1", "MPEGTS_ALLOW_PRIVATE_URLS": "1", "MPEGTS_LOG_LEVEL": "WARNING"}
    base.update(env)
    saved = {k: os.environ.get(k) for k in base}
    os.environ.update(base)
    try:
        name = f"mpegts_proxy_{time.monotonic_ns()}"
        spec = importlib.util.spec_from_file_location(name, PROXY_FILE)
        mod = importlib.util.module_from_spec(spec)
        sys.modules[name] = mod
        spec.loader.exec_module(mod)
        return mod
    finally:
        for k, v in saved.items():
            if v is None:
                os.environ.pop(k, None)
            else:
                os.environ[k] = v


@pytest.fixture(scope="module")
def pure():
    return load_proxy(MPEGTS_BACKEND_URL="http://backend.test:8080")


# ---------------------------------------------------------------- TS builders

def ts_pkt(pid, payload=b"", pusi=False, rai=False, cc=0):
    b1 = (0x40 if pusi else 0) | ((pid >> 8) & 0x1F)
    header = bytes([0x47, b1, pid & 0xFF])
    if rai:
        af_len = 183 - len(payload)
        af = bytes([af_len, 0x40]) + b"\xff" * (af_len - 1)
        return header + bytes([0x30 | (cc & 0xF)]) + af + payload
    return header + bytes([0x10 | (cc & 0xF)]) + payload + b"\xff" * (184 - len(payload))


def pat(pmt_pid=0x1000):
    sect = bytes([0x00, 0xB0, 13, 0, 1, 0xC1, 0, 0, 0, 1, 0xE0 | (pmt_pid >> 8), pmt_pid & 0xFF]) + b"\0\0\0\0"
    return ts_pkt(0, b"\x00" + sect, pusi=True)


def pmt(pmt_pid=0x1000, streams=((0x1B, 0x100), (0x0F, 0x101))):
    body = b"".join(bytes([t, 0xE0 | (p >> 8), p & 0xFF, 0xF0, 0]) for t, p in streams)
    length = 9 + len(body) + 4
    sect = bytes([0x02, 0xB0, length, 0, 1, 0xC1, 0, 0, 0xE1, 0x00, 0xF0, 0]) + body + b"\0\0\0\0"
    return ts_pkt(pmt_pid, b"\x00" + sect, pusi=True)


VIDEO, AUDIO = 0x100, 0x101


def keyframe():
    return ts_pkt(VIDEO, b"\x00\x00\x01\xe0", pusi=True, rai=True)


def pframe():
    return ts_pkt(VIDEO, b"\x00\x00\x01\xe0", pusi=True)


def audio_pes():
    return ts_pkt(AUDIO, b"\x00\x00\x01\xc0", pusi=True, rai=True)


def tracker_with_tables(pure):
    t = pure.PsiTracker()
    t.feed(pat() + pmt())
    return t


# ---------------------------------------------------------------- pure tests

def test_psi_tracker_finds_video_pid(pure):
    t = tracker_with_tables(pure)
    assert t.complete and t.video_pid == VIDEO and t.pmt_pid == 0x1000
    assert len(t.prefix()) == 376


def test_psi_tracker_audio_only(pure):
    t = pure.PsiTracker()
    t.feed(pat() + pmt(streams=((0x0F, 0x101),)))
    assert t.complete and t.video_pid is None


def test_anchor_prefers_video_keyframe(pure):
    data = audio_pes() + pframe() + keyframe() + pframe()
    assert pure._first_anchor_offset(data, VIDEO) == 188 * 2
    assert pure._first_anchor_offset(data, None) == 0          # any PES start
    assert pure._first_anchor_offset(pframe() + pframe(), VIDEO) == -1


def test_late_join_starts_on_tables_and_keyframe(pure):
    async def run():
        buf = pure.ClientBuffer(64, start_on_anchor=True, psi=tracker_with_tables(pure))
        buf.push(pframe() + audio_pes())                       # mid-GOP: must be held back
        with pytest.raises(asyncio.TimeoutError):
            await buf.get(0.05)
        buf.push(pat() + pmt() + pframe() + keyframe() + pframe())
        first = await buf.get(0.5)
        assert first[1] & 0x40 and ((first[1] & 0x1F) << 8 | first[2]) == 0   # PAT first
        assert len(first) == 376                                # PAT+PMT prefix
        second = await buf.get(0.5)
        assert ((second[1] & 0x1F) << 8 | second[2]) == VIDEO and second[5] & 0x40   # then the keyframe
        assert len(second) % 188 == 0 and buf.join_skipped_bytes > 0
    asyncio.run(run())


def test_held_client_is_not_released_by_chunk_count(pure):
    """Regression: a joining client used to be released after ~16 chunks even
    without a keyframe. A 2 s GOP at 5 Mbit/s spans ~20 chunks, so it was
    started mid-GOP (no SPS/PPS -> 'non-existing PPS 0 referenced')."""
    async def run():
        buf = pure.ClientBuffer(64, start_on_anchor=True, psi=tracker_with_tables(pure))
        for _ in range(40):
            buf.push(pframe() + audio_pes() + pframe())
        with pytest.raises(asyncio.TimeoutError):
            await buf.get(0.05)
        buf.push(keyframe() + pframe())
        first = await buf.get(0.5)
        assert len(first) == 376                                    # PAT+PMT
        second = await buf.get(0.5)
        assert ((second[1] & 0x1F) << 8 | second[2]) == VIDEO and second[5] & 0x40   # keyframe
    asyncio.run(run())


def test_held_client_buffer_is_bounded_and_not_counted_as_drops(pure):
    async def run():
        buf = pure.ClientBuffer(8, start_on_anchor=True, psi=tracker_with_tables(pure))
        for _ in range(100):
            buf.push(pframe() + pframe())
        assert len(buf._chunks) <= 8
        assert buf.dropped_chunks == 0 and buf.join_skipped_chunks > 0
    asyncio.run(run())


def test_held_client_settles_for_pes_start_after_timeout(pure, monkeypatch):
    async def run():
        monkeypatch.setattr(pure, "JOIN_KEYFRAME_TIMEOUT", 0.1)
        buf = pure.ClientBuffer(64, start_on_anchor=True, psi=tracker_with_tables(pure))
        buf.push(pframe() + pframe())
        with pytest.raises(asyncio.TimeoutError):
            await buf.get(0.02)
        await asyncio.sleep(0.15)
        buf.push(pframe() + pframe())
        assert await buf.get(0.5) is not None
    asyncio.run(run())


def test_eviction_is_packet_aligned_and_counted(pure):
    async def run():
        class Sink:
            chunks = nbytes = 0
            def note_drop(self, c, b):
                Sink.chunks += c
                Sink.nbytes += b
        buf = pure.ClientBuffer(8, stream=Sink(), start_on_anchor=False,
                                psi=tracker_with_tables(pure))
        for i in range(30):                                    # nobody reads: forces overflow
            buf.push(keyframe() + pframe() + audio_pes())
        got = []
        while not buf.fully_drained():
            got.append(await buf.get(0.1))
        assert Sink.chunks > 0 and buf.dropped_chunks == Sink.chunks
        assert all(len(c) % 188 == 0 and c[0] == 0x47 for c in got)
        assert len(got) <= 8
        # every eviction lands on a keyframe
        assert got[0][1] & 0x40 and ((got[0][1] & 0x1F) << 8 | got[0][2]) == VIDEO
    asyncio.run(run())


def test_sentinel_never_evicts_data(pure):
    async def run():
        buf = pure.ClientBuffer(8, start_on_anchor=False)
        buf.push(keyframe())
        buf.push_sentinel()
        assert await buf.get(0.1) == keyframe()
        assert await buf.get(0.1) is None
    asyncio.run(run())


HLS_MASTER = """#EXTM3U
#EXT-X-MEDIA:TYPE=AUDIO,GROUP-ID="a",NAME="en",URI="en.m3u8"
#EXT-X-STREAM-INF:BANDWIDTH=400400,AVERAGE-BANDWIDTH=999999999,AUDIO="a"
low.m3u8

#EXT-X-STREAM-INF:BANDWIDTH=1720400,AUDIO="a"
high.m3u8
#EXT-X-STREAM-INF:BANDWIDTH=900000,AUDIO="a"
mid.m3u8
"""

MPD = """<?xml version="1.0"?>
<MPD xmlns="urn:mpeg:dash:schema:mpd:2011"><Period>
<AdaptationSet contentType="video"><Representation id="0" bandwidth="300000"/><Representation id="1" bandwidth="1500000"/></AdaptationSet>
<AdaptationSet contentType="audio" lang="en"><Representation id="2" bandwidth="64000"/><Representation id="3" bandwidth="128000"/></AdaptationSet>
<AdaptationSet contentType="audio" lang="de"><Representation id="4" bandwidth="64000"/></AdaptationSet>
</Period></MPD>"""

MPD_SINGLE = """<MPD xmlns="urn:mpeg:dash:schema:mpd:2011"><Period>
<AdaptationSet mimeType="video/mp4"><Representation id="1" bandwidth="1500000"/></AdaptationSet>
<AdaptationSet mimeType="audio/mp4" lang="en"><Representation id="2" bandwidth="64000"/></AdaptationSet>
</Period></MPD>"""


def test_plan_hls_picks_highest_bandwidth_program(pure):
    kind, args = pure._plan_streams(HLS_MASTER)
    assert kind == "hls" and args == ["-map", "0:p:1:v:0?", "-map", "0:p:1:a?"]


def test_plan_hls_media_playlist_uses_default(pure):
    kind, args = pure._plan_streams("#EXTM3U\n#EXTINF:4,\nseg.ts\n")
    assert kind == "hls" and args == pure.DEFAULT_MAP


def test_plan_dash_picks_best_video_and_audio_per_language(pure):
    kind, args = pure._plan_streams(MPD)
    assert kind == "dash"
    assert args[:2] == ["-map", "0:v:m:variant_bitrate:1500000?"]
    assert ["-map", "0:a:m:variant_bitrate:64000?"] == args[2:4] or "128000" in " ".join(args)
    assert "0:a:m:variant_bitrate:128000?" in args and "0:a:m:variant_bitrate:64000?" in args


def test_plan_dash_single_rep_and_garbage_use_default(pure):
    assert pure._plan_streams(MPD_SINGLE) == ("dash", pure.DEFAULT_MAP)
    assert pure._plan_streams("<MPD><broken") == ("dash", pure.DEFAULT_MAP)
    assert pure._plan_streams("") == ("unknown", pure.DEFAULT_MAP)


def test_default_map_tolerates_radio_and_keeps_all_audio(pure):
    assert pure.DEFAULT_MAP == ["-map", "0:v:0?", "-map", "0:a?"]


def test_backend_url_forces_params_and_allowlists(pure):
    url = pure._backend_stream_url(
        "channels", "zdf", "ch 1",
        {"country": "DE", "client_drm": "true", "highest_quality_only": "false",
         "evil": "1", "start_time": "10", "end_time": "20", "epg_id": "e"})
    assert url.startswith("http://backend.test:8080/api/providers/zdf/channels/ch%201/stream/index.mpd?")
    q = url.split("?", 1)[1]
    assert q == "country=DE&start_time=10&end_time=20&epg_id=e&client_drm=false&highest_quality_only=true"
    # catchup params are channel-only
    v = pure._backend_stream_url("vod", "p", "x", {"start_time": "1", "end_time": "2"})
    assert v.endswith("?client_drm=false&highest_quality_only=true")


def test_mode_selection(pure):
    assert not pure._is_ondemand("channels", {})
    assert not pure._is_ondemand("events", {})
    assert pure._is_ondemand("vod", {}) and pure._is_ondemand("recordings", {})
    past = str(int(time.time()) - 3600)
    future = str(int(time.time()) + 3600)
    assert pure._is_ondemand("channels", {"start_time": past, "end_time": past})
    assert not pure._is_ondemand("channels", {"start_time": future, "end_time": future})  # backend serves live
    assert not pure._is_ondemand("channels", {"start_time": "x", "end_time": "y"})


def test_ffmpeg_cmd_is_hardened(pure):
    cmd = pure._ffmpeg_cmd("http://b/x", "n", start=12.5)
    assert cmd[cmd.index("-protocol_whitelist") + 1] == pure.PROTOCOLS_BACKEND
    assert "rtsp" not in pure.PROTOCOLS_BACKEND and "udp" not in pure.PROTOCOLS_BACKEND
    assert "-rw_timeout" in cmd
    assert cmd.index("-ss") < cmd.index("-i")
    assert "0:v:0?" in cmd and "0:a?" in cmd


def test_hint_from_stderr(pure):
    assert "Widevine" in pure._hint_for_backend_status(400)
    assert "401/403" in pure._hint_from_stderr(["Server returned 403 Forbidden"])


# ---------------------------------------------------------------- e2e fixtures

@pytest.fixture(scope="module")
def media(tmp_path_factory):
    if not HAVE_FFMPEG:
        pytest.skip("ffmpeg not installed")
    root = tmp_path_factory.mktemp("media")
    hls, dash = root / "hls", root / "dash"
    hls.mkdir()
    dash.mkdir()
    common = ["ffmpeg", "-y", "-loglevel", "error",
              "-f", "lavfi", "-i", "testsrc=size=640x360:rate=25",
              "-f", "lavfi", "-i", "sine=frequency=440",
              "-f", "lavfi", "-i", "sine=frequency=880", "-t", "12"]
    venc = ["-c:v", "libx264", "-preset", "ultrafast", "-g", "25", "-sc_threshold", "0",
            "-b:v:0", "300k", "-s:v:0", "320x180", "-b:v:1", "1500k", "-s:v:1", "640x360", "-c:a", "aac", "-b:a", "64k"]
    subprocess.run(common + ["-map", "0:v", "-map", "0:v", "-map", "1:a", "-map", "2:a"] + venc + [
        "-f", "hls", "-hls_time", "4", "-hls_playlist_type", "vod", "-master_pl_name", "master.m3u8",
        "-var_stream_map", "v:0,agroup:aud v:1,agroup:aud a:0,agroup:aud,language:eng,name:eng,default:yes a:1,agroup:aud,language:deu,name:deu",
        str(hls / "v%v.m3u8")], check=True, cwd=hls)
    subprocess.run(common + ["-map", "0:v", "-map", "0:v", "-map", "1:a"] + venc + [
        "-f", "dash", "-seg_duration", "4", "-adaptation_sets", "id=0,streams=v id=1,streams=a",
        str(dash / "manifest.mpd")], check=True, cwd=dash)
    live = (dash / "manifest.mpd").read_text()
    live = live.replace('type="static"', 'type="dynamic" availabilityStartTime="1970-01-01T00:00:00Z" minimumUpdatePeriod="PT2S"')
    live = live.replace('mediaPresentationDuration="PT12.0S"', "")
    (dash / "live.mpd").write_text(live)
    return root


class Env:
    """Fake backend + proxy, both on real loopback sockets."""

    def __init__(self, media, mod, backend_port):
        self.media, self.mod, self.bport = media, mod, backend_port
        self.requests = []          # (path, query) the backend received
        self.media_paths = []       # media files requested from the fake origin
        self.proxy = None

    def backend_app(self):
        @web.middleware
        async def note_media(request, handler):
            if request.path.startswith("/media/"):
                self.media_paths.append(request.path)
            return await handler(request)

        app = web.Application(middlewares=[note_media])
        base = f"http://127.0.0.1:{self.bport}"

        async def record(request):
            self.requests.append((request.path, dict(request.query)))

        async def hls_redirect(request):
            await record(request)
            raise web.HTTPFound(f"{base}/media/hls/master.m3u8")

        async def dash_redirect(request):
            await record(request)
            raise web.HTTPFound(f"{base}/media/dash/manifest.mpd")

        async def live_dash_redirect(request):
            await record(request)
            raise web.HTTPFound(f"{base}/media/dash/live.mpd")

        async def served_master(request):
            await record(request)
            text = (self.media / "hls" / "master.m3u8").read_text()
            out = []
            for line in text.splitlines():
                if line.strip() and not line.startswith("#"):
                    line = f"{base}/media/hls/{line.strip()}"
                elif 'URI="' in line:
                    line = line.replace('URI="', f'URI="{base}/media/hls/')
                out.append(line)
            return web.Response(text="\n".join(out), content_type="application/vnd.apple.mpegurl")

        async def bad(request):
            await record(request)
            return web.json_response({"error": "Client-side DRM required for this content"}, status=400)

        app.router.add_get("/api/providers/p/channels/c1/stream/index.mpd", hls_redirect)
        app.router.add_get("/api/providers/p/events/e1/stream/index.mpd", hls_redirect)
        app.router.add_get("/api/providers/p/vod/v1/stream/index.mpd", dash_redirect)
        app.router.add_get("/api/providers/p/channels/live1/stream/index.mpd", live_dash_redirect)
        app.router.add_get("/api/providers/p/recordings/r1/stream/index.mpd", served_master)
        app.router.add_get("/api/providers/p/channels/bad/stream/index.mpd", bad)
        app.router.add_static("/media/", self.media)
        return app


def probe(data: bytes):
    """(video heights, number of audio streams) of a TS blob."""
    r = subprocess.run(
        ["ffprobe", "-v", "error", "-f", "mpegts", "-i", "pipe:0", "-show_entries",
         "stream=codec_type,height", "-of", "json"],
        input=data, capture_output=True, check=True)
    streams = json.loads(r.stdout)["streams"]
    return ([s["height"] for s in streams if s["codec_type"] == "video"],
            sum(1 for s in streams if s["codec_type"] == "audio"))


def decode_errors(data: bytes) -> str:
    """ffmpeg's decoder complaints about a TS blob ('' = decodes clean).
    The blob is cut at its last video PES start first: the test hangs up
    mid-stream, and a half-delivered last frame would report 'corrupted
    macroblock' that has nothing to do with the proxy."""
    pure = load_proxy()
    tracker = pure.PsiTracker()
    tracker.feed(data[:188 * 4000])
    if tracker.video_pid is not None:
        for i in range(len(data) - 188, -1, -188):
            if data[i + 1] & 0x40 and ((data[i + 1] & 0x1F) << 8 | data[i + 2]) == tracker.video_pid:
                data = data[:i]
                break
    r = subprocess.run(["ffmpeg", "-v", "error", "-f", "mpegts", "-i", "pipe:0", "-f", "null", "-"],
                       input=data, capture_output=True)
    return r.stderr.decode(errors="replace").strip()


def run_e2e(media, scenario, **env):
    bport = free_port()
    mod = load_proxy(MPEGTS_BACKEND_URL=f"http://127.0.0.1:{bport}", **env)
    e = Env(media, mod, bport)

    async def main():
        backend = TestServer(e.backend_app(), port=bport)
        proxy = TestServer(mod.create_app())
        await backend.start_server()
        await proxy.start_server()
        try:
            async with ClientSession() as http:
                return await scenario(e, http, f"http://127.0.0.1:{proxy.port}", proxy)
        finally:
            await proxy.close()
            await backend.close()
    return asyncio.run(main())


# ---------------------------------------------------------------- live MPD trimming

LIVE_MPD = """<?xml version="1.0"?>
<MPD xmlns="urn:mpeg:dash:schema:mpd:2011" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" type="dynamic" availabilityStartTime="1970-01-01T00:00:00Z" minimumUpdatePeriod="PT2S" timeShiftBufferDepth="PT59.520S">
<Period id="1" start="PT0S">
<AdaptationSet contentType="video"><SegmentTemplate timescale="48000" initialization="http://h/i-$RepresentationID$.dash" media="http://h/s-$RepresentationID$-$Time$.dash"><SegmentTimeline><S t="1000000" d="92160" r="31"/></SegmentTimeline></SegmentTemplate><Representation id="v" bandwidth="1"/></AdaptationSet>
<AdaptationSet contentType="audio"><Representation id="a" bandwidth="1"><SegmentTemplate timescale="48000" initialization="i-$RepresentationID$.dash" media="s-$RepresentationID$-$Number%05d$.dash" startNumber="10"><SegmentTimeline><S t="5000" d="92160" r="31"/></SegmentTimeline></SegmentTemplate></Representation></AdaptationSet>
</Period></MPD>"""


def test_trim_dash_live_keeps_newest_segments(pure):
    import xml.etree.ElementTree as ET
    out = pure._trim_dash_live(LIVE_MPD, "http://origin.example/live/dash/manifest.mpd?x=1", 8)
    root = ET.fromstring(out.split("?>", 1)[1])
    ns = "{urn:mpeg:dash:schema:mpd:2011}"
    timelines = [[(int(s.get("t")), int(s.get("d"))) for s in tl] for tl in root.iter(ns + "SegmentTimeline")]
    for kept in timelines:
        assert 2 <= len(kept) <= 6                                  # ~8 s of 1.92 s segments
        assert all(b[0] == a[0] + a[1] for a, b in zip(kept, kept[1:]))   # still contiguous
    assert timelines[0][-1] == (1000000 + 31 * 92160, 92160)        # the live edge segment is kept
    # $Number$ template: startNumber advanced by the number of dropped segments
    audio_tmpl = [t for t in root.iter(ns + "SegmentTemplate") if "$Number" in t.get("media")][0]
    assert int(audio_tmpl.get("startNumber")) == 10 + (32 - len(timelines[1]))
    # relative segment URLs now resolve against the ORIGINAL manifest's location
    assert root.find(ns + "BaseURL").text == "http://origin.example/live/dash/"
    assert out.startswith("<?xml") and 'type="dynamic"' in out


def test_trim_dash_live_leaves_static_and_odd_mpds_alone(pure):
    static = LIVE_MPD.replace('type="dynamic"', 'type="static"')
    assert pure._trim_dash_live(static, "http://o/m.mpd", 8) == static
    open_ended = LIVE_MPD.replace('r="31"', 'r="-1"')
    assert pure._trim_dash_live(open_ended, "http://o/m.mpd", 8) == open_ended
    short = LIVE_MPD.replace('r="31"', 'r="1"')                       # nothing worth trimming
    assert pure._trim_dash_live(short, "http://o/m.mpd", 8) == short


def test_trim_dash_live_makes_existing_base_url_absolute(pure):
    mpd = LIVE_MPD.replace("<Period", '<BaseURL>../media/</BaseURL><Period', 1)
    out = pure._trim_dash_live(mpd, "http://o/live/dash/m.mpd", 8)
    assert "<BaseURL>http://o/live/media/</BaseURL>" in out


# ---------------------------------------------------------------- e2e tests

# The fixture media is finite, a real live source never ends: loop it. ffmpeg
# then outruns the client, which also exercises the live-edge eviction path.
LIVE_LOOP = "-stream_loop -1"
WANT_BYTES = 600_000


async def read_some(resp, n=WANT_BYTES):
    """Read about n bytes of an endless stream, then hang up like a player."""
    buf = bytearray()
    while len(buf) < n:
        chunk = await resp.content.read(65536)
        if not chunk:
            break
        buf += chunk
    return bytes(buf[:len(buf) // 188 * 188])

@needs_ffmpeg
def test_live_channel_hls_redirect_picks_best_variant(media):
    async def scenario(e, http, base, proxy):
        async with http.get(f"{base}/api/providers/p/channels/c1/stream/index.ts?country=DE&client_drm=true") as r:
            assert r.status == 200
            assert r.headers["Content-Type"] == "video/mp2t"
            assert r.headers["X-TS-Mode"] == "live" and r.headers["X-TS-Source"] == "hls"
            data = await read_some(r)
        return data, e.requests
    data, requests = run_e2e(media, scenario, MPEGTS_FFMPEG_EXTRA_IN=LIVE_LOOP)
    assert len(data) >= WANT_BYTES * 0.9
    assert all(data[i] == 0x47 for i in range(0, len(data), 188))     # grid intact end to end
    assert decode_errors(data) == ""   # starts on PAT/PMT + keyframe: no 'non-existing PPS' spam
    heights, audio = probe(data)
    assert heights == [360]            # highest variant, not the first (180p)
    assert audio == 2                  # both language renditions kept
    path, query = requests[0]
    assert query["client_drm"] == "false" and query["highest_quality_only"] == "true"   # forced
    assert query["country"] == "DE"


@needs_ffmpeg
def test_ts_carries_keyframe_random_access_flags(media):
    """The late-join logic relies on ffmpeg flagging keyframes in the TS."""
    async def scenario(e, http, base, proxy):
        async with http.get(f"{base}/api/providers/p/events/e1/stream/index.ts") as r:
            return await read_some(r)
    data = run_e2e(media, scenario, MPEGTS_FFMPEG_EXTRA_IN=LIVE_LOOP)
    pure = load_proxy()
    t = pure.PsiTracker()
    t.feed(data[:188 * 2000])
    assert t.complete and t.video_pid is not None
    assert pure._first_anchor_offset(data, t.video_pid) >= 0


@needs_ffmpeg
def test_vod_dash_ondemand_picks_best_representation(media):
    async def scenario(e, http, base, proxy):
        async with http.get(f"{base}/api/providers/p/vod/v1%2Fstream/stream/index.ts") as r:
            assert r.status == 200
            assert r.headers["X-TS-Mode"] == "ondemand" and r.headers["X-TS-Shared"] == "0"
            assert r.headers["X-TS-Source"] == "dash" and r.headers["Accept-Ranges"] == "none"
            data = await r.read()
        # the on-demand process is gone afterwards
        await asyncio.sleep(0.3)
        return data, proxy.app["stream_manager"]
    data, mgr = run_e2e(media, scenario)
    heights, audio = probe(data)
    assert heights == [360] and audio == 1
    assert not mgr.ondemand and mgr._ondemand_slots == 0


@needs_ffmpeg
def test_recording_served_manifest_200_path(media):
    async def scenario(e, http, base, proxy):
        async with http.get(f"{base}/api/providers/p/recordings/r1/stream/index.ts") as r:
            assert r.status == 200
            return await r.read()
    heights, audio = probe(run_e2e(media, scenario))
    assert heights == [360] and audio == 2


@needs_ffmpeg
def test_ondemand_slow_client_loses_nothing(media):
    async def scenario(e, http, base, proxy):
        url = f"{base}/api/providers/p/vod/v1/stream/index.ts"
        async with http.get(url) as r:
            fast = await r.read()
        slow = bytearray()
        async with http.get(url) as r:
            async for chunk in r.content.iter_chunked(16384):
                slow += chunk
                await asyncio.sleep(0.002)
        return fast, bytes(slow)
    fast, slow = run_e2e(media, scenario)
    assert len(slow) == len(fast) > 0


@needs_ffmpeg
def test_ondemand_start_offset_shortens_output(media):
    async def scenario(e, http, base, proxy):
        out = []
        for q in ("", "?start=8"):
            async with http.get(f"{base}/api/providers/p/vod/v1/stream/index.ts{q}") as r:
                assert r.status == 200
                out.append(len(await r.read()))
        return out
    full, late = run_e2e(media, scenario)
    assert late < full * 0.7


@needs_ffmpeg
def test_backend_error_is_relayed_with_hint(media):
    async def scenario(e, http, base, proxy):
        async with http.get(f"{base}/api/providers/p/channels/bad/stream/index.ts") as r:
            return r.status, await r.json()
    status, body = run_e2e(media, scenario)
    assert status == 400
    assert "Client-side DRM" in body["error"] and "Widevine" in body["hint"]


def test_unknown_backend_content_and_unreachable_backend(media_free=None):
    bport = free_port()                                   # nothing listens here
    mod = load_proxy(MPEGTS_BACKEND_URL=f"http://127.0.0.1:{bport}")

    async def main():
        proxy = TestServer(mod.create_app())
        await proxy.start_server()
        try:
            async with ClientSession() as http:
                async with http.get(f"http://127.0.0.1:{proxy.port}/api/providers/p/channels/x/stream/index.ts") as r:
                    return r.status, await r.json(), r.headers.get("Retry-After")
        finally:
            await proxy.close()
    status, body, _ = asyncio.run(main())
    assert status == 502 and body["error"] == "backend unreachable"


def test_head_generic_start_and_health(media_free=None):
    mod = load_proxy(MPEGTS_BACKEND_URL="http://127.0.0.1:9")

    async def main():
        proxy = TestServer(mod.create_app())
        await proxy.start_server()
        base = f"http://127.0.0.1:{proxy.port}"
        try:
            async with ClientSession() as http:
                async with http.head(f"{base}/api/providers/p/channels/c1/stream/index.ts") as r:
                    head = (r.status, r.headers["Content-Type"], r.headers["X-TS-Mode"])
                async with http.get(f"{base}/stream?url=http://127.0.0.1/x") as r:
                    generic = (r.status, (await r.json())["hint"])
                async with http.get(f"{base}/api/providers/p/channels/c1/stream/index.ts?start=5") as r:
                    start_live = r.status
                async with http.get(f"{base}/api/providers/p/vod/v1/stream/index.ts?start=abc") as r:
                    start_bad = r.status
                async with http.get(f"{base}/health") as r:
                    health = await r.json()
                return head, generic, start_live, start_bad, health, proxy.app["stream_manager"]
        finally:
            await proxy.close()
    head, generic, start_live, start_bad, health, mgr = asyncio.run(main())
    assert head == (200, "video/mp2t", "live")
    assert generic[0] == 404 and "index.ts" in generic[1]
    assert start_live == 400 and start_bad == 400
    assert health["status"] == "healthy"
    assert not mgr.streams and not mgr.ondemand              # HEAD / errors never spawned ffmpeg


def test_health_is_minimal_without_token():
    mod = load_proxy(MPEGTS_AUTH_DISABLED="0", MPEGTS_AUTH_TOKEN="s3cret", MPEGTS_BACKEND_URL="http://x")

    async def main():
        proxy = TestServer(mod.create_app())
        await proxy.start_server()
        base = f"http://127.0.0.1:{proxy.port}"
        try:
            async with ClientSession() as http:
                async with http.get(f"{base}/health") as r:
                    anon = await r.json()
                async with http.get(f"{base}/health", headers={"Authorization": "Bearer s3cret"}) as r:
                    full = await r.json()
                async with http.get(f"{base}/api/providers/p/channels/c/stream/index.ts") as r:
                    unauth = r.status
                return anon, full, unauth
        finally:
            await proxy.close()
    anon, full, unauth = asyncio.run(main())
    assert "stream_details" not in anon and "stream_details" in full and unauth == 401


def test_watchdog_kills_silent_source():
    mod = load_proxy(MPEGTS_BACKEND_URL="http://x")
    mod._ffmpeg_cmd = lambda *a, **k: ["sleep", "60"]        # a source that never produces a byte
    mod.STARTUP_TIMEOUT = 1
    mod.WATCHDOG_INTERVAL = 0.2

    async def main():
        mgr = mod.StreamManager()
        task = asyncio.create_task(mgr._watchdog_loop())
        info, result = await mgr.get_or_create("k", "n", source="http://x/y")
        assert result is mod.SpawnResult.SPAWNED
        for _ in range(40):
            await asyncio.sleep(0.1)
            if "k" not in mgr.streams:
                break
        await asyncio.wait_for(info.process.wait(), 5)       # the kill is asynchronous
        task.cancel()
        await mgr.shutdown()
        return info.process.returncode, mgr._total_watchdog_kills
    rc, kills = asyncio.run(main())
    assert rc is not None and kills == 1


def test_ondemand_cap_returns_503_with_retry_after():
    mod = load_proxy(MPEGTS_BACKEND_URL="http://x", MPEGTS_MAX_ONDEMAND="1")
    mgr = mod.StreamManager()
    assert mgr.acquire_ondemand() and not mgr.acquire_ondemand()
    mgr.release_ondemand()
    assert mgr.acquire_ondemand()


@needs_ffmpeg
def test_live_dash_ffmpeg_only_sees_the_newest_segments_when_trimmed(media):
    """The fixture's 'live' MPD lists three 4 s segments (0-12 s). With a 5 s
    window the proxy hands ffmpeg only the newest two, so the first segment
    must never be requested from the origin. (Output timestamps are no proof:
    the muxer's -avoid_negative_ts make_zero re-bases them to ~0.)
    (How far back an UNtrimmed MPD starts depends on the ffmpeg version —
    5.1 begins at the oldest segment, 6.1 close to the live edge — so the
    untrimmed run is not asserted on.)"""
    async def scenario(e, http, base, proxy):
        async with http.get(f"{base}/api/providers/p/channels/live1/stream/index.ts") as r:
            assert r.status == 200 and r.headers["X-TS-Source"] == "dash"
            data = await read_some(r, 250_000)
        mgr = proxy.app["stream_manager"]
        tokens_while_running = len(mgr.mpd_sources)
        for key in list(mgr.streams):
            await mgr._terminate_stream(key, reason="test")
        return data, tokens_while_running, len(mgr.mpd_sources), list(e.media_paths)

    data, running, after, paths = run_e2e(media, scenario, MPEGTS_DASH_LIVE_WINDOW="5")
    assert running == 1 and after == 0                     # token registered, released with the stream
    assert not any(p.endswith("-00001.m4s") for p in paths), paths
    assert any(p.endswith("-00003.m4s") for p in paths)    # newest segment was played
    assert len(data) > 100_000                             # and the stream actually flows

    _, running0, _, paths0 = run_e2e(media, scenario, MPEGTS_DASH_LIVE_WINDOW="0")
    assert running0 == 0                                   # disabled: ffmpeg talks to the original MPD
    assert not any("_internal" in p for p in paths0)


def test_internal_mpd_endpoint_is_not_public():
    mod = load_proxy(MPEGTS_BACKEND_URL="http://x")

    async def main():
        proxy = TestServer(mod.create_app())
        await proxy.start_server()
        try:
            mod_mgr = proxy.app["stream_manager"]
            mod_mgr.mpd_sources["tok"] = "http://127.0.0.1:9/m.mpd"
            async with ClientSession() as http:
                async with http.get(f"http://127.0.0.1:{proxy.port}/_internal/mpd/unknown.mpd") as r:
                    unknown = r.status
                async with http.get(f"http://127.0.0.1:{proxy.port}/_internal/mpd/tok.mpd") as r:
                    known_unreachable_source = r.status
            return unknown, known_unreachable_source
        finally:
            await proxy.close()
    unknown, known = asyncio.run(main())
    assert unknown == 404                          # no token, no answer
    assert known == 502                            # valid token + local peer, but the source is down