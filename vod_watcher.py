#!/usr/bin/env python3
"""
vod_watcher.py — 24 / 7 YouTube & Twitch VOD recorder for Raspberry Pi 4
=========================================================================
OFF           – channel offline
LIVE (no rec) – live but keyword not found → not recording
LIVE / REC    – live, keyword match → recording
"""

import asyncio, contextlib, csv, datetime as dt, json, os, re, subprocess, sys, time
from pathlib import Path
from typing import Dict, Optional, Tuple

# ───── configuration ───── #
CHECK_FILE      = Path.home() / "code" / "vod_watch" / "checkme.txt"
VOD_ROOT        = Path("/mnt/media/VODs/processed")
LOG_ROOT        = Path("/mnt/media/logs/processed")

RELOAD_INTERVAL = 180      # reload checkme.txt every 3 min
PROBE_INTERVAL  = 180      # each channel’s own probe cycle (baseline)
PLATFORM_COOLDOWN = 60     # min gap between two probes on *same* platform
DASH_FPS        = 1        # dashboard refresh rate (frames/sec)

MAX_YT_HEIGHT   = 1080
CLEAR_CMD       = "clear" if os.getenv("TERM") else "cls"

USE_COLOR = sys.stdout.isatty() and ("TERM" in os.environ)
RED, YELLOW, GREEN, RESET = (
    ("\033[91m", "\033[93m", "\033[92m", "\033[0m") if USE_COLOR else ("", "", "", "")
)

DATE_RE = re.compile(r"\b\d{4}-\d{2}-\d{2}$")

# ───── shared cool‑down state ───── #
_last_probe_time: Dict[str, float] = {"youtube": 0.0, "twitch": 0.0}
_platform_locks: Dict[str, asyncio.Lock] = {
    "youtube": asyncio.Lock(),
    "twitch":  asyncio.Lock()
}
_spawn_count: Dict[str, int] = {"youtube": 0, "twitch": 0}
_platform_counts: Dict[str, int] = {"youtube": 1, "twitch": 1}

def platform_interval(platform: str) -> int:
    n = max(1, _platform_counts.get(platform, 1))
    return max(PROBE_INTERVAL, PLATFORM_COOLDOWN * n)

async def wait_for_platform_slot(platform: str) -> float:
    lock = _platform_locks[platform]
    async with lock:
        now = time.time()
        scheduled = max(now, _last_probe_time[platform] + PLATFORM_COOLDOWN)
        _last_probe_time[platform] = scheduled
        gap = scheduled - now
        if gap > 0:
            await asyncio.sleep(gap)
        return scheduled

# ───── helper functions ───── #
def yt_live_url(name: str) -> str:
    name = name.strip()
    if name.startswith("@"):
        path = name
    elif re.match(r"UC[A-Za-z0-9_-]{22}", name):
        path = f"channel/{name}"
    else:
        path = f"@{name}"
    return f"https://www.youtube.com/{path}/live"

def strip_end_date_time(text: str) -> str:
    parts = text.rstrip().split()
    if len(parts) >= 2 and DATE_RE.fullmatch(parts[-2]):
        return " ".join(parts[:-2]).rstrip(" -_/")
    return text

# ───── ChannelTask ───── #
class ChannelTask:
    def __init__(self, platform: str, name: str, keyword: str):
        self.platform = platform.lower().strip()
        self.name     = name.strip()
        self.keyword  = keyword.lower().strip()

        self.proc: Optional[subprocess.Popen] = None
        self.current_title = "<awaiting check>"
        self.live_raw: Optional[bool] = None
        self.keyword_ok = False

        idx = _spawn_count[self.platform]
        _spawn_count[self.platform] = idx + 1
        self.next_probe = time.time() + idx * PLATFORM_COOLDOWN
        self.loop: Optional[asyncio.Task] = None

    def start(self):
        self.loop = asyncio.create_task(self._poll_loop())

    async def stop(self):
        if self.loop:
            self.loop.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self.loop
        await self._stop_recording()

    def is_recording(self) -> bool:
        return self.proc and self.proc.poll() is None

    # ── poll loop ── #
    async def _poll_loop(self):
        while True:
            # wait for per‑platform cool‑down
            scheduled = await wait_for_platform_slot(self.platform)

            interval = platform_interval(self.platform)
            self.next_probe = scheduled + interval
            try:
                live, ok, title = await self._probe()
                self.live_raw, self.keyword_ok = live, ok

                if live:
                    title = strip_end_date_time(title or "")
                    if title:
                        self.current_title = title
                    elif not self.current_title:
                        self.current_title = "<title unavailable>"
                else:
                    self.current_title = "<not live>"

                if live and ok and not self.is_recording():
                    await self._start_recording(self.current_title)
                elif (not live or not ok) and self.is_recording():
                    await self._stop_recording()
            except Exception as e:
                print(f"[{self.name}] probe error: {e}", file=sys.stderr)

            await asyncio.sleep(interval)

    # ── probes ── #
    async def _probe(self) -> Tuple[bool, bool, str]:
        return await (self._probe_youtube() if self.platform == "youtube" else self._probe_twitch())

    async def _probe_youtube(self):
        url = yt_live_url(self.name)
        cmd = ["yt-dlp", "--skip-download", "--print", "%(is_live)s|%(title)s", url]

        proc = await asyncio.create_subprocess_exec(*cmd,
                    stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.DEVNULL)
        raw, _ = await proc.communicate()

        # append raw line to probe.log
        dt_now = dt.datetime.now().isoformat(timespec="seconds")
        probe_log = (LOG_ROOT / self.name / "probe.log")
        probe_log.parent.mkdir(parents=True, exist_ok=True)
        with probe_log.open("a", encoding="utf-8") as fh:
            fh.write(f"{dt_now} | {raw.decode(errors='replace').rstrip()}\n")

        if not raw:
            return False, False, ""

        try:
            flag, title = raw.decode().strip().split("|", 1)
        except ValueError:
            return False, False, ""

        live = flag.strip().lower() == "true"
        keyword_ok = (not self.keyword) or (self.keyword in title.lower())
        return live, keyword_ok, title

    async def _probe_twitch(self):
        url = f"https://twitch.tv/{self.name}"
        cmd = ["streamlink", "--json", url, "best"]
        p = await asyncio.create_subprocess_exec(*cmd,
                    stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
        out, _ = await p.communicate()
        if p.returncode != 0:
            return False, False, ""

        try:
            info = json.loads(out)
        except json.JSONDecodeError:
            return False, False, ""

        meta   = info.get("metadata", {})
        title  = meta.get("title") or info.get("title") or ""
        tags   = meta.get("tags")  or info.get("tags")  or []
        tagstr = " ".join(tags) if isinstance(tags, (list, tuple)) else str(tags)
        keyword_ok = (not self.keyword) or (self.keyword in f"{title} {tagstr}".lower())
        return True, keyword_ok, title

    # ── recording helpers ── #
    def _paths(self, title: str):
        title = strip_end_date_time(title)
        safe  = re.sub(r"[^\w\s.\-]+", "_", title)[:120].strip() or "live"
        day   = dt.datetime.now().strftime("%Y-%m-%d")
        vod_dir = VOD_ROOT / self.name
        log_dir = LOG_ROOT / self.name
        vod_dir.mkdir(parents=True, exist_ok=True)
        log_dir.mkdir(parents=True, exist_ok=True)
        base  = vod_dir / f"{day}_{safe}"
        idx   = 1
        vod   = base.with_suffix(".mp4")
        while vod.exists():
            idx += 1
            vod = vod_dir / f"{day}_{safe}_part-{idx}.mp4"
        log   = (log_dir / vod.stem).with_suffix(".log")
        return vod, log

    async def _start_recording(self, title: str):
        vod_fp, log_fp = self._paths(title)
        if self.platform == "youtube":
            url = yt_live_url(self.name)
            cmd = ["yt-dlp", url, "-o", str(vod_fp),
                   "-f", f"bestvideo[height<=?{MAX_YT_HEIGHT}]+bestaudio/best[height<=?{MAX_YT_HEIGHT}]",
                   "--merge-output-format", "mp4", "--no-part"]
        else:
            url = f"https://twitch.tv/{self.name}"
            cmd = ["streamlink", url, "best",
                   "--twitch-disable-hosting", "--twitch-disable-ads",
                   "-o", str(vod_fp)]
        print(f"[+] Recording {self.platform}::{self.name} → {vod_fp.name}")
        logf = open(log_fp, "a", buffering=1)
        logf.write(f"{dt.datetime.now().isoformat()}  START  {title}\n")
        self.proc = subprocess.Popen(cmd, stdout=logf, stderr=logf, text=True)

    async def _stop_recording(self):
        if self.is_recording():
            print(f"[ ] Stopping  {self.platform}::{self.name}")
            self.proc.terminate()
            try:
                await asyncio.wait_for(asyncio.to_thread(self.proc.wait), timeout=10)
            except asyncio.TimeoutError:
                self.proc.kill()
        self.proc = None

# ───── Supervisor ───── #
class Supervisor:
    def __init__(self):
        self.tasks: Dict[str, ChannelTask] = {}
        self.last_reload = 0.0
        self.stop_evt = asyncio.Event()
        self.reload_task: Optional[asyncio.Task] = None
        self.dash_task:   Optional[asyncio.Task] = None

    async def run(self):
        self.reload_task = asyncio.create_task(self._reload_loop())
        self.dash_task   = asyncio.create_task(self._dashboard_loop())
        await self.stop_evt.wait()
        for t in (self.reload_task, self.dash_task):
            if t: t.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await asyncio.gather(*(t for t in (self.reload_task, self.dash_task) if t))

    async def shutdown(self):
        self.stop_evt.set()
        for t in (self.reload_task, self.dash_task):
            if t: t.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await asyncio.gather(*(t for t in (self.reload_task, self.dash_task) if t))
        await asyncio.gather(*(task.stop() for task in self.tasks.values()), return_exceptions=True)

    # reload checkme.txt ------------------------------------------------------
    async def _reload_loop(self):
        await self._load_watchlist()
        while True:
            await asyncio.sleep(RELOAD_INTERVAL)
            await self._load_watchlist()

    async def _load_watchlist(self):
        self.last_reload = time.time()
        seen = set()
        if not CHECK_FILE.exists():
            return
        with CHECK_FILE.open() as fh:
            for row in csv.reader(fh):
                if not row or row[0].strip().startswith("#"):
                    continue
                platform, channel, *kw = [c.strip() for c in row]
                key = f"{platform.lower()}::{channel.lower()}"
                seen.add(key)
                if key not in self.tasks:
                    t = ChannelTask(platform, channel, kw[0] if kw else "")
                    self.tasks[key] = t
                    t.start()
        for key in list(self.tasks):
            if key not in seen:
                asyncio.create_task(self.tasks[key].stop())
                del self.tasks[key]

        _platform_counts.clear()
        for t in self.tasks.values():
            _platform_counts[t.platform] = _platform_counts.get(t.platform, 0) + 1

    # dashboard ---------------------------------------------------------------
    async def _dashboard_loop(self):
        while True:
            self._render_dash()
            await asyncio.sleep(1 / DASH_FPS)

    def _render_dash(self):
        reload_rem = max(0, int(self.last_reload + RELOAD_INTERVAL - time.time()))
        os.system(CLEAR_CMD)
        print(f"{dt.datetime.now():%Y-%m-%d %H:%M:%S}  –  VOD Watcher   "
              f"(Next reload {reload_rem}s)\n")
        print(f"{'Platform':8} {'Channel':20} {'Keyword':12} "
              f"{'State':14} {'Next':6} Title")
        print("-" * 135)

        for t in self.tasks.values():
            if t.live_raw is None:
                colour, state = YELLOW, "WAIT"
            elif t.live_raw:
                if t.keyword_ok and t.is_recording():
                    colour, state = GREEN, "LIVE / REC"
                elif not t.keyword_ok:
                    colour, state = YELLOW, "LIVE"
                else:
                    colour, state = YELLOW, "LIVE"
            else:
                colour, state = RED, "OFF"

            next_in = max(0, int(t.next_probe - time.time()))
            print(f"{t.platform:8} {t.name:20} {t.keyword[:11]:12} "
                  f"{colour}{state:14}{RESET} {next_in:6}s {t.current_title[:80]}")

# ───── entry ───── #
if __name__ == "__main__":
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    sup = Supervisor()
    try:
        loop.run_until_complete(sup.run())
    except KeyboardInterrupt:
        print("\nExiting cleanly…")
        loop.run_until_complete(sup.shutdown())
    finally:
        loop.close()
