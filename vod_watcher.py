#!/usr/bin/env python3
"""
vod_watcher.py — 24/7 YouTube & Twitch VOD recorder with logging, retries, colors, and clean shutdown
"""

import asyncio
import contextlib
import csv
import datetime as dt
import json
import logging
import logging.handlers
import os
import re
import subprocess
import sys
import time
from pathlib import Path
from typing import Dict, Optional, Tuple

from tenacity import AsyncRetrying, retry_if_exception_type, stop_after_attempt, wait_exponential

# ───── configuration ───── #
CHECK_FILE         = Path.home() / "code" / "vod_watch" / "checkme.txt"
VOD_ROOT           = Path("/mnt/media/VODs/processed")
LOG_ROOT           = Path("/mnt/media/logs/processed")
SCRIPT_DIR         = Path(__file__).parent.resolve()
LOG_DIR            = SCRIPT_DIR / "logs"
LOG_DIR.mkdir(parents=True, exist_ok=True)
MAIN_LOG           = LOG_DIR / "vod_watcher.log"
RELOAD_INTERVAL    = 180      # reload checkme.txt every 3 min
PROBE_INTERVAL     = 180      # baseline probe interval per channel
PLATFORM_COOLDOWN  = 60       # min gap between probes on same platform
DASH_FPS           = 1        # dashboard refresh rate (frames/sec)
MAX_YT_HEIGHT      = 1080
CLEAR_CMD          = "clear" if os.getenv("TERM") else "cls"

# ───── color setup ───── #
USE_COLOR = sys.stdout.isatty() and ("TERM" in os.environ)
if USE_COLOR:
    RED, YELLOW, GREEN, RESET = "\033[91m", "\033[93m", "\033[92m", "\033[0m"
else:
    RED = YELLOW = GREEN = RESET = ""

DATE_RE = re.compile(r"\b\d{4}-\d{2}-\d{2}$")

# ───── shared cooldown state ───── #
_last_probe_time: Dict[str, float] = {"youtube": 0.0, "twitch": 0.0}
_platform_locks: Dict[str, asyncio.Lock] = {
    "youtube": asyncio.Lock(),
    "twitch":  asyncio.Lock()
}
_spawn_count: Dict[str, int]   = {"youtube": 0, "twitch": 0}
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
        if scheduled > now:
            await asyncio.sleep(scheduled - now)
        return scheduled

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

# ───── logging setup ───── #
logger = logging.getLogger("vod_watcher")
logger.setLevel(logging.INFO)

# console handler
ch = logging.StreamHandler()
ch.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
logger.addHandler(ch)

# rotating file handler
file_handler = logging.handlers.RotatingFileHandler(
    str(MAIN_LOG),
    maxBytes=5_000_000,
    backupCount=3,
)
file_handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s"))
logger.addHandler(file_handler)

# ───── ChannelTask ───── #
class ChannelTask:
    def __init__(self, platform: str, name: str, keyword: str):
        self.platform     = platform.lower().strip()
        self.name         = name.strip()
        self.keyword      = keyword.lower().strip()
        self.proc: Optional[subprocess.Popen] = None

        self.current_title: str = "<awaiting check>"
        self.live_raw: Optional[bool] = None
        self.keyword_ok: bool = False

        idx = _spawn_count[self.platform]
        _spawn_count[self.platform] += 1
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
        return bool(self.proc and self.proc.poll() is None)

    async def _poll_loop(self):
        while True:
            scheduled = await wait_for_platform_slot(self.platform)
            interval  = platform_interval(self.platform)
            self.next_probe = scheduled + interval

            try:
                async for attempt in AsyncRetrying(
                    stop=stop_after_attempt(5),
                    wait=wait_exponential(multiplier=1, min=10, max=300),
                    retry=retry_if_exception_type((asyncio.TimeoutError, json.JSONDecodeError)),
                    reraise=True
                ):
                    with attempt:
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

            except Exception:
                logger.exception(f"{self.platform}::{self.name} probe failure")
            await asyncio.sleep(interval)

    async def _probe(self) -> Tuple[bool, bool, str]:
        if self.platform == "youtube":
            return await self._probe_youtube()
        else:
            return await self._probe_twitch()

    async def _probe_youtube(self) -> Tuple[bool,bool,str]:
        url = yt_live_url(self.name)
        cmd = [
            "yt-dlp", "--skip-download",
            "--print", "%(is_live)s|%(title)s", url
        ]
        proc = await asyncio.create_subprocess_exec(
            *cmd, stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.DEVNULL
        )
        raw, _ = await proc.communicate()

        # log to per-channel probe.log
        now = dt.datetime.now().isoformat()
        logdir = LOG_ROOT / self.name
        logdir.mkdir(parents=True, exist_ok=True)
        with (logdir / "probe.log").open("a", encoding="utf-8") as fh:
            fh.write(f"{now} | {raw.decode(errors='replace').rstrip()}\n")

        if not raw:
            return False, False, ""

        try:
            flag, title = raw.decode().strip().split("|", 1)
        except ValueError:
            return False, False, ""

        live       = flag.strip().lower() == "true"
        keyword_ok = (not self.keyword) or (self.keyword in title.lower())
        return live, keyword_ok, title

    async def _probe_twitch(self) -> Tuple[bool,bool,str]:
        url = f"https://twitch.tv/{self.name}"
        cmd = ["streamlink", "--json", url, "best"]
        p = await asyncio.create_subprocess_exec(
            *cmd, stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE
        )
        out, _ = await p.communicate()
        if p.returncode != 0:
            return False, False, ""

        info = json.loads(out)
        meta  = info.get("metadata", {}) or {}
        title = meta.get("title") or info.get("title") or ""
        tags  = meta.get("tags") or info.get("tags") or []
        tagstr = " ".join(tags) if isinstance(tags, (list,tuple)) else str(tags)
        keyword_ok = (not self.keyword) or (self.keyword in f"{title} {tagstr}".lower())
        return True, keyword_ok, title

    def _paths(self, title: str) -> Tuple[Path,Path]:
        title = strip_end_date_time(title)
        safe  = re.sub(r"[^\w\s.\-]+", "_", title)[:120].strip() or "live"
        day   = dt.datetime.now().strftime("%Y-%m-%d")
        vod_dir = VOD_ROOT / self.name
        log_dir = LOG_ROOT / self.name
        vod_dir.mkdir(parents=True, exist_ok=True)
        log_dir.mkdir(parents=True, exist_ok=True)

        base = vod_dir / f"{day}_{safe}"
        idx  = 1
        vod  = base.with_suffix(".mp4")
        while vod.exists():
            idx += 1
            vod = vod_dir / f"{day}_{safe}_part-{idx}.mp4"
        log = (log_dir / vod.stem).with_suffix(".log")
        return vod, log

    async def _start_recording(self, title: str):
        vod_fp, log_fp = self._paths(title)
        if self.platform == "youtube":
            url = yt_live_url(self.name)
            cmd = [
                "yt-dlp", url, "-o", str(vod_fp),
                "-f", f"bestvideo[height<=?{MAX_YT_HEIGHT}]+bestaudio/best[height<=?{MAX_YT_HEIGHT}]",
                "--merge-output-format", "mp4", "--no-part"
            ]
        else:
            url = f"https://twitch.tv/{self.name}"
            cmd = [
                "streamlink", url, "best",
                "--twitch-disable-hosting", "--twitch-disable-ads",
                "-o", str(vod_fp)
            ]
        logger.info(f"START {self.platform}::{self.name} → {vod_fp.name}")
        logf = open(log_fp, "a", buffering=1, encoding="utf-8")
        logf.write(f"{dt.datetime.now().isoformat()} START {title}\n")
        self.proc = subprocess.Popen(cmd, stdout=logf, stderr=logf, text=True)

    async def _stop_recording(self):
        if self.proc and self.proc.poll() is None:
            logger.info(f"STOP  {self.platform}::{self.name}")
            self.proc.terminate()
            try:
                await asyncio.wait_for(asyncio.to_thread(self.proc.wait), timeout=10)
            except asyncio.TimeoutError:
                self.proc.kill()
        self.proc = None

# ───── Supervisor ───── #
class Supervisor:
    def __init__(self):
        self.tasks: Dict[str,ChannelTask] = {}
        self.last_reload = 0.0
        self.stop_evt    = asyncio.Event()
        self.reload_task: Optional[asyncio.Task] = None
        self.dash_task:   Optional[asyncio.Task] = None

    async def run(self):
        self.reload_task = asyncio.create_task(self._reload_loop())
        self.dash_task   = asyncio.create_task(self._dashboard_loop())
        await self.stop_evt.wait()

    async def shutdown(self):
        logger.info("Shutting down supervisor…")
        self.stop_evt.set()
        for t in (self.reload_task, self.dash_task):
            if t: t.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await asyncio.gather(
                *(t for t in (self.reload_task, self.dash_task) if t),
                return_exceptions=True
            )
        await asyncio.gather(*(task.stop() for task in self.tasks.values()), return_exceptions=True)

    async def _reload_loop(self):
        await self._load_watchlist()
        while True:
            await asyncio.sleep(RELOAD_INTERVAL)
            await self._load_watchlist()

    async def _load_watchlist(self):
        self.last_reload = time.time()
        seen = set()
        if not CHECK_FILE.exists():
            logger.warning(f"{CHECK_FILE} does not exist, skipping reload.")
            return

        with CHECK_FILE.open(encoding="utf-8") as fh:
            for row in csv.reader(fh):
                if not row or row[0].strip().startswith("#"):
                    continue
                platform, channel, *kw = [c.strip() for c in row]
                platform = platform.lower()
                if platform not in ("youtube","twitch"):
                    logger.warning(f"Skipping unknown platform '{platform}' in checkme.txt")
                    continue

                key = f"{platform}::{channel.lower()}"
                seen.add(key)
                if key not in self.tasks:
                    task = ChannelTask(platform, channel, kw[0] if kw else "")
                    self.tasks[key] = task
                    task.start()
                    _platform_counts[platform] = _platform_counts.get(platform,0) + 1

        for key in list(self.tasks):
            if key not in seen:
                logger.info(f"Removing watch task {key}")
                await self.tasks[key].stop()
                del self.tasks[key]
        _platform_counts.clear()
        for t in self.tasks.values():
            _platform_counts[t.platform] = _platform_counts.get(t.platform,0) + 1

    async def _dashboard_loop(self):
        while True:
            os.system(CLEAR_CMD)
            reload_in = max(0, int(self.last_reload + RELOAD_INTERVAL - time.time()))
            print(f"{dt.datetime.now():%Y-%m-%d %H:%M:%S}  –  VOD Watcher   (next reload in {reload_in}s)\n")
            print(f"{'Platform':8} {'Channel':20} {'Keyword':12} {'State':10} {'Next':6} Title")
            print("-"*100)
            for t in self.tasks.values():
                if t.live_raw is None:
                    colour, state = YELLOW, "WAIT"
                elif t.live_raw:
                    if t.keyword_ok and t.is_recording():
                        colour, state = GREEN, "LIVE/REC"
                    elif not t.keyword_ok:
                        colour, state = YELLOW, "LIVE"
                    else:
                        colour, state = YELLOW, "LIVE"
                else:
                    colour, state = RED, "OFF"

                next_in = max(0, int(t.next_probe - time.time()))
                print(f"{t.platform:8} {t.name:20.20} {t.keyword[:11]:12} "
                      f"{colour}{state:10}{RESET} {next_in:6}s {t.current_title[:60]}")
            await asyncio.sleep(1/DASH_FPS)

# ───── entrypoint ───── #
def main():
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    sup = Supervisor()
    try:
        loop.run_until_complete(sup.run())
    except KeyboardInterrupt:
        logger.info("Keyboard interrupt received")
        loop.run_until_complete(sup.shutdown())
    finally:
        loop.close()
        logger.info("Exited cleanly")

if __name__ == "__main__":
    main()
