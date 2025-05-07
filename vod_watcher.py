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
from env import DISCORD_WEBHOOK_URL

import aiohttp # Added for Discord webhook
from tenacity import AsyncRetrying, retry_if_exception_type, stop_after_attempt, wait_exponential

# ───── configuration ───── #
CHECK_FILE         = Path.home() / "code" / "vod_watch" / "checkme.txt"
VOD_ROOT           = Path("/mnt/media/VODs/processed")
LOG_ROOT           = Path("/mnt/media/logs/processed")
SCRIPT_DIR         = Path(__file__).parent.resolve()
DETACHED_FILE      = SCRIPT_DIR / ".detached.json"
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
    RED, YELLOW, GREEN, BLUE, RESET = "\033[91m", "\033[93m", "\033[92m", "\033[94m", "\033[0m"
else:
    RED = YELLOW = GREEN = BLUE = RESET = ""

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

ch = logging.StreamHandler()
ch.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
logger.addHandler(ch)

file_handler = logging.handlers.RotatingFileHandler(
    str(MAIN_LOG),
    maxBytes=5_000_000,
    backupCount=3,
)
file_handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s"))
logger.addHandler(file_handler)


async def send_discord_notification(platform: str, channel_name: str, title: str):
    if not DISCORD_WEBHOOK_URL:
        logger.debug("Discord webhook URL not set, skipping notification.")
        return

    now_str = dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    message_content = (
        f"🔴 Recording Started!\n"
        f"**Date**: {now_str}\n"
        f"**Platform**: {platform.capitalize()}\n"
        f"**Channel**: {channel_name}\n"
        f"**Title**: {title}"
    )
    payload = {"content": message_content}

    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(DISCORD_WEBHOOK_URL, json=payload) as response:
                if response.status >= 200 and response.status < 300:
                    logger.debug(f"Discord notification sent for {channel_name}")
                else:
                    logger.warning(f"Failed to send Discord notification for {channel_name}: {response.status} {await response.text()}")
    except Exception as e:
        logger.error(f"Error sending Discord notification for {channel_name}: {e}")


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
        self.last_logged_title: Optional[str] = None

        idx = _spawn_count[self.platform]
        _spawn_count[self.platform] += 1
        self.next_probe = time.time() + idx * PLATFORM_COOLDOWN
        self.loop: Optional[asyncio.Task] = None
        self.current_vod_fp: Optional[Path] = None
        self.detached_pid: Optional[int] = None
        self._load_detached()

    def start(self):
        self.loop = asyncio.create_task(self._poll_loop())

    async def stop(self, abort_recording: bool = False):
        logger.debug(f"stop() called on {self.platform}::{self.name} – abort_recording={abort_recording}")
        if self.loop:
            self.loop.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self.loop
        if abort_recording:
            process_was_running_and_killed_by_this_abort = False
            if self.proc:  # Check if a process object exists
                if self.proc.poll() is None:  # Check if the process is currently running
                    try:
                        self.proc.kill()  # Attempt to kill it
                        process_was_running_and_killed_by_this_abort = True  # Mark that we killed an active process
                        logger.info(f"ABORT {self.platform}::{self.name} (process killed during abort)")
                    except Exception as e:
                        logger.warning(f"Failed to kill running process for {self.name} during abort: {e}")
                else:
                    logger.info(f"Process for {self.platform}::{self.name} had already terminated (exit code: {self.proc.poll()}) before abort action. VOD will not be deleted by this action.")

            if process_was_running_and_killed_by_this_abort:
                vod_fp = self.current_vod_fp
                if vod_fp and vod_fp.exists():
                    try:
                        vod_fp.unlink()
                        logger.info(f"Deleted VOD {vod_fp.name} because recording was aborted.")
                    except Exception as e:
                        logger.warning(f"Failed to delete VOD file {vod_fp} after abort: {e}")
                elif vod_fp:
                    logger.info(f"VOD file {vod_fp.name} for aborted recording was not found.")
        else:
            await self._stop_recording()

        self.proc = None
        self.current_vod_fp = None

    def _load_detached(self):
        if DETACHED_FILE.exists():
            try:
                data = json.loads(DETACHED_FILE.read_text())
                key = f"{self.platform}::{self.name.lower()}"
                pid = data.get(key)
                if pid:
                    os.kill(pid, 0)
                    self.detached_pid = pid
                else:
                    self.detached_pid = None
            except Exception:
                try:
                    data.pop(key, None)
                    DETACHED_FILE.write_text(json.dumps(data))
                except:
                    pass
                self.detached_pid = None

    def is_recording(self) -> bool:
        alive_child = bool(self.proc and self.proc.poll() is None)
        alive_detach = False
        if self.detached_pid:
            try:
                os.kill(self.detached_pid, 0)
                alive_detach = True
            except OSError:
                self.detached_pid = None
        return alive_child or alive_detach

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

                if live and self.current_title and self.current_title != self.last_logged_title:
                    logdir = LOG_ROOT / self.name
                    logdir.mkdir(parents=True, exist_ok=True)
                    now = dt.datetime.now().isoformat()
                    with (logdir / "stream_titles.log").open("a", encoding="utf-8") as fh:
                        fh.write(f"{now} {self.current_title}\n")
                    self.last_logged_title = self.current_title
                elif not live and self.last_logged_title is not None:
                    self.last_logged_title = None

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
        safe_raw = title.strip()
        safe = "".join(ch if ch not in (os.sep, "\0") else "_" for ch in safe_raw)
        safe = safe[:150].strip() or "live"
        day = dt.datetime.now().strftime("%Y-%m-%d")
        vod_dir = VOD_ROOT / self.name
        log_dir = LOG_ROOT / self.name
        vod_dir.mkdir(parents=True, exist_ok=True)
        log_dir.mkdir(parents=True, exist_ok=True)

        base = vod_dir / f"{day} {safe}"
        idx  = 1
        vod  = base.with_suffix(".mp4")
        while vod.exists():
            idx += 1
            vod = vod_dir / f"{day}_{safe}_part-{idx}.mp4"
        log = (log_dir / vod.stem).with_suffix(".log")
        return vod, log

    async def _start_recording(self, title: str):
        vod_fp, log_fp = self._paths(title)
        self.current_vod_fp = vod_fp

        # Send Discord notification
        asyncio.create_task(send_discord_notification(self.platform, self.name, title))

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
        
        # Write a minimal start message to the specific stream log
        try:
            with open(log_fp, "a", encoding="utf-8") as lf:
                lf.write(f"{dt.datetime.now().isoformat()} START {title} for {self.platform}::{self.name} on VOD file {vod_fp.name}\n")
        except Exception as e:
            logger.error(f"Failed to write to stream log {log_fp}: {e}")

        # Redirect yt-dlp/streamlink stdout/stderr to DEVNULL to prevent large log files
        self.proc = subprocess.Popen(
            cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, text=False, preexec_fn=os.setsid
        )


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

    async def shutdown(self, finish_recordings: bool = False):
        logger.info("Shutting down supervisor…")
        self.stop_evt.set()
        for t in (self.reload_task, self.dash_task):
            if t: t.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await asyncio.gather(
                *(t for t in (self.reload_task, self.dash_task) if t),
                return_exceptions=True
            )
        await asyncio.gather(
            *(
                task.stop(abort_recording=not finish_recordings)
                for task in self.tasks.values()
            ),
            return_exceptions=True
        )
        to_stop = []
        for task in self.tasks.values():
            if finish_recordings and task.is_recording():
                pid = None
                if task.proc and task.proc.poll() is None:
                    pid = task.proc.pid
                elif task.detached_pid:
                    pid = task.detached_pid
                if pid:
                    data = {}
                    if DETACHED_FILE.exists():
                        try:
                            data = json.loads(DETACHED_FILE.read_text())
                        except:
                            data = {}
                    key = f"{task.platform}::{task.name.lower()}"
                    data[key] = pid
                    DETACHED_FILE.write_text(json.dumps(data))
                    task.detached_pid = pid
            else:
                to_stop.append(task)
        await asyncio.gather(
            *(t.stop(abort_recording=True) for t in to_stop),
            return_exceptions=True
        )

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
                        colour, state = YELLOW, "LIVE" # Was LIVE/KW_MISS, changed to just LIVE if keyword_ok is true but not recording yet
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
        ans = input("Exit requested. Let ongoing recordings finish? [y/n]: ").strip().lower()
        finish = (ans == "y") # Corrected variable name for clarity
        loop.run_until_complete(sup.shutdown(finish_recordings=finish))
    finally:
        loop.close()
        logger.info("Exited cleanly")

if __name__ == "__main__":
    main()
