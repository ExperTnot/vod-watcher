#!/usr/bin/env python3
"""
vod_watcher.py — 24/7 YouTube & Twitch VOD recorder with logging, retries, colors, and clean shutdown
Linux only - will not work on Windows or MacOS.
"""

import asyncio
import contextlib
import csv
import datetime as dt
import json
import logging
import logging.handlers
import os
import platform
import re
import subprocess
import sys
import time
from pathlib import Path
from typing import Dict, Optional, Tuple

# Check if running on Linux
if platform.system() != "Linux":
    print("\033[91mERROR: VOD Watcher only supports Linux operating systems.\033[0m")
    print("Current OS detected:", platform.system())
    sys.exit(1)
from env import (
    DISCORD_WEBHOOK_URL,
    VOD_ROOT,
    LOG_ROOT,
    RELOAD_INTERVAL,
    PROBE_INTERVAL,
    PLATFORM_COOLDOWN,
    YOUTUBE_API_KEY,
    TWITCH_CLIENT_ID,
    TWITCH_CLIENT_SECRET,
)
import aiohttp
from tenacity import (
    AsyncRetrying,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

# ───── configuration ───── #
SCRIPT_DIR = Path(__file__).parent.resolve()
CHECK_FILE = SCRIPT_DIR / "checkme.txt"
VOD_ROOT = Path(VOD_ROOT)
LOG_ROOT = Path(LOG_ROOT)
DETACHED_FILE = SCRIPT_DIR / ".detached.json"
LOG_DIR = SCRIPT_DIR / "logs"
LOG_DIR.mkdir(parents=True, exist_ok=True)
MAIN_LOG = LOG_DIR / "vod_watcher.log"
RELOAD_INTERVAL = max(60, RELOAD_INTERVAL)
PROBE_INTERVAL = max(60, PROBE_INTERVAL)
PLATFORM_COOLDOWN = max(30, PLATFORM_COOLDOWN)
DASH_FPS = 1
MAX_YT_HEIGHT = 1080

# ───── color setup ───── #
USE_COLOR = sys.stdout.isatty() and ("TERM" in os.environ)
if USE_COLOR:
    RED, YELLOW, GREEN, BLUE, RESET = (
        "\033[91m",
        "\033[93m",
        "\033[92m",
        "\033[94m",
        "\033[0m",
    )
else:
    RED = YELLOW = GREEN = BLUE = RESET = ""

DATE_RE = re.compile(r"\b\d{4}-\d{2}-\d{2}$")

# ───── shared cooldown state ───── #
_last_probe_time: Dict[str, float] = {"youtube": 0.0, "twitch": 0.0}
_platform_locks: Dict[str, asyncio.Lock] = {
    "youtube": asyncio.Lock(),
    "twitch": asyncio.Lock(),
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
file_handler.setFormatter(
    logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s")
)
logger.addHandler(file_handler)


async def get_twitch_access_token() -> Optional[str]:
    """Get an OAuth token for the Twitch API."""
    if (
        not TWITCH_CLIENT_ID
        or not TWITCH_CLIENT_SECRET
        or TWITCH_CLIENT_ID.strip() == ""
        or TWITCH_CLIENT_SECRET.strip() == ""
    ):
        logger.debug("Twitch client credentials not set, skipping thumbnail fetch.")
        return None

    try:
        params = {
            "client_id": TWITCH_CLIENT_ID,
            "client_secret": TWITCH_CLIENT_SECRET,
            "grant_type": "client_credentials",
        }

        async with aiohttp.ClientSession() as session:
            async with session.post(
                "https://id.twitch.tv/oauth2/token", params=params
            ) as response:
                if response.status == 200:
                    data = await response.json()
                    return data.get("access_token")
                else:
                    logger.warning(
                        f"Failed to get Twitch access token: {response.status}"
                    )
    except Exception as e:
        logger.error(f"Error getting Twitch access token: {e}")

    return None


async def get_twitch_channel_thumbnail(channel_name: str) -> Optional[str]:
    """Fetch the Twitch channel's profile image URL using the Twitch API."""
    access_token = await get_twitch_access_token()
    if not access_token:
        return None

    headers = {"Client-ID": TWITCH_CLIENT_ID, "Authorization": f"Bearer {access_token}"}

    params = {"login": channel_name}

    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(
                "https://api.twitch.tv/helix/users", headers=headers, params=params
            ) as response:
                if response.status == 200:
                    data = await response.json()
                    if data.get("data") and len(data["data"]) > 0:
                        return data["data"][0].get("profile_image_url")
                else:
                    logger.warning(
                        f"Failed to fetch Twitch channel thumbnail for {channel_name}: {response.status}"
                    )
    except Exception as e:
        logger.error(f"Error fetching Twitch channel thumbnail for {channel_name}: {e}")
    return None


async def get_youtube_channel_thumbnail(channel_name: str) -> Optional[str]:
    """Fetch the YouTube channel's thumbnail URL using the YouTube API."""
    if not YOUTUBE_API_KEY or YOUTUBE_API_KEY.strip() == "":
        logger.debug("YouTube API key not set, skipping thumbnail fetch.")
        return None

    username = channel_name[1:] if channel_name.startswith("@") else channel_name

    params = {"part": "snippet", "forHandle": username, "key": YOUTUBE_API_KEY}

    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(
                "https://www.googleapis.com/youtube/v3/channels", params=params
            ) as response:
                if response.status == 200:
                    data = await response.json()
                    if data.get("items") and len(data["items"]) > 0:
                        item = data["items"][0]
                        if "snippet" in item and "thumbnails" in item["snippet"]:
                            return (
                                item["snippet"]["thumbnails"]
                                .get("medium", {})
                                .get("url")
                            )
                else:
                    logger.warning(
                        f"Failed to fetch YouTube channel thumbnail for {channel_name}: {response.status}"
                    )
    except Exception as e:
        logger.error(
            f"Error fetching YouTube channel thumbnail for {channel_name}: {e}"
        )
    return None


async def send_discord_notification(platform: str, channel_name: str, title: str):
    # Log the notification in console for user feedback even if webhook isn't available
    logger.info(f"Recording started: {platform.capitalize()}/{channel_name} - {title}")

    if not DISCORD_WEBHOOK_URL:
        logger.debug("Discord webhook URL not set, skipping webhook notification.")
        return

    now_str = dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    embed = {
        "title": f"🔴 {channel_name}",
        "description": title,
        "fields": [
            {"name": "Platform", "value": platform.capitalize(), "inline": True},
            {"name": "Date", "value": now_str, "inline": True},
        ],
        "timestamp": dt.datetime.now().isoformat(),
    }

    # Get profile image based on platform type
    thumbnail_url = None
    if platform.lower() == "youtube":
        thumbnail_url = await get_youtube_channel_thumbnail(channel_name)
        if thumbnail_url:
            logger.debug(f"Added thumbnail for YouTube channel {channel_name}")
    elif platform.lower() == "twitch":
        thumbnail_url = await get_twitch_channel_thumbnail(channel_name)
        if thumbnail_url:
            logger.debug(f"Added thumbnail for Twitch channel {channel_name}")

    if thumbnail_url:
        embed["thumbnail"] = {"url": thumbnail_url}

    payload = {"embeds": [embed]}

    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(DISCORD_WEBHOOK_URL, json=payload) as response:
                if response.status >= 200 and response.status < 300:
                    logger.debug(f"Discord notification sent for {channel_name}")
                else:
                    logger.warning(
                        f"Failed to send Discord notification for {channel_name}: {response.status} {await response.text()}"
                    )
    except Exception as e:
        logger.error(f"Error sending Discord notification for {channel_name}: {e}")


# ───── ChannelTask ───── #
class ChannelTask:
    def __init__(self, platform: str, name: str, keyword: str):
        self.platform = platform.lower().strip()
        self.name = name.strip()
        self.keyword = keyword.lower().strip()
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

    async def _stop_task_loop(self):
        """Stop only the task loop without stopping the recording process.
        This is used when detaching processes during shutdown.
        """
        if self.loop:
            self.loop.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self.loop
            self.loop = None
            logger.debug(
                f"Cancelled task loop for {self.platform}::{self.name} (process remains running)"
            )
        return

    async def stop(self, abort_recording: bool = False):
        logger.debug(
            f"stop() called on {self.platform}::{self.name} – abort_recording={abort_recording}"
        )
        if self.loop:
            self.loop.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self.loop
        if abort_recording:
            process_was_running_and_killed_by_this_abort = False

            # First check if we have an active process
            if self.proc:
                if self.proc.poll() is None:
                    try:
                        self.proc.kill()
                        process_was_running_and_killed_by_this_abort = True
                        logger.info(
                            f"ABORT {self.platform}::{self.name} (process killed during abort)"
                        )
                    except Exception as e:
                        logger.warning(
                            f"Failed to kill running process for {self.name} during abort: {e}"
                        )
                else:
                    logger.info(
                        f"Process for {self.platform}::{self.name} had already terminated (exit code: {self.proc.poll()}) before abort action. VOD will not be deleted by this action."
                    )
            # Check if we have a detached process to kill
            elif self.detached_pid:
                try:
                    os.kill(self.detached_pid, 0)
                    os.kill(self.detached_pid, 15)
                    await asyncio.sleep(0.5)
                    try:
                        os.kill(self.detached_pid, 0)
                        os.kill(self.detached_pid, 9)
                        logger.info(
                            f" - {self.platform}::{self.name} (detached process killed)"
                        )
                    except OSError:
                        logger.info(
                            f" - {self.platform}::{self.name} (detached process terminated gracefully)"
                        )
                    process_was_running_and_killed_by_this_abort = True

                    if DETACHED_FILE.exists():
                        try:
                            data = json.loads(DETACHED_FILE.read_text())
                            key = f"{self.platform}::{self.name.lower()}"
                            if key in data:
                                data.pop(key)
                                DETACHED_FILE.write_text(json.dumps(data))
                                logger.debug(
                                    f"Removed {key} from detached processes file"
                                )
                        except Exception as e:
                            logger.warning(
                                f"Failed to update detached process file: {e}"
                            )
                except OSError:
                    logger.info(
                        f"Detached process for {self.platform}::{self.name} is no longer running"
                    )
                    process_was_running_and_killed_by_this_abort = False
                self.detached_pid = None
            # Check for completed detached processes from previous runs
            elif (
                hasattr(self, "detached_process_completed")
                and self.detached_process_completed
            ):
                logger.info(
                    f"Detached process for {self.platform}::{self.name} already completed. VOD preserved."
                )
                process_was_running_and_killed_by_this_abort = False

            if process_was_running_and_killed_by_this_abort:
                vod_fp = self.current_vod_fp
                if vod_fp and vod_fp.exists():
                    try:
                        vod_fp.unlink()
                        logger.info(
                            f"Deleted VOD {vod_fp.name} because recording was aborted."
                        )
                    except Exception as e:
                        logger.warning(
                            f"Failed to delete VOD file {vod_fp} after abort: {e}"
                        )
                elif vod_fp:
                    logger.info(
                        f"VOD file {vod_fp.name} for aborted recording was not found."
                    )
        else:
            await self._stop_recording()

        self.proc = None
        self.current_vod_fp = None

    def _load_detached(self):
        if DETACHED_FILE.exists():
            try:
                data = json.loads(DETACHED_FILE.read_text())
                key = f"{self.platform}::{self.name.lower()}"
                process_data = data.get(key)

                # Check if the data is a dict (new format) or just a PID (old format)
                if isinstance(process_data, dict):
                    pid = process_data.get("pid")
                    if "title" in process_data:
                        self.current_title = process_data["title"]
                    if "vod_path" in process_data and process_data["vod_path"]:
                        self.current_vod_fp = Path(process_data["vod_path"])
                    self.detached_data = process_data
                elif isinstance(process_data, int):
                    pid = process_data
                    self.detached_data = None
                else:
                    pid = None
                    self.detached_data = None

                # Check if process is still running
                if pid:
                    try:
                        os.kill(pid, 0)
                        self.detached_pid = pid
                        self.detached_process_completed = False
                    except OSError:
                        # Process no longer running - mark as completed
                        self.detached_pid = None
                        self.detached_process_completed = True
                        logger.info(
                            f"Detached process for {self.platform}::{self.name} has completed naturally. VOD preswerved."
                        )
                else:
                    self.detached_pid = None
                    self.detached_process_completed = False
            except Exception as e:
                logger.debug(f"Error loading detached process data: {e}")
                try:
                    data.pop(key, None)
                    DETACHED_FILE.write_text(json.dumps(data))
                except Exception:
                    pass
                self.detached_pid = None
                self.detached_process_completed = False
                self.detached_data = None

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
            interval = platform_interval(self.platform)
            self.next_probe = scheduled + interval

            try:
                async for attempt in AsyncRetrying(
                    stop=stop_after_attempt(5),
                    wait=wait_exponential(multiplier=1, min=10, max=300),
                    retry=retry_if_exception_type(
                        (asyncio.TimeoutError, json.JSONDecodeError)
                    ),
                    reraise=True,
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

                if (
                    live
                    and self.current_title
                    and self.current_title != self.last_logged_title
                ):
                    logdir = LOG_ROOT / self.name
                    logdir.mkdir(parents=True, exist_ok=True)
                    now = dt.datetime.now().isoformat()
                    with (logdir / "stream_titles.log").open(
                        "a", encoding="utf-8"
                    ) as fh:
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

    async def _probe_youtube(self) -> Tuple[bool, bool, str]:
        url = yt_live_url(self.name)
        cmd = ["yt-dlp", "--skip-download", "--print", "%(is_live)s|%(title)s", url]
        proc = await asyncio.create_subprocess_exec(
            *cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.DEVNULL
        )
        raw, _ = await proc.communicate()

        if not raw:
            return False, False, ""

        try:
            flag, title = raw.decode().strip().split("|", 1)
        except ValueError:
            return False, False, ""

        live = flag.strip().lower() == "true"
        keyword_ok = (not self.keyword) or (self.keyword in title.lower())
        return live, keyword_ok, title

    async def _probe_twitch(self) -> Tuple[bool, bool, str]:
        url = f"https://twitch.tv/{self.name}"
        cmd = ["streamlink", "--json", url, "best"]
        p = await asyncio.create_subprocess_exec(
            *cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
        )
        out, _ = await p.communicate()
        if p.returncode != 0:
            return False, False, ""

        info = json.loads(out)
        meta = info.get("metadata", {}) or {}
        title = meta.get("title") or info.get("title") or ""
        tags = meta.get("tags") or info.get("tags") or []
        tagstr = " ".join(tags) if isinstance(tags, (list, tuple)) else str(tags)

        # Default keyword check based on title and tags
        keyword_ok = (not self.keyword) or (self.keyword in f"{title} {tagstr}".lower())

        # If keyword doesn't match in title/tags, also check if it matches the category
        if not keyword_ok and self.keyword:
            category = meta.get("category") or ""

            if category:
                logger.debug(
                    f"Twitch channel {self.name} is streaming in category: {category}"
                )

                if self.keyword.lower() == category.lower():
                    logger.info(
                        f"Twitch channel {self.name} has category '{category}' matching keyword '{self.keyword}', recording even though title doesn't contain keyword"
                    )
                    keyword_ok = True

        return True, keyword_ok, title

    def _paths(self, title: str) -> Tuple[Path, Path]:
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
        idx = 1
        vod = base.with_suffix(".mp4")
        while vod.exists():
            idx += 1
            vod = vod_dir / f"{day} {safe} ({idx}).mp4"
        log = (log_dir / vod.stem).with_suffix(".log")
        return vod, log

    async def _start_recording(self, title: str):
        vod_fp, log_fp = self._paths(title)
        self.current_vod_fp = vod_fp

        if DISCORD_WEBHOOK_URL:
            asyncio.create_task(
                send_discord_notification(self.platform, self.name, title)
            )

        if self.platform == "youtube":
            url = yt_live_url(self.name)
            cmd = [
                "yt-dlp",
                url,
                "-o",
                str(vod_fp),
                "-f",
                f"bestvideo[height<=?{MAX_YT_HEIGHT}]+bestaudio/best[height<=?{MAX_YT_HEIGHT}]",
                "--merge-output-format",
                "mp4",
                "--no-part",
            ]
        else:
            url = f"https://twitch.tv/{self.name}"
            cmd = [
                "streamlink",
                url,
                "best",
                "--twitch-disable-hosting",
                "--twitch-disable-ads",
                "-o",
                str(vod_fp),
            ]

        logger.info(f"START {self.platform}::{self.name} → {vod_fp.name}")

        try:
            with open(log_fp, "a", encoding="utf-8") as lf:
                lf.write(
                    f"{dt.datetime.now().isoformat()} START {title} for {self.platform}::{self.name} on VOD file {vod_fp.name}\n"
                )
        except Exception as e:
            logger.error(f"Failed to write to stream log {log_fp}: {e}")

        self.proc = subprocess.Popen(
            cmd,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL
            if not self.platform == "youtube"
            else open(log_fp, "a", encoding="utf-8"),
            text=False,
            preexec_fn=os.setsid,
        )

    async def _stop_recording(self):
        # Logging for process state
        if self.proc is None:
            logger.debug(
                f"Process for {self.platform}::{self.name} is already None, nothing to stop"
            )
            return

        poll_result = self.proc.poll()
        if poll_result is not None:
            logger.warning(
                f"Process for {self.platform}::{self.name} had already terminated with code {poll_result} before stop was called"
            )
            self.proc = None
            return

        # Process is still running, need to terminate it
        process_id = self.proc.pid
        logger.info(f"STOP {self.platform}::{self.name} (PID: {process_id})")

        # Now terminate the process
        try:
            logger.debug(f"Sending terminate signal to PID {process_id}")
            self.proc.terminate()
            try:
                logger.debug(f"Waiting for process to exit... PID {process_id}")
                await asyncio.wait_for(asyncio.to_thread(self.proc.wait), timeout=10)
                logger.debug(
                    f"Process terminated successfully: PID {process_id}"
                )
            except asyncio.TimeoutError:
                logger.warning(
                    f"Process {process_id} didn't terminate within timeout, sending kill signal"
                )
                self.proc.kill()
                logger.debug(f"Kill signal sent to process {process_id}")
        except Exception as term_e:
            logger.error(f"Error in process termination: {term_e}")
            try:
                self.proc.kill()
                logger.debug("Fallback kill signal sent")
            except Exception as kill_e:
                logger.error(f"Failed fallback kill: {kill_e}")

        self.proc = None
        logger.debug(
            f"Process reference cleared for {self.platform}::{self.name}"
        )


# ───── Supervisor ───── #
class Supervisor:
    def __init__(self):
        self.tasks: Dict[str, ChannelTask] = {}
        self.last_reload = 0.0
        self.stop_evt = asyncio.Event()
        self.reload_task: Optional[asyncio.Task] = None
        self.dash_task: Optional[asyncio.Task] = None

    async def run(self):
        self.reload_task = asyncio.create_task(self._reload_loop())
        self.dash_task = asyncio.create_task(self._dashboard_loop())
        await self.stop_evt.wait()

    async def shutdown(self, finish_recordings: bool = False):
        logger.info("Shutting down supervisor…")
        self.stop_evt.set()

        # Cancel dashboard and reload tasks
        for t in (self.reload_task, self.dash_task):
            if t:
                t.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await asyncio.gather(
                *(t for t in (self.reload_task, self.dash_task) if t),
                return_exceptions=True,
            )

        # First identify all recording tasks
        recording_tasks = [task for task in self.tasks.values() if task.is_recording()]

        if not recording_tasks:
            logger.info("No channels are currently recording")
            # Stop all tasks before exiting
            await asyncio.gather(
                *(task.stop(abort_recording=True) for task in self.tasks.values()),
                return_exceptions=True,
            )
            return

        # List channels that are recording
        if finish_recordings:
            logger.info(f"Continuing {len(recording_tasks)} recordings in background:")
        else:
            logger.info(f"Aborting {len(recording_tasks)} recordings:")

        to_keep = []
        to_stop = []
        continued_count = 0

        # Sort tasks into those to keep running and those to stop
        for task in self.tasks.values():
            if finish_recordings and task.is_recording():
                to_keep.append(task)
            else:
                to_stop.append(task)
                # Ensure any detached process is included for termination
                if task.detached_pid and not finish_recordings:
                    logger.debug(
                        f"Including detached process for {task.platform}::{task.name} for termination"
                    )
                    # Make sure proc is None so we directly handle the detached process
                    task.proc = None

        # Stop tasks that shouldn't continue recording
        await asyncio.gather(
            *(task.stop(abort_recording=True) for task in to_stop),
            return_exceptions=True,
        )

        # Now handle tasks that should keep recording
        for task in to_keep:
            # Check if still recording after the other tasks were stopped
            if task.is_recording():
                pid = None
                if task.proc and task.proc.poll() is None:
                    pid = task.proc.pid
                elif task.detached_pid:
                    pid = task.detached_pid

                if pid:
                    continued_count += 1
                    data = {}
                    if DETACHED_FILE.exists():
                        try:
                            data = json.loads(DETACHED_FILE.read_text())
                        except Exception:
                            data = {}

                    key = f"{task.platform}::{task.name.lower()}"

                    # Save comprehensive information about the recording
                    process_data = {
                        "pid": pid,
                        "platform": task.platform,
                        "channel": task.name,
                        "title": task.current_title,
                        "keyword": task.keyword,
                        "vod_path": str(task.current_vod_fp)
                        if task.current_vod_fp
                        else None,
                        "timestamp": dt.datetime.now().isoformat(),
                    }

                    data[key] = process_data
                    DETACHED_FILE.write_text(json.dumps(data))
                    task.detached_pid = pid

                    logger.info(
                        f"  - {task.platform}::{task.name} - {task.current_title}"
                    )

        # Cancel task loops but don't terminate the processes
        await asyncio.gather(
            *(task._stop_task_loop() for task in to_keep),
            return_exceptions=True,
        )

        # Final summary
        if finish_recordings and continued_count > 0:
            logger.info(f"Total: {continued_count} recordings continuing in background")
        elif finish_recordings:
            logger.info("No recordings could be continued in background")

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
                if platform not in ("youtube", "twitch"):
                    logger.warning(
                        f"Skipping unknown platform '{platform}' in checkme.txt"
                    )
                    continue

                key = f"{platform}::{channel.lower()}"
                seen.add(key)
                if key not in self.tasks:
                    task = ChannelTask(platform, channel, kw[0] if kw else "")
                    self.tasks[key] = task
                    task.start()
                    _platform_counts[platform] = _platform_counts.get(platform, 0) + 1

        for key in list(self.tasks):
            if key not in seen:
                logger.info(f"Removing watch task {key}")
                await self.tasks[key].stop()
                del self.tasks[key]
        _platform_counts.clear()
        for t in self.tasks.values():
            _platform_counts[t.platform] = _platform_counts.get(t.platform, 0) + 1

    async def _dashboard_loop(self):
        while True:
            os.system("clear")
            reload_in = max(0, int(self.last_reload + RELOAD_INTERVAL - time.time()))
            print(
                f"{dt.datetime.now():%Y-%m-%d %H:%M:%S}  –  VOD Watcher   (next reload in {reload_in}s)\n"
            )
            print(
                f"{'Platform':8} {'Channel':20} {'Keyword':12} {'State':10} {'Next':6} Title"
            )
            print("-" * 100)
            for t in self.tasks.values():
                # Determine state and color
                if t.detached_pid:
                    # Check if detached process is still alive
                    try:
                        os.kill(t.detached_pid, 0)
                        is_alive = True
                    except OSError:
                        is_alive = False
                        t.detached_pid = None

                    if is_alive:
                        colour, state = BLUE, "DETACHED"
                elif t.live_raw is None:
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

                if t.detached_pid:
                    next_str = "--"
                else:
                    next_str = f"{next_in}s"

                print(
                    f"{t.platform:8} {t.name:20.20} {t.keyword[:11]:12} "
                    f"{colour}{state:10}{RESET} {next_str:6} {t.current_title[:60]}"
                )
            await asyncio.sleep(1 / DASH_FPS)


# ───── entrypoint ───── #
def _verify_ffmpeg():
    """Check if FFmpeg is installed."""
    is_win = sys.platform.startswith("win")
    try:
        result = subprocess.run(
            ["ffmpeg", "-version"],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            creationflags=subprocess.CREATE_NO_WINDOW if is_win else 0,
        )

        if result.returncode == 0:
            logger.info("FFmpeg found")
            return True

        logger.error("FFmpeg check failed")
        return False
    except FileNotFoundError:
        tip = "Download from ffmpeg.org" if is_win else "Use apt/yum install ffmpeg"
        logger.error(f"FFmpeg not found. {tip}")
        return False
    except Exception as e:
        logger.error(f"FFmpeg error: {e}")
        return False


def verify_paths():
    logger.info("Verifying file paths and permissions...")

    if not _verify_directories():
        return False

    if not _verify_files():
        return False

    if not _verify_ffmpeg():
        return False

    logger.info("File path verification successful")
    return True


def _verify_directories():
    directories = [
        (VOD_ROOT, "VOD output directory"),
        (LOG_ROOT, "Log directory"),
        (LOG_DIR, "Program log directory"),
        (SCRIPT_DIR, "Script directory"),
    ]

    for path, description in directories:
        if not path.exists():
            try:
                path.mkdir(parents=True, exist_ok=True)
                logger.info(f"Created {description} at {path}")
            except (PermissionError, OSError) as e:
                logger.error(f"Cannot create {description} at {path}: {e}")
                return False

        test_file = path / ".write_test"
        try:
            test_file.write_text("test")
            test_file.unlink()
        except (PermissionError, OSError) as e:
            logger.error(f"No write permission for {description} at {path}: {e}")
            return False

    return True


def _verify_files():
    if not CHECK_FILE.exists():
        try:
            example_content = (
                "# Format: platform,channel,keyword\n"
                "# Example:\n"
                "# youtube,@channelname,keyword\n"
                "# twitch,channelname,keyword\n"
            )
            CHECK_FILE.write_text(example_content)
            logger.info(f"Created example configuration at {CHECK_FILE}")
        except (PermissionError, OSError) as e:
            logger.error(f"Cannot create configuration file at {CHECK_FILE}: {e}")
            return False

    try:
        CHECK_FILE.read_text()
    except (PermissionError, OSError) as e:
        logger.error(f"Cannot read configuration file at {CHECK_FILE}: {e}")
        return False

    try:
        if DETACHED_FILE.exists():
            content = DETACHED_FILE.read_text()
            try:
                json.loads(content)
            except json.JSONDecodeError:
                logger.warning(
                    "Detached process file contains invalid JSON, resetting it"
                )
                DETACHED_FILE.write_text("{}")
        else:
            DETACHED_FILE.write_text("{}")
            logger.info(f"Created detached process state file at {DETACHED_FILE}")
    except (PermissionError, OSError) as e:
        logger.error(
            f"Cannot access detached process state file at {DETACHED_FILE}: {e}"
        )
        return False

    return True


def main():
    if not verify_paths():
        logger.error("Exiting due to file system permission/access errors.")
        sys.exit(1)

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    sup = Supervisor()
    try:
        loop.run_until_complete(sup.run())
    except KeyboardInterrupt:
        logger.info("Keyboard interrupt received")
        ans = (
            input("Exit requested. Let ongoing recordings finish? [y/n]: ")
            .strip()
            .lower()
        )
        finish = ans == "y"
        loop.run_until_complete(sup.shutdown(finish_recordings=finish))
    finally:
        loop.close()
        logger.info("Exited cleanly")


if __name__ == "__main__":
    main()
