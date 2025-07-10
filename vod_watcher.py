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
import subprocess
import sys
import time
from pathlib import Path
from typing import Dict, Optional, Tuple
from tenacity import (
    AsyncRetrying,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

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
)
from api import send_discord_notification
from verifications import verify_paths
from utils import (
    _spawn_count,
    _platform_counts,
    platform_interval,
    wait_for_platform_slot,
    yt_live_url,
    strip_end_date_time,
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
RELOAD_INTERVAL = max(45, RELOAD_INTERVAL)
PROBE_INTERVAL = max(45, PROBE_INTERVAL)
PLATFORM_COOLDOWN = max(20, PLATFORM_COOLDOWN)
MAX_YT_HEIGHT = 1080

STREAMLINK_SEGMENT_ATTEMPTS = 10 # default is 3
STREAMLINK_SEGMENT_TIMEOUT = 10 # default is 10
STREAMLINK_TIMEOUT = 120 # default is 60
YTDLT_ATTEMPTS = 15

# ───── color and terminal setup ───── #
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

CURSOR_HOME = "\033[H"  # Move cursor to top left
ERASE_DOWN = "\033[J"  # Erase from cursor to end of screen

DASH_FPS = 2

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


# ───── ChannelTask ───── #
class ChannelTask:
    """Manages monitoring and recording for a single streaming channel.

    This class is responsible for periodically checking if a channel is live,
    matching against keywords, and starting/stopping recordings as needed.
    It handles both YouTube and Twitch channels and manages the lifecycle
    of recording processes including detachment during shutdown.
    """

    def __init__(self, platform: str, name: str, keyword: str):
        self.platform = platform.lower().strip()
        self.name = name.strip()
        self.keyword = keyword.lower().strip()
        self.proc: Optional[subprocess.Popen] = None
        self.vod_fp: Optional[Path] = (
            None  # Path to the VOD file (.ts for Twitch, .mp4 for YouTube)
        )
        self.log_file_handle = None  # File handle for stderr logs

        self.current_title: str = "<awaiting check>"
        self.live_raw: Optional[bool] = None
        self.keyword_ok: bool = False
        self.last_logged_title: Optional[str] = None

        idx = _spawn_count[self.platform]
        _spawn_count[self.platform] += 1
        self.next_probe = time.time() + idx * PLATFORM_COOLDOWN
        self.loop: Optional[asyncio.Task] = None
        # self.current_vod_fp will point to the active recording file (.ts for Twitch, .mp4 for YouTube)
        self.current_vod_fp: Optional[Path] = None
        self.detached_pid: Optional[int] = None
        self._load_detached()

    def start(self):
        """Start the channel monitoring task.

        Creates an asyncio task that periodically checks if the channel is live
        and handles recording start/stop based on live status and keywords.
        """
        self.loop = asyncio.create_task(self._poll_loop())

    async def _stop_task_loop(self):
        """Stop only the task loop without stopping the recording process.

        This is used when detaching processes during shutdown to allow recordings
        to continue even after the monitoring has stopped.

        The recording process will continue running in the background.
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

    async def stop(self, abort_recording: bool = False, delete_partials: bool = True):
        """Stop the channel monitoring task and optionally abort recording.

        Args:
            abort_recording: If True, also stops any active recording process.
                If False, allows recording to continue running.
            delete_partials: If True, deletes partial VOD files when stopping recordings.
                If False, preserves partial VOD files even when stopping recordings.
        """
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
                        # Use our _stop_recording method to properly log the abort
                        await self._stop_recording(reason="Recording aborted by user")
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
                    if delete_partials:
                        try:
                            vod_fp.unlink()
                            logger.info(
                                f"Deleted VOD {vod_fp.name} because recording was aborted."
                            )
                        except Exception as e:
                            logger.warning(
                                f"Failed to delete VOD file {vod_fp} after abort: {e}"
                            )
                    else:
                        logger.info(f"Kept partial VOD {vod_fp.name} as requested.")
                elif vod_fp:
                    logger.info(
                        f"VOD file {vod_fp.name} for aborted recording was not found."
                    )
        else:
            await self._stop_recording()

        self.proc = None
        self.current_vod_fp = None
        self.ts_vod_fp = None
        self.mp4_vod_fp = None
        self.conversion_pending = False
        self.conversion_last_status = None

    def _load_detached(self):
        """Load information about previously detached recording processes.

        Reads the detached process state file to check if this channel has a
        recording process that was detached during a previous run. If found,
        it verifies if the process is still running and updates the task state accordingly.
        """
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
                        loaded_path = Path(process_data["vod_path"])
                        self.current_vod_fp = loaded_path
                        if self.platform == "twitch" and loaded_path.suffix == ".ts":
                            self.ts_vod_fp = loaded_path
                            self.mp4_vod_fp = loaded_path.with_suffix(".mp4")
                        elif (
                            self.platform == "youtube" and loaded_path.suffix == ".mp4"
                        ):
                            self.mp4_vod_fp = loaded_path
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
                            f"Detached process for {self.platform}::{self.name} has completed naturally. VOD preserved."
                        )
                        # If the completed VOD is a .ts file, schedule it for conversion
                        if (
                            self.detached_process_completed
                            and self.current_vod_fp
                            and self.current_vod_fp.exists()
                            and self.current_vod_fp.suffix == ".ts"
                        ):
                            logger.info(
                                f"Detached recording {self.current_vod_fp.name} completed. Scheduling conversion."
                            )
                            if (
                                self.platform == "twitch"
                                and self.current_vod_fp.suffix == ".ts"
                            ):  # Only convert if it's a Twitch .ts file
                                logger.info(
                                    f"Detached Twitch recording {self.current_vod_fp.name} completed. Scheduling conversion."
                                )
                                self.conversion_pending = True  # Mark for conversion
                                # Clean up the entry from detached file to prevent re-processing
                                if DETACHED_FILE.exists():
                                    try:
                                        _data = json.loads(DETACHED_FILE.read_text())
                                        _key = f"{self.platform}::{self.name.lower()}"
                                        if _key in _data:
                                            _data.pop(_key)
                                            DETACHED_FILE.write_text(json.dumps(_data))
                                            logger.debug(
                                                f"Removed {_key} from detached processes file after scheduling conversion."
                                            )
                                    except Exception as e_json:
                                        logger.warning(
                                            f"Failed to update detached process file for {_key} after scheduling conversion: {e_json}"
                                        )
                                asyncio.create_task(
                                    self._convert_ts_to_mp4()
                                )  # ts_filepath is now self.ts_vod_fp
                            elif self.platform == "youtube":
                                logger.info(
                                    f"Detached YouTube recording {self.current_vod_fp.name} completed. No conversion needed."
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
        """Check if the channel is currently being recorded.

        Verifies if either an active recording process is running or a detached
        process is still alive.

        Returns:
            bool: True if recording is active, False otherwise
        """
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
        """Main monitoring loop that periodically checks if the channel is live.

        This method runs in a continuous loop, respecting rate limits, and:
        1. Checks if the channel is live with _probe()
        2. Starts recording if the channel is live and matches keywords
        3. Stops recording if the channel goes offline or stops matching keywords
        4. Logs stream titles to a file
        """
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
                elif (
                    not live or not ok
                ) and self.is_recording():  # Stream ended or keyword no longer matches
                    reason = (
                        "Stream went offline"
                        if not live
                        else "Keyword no longer matched"
                    )
                    await self._stop_recording(
                        reason=reason
                    )  # Stops streamlink process

                    # Log the completed recording
                    if self.vod_fp and self.vod_fp.exists():
                        file_type = (
                            ".ts file" if self.platform == "twitch" else ".mp4 file"
                        )
                        logger.info(
                            f"{self.platform.capitalize()} stream {self.name} ended. VOD saved as {self.vod_fp.name} ({file_type})"
                        )

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
        """Check if a channel is live based on its platform."""
        if self.platform == "youtube":
            return await self._probe_youtube()
        else:
            return await self._probe_twitch()

    async def _probe_youtube(self) -> Tuple[bool, bool, str]:
        """Check if a YouTube channel is currently live streaming.

        Uses yt-dlp to check the live status and fetch the stream title.

        Returns:
            Tuple containing:
            - bool: True if channel is live, False otherwise
            - bool: True if keyword matches (or no keyword), False otherwise
            - str: Stream title if available, empty string otherwise
        """
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
        """Check if a Twitch channel is currently live streaming.

        Uses streamlink to check the live status and fetch the stream title,
        tags, and category. Considers a keyword match if it appears in the title,
        tags, or exactly matches the category name.

        Returns:
            Tuple containing:
            - bool: True if channel is live, False otherwise
            - bool: True if keyword matches title, tags, or category (or no keyword)
            - str: Stream title if available, empty string otherwise
        """
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
                    logger.debug(
                        f"Twitch channel {self.name} has category '{category}' matching keyword '{self.keyword}', recording even though title doesn't contain keyword"
                    )
                    keyword_ok = True

        return True, keyword_ok, title

    def _paths(self, title: str) -> Tuple[Path, Path]:
        """Generate paths for the VOD file and its log file.

        Creates file paths with the stream date and sanitized title, ensuring
        unique filenames by adding a counter suffix if needed.

        Args:
            title: The stream title to use in the filenames

        Returns:
            Tuple containing:
            - Path: Path to the VOD file (.mp4)
            - Path: Path to the log file (.log)
        """
        title = strip_end_date_time(title)
        safe_raw = title.strip()
        safe = "".join(ch if ch not in (os.sep, "\0") else "_" for ch in safe_raw)
        safe = safe[:150].strip() or "live"
        day = dt.datetime.now().strftime("%Y-%m-%d")
        vod_dir = VOD_ROOT / self.name
        log_dir = LOG_ROOT / self.name
        vod_dir.mkdir(parents=True, exist_ok=True)
        log_dir.mkdir(parents=True, exist_ok=True)

        base_name_template = f"{day} {safe}"
        idx = 1

        # Determine extension based on platform
        file_extension = ".ts" if self.platform == "twitch" else ".mp4"

        # Generate unique filename
        current_path_attempt = vod_dir / f"{base_name_template}{file_extension}"
        while current_path_attempt.exists():
            idx += 1
            current_path_attempt = (
                vod_dir / f"{base_name_template} ({idx}){file_extension}"
            )

        final_vod_path = current_path_attempt
        log_fp = (log_dir / final_vod_path.stem).with_suffix(".log")

        self.vod_fp = final_vod_path
        return (
            final_vod_path,
            log_fp,
        )  # final_vod_path is .ts for Twitch, .mp4 for YouTube

    async def _start_recording(self, title: str):
        # Always call _paths() to generate fresh paths for every new recording
        logger.debug(
            f"[{self.platform}::{self.name}] Generating new file paths for recording"
        )
        self._paths(title)

        if not self.vod_fp:
            logger.error(
                f"[{self.platform}::{self.name}] vod_fp not set before starting recording."
            )
            return

        if self.platform not in ["twitch", "youtube"]:
            logger.error(
                f"[{self.platform}::{self.name}] Unknown platform for recording."
            )
            return

        vod_fp, log_fp = (
            self.vod_fp,
            (LOG_ROOT / self.name / self.vod_fp.stem).with_suffix(".log"),
        )

        """Start recording a live stream.

        Creates output directories if needed, generates unique filenames based on
        stream title and date, sends a Discord notification if configured, and
        launches the appropriate recording process based on the platform.

        Args:
            title: The title of the stream being recorded
        """
        # vod_fp and log_fp are now set by the logic at the beginning of _start_recording
        # self.current_vod_fp is already set to ts_vod_fp for Twitch or mp4_vod_fp for YouTube

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
                "--live-from-start",
            ]
        else:
            url = f"https://twitch.tv/{self.name}"
            cmd = [
                "yt-dlp",
                url,
                "-o",
                str(vod_fp),
                "--retries", str(YTDLT_ATTEMPTS),
                "--fragment-retries", "infinite",
                "--concurrent-fragments", "5",
            ]

            # cmd = [
            #     "streamlink",
            #     url,
            #     "best",
            #     "--twitch-disable-hosting",
            #     "--twitch-disable-ads",
            #     "-o",
            #     str(vod_fp),
            #     "--stream-segment-attempts", str(STREAMLINK_SEGMENT_ATTEMPTS),
            #     "--stream-segment-timeout", str(STREAMLINK_SEGMENT_TIMEOUT),
            #     "--stream-timeout", str(STREAMLINK_TIMEOUT),
            # ]

        logger.info(f"START {self.platform}::{self.name} → {vod_fp.name}")

        # Close any existing log file handle first
        if self.log_file_handle is not None:
            try:
                self.log_file_handle.close()
            except Exception:
                pass
            self.log_file_handle = None

        try:
            # Create parent directories if they don't exist
            log_fp.parent.mkdir(parents=True, exist_ok=True)

            # Open the log file and keep the handle
            self.log_file_handle = open(log_fp, "a", encoding="utf-8")

            # Write the initial log message
            self.log_file_handle.write(
                f"{dt.datetime.now().isoformat()} START {title} for {self.platform}::{self.name} on VOD file {vod_fp.name}\n"
            )
            self.log_file_handle.flush()
        except Exception as e:
            logger.error(f"Failed to write to stream log {log_fp}: {e}")
            if self.log_file_handle is not None:
                try:
                    self.log_file_handle.close()
                except Exception:
                    pass
                self.log_file_handle = None

        self.proc = subprocess.Popen(
            cmd,
            stdout=self.log_file_handle if self.log_file_handle else subprocess.DEVNULL,
            stderr=subprocess.STDOUT,
            text=False,
            preexec_fn=os.setsid,
        )

    async def _stop_recording(self, reason: str = "Unknown reason"):
        """Stop an active recording process.

        Attempts to gracefully terminate the recording process. If the process
        doesn't exit within 10 seconds, it will be forcibly killed. Updates the
        process state to None after termination.

        Args:
            reason: The reason why the recording is being stopped, will be logged
        """
        # Logging for process state
        if self.proc is None:
            logger.debug(
                f"Process for {self.platform}::{self.name} is already None, nothing to stop"
            )
            return

        process_id = self.proc.pid

        # Write the termination reason to the log file before closing it
        if self.log_file_handle is not None:
            try:
                timestamp = dt.datetime.now().isoformat()
                self.log_file_handle.write(
                    f"\n\n{timestamp} STOPPING RECORDING: {reason}\n"
                )
                self.log_file_handle.write(f"{timestamp} Process ID: {process_id}\n")
                self.log_file_handle.flush()
            except Exception as e:
                logger.error(f"Error writing termination reason to log: {e}")

        # Close the log file handle if it's open (but only after writing the reason)
        if self.log_file_handle is not None:
            try:
                self.log_file_handle.close()
            except Exception as e:
                logger.error(f"Error closing log file: {e}")
            self.log_file_handle = None

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
                logger.debug(f"Process terminated successfully: PID {process_id}")
            except asyncio.TimeoutError:
                logger.warning(
                    f"Process for {self.platform}::{self.name} did not exit after terminate(), sending KILL signal"
                )
                self.proc.kill()
                await asyncio.to_thread(self.proc.wait)
                logger.debug(f"Process killed: PID {process_id}")

            self.proc = None

        except Exception as e:
            logger.error(f"Error stopping recording process: {e}")
            self.proc = None
        logger.debug(f"Process reference cleared for {self.platform}::{self.name}")

    async def _convert_ts_to_mp4(self):
        """Convert the current task's .ts file to .mp4 using FFmpeg and log file details.
        
        This method:
        1. Logs duration and filetype of the .ts file using ffprobe
        2. Converts .ts to .mp4 asynchronously using ffmpeg
        3. Logs duration and filetype of the resulting .mp4 file
        4. Deletes the .ts file if conversion was successful
        """
        if self.platform != "twitch" or not self.ts_vod_fp:
            logger.debug(f"_convert_ts_to_mp4 called for non-Twitch or missing ts_vod_fp for {self.name}. Skipping.")
            return

        ts_filepath = self.ts_vod_fp
        mp4_filepath = self.mp4_vod_fp

        # Get log file for this VOD
        log_fp = (LOG_ROOT / self.name / ts_filepath.stem).with_suffix(".log")
        log_fp.parent.mkdir(parents=True, exist_ok=True)

        if not ts_filepath.exists():
            logger.error(f"TS file {ts_filepath} not found for conversion for {self.name}.")
            self.conversion_last_status = "TS_MISSING"
            return

        if not mp4_filepath:
            logger.error(f"MP4 output path not set for conversion of {ts_filepath.name} for {self.name}.")
            self.conversion_last_status = "MP4_PATH_MISSING"
            return

        if mp4_filepath.exists():
            logger.warning(
                f"MP4 file {mp4_filepath.name} already exists. Skipping conversion for {ts_filepath.name} for {self.name}."
            )
            self.conversion_last_status = "MP4_EXISTS"
            return

        logger.info(f"CONVERTING {ts_filepath.name} to {mp4_filepath.name} for {self.name}")
        self.conversion_last_status = "CONVERTING"

        # 1. Log source file details using ffprobe
        await self._log_file_details(ts_filepath, log_fp, "TS SOURCE FILE")
        
        # 2. Convert the file
        cmd = [
            "ffmpeg",
            "-hide_banner",
            "-i", str(ts_filepath),
            "-c", "copy",  # Just copy streams without re-encoding
            "-movflags", "+faststart",  # Optimize for web streaming
            str(mp4_filepath)
        ]

        try:
            with open(log_fp, "a", encoding="utf-8") as lf:
                lf.write(f"{dt.datetime.now().isoformat()} FFMPEG CONVERSION STARTED: {ts_filepath.name} → {mp4_filepath.name}\n")
                lf.write(f"Command: {' '.join(cmd)}\n")
            
            proc = await asyncio.create_subprocess_exec(
                *cmd,
                stdout=subprocess.DEVNULL,  # Suppress verbose FFmpeg output
                stderr=subprocess.PIPE      # Capture stderr for error reporting
            )
            _, stderr_output = await proc.communicate()  # Wait for FFmpeg to complete

            if proc.returncode == 0:
                # 3. Log successful conversion and output file details
                logger.info(f"SUCCESS converting {ts_filepath.name} to {mp4_filepath.name} for {self.name}")
                with open(log_fp, "a", encoding="utf-8") as lf:
                    lf.write(f"{dt.datetime.now().isoformat()} FFMPEG CONVERSION SUCCESS\n")
                
                # Log the output MP4 file details
                await self._log_file_details(mp4_filepath, log_fp, "MP4 OUTPUT FILE")
                
                self.conversion_last_status = "SUCCESS"
                
                # 4. Delete the original .ts file
                if ts_filepath.exists():
                    try:
                        ts_filepath.unlink()
                        logger.info(f"Deleted original .ts file: {ts_filepath.name}")
                        with open(log_fp, "a", encoding="utf-8") as lf:
                            lf.write(f"{dt.datetime.now().isoformat()} DELETED ORIGINAL TS FILE: {ts_filepath.name}\n")
                    except Exception as e_del:
                        logger.error(f"Failed to delete original .ts file {ts_filepath}: {e_del}")
                        with open(log_fp, "a", encoding="utf-8") as lf:
                            lf.write(f"{dt.datetime.now().isoformat()} ERROR DELETING TS FILE: {str(e_del)}\n")
            else:
                # Log conversion failure
                error_message = stderr_output.decode(errors='ignore').strip()
                logger.error(
                    f"FAILED converting {ts_filepath.name} for {self.name} (FFmpeg exited with {proc.returncode}). Error: {error_message[:500]}..."
                )
                with open(log_fp, "a", encoding="utf-8") as lf:
                    lf.write(f"{dt.datetime.now().isoformat()} FFMPEG CONVERSION FAILED (exit code {proc.returncode})\n")
                    lf.write(f"Error: {error_message}\n")
                self.conversion_last_status = f"FAILED_CODE_{proc.returncode}"

        except Exception as e:
            logger.exception(f"Error during FFmpeg conversion for {ts_filepath.name} for {self.name}: {e}")
            with open(log_fp, "a", encoding="utf-8") as lf:
                lf.write(f"{dt.datetime.now().isoformat()} EXCEPTION DURING CONVERSION: {str(e)}\n")
            self.conversion_last_status = "EXCEPTION"
            
    async def _log_file_details(self, file_path: Path, log_file: Path, header: str):
        """Log file details using ffprobe.
        
        Args:
            file_path: Path to the file to analyze
            log_file: Path to the log file to write to
            header: Header description for the log entry
        """
        if not file_path.exists():
            logger.error(f"Cannot log details for non-existent file: {file_path}")
            return
            
        try:
            # Get file info using ffprobe
            cmd = [
                "ffprobe",
                "-v", "error",
                "-show_entries", "format=filename,format_name,duration,size,bit_rate",
                "-show_entries", "stream=codec_name,codec_type,width,height,bit_rate,sample_rate",
                "-of", "json",
                str(file_path)
            ]
            
            proc = await asyncio.create_subprocess_exec(
                *cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE
            )
            stdout, stderr = await proc.communicate()
            
            if proc.returncode == 0:
                # Parse the JSON output
                probe_data = json.loads(stdout)
                
                with open(log_file, "a", encoding="utf-8") as lf:
                    lf.write(f"{dt.datetime.now().isoformat()} {header} DETAILS:\n")
                    
                    # Format size in human-readable format
                    size_bytes = int(probe_data.get('format', {}).get('size', 0))
                    size_mb = size_bytes / (1024 * 1024)
                    size_gb = size_mb / 1024
                    
                    # Log format information
                    format_info = probe_data.get('format', {})
                    lf.write(f"Filename: {file_path.name}\n")
                    lf.write(f"Format: {format_info.get('format_name', 'unknown')}\n")
                    lf.write(f"Duration: {format_info.get('duration', 'unknown')} seconds\n")
                    lf.write(f"Size: {size_gb:.2f} GB ({size_bytes} bytes)\n")
                    lf.write(f"Bitrate: {format_info.get('bit_rate', 'unknown')} bps\n")
                    
                    # Log stream information
                    lf.write("Streams:\n")
                    for i, stream in enumerate(probe_data.get('streams', [])):
                        codec_type = stream.get('codec_type', 'unknown')
                        codec_name = stream.get('codec_name', 'unknown')
                        lf.write(f"  Stream #{i}: {codec_type} ({codec_name})")
                        
                        if codec_type == 'video':
                            width = stream.get('width', 'unknown')
                            height = stream.get('height', 'unknown')
                            lf.write(f", {width}x{height}")
                            
                        if codec_type == 'audio':
                            sample_rate = stream.get('sample_rate', 'unknown')
                            lf.write(f", {sample_rate} Hz")
                            
                        lf.write("\n")
                    
                    lf.write("\n")
                logger.debug(f"Logged file details for {file_path.name}")
            else:
                error = stderr.decode(errors='ignore').strip()
                logger.error(f"ffprobe failed for {file_path.name}: {error}")
                with open(log_file, "a", encoding="utf-8") as lf:
                    lf.write(f"{dt.datetime.now().isoformat()} FFPROBE FAILED: {error}\n")
                    
        except Exception as e:
            logger.error(f"Error logging file details for {file_path.name}: {e}")
            with open(log_file, "a", encoding="utf-8") as lf:
                lf.write(f"{dt.datetime.now().isoformat()} ERROR LOGGING FILE DETAILS: {str(e)}\n")


# ───── Supervisor ───── #
class Supervisor:
    """Manages the collection of channel monitoring tasks.

    The Supervisor is responsible for loading channel configurations, creating and
    managing ChannelTask instances, providing a dashboard with status information,
    and handling graceful shutdown of all tasks.
    """

    def __init__(self):
        self.tasks: Dict[str, ChannelTask] = {}
        self.last_reload = 0.0
        self.stop_evt = asyncio.Event()
        self.reload_task: Optional[asyncio.Task] = None
        self.dash_task: Optional[asyncio.Task] = None

    async def run(self):
        """Start the supervisor and wait for the stop event.

        Creates tasks for reloading channel configurations and updating the dashboard,
        then waits for the stop event to be triggered by a shutdown request.
        """
        self.reload_task = asyncio.create_task(self._reload_loop())
        self.dash_task = asyncio.create_task(self._dashboard_loop())
        await self.stop_evt.wait()

    async def _cancel_system_tasks(self):
        """Cancel dashboard and reload tasks."""
        for t in (self.reload_task, self.dash_task):
            if t:
                t.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await asyncio.gather(
                *(t for t in (self.reload_task, self.dash_task) if t),
                return_exceptions=True,
            )

    async def _stop_all_tasks(self, delete_partials: bool):
        """Stop all tasks when no recordings are active."""
        await asyncio.gather(
            *(
                task.stop(abort_recording=True, delete_partials=delete_partials)
                for task in self.tasks.values()
            ),
            return_exceptions=True,
        )

    def _sort_tasks_by_recording_status(self, finish_recordings: bool):
        """Sort tasks into those to keep running and those to stop.

        Args:
            finish_recordings: If True, allows active recordings to continue.

        Returns:
            Tuple of (tasks_to_keep, tasks_to_stop)
        """
        to_keep = []
        to_stop = []

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

        return to_keep, to_stop

    async def _stop_selected_tasks(self, tasks, delete_partials: bool):
        """Stop the specified tasks.

        Args:
            tasks: List of tasks to stop
            delete_partials: If True, deletes partial VOD files when stopping recordings
        """
        await asyncio.gather(
            *(
                task.stop(abort_recording=True, delete_partials=delete_partials)
                for task in tasks
            ),
            return_exceptions=True,
        )

    def _save_detached_process_data(self, task, pid):
        """Save information about a detached recording process.

        Args:
            task: The channel task that will continue recording
            pid: Process ID of the recording process

        Returns:
            True if the process data was saved successfully
        """
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
            "vod_path": str(task.current_vod_fp) if task.current_vod_fp else None,
            "timestamp": dt.datetime.now().isoformat(),
        }

        data[key] = process_data
        DETACHED_FILE.write_text(json.dumps(data))
        task.detached_pid = pid

        logger.info(f"  - {task.platform}::{task.name} - {task.current_title}")
        return True

    async def _handle_continued_recordings(self, tasks_to_keep):
        """Handle tasks that should continue recording in the background.

        Args:
            tasks_to_keep: List of tasks to keep running

        Returns:
            Number of recordings that will continue in the background
        """
        continued_count = 0

        for task in tasks_to_keep:
            # Check if still recording after the other tasks were stopped
            if task.is_recording():
                pid = None
                if task.proc and task.proc.poll() is None:
                    pid = task.proc.pid
                elif task.detached_pid:
                    pid = task.detached_pid

                if pid:
                    if self._save_detached_process_data(task, pid):
                        continued_count += 1

        # Cancel task loops but don't terminate the processes
        await asyncio.gather(
            *(task._stop_task_loop() for task in tasks_to_keep),
            return_exceptions=True,
        )

        return continued_count

    async def shutdown(
        self, finish_recordings: bool = False, delete_partials: bool = True
    ):
        """Gracefully shut down the supervisor and all managed tasks.

        Cancels the reload and dashboard tasks, then handles all channel tasks
        according to the finish_recordings and delete_partials parameters.

        Args:
            finish_recordings: If True, allows active recordings to continue in
                the background. If False, stops all recordings.
            delete_partials: If True, deletes partial VOD files when stopping recordings.
                If False, preserves partial VOD files even when stopping recordings.
        """
        logger.info("Shutting down supervisor…")
        self.stop_evt.set()

        # Cancel dashboard and reload tasks
        await self._cancel_system_tasks()

        # First identify all recording tasks
        recording_tasks = [task for task in self.tasks.values() if task.is_recording()]

        if not recording_tasks:
            logger.info("No channels are currently recording")
            # Stop all tasks before exiting
            await self._stop_all_tasks(delete_partials)
            return

        # List channels that are recording
        if finish_recordings:
            logger.info(f"Continuing {len(recording_tasks)} recordings in background:")
        else:
            logger.info(f"Aborting {len(recording_tasks)} recordings:")

        # Sort tasks into those to keep running and those to stop
        to_keep, to_stop = self._sort_tasks_by_recording_status(finish_recordings)

        # Stop tasks that shouldn't continue recording
        await self._stop_selected_tasks(to_stop, delete_partials)

        # Handle tasks that should keep recording
        continued_count = await self._handle_continued_recordings(to_keep)

        # Final summary
        if finish_recordings and continued_count > 0:
            logger.info(f"Total: {continued_count} recordings continuing in background")
        elif finish_recordings:
            logger.info("No recordings could be continued in background")

    async def _reload_loop(self):
        """Background task that periodically reloads the channel watchlist.

        Runs in an infinite loop, periodically checking for changes to the
        watchlist file and updating channel tasks accordingly.
        """
        await self._load_watchlist()
        while True:
            await asyncio.sleep(RELOAD_INTERVAL)
            await self._load_watchlist()

    async def _load_watchlist(self):
        """Load or reload the channel watchlist from the checkme.txt file.

        Reads the CSV format file to discover channels to monitor, creates new
        ChannelTask instances for new entries, and removes tasks for channels
        that are no longer in the watchlist.
        """
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
                        f"Skipping unknown platform '{platform}' in {CHECK_FILE}."
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
        """Background task that periodically updates the console dashboard.

        Provides a real-time status display showing all monitored channels,
        their live status, and recording status at regular intervals.
        Uses ANSI escape sequences for efficient screen updates without flashing.
        Limits updates based on global DASH_FPS setting to prevent excessive CPU usage.
        """
        update_interval = 1.0 / DASH_FPS
        first_run = True

        while True:
            # Build all lines in a list before printing
            display_lines = []
            reload_in = max(0, int(self.last_reload + RELOAD_INTERVAL - time.time()))

            # Add header lines
            display_lines.append(
                f"{dt.datetime.now():%Y-%m-%d %H:%M:%S}  -  VOD Watcher   (next reload in {reload_in}s)\n"
            )
            display_lines.append(
                f"{'Platform':8} {'Channel':20} {'Keyword':12} {'State':10} {'Next':6} Title"
            )
            display_lines.append("-" * 100)

            # Add each task's information
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

                # Add formatted line to display
                display_lines.append(
                    f"{t.platform:8} {t.name:20.20} {t.keyword[:11]:12} "
                    f"{colour}{state:10}{RESET} {next_str:6} {t.current_title[:60]}"
                )

            if first_run:
                # First run - just print everything
                print("\n".join(display_lines))
                first_run = False
            else:
                # Move cursor to home position and erase the screen from there
                print(f"{CURSOR_HOME}{ERASE_DOWN}", end="")
                print("\n".join(display_lines))

            # Wait before updating again, using global FPS setting
            await asyncio.sleep(update_interval)


def main():
    """Application entry point.

    Verifies all required paths and dependencies, initializes the event loop,
    creates a Supervisor instance, and runs the main application loop.

    Handles keyboard interrupts by allowing the user to choose whether to
    let ongoing recordings finish before shutting down.
    """
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
            input("Exit requested. Let ongoing recordings finish? [Y/n] (default: Y): ")
            .strip()
            .lower()
        )
        finish = ans == "" or ans == "y"

        delete_partials = False
        if not finish:
            ans_delete = (
                input("Delete partial VOD recordings? [Y/n] (default: N): ")
                .strip()
                .lower()
            )
            delete_partials = ans_delete == "y"

        loop.run_until_complete(
            sup.shutdown(finish_recordings=finish, delete_partials=delete_partials)
        )
    finally:
        loop.close()
        logger.info("Exited cleanly")


if __name__ == "__main__":
    main()
