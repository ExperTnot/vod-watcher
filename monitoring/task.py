import asyncio
import contextlib
import json
import logging
import os
import time
from pathlib import Path
from typing import Optional

from tenacity import (
    AsyncRetrying,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

from globals import DETACHED_FILE, LOG_ROOT
from monitoring.probes import TwitchProbe, YouTubeProbe
from monitoring.recorders import TwitchRecorder, YouTubeRecorder
from utils import rate_limiter, strip_end_date_time

logger = logging.getLogger("vod_watcher")


class ChannelTask:
    """Manages monitoring and recording for a single streaming channel."""

    def __init__(self, platform: str, name: str, keyword: str):
        self.platform = platform.lower()
        self.name = name
        self.keyword = keyword
        self.loop: Optional[asyncio.Task] = None

        # State
        self.current_title: Optional[str] = None
        self.last_logged_title: Optional[str] = None
        self.live_raw: bool = False
        self.keyword_ok: bool = False
        self.next_probe: float = 0.0

        # Orphan detection state - let recorders detect stream end naturally
        self.offline_detected_at: Optional[float] = None
        self.MAX_OFFLINE_WAIT = 1800  # 30 minutes max to wait after offline detection

        # Detached process state
        self.detached_pid: Optional[int] = None
        self.detached_process_completed: bool = False
        self.detached_data: Optional[dict] = None

        # Components
        if self.platform == "youtube":
            self.probe = YouTubeProbe(name, keyword)
            self.recorder = YouTubeRecorder(name)
        elif self.platform == "twitch":
            self.probe = TwitchProbe(name, keyword)
            self.recorder = TwitchRecorder(name)
        else:
            raise ValueError(f"Unknown platform: {platform}")

        self._load_detached()

    def start(self):
        """Start the channel monitoring task."""
        self.loop = asyncio.create_task(self._poll_loop())

    async def stop(self, abort_recording: bool = False, delete_partials: bool = True):
        """Stop the channel monitoring task and optionally abort recording."""
        logger.debug(f"stop() called on {self.platform}::{self.name}")

        if self.loop:
            self.loop.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self.loop

        if abort_recording:
            await self._handle_abort(delete_partials)
        else:
            await self.recorder.stop()

    async def _handle_abort(self, delete_partials: bool):
        """Handle aborting the recording."""
        # Check active process
        if self.recorder.is_recording():
            await self.recorder.stop(reason="Recording aborted by user")
            if delete_partials:
                self.recorder.cleanup_partials()
            return

        # Check detached process
        if self.detached_pid:
            try:
                os.kill(self.detached_pid, 15)  # Terminate
                await asyncio.sleep(0.5)
                try:
                    os.kill(self.detached_pid, 9)  # Kill
                except OSError:
                    pass

                # Clean up detached file
                if DETACHED_FILE.exists():
                    try:
                        data = json.loads(DETACHED_FILE.read_text())
                        key = f"{self.platform}::{self.name.lower()}"
                        if key in data:
                            data.pop(key)
                            DETACHED_FILE.write_text(json.dumps(data))
                    except Exception:
                        pass

                if delete_partials:
                    self.recorder.cleanup_partials()

            except OSError:
                pass
            self.detached_pid = None

    def _load_detached(self):
        """Load information about previously detached recording processes."""
        if not DETACHED_FILE.exists():
            return

        try:
            data = json.loads(DETACHED_FILE.read_text())
            key = f"{self.platform}::{self.name.lower()}"
            process_data = data.get(key)

            if isinstance(process_data, dict):
                pid = process_data.get("pid")
                if "title" in process_data:
                    self.current_title = process_data["title"]
                if "vod_path" in process_data:
                    self.recorder.current_vod_fp = Path(process_data["vod_path"])
                    if self.platform == "twitch":
                        self.recorder.final_vod_fp = (
                            self.recorder.current_vod_fp.with_suffix(".mp4")
                        )
                    else:
                        self.recorder.final_vod_fp = self.recorder.current_vod_fp

                self.detached_data = process_data

                if pid:
                    try:
                        os.kill(pid, 0)
                        self.detached_pid = pid
                        self.detached_process_completed = False
                    except OSError:
                        self.detached_pid = None
                        self.detached_process_completed = True
                        # Trigger conversion if needed
                        if (
                            self.platform == "twitch"
                            and self.recorder.current_vod_fp
                            and self.recorder.current_vod_fp.suffix == ".ts"
                        ):
                            asyncio.create_task(self.recorder._convert_ts_to_mp4())

        except Exception as e:
            logger.debug(f"Error loading detached process data: {e}")

    def is_recording(self) -> bool:
        """Check if the channel is currently being recorded."""
        if self.recorder.is_recording():
            return True

        if self.detached_pid:
            try:
                os.kill(self.detached_pid, 0)
                return True
            except OSError:
                self.detached_pid = None

        return False

    async def _poll_loop(self):
        """Main monitoring loop."""
        while True:
            scheduled = await rate_limiter.wait_for_slot(self.platform)
            interval = rate_limiter.get_interval(self.platform)
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
                        live, ok, title = await self.probe.check()

                self.live_raw, self.keyword_ok = live, ok

                if live:
                    title = strip_end_date_time(title or "")
                    if title:
                        self.current_title = title
                    elif not self.current_title:
                        self.current_title = "<title unavailable>"
                    # Reset offline detection when stream is live again
                    self.offline_detected_at = None
                else:
                    self.current_title = "<not live>"

                if live and ok and not self.is_recording():
                    await self.recorder.start(self.current_title)
                elif not live and self.is_recording() and self.offline_detected_at is None:
                    # Stream went offline - let recorder detect actual stream end
                    # Don't terminate immediately, let yt-dlp/streamlink exit naturally
                    self.offline_detected_at = time.time()
                    logger.info(
                        f"Stream offline detected for {self.platform}::{self.name}, "
                        f"letting recorder drain naturally"
                    )

                # Check for orphaned recordings - only terminate if running too long
                await self._check_orphaned_recording()

                # Logging titles
                if (
                    live
                    and self.current_title
                    and self.current_title != self.last_logged_title
                ):
                    logdir = LOG_ROOT / self.name
                    logdir.mkdir(parents=True, exist_ok=True)
                    now = os.path.getmtime(logdir)
                    import datetime as dt

                    now_str = dt.datetime.now().isoformat()
                    with (logdir / "stream_titles.log").open(
                        "a", encoding="utf-8"
                    ) as fh:
                        fh.write(f"{now_str} {self.current_title}\n")
                    self.last_logged_title = self.current_title
                elif not live:
                    self.last_logged_title = None

            except Exception:
                logger.exception(f"{self.platform}::{self.name} probe failure")

            await asyncio.sleep(interval)

    async def _check_orphaned_recording(self):
        """Check for recordings running too long after offline detection."""
        if (
            self.offline_detected_at is None
            or not self.is_recording()
        ):
            return

        time_since_offline = time.time() - self.offline_detected_at
        if time_since_offline > self.MAX_OFFLINE_WAIT:
            logger.warning(
                f"Recording still running {int(time_since_offline)}s after offline detection, "
                f"terminating orphaned process for {self.platform}::{self.name}"
            )
            await self.recorder.stop(reason="Orphaned recording")
            self.offline_detected_at = None
