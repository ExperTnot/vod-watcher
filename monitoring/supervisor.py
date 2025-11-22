import asyncio
import contextlib
import csv
import datetime as dt
import json
import logging
import math
import os
import shutil
import sys
import time
from typing import Dict, Optional

from globals import (
    CHECK_FILE,
    DETACHED_FILE,
    RELOAD_INTERVAL,
    TWITCH_OAUTH_TOKEN,
)
from monitoring.task import ChannelTask
from utils import rate_limiter


logger = logging.getLogger("vod_watcher")

# Terminal setup
USE_COLOR = sys.stdout.isatty() and ("TERM" in os.environ)
if USE_COLOR:
    RED, YELLOW, GREEN, BLUE, MAGENTA, RESET = (
        "\033[91m",
        "\033[93m",
        "\033[92m",
        "\033[94m",
        "\033[95m",
        "\033[0m",
    )
else:
    RED = YELLOW = GREEN = BLUE = MAGENTA = RESET = ""

CURSOR_HOME = "\033[H"
ERASE_DOWN = "\033[J"
DASH_FPS = 2
PAGE_SWITCH_INTERVAL = 10.0  # Switch pages every 10 seconds


class Supervisor:
    """Manages the collection of channel monitoring tasks."""

    def __init__(self):
        self.tasks: Dict[str, ChannelTask] = {}
        self.last_reload = 0.0
        self.stop_evt = asyncio.Event()
        self.reload_task: Optional[asyncio.Task] = None
        self.dash_task: Optional[asyncio.Task] = None

    async def run(self):
        """Start the supervisor and wait for the stop event."""
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
        """Sort tasks into those to keep running and those to stop."""
        to_keep = []
        to_stop = []

        for task in self.tasks.values():
            if finish_recordings and task.is_recording():
                to_keep.append(task)
            else:
                to_stop.append(task)
                if task.detached_pid and not finish_recordings:
                    logger.debug(
                        f"Including detached process for {task.platform}::{task.name} for termination"
                    )
                    task.detached_pid = None

        return to_keep, to_stop

    async def _stop_selected_tasks(self, tasks, delete_partials: bool):
        """Stop the specified tasks."""
        await asyncio.gather(
            *(
                task.stop(abort_recording=True, delete_partials=delete_partials)
                for task in tasks
            ),
            return_exceptions=True,
        )

    def _detach_conversion_processes(self) -> int:
        """Ensure any unfinished TS to MP4 conversions continue after shutdown."""
        detached_count = 0

        for task in self.tasks.values():
            if task.platform != "twitch":
                continue

            recorder = task.recorder
            if not hasattr(recorder, "conversion_process"):
                continue

            ts_fp = recorder.current_vod_fp
            mp4_fp = recorder.final_vod_fp

            if not ts_fp or ts_fp.suffix != ".ts" or not ts_fp.exists():
                continue

            if mp4_fp and mp4_fp.exists():
                continue

            if (
                recorder.conversion_process
                and recorder.conversion_process.returncode is None
            ):
                logger.info(
                    f"Leaving existing FFmpeg conversion running for {task.name}: PID={recorder.conversion_process.pid}"
                )
                detached_count += 1
                recorder.conversion_process = None
                continue

        if detached_count:
            logger.info(
                f"{detached_count} conversion(s) will finish in the background."
            )
        else:
            logger.debug("No conversion processes needed detaching.")

        return detached_count

    def _save_detached_process_data(self, task, pid):
        """Save information about a detached recording process."""
        data = {}
        if DETACHED_FILE.exists():
            try:
                data = json.loads(DETACHED_FILE.read_text())
            except Exception:
                data = {}

        key = f"{task.platform}::{task.name.lower()}"

        process_data = {
            "pid": pid,
            "platform": task.platform,
            "channel": task.name,
            "title": task.current_title,
            "keyword": task.keyword,
            "vod_path": str(task.recorder.current_vod_fp)
            if task.recorder.current_vod_fp
            else None,
            "timestamp": dt.datetime.now().isoformat(),
        }

        data[key] = process_data
        DETACHED_FILE.write_text(json.dumps(data))
        task.detached_pid = pid

        logger.info(f"  - {task.platform}::{task.name} - {task.current_title}")
        return True

    async def _handle_continued_recordings(self, tasks_to_keep):
        """Handle tasks that should continue recording in the background."""
        continued_count = 0

        for task in tasks_to_keep:
            if task.is_recording():
                pid = None
                if task.recorder.proc and task.recorder.proc.poll() is None:
                    pid = task.recorder.proc.pid
                elif task.detached_pid:
                    pid = task.detached_pid

                if pid:
                    if self._save_detached_process_data(task, pid):
                        continued_count += 1

        for task in tasks_to_keep:
            if task.loop:
                task.loop.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await task.loop
                task.loop = None

        return continued_count

    async def shutdown(
        self, finish_recordings: bool = False, delete_partials: bool = True
    ):
        """Gracefully shut down the supervisor and all managed tasks."""
        logger.info("Shutting down supervisor...")
        self.stop_evt.set()

        await self._cancel_system_tasks()
        self._detach_conversion_processes()

        recording_tasks = [task for task in self.tasks.values() if task.is_recording()]

        if not recording_tasks:
            logger.info("No channels are currently recording")
            await self._stop_all_tasks(delete_partials)
            return

        if finish_recordings:
            logger.info(f"Continuing {len(recording_tasks)} recordings in background:")
        else:
            logger.info(f"Aborting {len(recording_tasks)} recordings:")

        to_keep, to_stop = self._sort_tasks_by_recording_status(finish_recordings)

        await self._stop_selected_tasks(to_stop, delete_partials)

        continued_count = await self._handle_continued_recordings(to_keep)

        if finish_recordings and continued_count > 0:
            logger.info(f"Total: {continued_count} recordings continuing in background")
        elif finish_recordings:
            logger.info("No recordings could be continued in background")

    async def _reload_loop(self):
        """Background task that periodically reloads the channel watchlist."""
        await self._load_watchlist()
        while True:
            await asyncio.sleep(RELOAD_INTERVAL)
            await self._load_watchlist()

    async def _load_watchlist(self):
        """Load or reload the channel watchlist from the checkme.txt file."""
        self.last_reload = time.time()
        seen = set()
        if not CHECK_FILE.exists():
            logger.warning(f"{CHECK_FILE} does not exist, skipping reload.")
            return

        platform_counts = {}

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

                platform_counts[platform] = platform_counts.get(platform, 0) + 1

        for key in list(self.tasks):
            if key not in seen:
                logger.info(f"Removing watch task {key}")
                await self.tasks[key].stop()
                del self.tasks[key]

        # Update rate limiter with new counts
        rate_limiter.update_counts(platform_counts)

    async def _dashboard_loop(self):
        """Background task that periodically updates the console dashboard."""
        update_interval = 1.0 / DASH_FPS
        first_run = True

        # Paging state
        current_page = 0
        last_page_switch = time.time()

        has_oauth = (
            "YES" if bool(TWITCH_OAUTH_TOKEN and TWITCH_OAUTH_TOKEN.strip()) else "NO"
        )

        while True:
            display_lines = []
            reload_in = max(0, int(self.last_reload + RELOAD_INTERVAL - time.time()))

            # Get terminal size
            try:
                term_columns, term_lines = shutil.get_terminal_size()
            except Exception:
                term_columns, term_lines = 80, 24

            # Header takes ~3 lines
            header_lines = 3
            available_lines = max(
                1, term_lines - header_lines - 1
            )  # -1 for cursor/safety

            sorted_tasks = sorted(self.tasks.values(), key=lambda t: t.name.lower())
            total_tasks = len(sorted_tasks)
            total_pages = (
                math.ceil(total_tasks / available_lines) if total_tasks > 0 else 1
            )

            # Cycle pages
            now = time.time()
            time_since_switch = now - last_page_switch
            if time_since_switch > PAGE_SWITCH_INTERVAL:
                current_page = (current_page + 1) % total_pages
                last_page_switch = now
                time_since_switch = 0

            switch_in = max(0, int(PAGE_SWITCH_INTERVAL - time_since_switch))

            # Ensure current page is valid (in case tasks decreased)
            if current_page >= total_pages:
                current_page = 0

            start_idx = current_page * available_lines
            end_idx = start_idx + available_lines
            page_tasks = sorted_tasks[start_idx:end_idx]

            page_info = (
                f"  [Page {current_page + 1}/{total_pages} (next in {switch_in}s)]"
                if total_pages > 1
                else ""
            )

            display_lines.append(
                f"{dt.datetime.now():%Y-%m-%d %H:%M:%S}  -  VOD Watcher  (next reload in {reload_in}s)  Twitch OAuth: {has_oauth}{page_info}\n"
            )
            display_lines.append(
                f"{'Platform':8} {'Channel':20} {'Keyword':12} {'State':10} {'Next':6} Title"
            )
            display_lines.append("-" * min(100, term_columns - 1))

            for t in page_tasks:
                if t.detached_pid:
                    try:
                        os.kill(t.detached_pid, 0)
                        is_alive = True
                    except OSError:
                        is_alive = False
                        t.detached_pid = None

                    if is_alive:
                        colour, state = BLUE, "DETACHED"
                    else:
                        colour, state = RED, "OFF"
                elif t.live_raw is None:
                    colour, state = YELLOW, "WAIT"
                elif t.live_raw:
                    if t.keyword_ok and t.is_recording():
                        colour, state = GREEN, "LIVE/REC"
                    elif not t.keyword_ok:
                        colour, state = YELLOW, "LIVE"
                    else:
                        colour, state = YELLOW, "LIVE"
                elif t.recorder.conversion_pending:
                    colour, state = MAGENTA, "CONV"
                else:
                    colour, state = RED, "OFF"

                next_in = max(0, int(t.next_probe - time.time()))

                if t.detached_pid:
                    next_str = "--"
                else:
                    next_str = f"{next_in}s"

                # Truncate title to fit terminal
                # Fixed width cols: 8+1+20+1+12+1+10+1+6+1 = 61 chars
                # Title space = term_columns - 61
                title_space = max(10, term_columns - 61)
                title_str = str(t.current_title).replace("\n", " ")
                if len(title_str) > title_space:
                    title_str = title_str[: title_space - 3] + "..."

                display_lines.append(
                    f"{t.platform:8} {t.name:20.20} {t.keyword[:11]:12} "
                    f"{colour}{state:10}{RESET} {next_str:6} {title_str}"
                )

            # Fill empty lines to clear previous output if page is short
            while len(display_lines) < term_lines - 1:  # -1 to avoid scrolling
                display_lines.append("")

            output = "\n".join(display_lines)

            if first_run:
                print(output)
                first_run = False
            else:
                # Use ERASE_DOWN to clear any leftover text from previous frames
                print(f"{CURSOR_HOME}{ERASE_DOWN}{output}", end="")

            await asyncio.sleep(update_interval)
