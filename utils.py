"""
utils.py — Utility functions for vod_watcher
"""

import asyncio
import re
import time
import datetime as dt
import subprocess
from typing import Dict, Optional
from env import PROBE_INTERVAL, PLATFORM_COOLDOWN

# Compile regex pattern for date detection
DATE_RE = re.compile(r"\b\d{4}-\d{2}-\d{2}$")

# Shared cooldown state
_last_probe_time: Dict[str, float] = {"youtube": 0.0, "twitch": 0.0}
_platform_locks: Dict[str, asyncio.Lock] = {
    "youtube": asyncio.Lock(),
    "twitch": asyncio.Lock(),
}
_spawn_count: Dict[str, int] = {"youtube": 0, "twitch": 0}
_platform_counts: Dict[str, int] = {"youtube": 1, "twitch": 1}


def platform_interval(platform: str) -> int:
    """Calculate the interval between platform probes based on the platform type and count.

    Args:
        platform: The platform to calculate interval for

    Returns:
        The calculated interval in seconds between probes for the specified platform
    """
    n = max(1, _platform_counts.get(platform, 1))
    return max(PROBE_INTERVAL, PLATFORM_COOLDOWN * n)


async def wait_for_platform_slot(platform: str) -> float:
    """Wait for the next available slot to probe a platform API.

    Implements rate limiting for API calls based on platform type.

    Args:
        platform: The platform to wait for

    Returns:
        The scheduled time (timestamp) when the slot became available
    """
    lock = _platform_locks[platform]
    async with lock:
        now = time.time()
        scheduled = max(now, _last_probe_time[platform] + PLATFORM_COOLDOWN)
        _last_probe_time[platform] = scheduled
        if scheduled > now:
            await asyncio.sleep(scheduled - now)
        return scheduled


def yt_live_url(name: str) -> str:
    """Construct a YouTube live URL from a channel name or ID.

    Handles different YouTube channel name formats (handle, channel ID, or regular name).

    Args:
        name: The YouTube channel name or ID

    Returns:
        A complete URL to the channels live stream page
    """
    name = name.strip()
    if name.startswith("@"):
        path = name
    elif re.match(r"UC[A-Za-z0-9_-]{22}", name):
        path = f"channel/{name}"
    else:
        path = f"@{name}"
    return f"https://www.youtube.com/{path}/live"


def strip_end_date_time(text: str) -> str:
    """Remove date and time from the end of a string.

    This is used to clean up stream titles that may have dates appended to them.

    Args:
        text: The string to process

    Returns:
        The input string with any trailing date removed
    """
    parts = text.rstrip().split()
    if len(parts) >= 2 and DATE_RE.fullmatch(parts[-2]):
        return " ".join(parts[:-2]).rstrip(" -_/")
    return text

def log_new_line_file(path, message):
    with open(path, "a+", encoding="utf-8") as lf:
        lf.write(f"{dt.datetime.now().isoformat()} {message}\n")

async def get_video_duration(filepath) -> Optional[float]:
    """Get video duration in seconds using ffprobe.
    
    Returns:
        Duration in seconds as float, or None if unable to determine.
    """
    try:
        cmd = [
            "ffprobe",
            "-v", "quiet",
            "-show_entries", "format=duration",
            "-of", "csv=p=0",
            str(filepath)
        ]
        
        proc = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE
        )
        
        stdout, _ = await proc.communicate()
        
        if proc.returncode == 0:
            duration_str = stdout.decode().strip()
            if duration_str:
                return float(duration_str)
    except Exception:
        pass
    
    return None
