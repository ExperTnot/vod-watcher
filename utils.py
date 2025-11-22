"""
utils.py — Utility functions for vod_watcher
"""

import asyncio
import re
import time
import datetime as dt
import subprocess
from typing import Dict, Optional
from globals import PROBE_INTERVAL, PLATFORM_COOLDOWN

# Compile regex pattern for date detection
DATE_RE = re.compile(r"\b\d{4}-\d{2}-\d{2}$")


class RateLimiter:
    """Manages rate limiting for platform probes."""

    def __init__(self):
        self._last_probe_time: Dict[str, float] = {"youtube": 0.0, "twitch": 0.0}
        self._platform_locks: Dict[str, asyncio.Lock] = {
            "youtube": asyncio.Lock(),
            "twitch": asyncio.Lock(),
        }
        self._platform_counts: Dict[str, int] = {"youtube": 1, "twitch": 1}

    def update_counts(self, counts: Dict[str, int]):
        """Update the number of active channels per platform."""
        self._platform_counts.update(counts)
        # Ensure at least 1 to avoid division by zero or invalid intervals
        for k in self._platform_counts:
            self._platform_counts[k] = max(1, self._platform_counts[k])

    def get_interval(self, platform: str) -> int:
        """Calculate the interval between platform probes."""
        n = self._platform_counts.get(platform, 1)
        return max(PROBE_INTERVAL, PLATFORM_COOLDOWN * n)

    async def wait_for_slot(self, platform: str) -> float:
        """Wait for the next available slot to probe a platform API."""
        lock = self._platform_locks.get(platform)
        if not lock:
            # Should not happen if initialized correctly, but safe fallback
            return time.time()

        async with lock:
            now = time.time()
            scheduled = max(now, self._last_probe_time[platform] + PLATFORM_COOLDOWN)
            self._last_probe_time[platform] = scheduled
            if scheduled > now:
                await asyncio.sleep(scheduled - now)
            return scheduled


# Global instance
rate_limiter = RateLimiter()


def yt_live_url(name: str) -> str:
    """Construct a YouTube live URL from a channel name or ID."""
    name = name.strip()
    if name.startswith("@"):
        path = name
    elif re.match(r"UC[A-Za-z0-9_-]{22}", name):
        path = f"channel/{name}"
    else:
        path = f"@{name}"
    return f"https://www.youtube.com/{path}/live"


def strip_end_date_time(text: str) -> str:
    """Remove date and time from the end of a string."""
    parts = text.rstrip().split()
    if len(parts) >= 2 and DATE_RE.fullmatch(parts[-2]):
        return " ".join(parts[:-2]).rstrip(" -_/")
    return text


def log_new_line_file(path, message):
    with open(path, "a+", encoding="utf-8") as lf:
        lf.write(f"{dt.datetime.now().isoformat()} {message}\n")


async def get_video_duration(filepath) -> Optional[float]:
    """Get video duration in seconds using ffprobe."""
    try:
        cmd = [
            "ffprobe",
            "-v",
            "quiet",
            "-show_entries",
            "format=duration",
            "-of",
            "csv=p=0",
            str(filepath),
        ]

        proc = await asyncio.create_subprocess_exec(
            *cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE
        )

        stdout, _ = await proc.communicate()

        if proc.returncode == 0:
            duration_str = stdout.decode().strip()
            if duration_str:
                return float(duration_str)
    except Exception:
        pass

    return None
