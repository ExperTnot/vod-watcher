import asyncio
import json
import logging
from abc import ABC, abstractmethod
from typing import Tuple

from utils import yt_live_url

logger = logging.getLogger("vod_watcher")


class Probe(ABC):
    """Abstract base class for platform probes."""

    def __init__(self, channel_name: str, keyword: str = ""):
        self.channel_name = channel_name
        self.keyword = keyword

    @abstractmethod
    async def check(self) -> Tuple[bool, bool, str]:
        """Check if the channel is live.

        Returns:
            Tuple containing:
            - bool: True if channel is live, False otherwise
            - bool: True if keyword matches (or no keyword), False otherwise
            - str: Stream title if available, empty string otherwise
        """
        pass


class YouTubeProbe(Probe):
    """Probe for YouTube channels."""

    async def check(self) -> Tuple[bool, bool, str]:
        """Check if a YouTube channel is currently live streaming.

        Uses yt-dlp to check the live status and fetch the stream title.
        """
        url = yt_live_url(self.channel_name)
        cmd = ["yt-dlp", "--skip-download", "--print", "%(is_live)s|%(title)s", url]

        try:
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
        except Exception as e:
            logger.error(f"YouTube probe failed for {self.channel_name}: {e}")
            return False, False, ""


class TwitchProbe(Probe):
    """Probe for Twitch channels."""

    async def check(self) -> Tuple[bool, bool, str]:
        """Check if a Twitch channel is currently live streaming.

        Uses streamlink to check the live status and fetch the stream title,
        tags, and category.
        """
        url = f"https://twitch.tv/{self.channel_name}"
        cmd = ["streamlink", "--json", url, "best"]

        try:
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
            keyword_ok = (not self.keyword) or (
                self.keyword in f"{title} {tagstr}".lower()
            )

            # If keyword doesn't match in title/tags, also check if it matches the category
            if not keyword_ok and self.keyword:
                category = meta.get("category") or ""

                if category:
                    logger.debug(
                        f"Twitch channel {self.channel_name} is streaming in category: {category}"
                    )

                    if self.keyword.lower() == category.lower():
                        logger.debug(
                            f"Twitch channel {self.channel_name} has category '{category}' matching keyword '{self.keyword}', recording even though title doesn't contain keyword"
                        )
                        keyword_ok = True

            return True, keyword_ok, title
        except Exception as e:
            logger.error(f"Twitch probe failed for {self.channel_name}: {e}")
            return False, False, ""
