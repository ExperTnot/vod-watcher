"""
api.py — External API functions for vod_watcher
"""

import datetime as dt
import logging
from typing import Optional

import aiohttp

# Import configuration variables from env.py
from env import (
    DISCORD_WEBHOOK_URL,
    YOUTUBE_API_KEY,
    TWITCH_CLIENT_ID,
    TWITCH_CLIENT_SECRET,
)

# Setup logging
logger = logging.getLogger("vod_watcher")


async def get_twitch_access_token() -> Optional[str]:
    """Obtain an OAuth access token for the Twitch API.

    Uses client credentials flow to authenticate with Twitch servers.
    Requires TWITCH_CLIENT_ID and TWITCH_CLIENT_SECRET to be set.

    Returns:
        str or None: The OAuth access token if successful, None otherwise
    """
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
    """Fetch a Twitch channel's profile image URL.

    Uses the Twitch Helix API to retrieve user profile data for the given channel.

    Args:
        channel_name: The Twitch channel login name to fetch thumbnail for

    Returns:
        str or None: URL to the channel's profile image if successful, None otherwise
    """
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
    """Fetch a YouTube channel's thumbnail URL.

    Uses the YouTube Data API v3 to retrieve the channel's thumbnail.
    Requires YOUTUBE_API_KEY to be set.

    Args:
        channel_name: The YouTube channel name or handle to fetch thumbnail for

    Returns:
        str or None: URL to the channel's thumbnail image if successful, None otherwise
    """
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
    """Send a Discord webhook notification when a stream recording starts.

    Creates a rich embed with the channel name, stream title, platform, timestamp,
    and channel profile image (if available).

    Args:
        platform: The platform name
        channel_name: The channel name that is being recorded
        title: The title of the stream being recorded
    """
    logger.info(f"Recording started: {platform.capitalize()}/{channel_name} - {title}")

    if not DISCORD_WEBHOOK_URL:
        logger.debug("Discord webhook URL not set, skipping webhook notification.")
        return

    now_str = dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    embed = {
        "title": f"{"🔴" if platform.lower() == 'youtube' else "🟣"} {channel_name}",
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
