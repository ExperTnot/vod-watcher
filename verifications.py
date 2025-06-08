"""
verifications.py — System verification functions for vod_watcher
"""

import json
import logging
import subprocess
import sys
from pathlib import Path
from env import VOD_ROOT, LOG_ROOT

# Setup logger
logger = logging.getLogger("vod_watcher")

# Define paths
SCRIPT_DIR = Path(__file__).parent.resolve()
CHECK_FILE = SCRIPT_DIR / "checkme.txt"
LOG_DIR = SCRIPT_DIR / "logs"
DETACHED_FILE = SCRIPT_DIR / ".detached.json"
VOD_ROOT = Path(VOD_ROOT)
LOG_ROOT = Path(LOG_ROOT)


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
    """Verify all required paths, files, and dependencies.

    Main verification function that checks:
    1. Required directories exist and are writable
    2. Required files exist and are readable/writable
    3. FFmpeg is installed and available

    Returns:
        bool: True if all verifications passed, False otherwise
    """
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
    """Verify that all required directories exist and are writable.

    Creates directories if they don't exist. Tests write permissions
    by creating and deleting a temporary test file in each directory.

    Returns:
        bool: True if all directories exist and are writable, False otherwise
    """
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
    """Verify that all required files exist and are readable/writable.

    Creates configuration file with example content if it doesn't exist.
    Tests read permissions on the configuration file.
    Verifies the detached process state file is valid JSON, resetting it if not.

    Returns:
        bool: True if all file checks pass, False otherwise
    """
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


def is_mp4_valid(mp4_path: Path) -> bool:
    """Perform a basic validation on an MP4 file."""
    if not mp4_path.exists():
        return False
    # Basic check: non-zero size. More robust checks (e.g., ffprobe) can be added later.
    if mp4_path.stat().st_size > 0:
        return True
    logger.warning(f"MP4 file {mp4_path.name} is zero-sized or invalid.")
    return False


def verify_vod_files(channel_name: str):
    """Verify VOD files for a given channel, cleaning up .ts files if a valid .mp4 exists."""
    if not channel_name:
        logger.error("verify_vod_files called with no channel name.")
        return

    channel_vod_dir = VOD_ROOT / channel_name
    if not channel_vod_dir.exists() or not channel_vod_dir.is_dir():
        logger.debug(f"VOD directory for channel {channel_name} not found at {channel_vod_dir}. Skipping VOD file verification.")
        return

    logger.info(f"Verifying VOD files for channel: {channel_name} in {channel_vod_dir}")

    for item in channel_vod_dir.iterdir():
        if item.is_file() and item.suffix == ".mp4":
            mp4_file = item
            ts_file = mp4_file.with_suffix(".ts")

            if ts_file.exists():
                logger.debug(f"Found MP4: {mp4_file.name} and corresponding TS: {ts_file.name}")
                if is_mp4_valid(mp4_file):
                    try:
                        ts_file.unlink()
                        logger.info(f"Deleted .ts file {ts_file.name} as valid .mp4 {mp4_file.name} exists.")
                    except OSError as e:
                        logger.error(f"Failed to delete .ts file {ts_file.name}: {e}")
                else:
                    logger.warning(
                        f"MP4 file {mp4_file.name} is invalid or zero-sized. Corresponding .ts file {ts_file.name} will be kept."
                    )
            # else: MP4 exists, TS does not. This is normal for YouTube or already cleaned up Twitch VODs.

    logger.info(f"Finished VOD file verification for channel: {channel_name}")

