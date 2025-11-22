import asyncio
import datetime as dt
import logging
import os
import subprocess
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Optional, Tuple

from api import send_discord_notification
from globals import (
    DISCORD_WEBHOOK_URL,
    LOG_ROOT,
    MAX_YT_HEIGHT,
    STREAMLINK_SEGMENT_ATTEMPTS,
    STREAMLINK_SEGMENT_TIMEOUT,
    STREAMLINK_TIMEOUT,
    TWITCH_OAUTH_TOKEN,
    VOD_ROOT,
)
from utils import (
    get_video_duration,
    log_new_line_file,
    strip_end_date_time,
    yt_live_url,
)

logger = logging.getLogger("vod_watcher")


class Recorder(ABC):
    """Abstract base class for platform recorders."""

    def __init__(self, channel_name: str):
        self.channel_name = channel_name
        self.proc: Optional[subprocess.Popen] = None
        self.current_vod_fp: Optional[Path] = None
        self.final_vod_fp: Optional[Path] = None
        self.log_file_handle = None
        self.conversion_pending = False
        self.conversion_last_status = "NONE"
        self.conversion_process: Optional[asyncio.subprocess.Process] = None

    @property
    @abstractmethod
    def platform(self) -> str:
        pass

    def is_recording(self) -> bool:
        """Check if the recording process is running."""
        return self.proc is not None and self.proc.poll() is None

    def cleanup_partials(self):
        """Clean up partial recording files."""
        if self.current_vod_fp and self.current_vod_fp.exists():
            try:
                self.current_vod_fp.unlink()
                logger.info(f"Deleted partial VOD {self.current_vod_fp.name}")
            except Exception as e:
                logger.warning(f"Failed to delete partial VOD: {e}")

    def _generate_paths(self, title: str) -> Tuple[Path, Path]:
        """Generate paths for the VOD file and its log file."""
        title = strip_end_date_time(title).strip()
        safe = (
            "".join(ch if ch not in (os.sep, "\0") else "_" for ch in title) or "live"
        )
        day = dt.datetime.now().strftime("%Y-%m-%d")

        vod_dir = VOD_ROOT / self.channel_name
        log_dir = LOG_ROOT / self.channel_name
        vod_dir.mkdir(parents=True, exist_ok=True)
        log_dir.mkdir(parents=True, exist_ok=True)

        ext = ".ts" if self.platform == "twitch" else ".mp4"
        base = f"{day} {safe}"

        # Generate unique VOD filename
        for idx in range(1, 100):
            name = f"{base}{ext}" if idx == 1 else f"{base} ({idx}){ext}"
            vod_fp = vod_dir / name
            if not vod_fp.exists():
                break
        else:
            # Fallback if 100 variations exist
            name = f"{base} ({dt.datetime.now().timestamp()}){ext}"
            vod_fp = vod_dir / name

        log_fp = log_dir / f"{vod_fp.stem}.log"

        self.current_vod_fp = vod_fp
        self.final_vod_fp = (
            vod_fp.with_suffix(".mp4") if self.platform == "twitch" else vod_fp
        )

        return vod_fp, log_fp

    async def start(self, title: str):
        """Start recording a live stream."""
        vod_fp, log_fp = self._generate_paths(title)

        if DISCORD_WEBHOOK_URL:
            asyncio.create_task(
                send_discord_notification(self.platform, self.channel_name, title)
            )

        cmd = self._get_command(vod_fp)

        logger.info(f"START {self.platform}::{self.channel_name} -> {vod_fp.name}")

        # Setup logging
        if self.log_file_handle:
            try:
                self.log_file_handle.close()
            except Exception:
                pass

        try:
            log_fp.parent.mkdir(parents=True, exist_ok=True)
            self.log_file_handle = open(log_fp, "a", encoding="utf-8")
            self.log_file_handle.write(
                f"{dt.datetime.now().isoformat()} START {title} for {self.platform}::{self.channel_name} on VOD file {vod_fp.name}\n"
            )
            self.log_file_handle.flush()
        except Exception as e:
            logger.error(f"Failed to write to stream log {log_fp}: {e}")
            self.log_file_handle = None

        self.proc = subprocess.Popen(
            cmd,
            stdout=self.log_file_handle if self.log_file_handle else subprocess.DEVNULL,
            stderr=subprocess.STDOUT,
            text=False,
            preexec_fn=os.setsid
            if hasattr(os, "setsid")
            else None,  # Windows doesn't have setsid
        )

    async def stop(self, reason: str = "Unknown reason"):
        """Stop the recording process."""
        if self.proc is None:
            return

        process_id = self.proc.pid

        if self.log_file_handle:
            try:
                timestamp = dt.datetime.now().isoformat()
                self.log_file_handle.write(
                    f"\n\n{timestamp} STOPPING RECORDING: {reason}\n"
                )
                self.log_file_handle.write(f"{timestamp} Process ID: {process_id}\n")
                self.log_file_handle.flush()
                self.log_file_handle.close()
            except Exception as e:
                logger.error(f"Error closing log file: {e}")
            self.log_file_handle = None

        if self.proc.poll() is not None:
            self.proc = None
            return

        logger.info(f"STOP {self.platform}::{self.channel_name} (PID: {process_id})")

        try:
            self.proc.terminate()
            try:
                await asyncio.wait_for(asyncio.to_thread(self.proc.wait), timeout=10)
            except asyncio.TimeoutError:
                self.proc.kill()
                await asyncio.to_thread(self.proc.wait)
        except Exception as e:
            logger.error(f"Error stopping recording process: {e}")

        self.proc = None

        # Trigger post-processing if needed
        if (
            self.platform == "twitch"
            and self.current_vod_fp
            and self.current_vod_fp.exists()
        ):
            asyncio.create_task(self._convert_ts_to_mp4())

    @abstractmethod
    def _get_command(self, output_path: Path) -> list:
        pass

    async def _convert_ts_to_mp4(self):
        """Convert .ts to .mp4 (Twitch specific)."""
        # This logic is specific to Twitch but placed here for now or can be overridden
        pass


class YouTubeRecorder(Recorder):
    @property
    def platform(self) -> str:
        return "youtube"

    def _get_command(self, output_path: Path) -> list:
        url = yt_live_url(self.channel_name)
        # Ensure .mp4 extension
        if output_path.suffix != ".mp4":
            output_path = output_path.with_suffix(".mp4")

        return [
            "yt-dlp",
            url,
            "-o",
            str(output_path),
            "-f",
            f"bestvideo[ext=mp4][height<=?{MAX_YT_HEIGHT}]+bestaudio[ext=m4a]/best[ext=mp4]/best",
            "--live-from-start",
        ]

    def cleanup_partials(self):
        """Clean up yt-dlp temporary files."""
        super().cleanup_partials()

        if not self.current_vod_fp:
            return

        # yt-dlp creates files like:
        # base.f137.mp4.part
        # base.f137.mp4.ytdl
        # base.f140.mp4.part-Frag123
        # where "base" is the filename without extension

        base_stem = self.current_vod_fp.stem  # filename without .mp4
        parent_dir = self.current_vod_fp.parent

        if not parent_dir.exists():
            return

        for item in parent_dir.iterdir():
            if not item.is_file():
                continue

            # Check if file starts with the base stem (without extension)
            # This catches files like "base.f137.mp4.part" and "base.f140.mp4.ytdl"
            if item.name.startswith(base_stem):
                if ".part" in item.name or item.name.endswith(".ytdl"):
                    try:
                        item.unlink()
                        logger.info(f"Deleted partial artifact {item.name}")
                    except Exception as e:
                        logger.warning(
                            f"Failed to delete partial artifact {item.name}: {e}"
                        )


class TwitchRecorder(Recorder):
    @property
    def platform(self) -> str:
        return "twitch"

    def _get_command(self, output_path: Path) -> list:
        url = f"https://twitch.tv/{self.channel_name}"
        cmd = [
            "streamlink",
            url,
            "best",
            "--twitch-disable-ads",
            "--twitch-disable-hosting",
            "-o",
            str(output_path),
            "--stream-segment-attempts",
            str(STREAMLINK_SEGMENT_ATTEMPTS),
            "--stream-segment-timeout",
            str(STREAMLINK_SEGMENT_TIMEOUT),
            "--stream-timeout",
            str(STREAMLINK_TIMEOUT),
        ]
        if TWITCH_OAUTH_TOKEN:
            cmd.extend(
                ["--twitch-api-header", f"Authorization=OAuth {TWITCH_OAUTH_TOKEN}"]
            )
        return cmd

    async def _convert_ts_to_mp4(self):
        """Convert the current task's .ts file to .mp4 using FFmpeg."""
        if not self.current_vod_fp:
            return

        ts_filepath = self.current_vod_fp
        mp4_filepath = self.final_vod_fp

        if not ts_filepath.exists() or not mp4_filepath:
            return

        if mp4_filepath.exists():
            return

        log_fp = (LOG_ROOT / self.channel_name / ts_filepath.stem).with_suffix(".log")

        logger.info(
            f"CONVERTING {ts_filepath.name} to {mp4_filepath.name} for {self.channel_name}"
        )
        self.conversion_last_status = "CONVERTING"
        self.conversion_pending = True

        cmd = [
            "ffmpeg",
            "-hide_banner",
            "-loglevel",
            "error",
            "-i",
            str(ts_filepath),
            "-c",
            "copy",
            "-movflags",
            "+faststart",
            str(mp4_filepath),
        ]

        try:
            proc = await asyncio.create_subprocess_exec(
                *cmd,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.PIPE,
                preexec_fn=os.setsid if hasattr(os, "setsid") else None,
            )
            self.conversion_process = proc
            _, _ = await proc.communicate()

            if proc.returncode == 0:
                logger.info(f"SUCCESS converting {ts_filepath.name}")
                self.conversion_last_status = "SUCCESS"
                log_new_line_file(log_fp, "FFMPEG CONVERSION SUCCESS")

                # Duration check logic (simplified for brevity, can be expanded)
                ts_duration = await get_video_duration(ts_filepath)
                mp4_duration = await get_video_duration(mp4_filepath)

                if ts_duration and mp4_duration:
                    diff = ts_duration - mp4_duration
                    log_new_line_file(
                        log_fp,
                        f"DURATION CHECK - TS: {ts_duration}, MP4: {mp4_duration}, Diff: {diff}",
                    )

                # Delete original
                try:
                    ts_filepath.unlink(missing_ok=True)
                    log_new_line_file(
                        log_fp, f"DELETED ORIGINAL TS FILE: {ts_filepath.name}"
                    )
                except Exception as e:
                    logger.error(f"Failed to delete TS file: {e}")
            else:
                logger.error(f"Conversion failed for {ts_filepath.name}")
                self.conversion_last_status = "FAILED"
        except Exception as e:
            logger.error(f"Error during conversion: {e}")
            self.conversion_last_status = "ERROR"
        finally:
            self.conversion_pending = False
            self.conversion_process = None
