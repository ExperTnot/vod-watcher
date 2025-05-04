# VOD Watcher

A 24/7 YouTube & Twitch VOD recorder for Raspberry Pi 4 (or any Linux box).
Continuously monitors channels, applies keyword filters, and automatically records live streams to MP4.

---

## Features

* **Multi-platform**: YouTube & Twitch support
* **Keyword filtering**: Only record when your keyword appears in the stream title/tags
* **Auto-retry**: Probes have exponential back-off on failures
* **Rotating logs**: Both per-channel probe logs and a main watcher log
* **Live dashboard**: Terminal view with colors and countdown timers

---

## Installation

1. **Clone or copy** the repo:

2. **Install system deps**

   * Python 3.9+ (asyncio + `logging.handlers` used)
   * `ffmpeg` (for `yt-dlp` merging)

3. **Install Python packages**:

   ```bash
   pip3 install -r requirements.txt
   ```

4. **Install CLI tools**:

   ```bash
   # yt-dlp provides the YouTube downloader
   pip3 install yt-dlp
   # streamlink handles Twitch HLS streaming
   pip3 install streamlink
   ```

---

## Configuration

1. Rename `NAME_MEcheckme.txt` to `checkme.txt`.
2. Populate `checkme.txt` with lines like:

```csv
# platform, channel_or_id, keyword
twitch,xqc,drama
youtube,@pewdiepie,chatting
```

* **platform**: `youtube` or `twitch`
* **channel\_or\_id**: YouTube handle/ID or Twitch username
* **keyword**: (optional) only record when this appears in the title/tags

---

## Usage

```bash
python3 vod_watcher.py
```

You’ll see a continuously-updating dashboard:

```
2025-05-04 12:34:56  –  VOD Watcher   (next reload in 120s)

Platform Channel              Keyword      State      Next   Title
----------------------------------------------------------------------------
twitch   xqc                  drama        LIVE/REC   15s    🐦LIVE🐦CLICK🐦DRAMA🐦NEWS
youtube  @pewdiepie           chatting     OFF        42s    <not live>
```

* **CTRL+C** exists the program.

> WARNING - This shuts down all recording processes as well.

---

## Log Files

* **Main watcher log**:
  `./logs/vod_watcher.log` (rotating, up to 5 MB each, 3 backups)

* **Per-channel recording logs**:
  `/mnt/media/logs/processed/<channel>/*.log`
  Records start/stop timestamps and the `yt-dlp`/`streamlink` output.

To follow the main log in real time:

```bash
tail -f logs/vod_watcher.log
```

---

## Customization

You can tweak these constants at the top of `vod_watcher.py`:

* `RELOAD_INTERVAL` – how often to re-read `checkme.txt` (seconds)
					DO NOT SET THIS BELOW 60sec
* `PROBE_INTERVAL`  – baseline delay between probes (per channel)
					DO NOT SET THIS TOO LOW OR THE PLATFORMS WILL BLOCK YOU
* `PLATFORM_COOLDOWN` – minimum gap between probes on the same platform
					  DO NOT SET THIS TOO LOW OR THE PLATFORM WILL BLOCK YOU
* `MAX_YT_HEIGHT`   – maximum video height for YouTube recordings

---

## Troubleshooting

* **streamlink / yt-dlp not found**
  Ensure you installed them via `pip install streamlink yt-dlp` or your OS package manager.

* **Permission denied writing to `/mnt/media/...`**
  Verify that your user has write access to the mount points.

---
