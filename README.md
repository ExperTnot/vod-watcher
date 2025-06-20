# VOD Watcher

A 24/7 YouTube & Twitch VOD recorder for Linux and Raspberry Pi.
Continuously monitors channels, applies keyword filters, and automatically records livestreams to MP4 files.

---

## Table of Contents

- [Features](#features)
- [Requirements](#requirements)
- [Installation](#installation)
- [Setup](#setup)
- [Optional Discord Integration](#optional-discord-integration)
- [Usage](#usage)
- [Running in the Background with tmux (recommended)](#running-in-the-background-with-tmux-recommended)
- [Log Files](#log-files)
- [Customization](#customization)
- [Troubleshooting](#troubleshooting)

---

## Features

* **Multi-service**: YouTube & Twitch support
* **Smart filtering**:
  * Record when keywords appear in the stream title
  * Record when the stream's category matches your keyword (case-insensitive)
* **Detached recordings**: Continue capturing streams even after program exits (optional)
* **Discord notifications**: Receive alerts when recordings start (optional)
* **Auto-retry**: Built-in exponential back-off for stability
* **Rotating logs**: Both per-channel and main watcher logs
* **Live dashboard**: Terminal view with colors and countdown timers

---

## Requirements

* **Linux** operating system (Ubuntu, Debian, Raspberry Pi OS, etc.)
* **Python 3.9+**
* **FFmpeg**: Required for video processing
* **External tools**: `requirements.txt`

---

## Installation

1. **Clone or download** this repository

2. **Install FFmpeg**:
   ```bash
   # Debian/Ubuntu/Raspberry Pi OS
   sudo apt install ffmpeg
   
   # Fedora
   sudo dnf install ffmpeg
   
   # Arch Linux
   sudo pacman -S ffmpeg
   ```

3. **Create a virtual environment** (recommended):
   ```bash
   # Create a virtual environment in .venv directory
   python3 -m venv .venv
   
   # Activate the virtual environment
   source .venv/bin/activate
   ```

4. **Install Python dependencies** (inside the virtual environment):
   ```bash
   pip install -r requirements.txt
   ```

---

## Setup

1. **Rename setup files**:
   * Rename `NAME_MEenv.py` to `env.py`
   * Rename `NAME_MEcheckme.txt` to `checkme.txt`

2. **Configure environment**:
   Edit `env.py` and set the required paths:
   ```python
   # Required settings
   VOD_ROOT = "/path/to/vod/storage"  # Where recordings will be saved
   LOG_ROOT = "/path/to/log/storage" # Where log files will be saved
   
   # Optional settings (leave empty if not needed)
   DISCORD_WEBHOOK_URL = ""  # For Discord notifications
   YOUTUBE_API_KEY = ""     # For YouTube channel profile pictures
   TWITCH_CLIENT_ID = ""    # For Twitch channel profile pictures
   TWITCH_CLIENT_SECRET = "" # For Twitch channel profile pictures
   ```

3. **Configure channels**:
   Edit `checkme.txt` with your channels and keywords:
   ```csv
   # platform, channel_or_id, keyword
   twitch,xqc,drama
   youtube,@pewdiepie,chatting
   ```

   * **platform**: Either `youtube` or `twitch`
   * **channel\_or\_id**: 
     * For YouTube: Channel handle (with @) or channel ID
     * For Twitch: Username
   * **keyword**: 
     * Records when this appears in the title
     * For Twitch: Also records when stream category exactly matches this keyword (case-insensitive)
     * Leave empty to record all streams from this channel

---

## Optional Discord Integration

To enable Discord notifications when recordings start:

1. Create a webhook in your Discord server (Server Settings → Integrations → Webhooks)
2. Copy the webhook URL to `DISCORD_WEBHOOK_URL` in your `env.py`

For profile pictures in notifications:

* **YouTube profile picture**: Get an API key from [Google Cloud Console](https://console.cloud.google.com/)
* **Twitch profile picture**: Register an application on the [Twitch Developer Console](https://dev.twitch.tv/console)

---

## Usage

```bash
python3 vod_watcher.py
```

You'll see a continuously-updating dashboard:

```
2025-05-04 12:34:56  –  VOD Watcher   (next reload in 120s)

Platform Channel              Keyword      State      Next   Title
----------------------------------------------------------------------------
twitch   xqc                  drama        LIVE/REC   15s    🐦LIVE🐦CLICK🐦DRAMA🐦NEWS
youtube  @pewdiepie           chatting     OFF        42s    <not live>
```

* **CTRL+C** exits the program.

You will be asked if you want to stop recording processes or let them finish. If you choose to let them finish, the recordings will continue as "detached processes" even after the main program exits.

> **Detached Recording Feature**: When you let recordings continue after program exit, VOD Watcher tracks these in a `.detached.json` file. Next time you start the program, it automatically recognizes and monitors these ongoing recordings.

---

## Running in the Background with tmux (recommended)

[tmux](https://github.com/tmux/tmux/wiki) is a terminal multiplexer that allows you to run processes in the background and detach/reattach to sessions, making it perfect for keeping VOD Watcher running 24/7.

### Basic tmux Commands

1. **Create a new named session**:
   ```bash
   tmux new -s vod-watcher
   ```
   This creates a new session named "vod-watcher" and attaches to it.

2. **Start VOD Watcher** inside the tmux session:
   ```bash
   source .venv/bin/activate
   python3 vod_watcher.py
   ```

3. **Detach from the session** (without stopping VOD Watcher):
   Press `Ctrl+b` then `d`
   
   Your VOD Watcher will continue running in the background, even if you close your terminal or disconnect from SSH.

4. **List all tmux sessions**:
   ```bash
   tmux ls
   ```
   You'll see something like: `vod-watcher: 1 windows (created Thu Jun 18 21:58:55 2025) [80x24]`

5. **Reattach to the session** later to check status or make changes:
   ```bash
   tmux a -t vod-watcher
   ```

6. **Exit the session** if you want to stop it (stop VOD Watcher first):
   Press `Ctrl+d` to exit the session.

---

## Log Files

* **Main log**: `./logs/vod_watcher.log` (rotating, up to 5MB each, 3 backups)
* **Channel logs**: Stored in your configured `LOG_ROOT` directory
* **Detached process tracking**: `.detached.json` tracks recordings that continue after program exit

To follow the main log in real time:

```bash
tail -f logs/vod_watcher.log
```

---

## Customization

You can adjust these settings in `env.py`:

* `RELOAD_INTERVAL` – How often to reload the channel list (min 45s)
* `PROBE_INTERVAL` – Base delay between checking the same channel (min 45s)
* `PLATFORM_COOLDOWN` – Minimum gap between probes on the same platform (min 20s)

**IMPORTANT**: Setting intervals too low may get you rate-limited or IP-banned by YouTube/Twitch!

---

## Troubleshooting

* **No recordings being made**
  * Verify your `checkme.txt` format is correct
  * Check console output and logs for errors
  * Ensure FFmpeg, yt-dlp, and streamlink are properly installed

* **Discord notifications not working**
  * Verify your webhook URL is correct
  * Check that your bot has permission to post in the channel

* **Permission issues**
  * Ensure you have write access to both VOD_ROOT and LOG_ROOT directories

---
