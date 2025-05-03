#!/usr/bin/env python3
"""
vod_watcher.py — 24 / 7 YouTube & Twitch VOD recorder for Raspberry Pi 4
=========================================================================
OFF           – channel offline
LIVE (no rec) – live but keyword not found → not recording
LIVE / REC    – live, keyword match → recording
"""

import asyncio, contextlib, csv, datetime as dt, json, os, re, subprocess, sys, time
from pathlib import Path
from typing import Dict, Optional, Tuple

# ───── configuration ───── #
CHECK_FILE      = Path.home() / "code" / "vod_watch" / "checkme.txt"
VOD_ROOT        = Path("/mnt/media/VODs/processed")
LOG_ROOT        = Path("/mnt/media/logs/processed")

RELOAD_INTERVAL = 180      # checkme.txt reload interval  (seconds)
PROBE_INTERVAL  = 180      # live‑status probe interval  (seconds)
DASH_FPS        = 1        # dashboard refresh rate      (frames / sec)

MAX_YT_HEIGHT   = 1080
CLEAR_CMD       = "clear" if os.getenv("TERM") else "cls"

USE_COLOR = sys.stdout.isatty() and ("TERM" in os.environ)
RED, YELLOW, GREEN, RESET = (
    ("\033[91m", "\033[93m", "\033[92m", "\033[0m") if USE_COLOR else ("", "", "", "")
)

DATE_RE  = re.compile(r"\b\d{4}-\d{2}-\d{2}$")

# ----- yt helpers ----- #
def yt_live_url(name: str) -> str:
    """
    Return the /live URL for a YouTube channel given:
      • @handle (e.g. @MonoMonet)
      • channel ID starting with UC…      (YouTube’s canonical ID)
      • plain username (legacy channels)

    Rules:
      @handle      →  youtube.com/@handle/live
      UCxxxxxxxxxx →  youtube.com/channel/UC…/live
      else         →  youtube.com/@username/live   (best works today)
    """
    name = name.strip()
    if name.startswith("@"):
        path = name                      # already has '@'
    elif re.match(r"UC[A-Za-z0-9_-]{22}", name):
        path = f"channel/{name}"
    else:
        path = f"@{name}"                # treat as legacy user name
    return f"https://www.youtube.com/{path}/live"

def strip_end_date_time(text: str) -> str:
    """
    If the *last two whitespace‑separated tokens* look like
    '<YYYY‑MM‑DD> <HH:MM>' remove them.
    """
    parts = text.rstrip().split()
    if not parts:
        return text

    if len(parts) >= 2 and DATE_RE.fullmatch(parts[-2]):
        return " ".join(parts[:-2]).rstrip(" -_/")

    return text

# ───── channel task ───── #
class ChannelTask:
    def __init__(self, platform: str, name: str, keyword: str):
        self.platform = platform.lower().strip()
        self.name     = name.strip()
        self.keyword  = keyword.lower().strip()

        self.proc: Optional[subprocess.Popen] = None
        self.current_title = ""
        self.live_raw   = False
        self.keyword_ok = False

        self.next_probe = time.time()          # for countdown display
        self.loop: Optional[asyncio.Task] = None

    # lifecycle --------------------------------------------------------------
    def start(self): self.loop = asyncio.create_task(self._poll_loop())

    async def stop(self):
        if self.loop:
            self.loop.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self.loop
        await self._stop_recording()

    def is_recording(self) -> bool:
        return self.proc and self.proc.poll() is None

    # main poll loop ---------------------------------------------------------
    async def _poll_loop(self):
        while True:
            self.next_probe = time.time() + PROBE_INTERVAL
            try:
                live, ok, title = await self._probe()
                self.live_raw, self.keyword_ok = live, ok

                if live:
                    title = strip_end_date_time(title)
                    if title:
                        self.current_title = title
                    elif not self.current_title:
                        self.current_title = "<title unavailable>"
                else:
                    self.current_title = "<not live>"

                if live and ok and not self.is_recording():
                    await self._start_recording(title)
                elif (not live or not ok) and self.is_recording():
                    await self._stop_recording()
            except Exception as e:
                print(f"[{self.name}] probe error: {e}", file=sys.stderr)

            await asyncio.sleep(PROBE_INTERVAL)

    # probes -----------------------------------------------------------------
    async def _probe(self) -> Tuple[bool, bool, str]:
        return (await self._probe_youtube()) if self.platform == "youtube" else (await self._probe_twitch())

    async def _probe_youtube(self):
        """
        Return (is_live, keyword_ok, title) for a YouTube channel.

        • Accepts 'True', 'true', ' TRUE ', etc.
        • Falls back cleanly if yt‑dlp returns nothing or a malformed line.
        • Appends the raw yt‑dlp output to …/<channel>/probe.log for debugging.
        """
        url = yt_live_url(self.name)
        cmd = ["yt-dlp", "--skip-download", "--print", "%(is_live)s|%(title)s", url]

        proc = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.DEVNULL
        )
        raw, _ = await proc.communicate()

        # ── DEBUG LOG ─────────────────────────────────────────────
        #   Append the exact line we got, with a timestamp.
        from pathlib import Path, PurePath
        import datetime as dt
        probe_log = (LOG_ROOT / self.name / "probe.log")
        probe_log.parent.mkdir(parents=True, exist_ok=True)
        with probe_log.open("a", encoding="utf-8") as fh:
            ts = dt.datetime.now().isoformat(timespec="seconds")
            fh.write(f"{ts} | {raw.decode(errors='replace').rstrip()}\n")
            fh.write(f"{ts} | {cmd}")
        # ──────────────────────────────────────────────────────────

        if not raw:
            return False, False, ""

        try:
            live_flag, title = raw.decode().strip().split("|", 1)
        except ValueError:
            return False, False, ""

        live = live_flag.strip().lower() == "true"
        keyword_ok = (not self.keyword) or (self.keyword in title.lower())
        return live, keyword_ok, title


    async def _probe_twitch(self):
        """
        Check whether a Twitch channel is live and extract its title + tags.

        Streamlink JSON layouts:
        ───────────────────────────────────────────────────────────
        * old  (≤ 5.5): {"title": "...", "tags": ["a", "b"], ...}
        * new  (≥ 5.6): {"metadata": {"title": "...", "tags": ["a", "b"], ...}, ...}
        """
        url = f"https://twitch.tv/{self.name}"
        cmd = ["streamlink", "--json", url, "best"]
        proc = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE
        )
        out, _ = await proc.communicate()

        if proc.returncode != 0:
            return False, False, ""

        try:
            info = json.loads(out)
        except json.JSONDecodeError:
            return False, False, ""

        meta   = info.get("metadata", {})
        title  = meta.get("title") or info.get("title") or ""
        tags   = meta.get("tags")  or info.get("tags")  or []
        tagstr = " ".join(tags) if isinstance(tags, (list, tuple)) else str(tags)

        keyword_ok = (not self.keyword) or (self.keyword in f"{title} {tagstr}".lower())
        return True, keyword_ok, title

    # recorder helpers -------------------------------------------------------
    def _paths(self, title: str):
        title = strip_end_date_time(title)
        safe = re.sub(r"[^\w\s.\-]+", "_", title)[:120].strip() or "live"
        day  = dt.datetime.now().strftime("%Y-%m-%d")
        vod_dir = VOD_ROOT / self.name
        log_dir = LOG_ROOT / self.name
        vod_dir.mkdir(parents=True, exist_ok=True)
        log_dir.mkdir(parents=True, exist_ok=True)

        base = vod_dir / f"{day}_{safe}"
        idx  = 1
        vod_fp = base.with_suffix(".mp4")
        while vod_fp.exists():
            idx += 1
            vod_fp = vod_dir / f"{day}_{safe}_part-{idx}.mp4"
        log_fp = (log_dir / vod_fp.stem).with_suffix(".log")
        return vod_fp, log_fp

    async def _start_recording(self, title: str):
        vod, log = self._paths(title)
        if self.platform == "youtube":
            url = f"https://www.youtube.com/{'@' if '@' in self.name else 'channel/'}{self.name}/live"
            cmd = ["yt-dlp", url, "-o", str(vod),
                   "-f", f"bestvideo[height<=?{MAX_YT_HEIGHT}]+bestaudio/best[height<=?{MAX_YT_HEIGHT}]",
                   "--merge-output-format", "mp4", "--no-part"]
        else:
            url = f"https://twitch.tv/{self.name}"
            cmd = ["streamlink", url, "best",
                   "--twitch-disable-hosting", "--twitch-disable-ads",
                   "-o", str(vod)]
        print(f"[+] Recording {self.platform}::{self.name} → {vod.name}")
        logf = open(log, "a", buffering=1)
        logf.write(f"{dt.datetime.now().isoformat()}  START  {title}\n")
        self.proc = subprocess.Popen(cmd, stdout=logf, stderr=logf, text=True)

    async def _stop_recording(self):
        if self.is_recording():
            print(f"[ ] Stopping  {self.platform}::{self.name}")
            self.proc.terminate()
            try:
                await asyncio.wait_for(asyncio.to_thread(self.proc.wait), timeout=10)
            except asyncio.TimeoutError:
                self.proc.kill()
        self.proc = None

# ───── supervisor ───── #
class Supervisor:
    def __init__(self):
        self.tasks: Dict[str, ChannelTask] = {}
        self.last_reload = 0.0
        self.stop_evt = asyncio.Event()
        self.reload_task: Optional[asyncio.Task] = None
        self.dash_task:   Optional[asyncio.Task] = None

    # main ------------------------------------------------------------
    async def run(self):
        self.reload_task = asyncio.create_task(self._reload_loop())
        self.dash_task   = asyncio.create_task(self._dashboard_loop())
        await self.stop_evt.wait()

        # cancel loops gracefully
        for t in (self.reload_task, self.dash_task):
            if t: t.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await asyncio.gather(*(t for t in (self.reload_task, self.dash_task) if t))

    async def shutdown(self):
        self.stop_evt.set()
        for t in (self.reload_task, self.dash_task):
            if t: t.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await asyncio.gather(*(t for t in (self.reload_task, self.dash_task) if t))
        await asyncio.gather(*(task.stop() for task in self.tasks.values()), return_exceptions=True)

    # reload loop -----------------------------------------------------
    async def _reload_loop(self):
        await self._load_watchlist()
        while True:
            await asyncio.sleep(RELOAD_INTERVAL)
            await self._load_watchlist()

    async def _load_watchlist(self):
        self.last_reload = time.time()
        seen = set()
        if not CHECK_FILE.exists():
            return
        with CHECK_FILE.open() as fh:
            for row in csv.reader(fh):
                if not row or row[0].strip().startswith("#"):
                    continue
                platform, channel, *kw = [c.strip() for c in row]
                key = f"{platform.lower()}::{channel.lower()}"
                seen.add(key)
                if key not in self.tasks:
                    task = ChannelTask(platform, channel, kw[0] if kw else "")
                    self.tasks[key] = task
                    task.start()
        # prune removed
        for key in list(self.tasks):
            if key not in seen:
                asyncio.create_task(self.tasks[key].stop())
                del self.tasks[key]

    # dashboard loop --------------------------------------------------
    async def _dashboard_loop(self):
        while True:
            self._render_dash()
            await asyncio.sleep(1 / DASH_FPS)

    def _render_dash(self):
        reload_rem = max(0, int(self.last_reload + RELOAD_INTERVAL - time.time()))
        # compute earliest next probe among tasks
        if self.tasks:
            probe_rem = max(0, int(min(t.next_probe for t in self.tasks.values()) - time.time()))
        else:
            probe_rem = 0

        os.system(CLEAR_CMD)
        print(f"{dt.datetime.now():%Y-%m-%d %H:%M:%S}  –  VOD Watcher   "
              f"(Next reload {reload_rem}s | Next probe {probe_rem}s)\n")
        print(f"{'Platform':8} {'Channel':20} {'Keyword':12} {'State':14} Title")
        print("-" * 120)

        for t in self.tasks.values():
            if t.live_raw:
                if t.keyword_ok and t.is_recording():
                    colour, state = GREEN, "LIVE / REC"
                else:
                    colour, state = YELLOW, "LIVE"
            else:
                colour, state = RED, "OFF"

            print(f"{t.platform:8} {t.name:20} {t.keyword[:11]:12} "
                  f"{colour}{state:14}{RESET} {t.current_title[:80]}")

# ───── entry ───── #
if __name__ == "__main__":
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    sup = Supervisor()
    try:
        loop.run_until_complete(sup.run())
    except KeyboardInterrupt:
        print("\nExiting cleanly…")
        loop.run_until_complete(sup.shutdown())
    finally:
        loop.close()
