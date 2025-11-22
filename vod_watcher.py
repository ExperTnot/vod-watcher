#!/usr/bin/env python3
"""
vod_watcher.py — 24/7 YouTube & Twitch VOD recorder with logging, retries, colors, and clean shutdown
Linux only - will not work on Windows or MacOS.
"""

import asyncio
import logging
import logging.handlers
import platform
import sys
from pathlib import Path

# Check if running on Linux
if platform.system() != "Linux":
    print("\033[91mERROR: VOD Watcher only supports Linux operating systems.\033[0m")
    print("Current OS detected:", platform.system())
    sys.exit(1)

from globals import LOG_ROOT, VOD_ROOT
from monitoring.supervisor import Supervisor
from verifications import verify_paths

# ───── configuration ───── #
SCRIPT_DIR = Path(__file__).parent.resolve()
LOG_DIR = SCRIPT_DIR / "logs"
LOG_DIR.mkdir(parents=True, exist_ok=True)
MAIN_LOG = LOG_DIR / "vod_watcher.log"

# ───── logging setup ───── #
logger = logging.getLogger("vod_watcher")
logger.setLevel(logging.INFO)

# File handler
file_handler = logging.handlers.RotatingFileHandler(
    str(MAIN_LOG),
    maxBytes=5_000_000,
    backupCount=3,
)
file_handler.setFormatter(
    logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s")
)
logger.addHandler(file_handler)


def main():
    """Application entry point."""
    if not verify_paths():
        logger.error("Exiting due to file system permission/access errors.")
        sys.exit(1)

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    sup = Supervisor()
    try:
        loop.run_until_complete(sup.run())
    except KeyboardInterrupt:
        logger.info("Keyboard interrupt received")
        ans = (
            input("Exit requested. Let ongoing recordings finish? [Y/n] (default: Y): ")
            .strip()
            .lower()
        )
        finish = ans == "" or ans == "y"

        delete_partials = False
        if not finish:
            ans_delete = (
                input("Delete partial VOD recordings? [Y/n] (default: N): ")
                .strip()
                .lower()
            )
            delete_partials = ans_delete == "y"

        loop.run_until_complete(
            sup.shutdown(finish_recordings=finish, delete_partials=delete_partials)
        )
    finally:
        loop.close()
        logger.info("Exited cleanly")


if __name__ == "__main__":
    main()
