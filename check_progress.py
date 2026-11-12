"""Check channel download progress without downloading anything.

Usage:
    python check_progress.py <channel_url> [output_dir]

Flat-scans the channel (no downloads), walks the output folder, and reports
how many videos are already fully downloaded, how many remain, and which
folders hold interrupted/partial downloads.
"""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Any, Dict, List

import downloader

# Windows console default (cp1252) can't render some title characters
sys.stdout.reconfigure(encoding="utf-8", errors="replace")

DEFAULT_OUTPUT = Path(r"D:\Projects\YouTube_Downloads")


def main() -> None:
    if len(sys.argv) < 2:
        print(__doc__)
        sys.exit(1)
    url = sys.argv[1]
    out = Path(sys.argv[2]) if len(sys.argv) > 2 else DEFAULT_OUTPUT

    frames: List[Dict[str, Any]] = []
    job = downloader.DownloadJob(url, out, frames.append)
    job._scan()  # network flat-scan + disk resume check (no downloading)

    count = next((f for f in frames if f["event"] == "count"), None)
    if count:
        print(f"Channel        : {count.get('channel')}")
        print(f"Total videos   : {count.get('total')}")
        print(f"Already done   : {count.get('already')}")
        print(f"Remaining      : {count.get('remaining')}")

    if not out.is_dir():
        print(f"\nOutput folder does not exist: {out}")
        return
    partial = sorted(
        (sub for sub in out.iterdir() if sub.is_dir() and job._find_media(sub) is None),
        key=lambda p: p.name.lower(),
    )
    if partial:
        print(f"\nInterrupted / empty folders ({len(partial)}):")
        for sub in partial:
            print(f"  - {sub.name}")
            for f in sorted(sub.iterdir())[:6]:
                if f.is_file():
                    print(f"      {f.name}  ({f.stat().st_size / 1e6:.1f} MB)")


if __name__ == "__main__":
    main()
