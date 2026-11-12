"""Native binary management: locate on PATH / ./bin, auto-download if missing.

yt-dlp ships as a Python module (pip), but two native tools are needed for a
fully functional pipeline:

- **FFmpeg** — stream merging (video+audio -> mp4) and thumbnail conversion
  (WebP -> JPG).
- **Deno** — JavaScript runtime required by recent yt-dlp versions to solve
  YouTube's JS challenges. Without it, some high-res formats may be missing.

On first run this module fetches static builds into ./bin/ so the app works
out of the box.

Resolution order (per tool):
  1. ``<TOOL>_PATH`` environment variable (explicit override)
  2. ``<project>/bin/<tool>.exe`` (previous auto-download)
  3. ``<tool>`` found on the system PATH
  4. Auto-download from a static build mirror -> ``<project>/bin/<tool>.exe``
"""

from __future__ import annotations

import logging
import os
import re
import shutil
import tempfile
import threading
import urllib.request
import zipfile
from pathlib import Path
from typing import List, Optional, Pattern

logger = logging.getLogger("downloader.binaries")

BIN_DIR = Path(__file__).resolve().parent / "bin"

# Static FFmpeg builds for Windows. Both archives ship <root>/bin/ffmpeg.exe.
FFMPEG_SOURCES: List[str] = [
    "https://www.gyan.dev/ffmpeg/builds/ffmpeg-release-essentials.zip",
    "https://github.com/BtbN/FFmpeg-Builds/releases/latest/download/ffmpeg-master-latest-win64-gpl.zip",
]

# Official single-file Deno build for Windows (zip contains deno.exe at root).
DENO_SOURCES: List[str] = [
    "https://github.com/denoland/deno/releases/latest/download/deno-x86_64-pc-windows-msvc.zip",
]

FFMPEG_MEMBER_RE = re.compile(r"(?:^|/)bin/(ffmpeg|ffprobe)\.exe$")
DENO_MEMBER_RE = re.compile(r"(?:^|/)deno\.exe$")

_LOCK = threading.Lock()


def _fmt_bytes(num: float) -> str:
    for unit in ("B", "KiB", "MiB", "GiB"):
        if num < 1024 or unit == "GiB":
            return f"{num:.1f} {unit}"
        num /= 1024
    return f"{num:.1f} GiB"


def _find_existing(tool: str) -> Optional[Path]:
    """Return an already-available executable for ``tool``, if any (no download)."""
    env = os.environ.get(f"{tool.upper()}_PATH")
    if env:
        p = Path(env).expanduser()
        if p.is_file():
            return p
        logger.warning("%s_PATH is set but %s does not exist", tool.upper(), p)
    local = BIN_DIR / f"{tool}.exe"
    if local.is_file():
        return local
    which = shutil.which(tool)
    if which:
        return Path(which)
    if tool == "ffmpeg":
        # imageio-ffmpeg (PyPI) ships the gyan.dev static build; its wide
        # deployment gives the file established reputation, so Windows Smart
        # App Control accepts it where fresh unsigned builds are blocked.
        try:
            import imageio_ffmpeg

            return Path(imageio_ffmpeg.get_ffmpeg_exe())
        except Exception:
            pass
    return None


def ffmpeg_status() -> str:
    """Human-readable FFmpeg status for the health endpoint (never downloads)."""
    found = _find_existing("ffmpeg")
    return str(found) if found else "not found (auto-downloads on first run)"


def deno_status() -> str:
    """Human-readable Deno status for the health endpoint (never downloads)."""
    found = _find_existing("deno")
    return str(found) if found else "not found (auto-downloads on first run)"


def ensure_ffmpeg() -> Path:
    """Return a usable ffmpeg path, downloading a static build if necessary."""
    return _ensure_tool("ffmpeg", FFMPEG_SOURCES, FFMPEG_MEMBER_RE)


def ensure_deno() -> Path:
    """Return a usable deno path, downloading an official build if necessary."""
    return _ensure_tool("deno", DENO_SOURCES, DENO_MEMBER_RE)


def _ensure_tool(tool: str, sources: List[str], member_re: Pattern[str]) -> Path:
    with _LOCK:
        found = _find_existing(tool)
        if found:
            return found
        logger.info("%s not found locally - downloading ...", tool)
        last_error: Optional[Exception] = None
        for url in sources:
            try:
                _fetch_and_extract(url, member_re)
                exe = BIN_DIR / f"{tool}.exe"
                if not exe.is_file():
                    raise RuntimeError(f"archive did not contain {tool}.exe")
                logger.info("%s installed at %s", tool, exe)
                return exe
            except Exception as exc:  # try the next mirror
                logger.warning("%s download failed from %s: %s", tool, url, exc)
                last_error = exc
        raise RuntimeError(
            f"Could not obtain {tool} automatically. Install it manually, put "
            f"{tool}.exe on PATH (or in ./bin), or set the {tool.upper()}_PATH "
            f"environment variable. Last error: {last_error}"
        )


def _fetch_and_extract(url: str, member_re: Pattern[str]) -> None:
    BIN_DIR.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix="bin_dl_") as tmp:
        zip_path = Path(tmp) / "archive.zip"
        _download_file(url, zip_path)
        with zipfile.ZipFile(zip_path) as zf:
            members = [m for m in zf.namelist() if member_re.search(m)]
            if not members:
                raise RuntimeError("archive layout not recognised")
            for member in members:
                target = BIN_DIR / Path(member).name
                with zf.open(member) as src, open(target, "wb") as dst:
                    shutil.copyfileobj(src, dst)
                logger.info("Extracted %s", target)


def _download_file(url: str, dest: Path) -> None:
    request = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
    with urllib.request.urlopen(request, timeout=60) as response, open(dest, "wb") as out:
        total = int(response.headers.get("Content-Length") or 0)
        downloaded = 0
        next_log = 8 * 1024 * 1024  # log roughly every 8 MiB
        while True:
            chunk = response.read(1024 * 1024)
            if not chunk:
                break
            out.write(chunk)
            downloaded += len(chunk)
            if downloaded >= next_log:
                pct = f" ({downloaded / total * 100:.0f}%)" if total else ""
                logger.info(
                    "Downloading %s ... %s%s", url.rsplit('/', 1)[-1],
                    _fmt_bytes(downloaded), pct,
                )
                next_log += 8 * 1024 * 1024
    logger.info("Archive downloaded (%s)", _fmt_bytes(dest.stat().st_size))
