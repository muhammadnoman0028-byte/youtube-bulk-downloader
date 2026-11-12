"""yt-dlp engine wrapper for bulk channel downloads.

Bridges yt-dlp's synchronous API with a thread-safe frame callback that the
FastAPI layer broadcasts to every connected WebSocket client.

Frame schema (JSON, one dict per event):
  {"event": "job",       "job_id": ..., "status": "started|completed|failed"}
  {"event": "count",     "job_id": ..., "total": N, "channel": "...",
                         "already": N, "remaining": N}
  {"event": "progress",  "job_id": ..., "status": str, "filename": str,
                         "percent": float|None, "speed": str, "eta": str}
  {"event": "video_done","job_id": ..., "index": k, "total": N,
                         "title": str, "path": str}
  {"event": "log",       "job_id": ..., "message": str, "level": str}
"""

from __future__ import annotations

import json
import logging
import re
import shutil
import threading
import time
import uuid
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Set

import yt_dlp
from yt_dlp.utils import make_archive_id, sanitize_filename

import binaries

logger = logging.getLogger("downloader")

Frame = Dict[str, Any]
FrameEmitter = Callable[[Frame], None]

_VIDEO_EXTS = (".mp4", ".mkv", ".webm", ".m4v", ".mov", ".avi", ".flv")
# Matches unmerged format streams (e.g. "Title.f137.mp4") left behind when a
# download is interrupted — they are NOT final files.
_FRAGMENT_RE = re.compile(r"\.f\d+$")


def _fmt_bytes(num: Optional[float]) -> str:
    if not num:
        return "—"
    value = float(num)
    for unit in ("B", "KiB", "MiB", "GiB", "TiB"):
        if value < 1024:
            return f"{int(value)} B" if unit == "B" else f"{value:.2f} {unit}"
        value /= 1024
    return f"{value:.2f} PiB"


def _fmt_speed(num: Optional[float]) -> str:
    return f"{_fmt_bytes(num)}/s" if num else "—"


def _fmt_eta(seconds: Optional[float]) -> str:
    if seconds is None or seconds < 0:
        return "—"
    seconds = int(seconds)
    hours, rem = divmod(seconds, 3600)
    minutes, secs = divmod(rem, 60)
    return f"{hours:02d}:{minutes:02d}:{secs:02d}" if hours else f"{minutes:02d}:{secs:02d}"


def _basename(path: Any) -> str:
    try:
        return Path(path).name if path else ""
    except (TypeError, ValueError):
        return str(path)


class _LogCapture:
    """yt-dlp logger: routes extraction/download/postprocess output to frames."""

    def __init__(self, emit_log: Callable[[str, str], None]) -> None:
        self._emit_log = emit_log

    def debug(self, msg: str) -> None:
        # yt-dlp routes its normal output lines through logger.debug;
        # keep the tagged stage lines ([download]/[Merger]/[youtube] ...).
        if msg and msg.startswith("["):
            self._emit_log(msg, "info")

    def info(self, msg: str) -> None:
        self.debug(msg)

    def warning(self, msg: str) -> None:
        if msg:
            self._emit_log(f"⚠ {msg}", "warning")

    def error(self, msg: str) -> None:
        if msg:
            self._emit_log(f"✖ {msg}", "error")


class _SilentLogger:
    """Suppresses yt-dlp output for the metadata pre-scan."""

    def debug(self, msg: str) -> None: ...
    def info(self, msg: str) -> None: ...
    def warning(self, msg: str) -> None: ...
    def error(self, msg: str) -> None: ...


class DownloadJob:
    """One bulk-download run for a channel, playlist, or single video URL."""

    def __init__(
        self,
        url: str,
        output_dir: Path,
        on_frame: FrameEmitter,
        job_id: Optional[str] = None,
    ) -> None:
        self.url = url
        self.output_dir = Path(output_dir)
        self.on_frame = on_frame
        self.job_id = job_id or uuid.uuid4().hex[:12]
        self.last_status: str = "queued"
        self._started = time.time()
        self._total: Optional[int] = None
        self._entries: List[Dict[str, Any]] = []
        self._done: Set[str] = set()
        self._saved_this_run = 0
        self._converted: Set[str] = set()  # thumbnail-conversion log dedupe
        self._lock = threading.Lock()

    # ------------------------------------------------------------------ #
    # frame emission                                                     #
    # ------------------------------------------------------------------ #
    def emit(self, event: str, **fields: Any) -> None:
        try:
            self.on_frame({"event": event, "job_id": self.job_id, **fields})
        except Exception:
            logger.exception("failed to emit frame (event=%s)", event)

    def _log(self, message: str, level: str = "info") -> None:
        self.emit("log", message=message, level=level)

    # ------------------------------------------------------------------ #
    # pre-scan: video count + channel title                              #
    # ------------------------------------------------------------------ #
    def _scan(self) -> None:
        opts = {
            "extract_flat": "in_playlist",
            "skip_download": True,
            "quiet": True,
            "logger": _SilentLogger(),
        }
        with yt_dlp.YoutubeDL(opts) as ydl:
            info = ydl.extract_info(self.url, download=False)
        if info is None:
            raise RuntimeError("yt-dlp returned no channel info")

        entries = info.get("entries") or []
        if not isinstance(entries, list):
            entries = list(entries)
        channel = (
            info.get("channel")
            or info.get("uploader")
            or info.get("title")
            or "channel"
        )
        if entries:
            self._total = len(entries)
            self._entries = [e for e in entries if isinstance(e, dict)]
            self._log(f"Found {self._total} video(s) in '{channel}'")
        else:  # single video URL
            self._total = 1
            self._entries = [info]
            self._log(f"Single video detected: '{info.get('title', 'video')}'")

        already = self._prepare_resume()
        self.emit(
            "count",
            total=self._total,
            channel=channel,
            already=already,
            remaining=max((self._total or 0) - already, 0),
        )

    # ------------------------------------------------------------------ #
    # resume: detect what is already on disk                              #
    # ------------------------------------------------------------------ #
    @property
    def _archive_file(self) -> Path:
        """yt-dlp download-archive file, kept inside the output folder."""
        return self.output_dir / "download_archive.txt"

    def _collect_existing(self) -> Dict[str, str]:
        """Map of video IDs → extractor keys for fully downloaded videos.

        A video counts as downloaded only when its subfolder holds a *final*
        media file — leftover fragment streams (``*.f137.mp4``) from an
        interrupted run do not count, so interrupted videos are re-fetched.
        IDs and extractor keys are read from each folder's ``.info.json``;
        folders missing one fall back to matching their folder name against
        entry titles.
        """
        ids: Dict[str, str] = {}
        if not self.output_dir.is_dir():
            return ids
        for sub in self.output_dir.iterdir():
            if not sub.is_dir() or self._find_media(sub) is None:
                continue
            info_file = next(sub.glob("*.info.json"), None)
            vid: Optional[str] = None
            ie_key: Optional[str] = None
            if info_file is not None:
                try:
                    data = json.loads(info_file.read_text(encoding="utf-8"))
                    vid = data.get("id")
                    ie_key = data.get("extractor_key") or data.get("extractor")
                except (OSError, ValueError):
                    vid = None
            if vid:
                # e.g. "Youtube" -> archive line "youtube <id>" (yt-dlp format)
                ids[vid] = ie_key or "Youtube"
                continue
            # fallback: recover the ID from the folder/title mapping
            for entry in self._entries:
                title = entry.get("title")
                entry_id = entry.get("id")
                if not title or not entry_id or entry_id in ids:
                    continue
                if sub.name in (
                    sanitize_filename(title),
                    sanitize_filename(title, restricted=True),
                ):
                    ids[entry_id] = entry.get("ie_key") or "Youtube"
                    break
        return ids

    def _prepare_resume(self) -> int:
        """Seed skip state from disk so the job resumes where it stopped.

        Collects every fully downloaded video ID in the output folder, writes
        them into yt-dlp's download-archive file (making the folder contents
        the single source of truth) and pre-counts them as done. Returns the
        number of this channel's videos already on disk.
        """
        channel_ids = {e.get("id") for e in self._entries if e.get("id")}
        if not channel_ids:
            return 0

        disk = self._collect_existing()
        # Archive lines must match yt-dlp's format exactly
        # (make_archive_id("Youtube", "abc") -> "youtube abc"), otherwise the
        # engine will not recognise already-downloaded IDs and will re-fetch.
        lines = sorted(make_archive_id(ie_key, vid) for vid, ie_key in disk.items())
        try:
            self._archive_file.write_text(
                "\n".join(lines) + ("\n" if lines else ""),
                encoding="utf-8",
            )
        except OSError as exc:
            logger.warning("could not write download archive: %s", exc)

        skip_ids = set(disk) & channel_ids
        self._done = set(skip_ids)
        if skip_ids:
            remaining = (self._total or 0) - len(skip_ids)
            self._log(
                f"↻ Resume: {len(skip_ids)} of {self._total} video(s) already "
                f"downloaded — fetching the remaining {remaining}",
                "success",
            )
        else:
            self._log("No completed videos found in the output folder — starting fresh")
        return len(skip_ids)

    # ------------------------------------------------------------------ #
    # yt-dlp hooks                                                       #
    # ------------------------------------------------------------------ #
    def _progress_hook(self, d: Dict[str, Any]) -> None:
        status = d.get("status")
        if status == "downloading":
            total = d.get("total_bytes") or d.get("total_bytes_estimate")
            done = d.get("downloaded_bytes") or 0
            percent = round(done / total * 100, 1) if total else None
            self.emit(
                "progress",
                status="downloading",
                filename=_basename(d.get("filename") or d.get("_filename_str")),
                percent=percent,
                speed=_fmt_speed(d.get("speed")),
                eta=_fmt_eta(d.get("eta")),
            )
        elif status == "finished":
            # Stream finished downloading (pre-merge); completion is counted
            # in the MoveFiles postprocessor hook, which fires exactly once
            # per fully processed video with the final file in place.
            self.emit(
                "progress",
                status="finished",
                filename=_basename(d.get("filename")),
                percent=100.0,
                speed="—",
                eta="—",
            )

    def _postprocessor_hook(self, d: Dict[str, Any]) -> None:
        if d.get("status") != "finished":
            return
        postprocessor = d.get("postprocessor", "")
        info = d.get("info_dict") or {}
        title = info.get("title") or "video"
        if postprocessor == "Merger":
            self.emit(
                "progress",
                status="merging",
                filename=title,
                percent=None,
                speed="—",
                eta="—",
            )
            self._log(f"Merging best video + audio for '{title}'")
        elif postprocessor == "MoveFiles":
            self._mark_done(info)
        elif postprocessor == "ThumbnailsConvertor":
            key = info.get("id") or info.get("title") or "video"
            if key not in self._converted:
                self._converted.add(key)
                self._log(f"Converted thumbnail for '{title}' to JPG")

    def _mark_done(self, info: Dict[str, Any]) -> None:
        title = info.get("title") or "video"
        video_id = info.get("id") or title
        with self._lock:
            if video_id in self._done:
                return
            self._done.add(video_id)
            index = len(self._done)
            self._saved_this_run += 1
        # The hook's info dict carries `filepath` from just before the move —
        # for merged videos that is already the final .mp4. Fallback: glob the
        # video's subfolder for a media file.
        path = info.get("filepath")
        folder = self.output_dir / sanitize_filename(title)
        if not path or Path(path).suffix.lower() not in _VIDEO_EXTS:
            path = self._find_media(folder)
        result = Path(path) if path else folder
        label = f"[{index}/{self._total}]" if self._total else f"[{index}]"
        self.emit(
            "video_done",
            index=index,
            total=self._total,
            title=title,
            path=str(result),
        )
        self._log(f"✔ {label} saved → {result}", "success")

    def _find_media(self, folder: Path) -> Optional[Path]:
        """Return the final media file in ``folder``, if any.

        Unmerged format streams (``Title.f137.mp4``) are skipped — they are
        leftovers of an interrupted download, not a finished video.
        """
        if not folder.is_dir():
            return None
        for ext in _VIDEO_EXTS:
            for hit in sorted(folder.glob(f"*{ext}")):
                if _FRAGMENT_RE.search(hit.stem):
                    continue
                return hit
        return None

    def _clean_playlist_folders(self) -> int:
        """Remove playlist container folders left by channel downloads.

        When downloading a channel, yt-dlp also writes playlist-level
        metadata (``<channel> - Videos/info.json``, description, avatar)
        into a sibling folder. The storage layout only wants per-video
        folders, so drop any direct subfolder that holds no media file.
        """
        removed = 0
        if not self.output_dir.is_dir():
            return removed
        for sub in self.output_dir.iterdir():
            if not sub.is_dir() or self._find_media(sub) is not None:
                continue
            try:
                shutil.rmtree(sub)
                removed += 1
                logger.info("removed playlist container folder %s", sub)
            except OSError as exc:
                logger.warning("could not remove %s: %s", sub, exc)
        return removed

    # ------------------------------------------------------------------ #
    # engine configuration + execution                                   #
    # ------------------------------------------------------------------ #
    def _build_opts(self, ffmpeg_path: Path, deno_path: Path) -> Dict[str, Any]:
        outtmpl = str(self.output_dir / "%(title)s" / "%(title)s.%(ext)s")
        return {
            # high-res video + audio, merged by FFmpeg into mp4
            "format": "bestvideo+bestaudio/best",
            "merge_output_format": "mp4",
            # Root_Folder/%(title)s/%(title)s.%(ext)s
            "outtmpl": outtmpl,
            "windowsfilenames": True,
            # side-car artifacts per video
            "writethumbnail": True,
            "writeinfojson": True,
            "writedescription": True,
            "postprocessors": [
                # convert WebP thumbnails right after they are written
                # (before download), exactly like --convert-thumbnails
                {"key": "FFmpegThumbnailsConvertor", "format": "jpg", "when": "before_dl"},
            ],
            # resilience: skip unavailable/private/age-restricted videos
            "ignoreerrors": True,
            # resume support: skip videos already recorded in the archive
            "download_archive": str(self._archive_file),
            # live progress + log capture
            "progress_hooks": [self._progress_hook],
            "postprocessor_hooks": [self._postprocessor_hook],
            "logger": _LogCapture(self._log),
            "noprogress": True,  # hook frames replace the terminal bar
            "quiet": True,
            # native binaries (auto-managed)
            "ffmpeg_location": str(ffmpeg_path),
            "js_runtimes": {"deno": {"path": str(deno_path)}},
            # network resilience
            "retries": 5,
            "fragment_retries": 10,
            "concurrent_fragment_downloads": 4,
            "socket_timeout": 30,
        }

    def run(self) -> None:
        """Blocking entry point; run inside a worker thread."""
        self.last_status = "running"
        self.emit(
            "job", status="started", url=self.url, output_dir=str(self.output_dir)
        )
        self._log(f"Job {self.job_id} started — target: {self.url}")

        try:
            self._scan()
        except Exception as exc:
            self._log(
                f"Channel pre-scan failed ({exc}) — starting download anyway",
                "warning",
            )

        try:
            ffmpeg_path = binaries.ensure_ffmpeg()
            deno_path = binaries.ensure_deno()
            self._log(f"FFmpeg ready: {ffmpeg_path}")
            self._log(f"Deno (JS runtime) ready: {deno_path}")
            opts = self._build_opts(ffmpeg_path, deno_path)
            with yt_dlp.YoutubeDL(opts) as ydl:
                ret = ydl.download([self.url])

            saved = len(self._done)
            new_saved = self._saved_this_run
            removed = self._clean_playlist_folders()
            if removed:
                self._log(
                    f"Removed {removed} empty playlist container folder(s) "
                    "(per-video layout kept)",
                )
            if ret == 0:
                self.last_status = "completed"
                self.emit(
                    "job",
                    status="completed",
                    completed=saved,
                    new_saved=new_saved,
                    total=self._total,
                    duration=round(time.time() - self._started, 1),
                )
                self._log(
                    f"✔ Job completed — {new_saved} new video(s) downloaded "
                    f"({saved} total on disk)",
                    "success",
                )
            else:
                self.last_status = "completed_with_errors"
                self.emit(
                    "job",
                    status="completed_with_errors",
                    completed=saved,
                    new_saved=new_saved,
                    total=self._total,
                )
                self._log(
                    f"Job finished with some skipped videos — {new_saved} new "
                    f"video(s) downloaded ({saved} total on disk); private/"
                    "unavailable items are skipped by design",
                    "warning",
                )
        except Exception as exc:
            self.last_status = "failed"
            logger.exception("job %s failed", self.job_id)
            self.emit("job", status="failed", message=f"{type(exc).__name__}: {exc}")
            self._log(f"✖ Job failed: {exc}", "error")
