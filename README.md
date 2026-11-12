# YouTube Channel Bulk Downloader

![Python](https://img.shields.io/badge/Python-3.10%2B-3776AB?logo=python&logoColor=white)
![FastAPI](https://img.shields.io/badge/FastAPI-backend-009688?logo=fastapi&logoColor=white)
![License: MIT](https://img.shields.io/badge/License-MIT-green)

Bulk-download every video from a YouTube channel (or playlist / single video)
with merged high-res MP4s, JPG thumbnails, raw metadata, and descriptions —
streamed to a dark-theme web dashboard over WebSockets. Fully **resumable**: interrupted jobs pick up exactly where they left off.

## Architecture

```
Browser (index.html) ──POST /api/v1/download──► FastAPI (main.py)
        ▲                                            │
        └────── WS /ws/progress ◄──────────── yt-dlp engine (downloader.py)
                                                │
                                          FFmpeg (binaries.py, auto-managed)
```

- **FastAPI + Uvicorn** — async REST backend, background task dispatch
- **yt-dlp (Python binding)** — channel scraping, downloads, progress hooks
- **FFmpeg** — merges `bestvideo+bestaudio` into MP4, converts WebP → JPG
- **WebSockets** — live frames: `%`, current file, speed, ETA, event log

## Quick Start

```powershell
cd D:\Projects\youtube-downloader

# (optional but recommended) create a virtual environment
python -m venv .venv
.\.venv\Scripts\Activate.ps1

pip install -r requirements.txt

# start the server
python main.py            # or: uvicorn main:app --host 127.0.0.1 --port 8000
```

Open **http://127.0.0.1:8000**, paste a channel URL, pick an output folder,
and hit *Start Bulk Download*.

> **FFmpeg**: if no `ffmpeg.exe` is found on PATH, the server auto-downloads a
> static Windows build into `.\bin\` on first run. You can also set the
> `FFMPEG_PATH` environment variable to point at a specific `ffmpeg.exe`.

> **yt-dlp-ejs**: required by recent yt-dlp versions to solve YouTube's JS
> challenge. Without it, YouTube throttles downloads (~30 KiB/s at 240p).
> It is in `requirements.txt` — `pip install -r requirements.txt` covers it.

## API

| Method | Path | Description |
|---|---|---|
| `POST` | `/api/v1/download` | Body `{"url": "...", "output_dir": "..."}` → `202 {"job_id", "status", "output_dir"}` |
| `GET` | `/api/v1/health` | Server, yt-dlp version, FFmpeg path, active jobs |
| `GET` | `/api/v1/jobs` | Job history with statuses |
| `WS` | `/ws/progress` | Live JSON frames (below) |

### WebSocket frame examples

```json
{"event": "progress", "job_id": "ab12cd34ef56", "status": "downloading",
 "filename": "My Video.f137.mp4", "percent": 42.3,
 "speed": "5.62 MiB/s", "eta": "01:24"}

{"event": "count", "job_id": "ab12cd34ef56", "total": 251,
 "channel": "Les Ombres du Passé", "already": 34, "remaining": 217}

{"event": "video_done", "job_id": "ab12cd34ef56", "index": 3, "total": 87,
 "title": "My Video", "path": "D:\\...\\YouTube_Downloads\\My Video\\My Video.mp4"}

{"event": "log", "job_id": "ab12cd34ef56", "message": "[Merger] Merging formats…",
 "level": "info"}
```

## Resume & incremental downloads

Every job resumes exactly where the last one stopped:

1. The channel is **flat-scanned** (no downloads) to list all video IDs.
2. The output folder is walked: every subfolder with a **final media file**
   counts as downloaded — leftover fragment streams (`*.f137.mp4`) or `.part`
   files from an interrupted run do **not** count, so interrupted videos are
   re-fetched.
3. The completed IDs are written to `download_archive.txt` inside the output
   folder (the folder contents are the single source of truth).
4. yt-dlp skips every archived ID — only missing videos are fetched.

Consequences:

- Cancel a 200-video job at #34, restart later → it continues from #34.
- Delete a video's folder and re-run the channel to fetch it again.
- Check progress without downloading anything:
  `python check_progress.py <channel_url> [output_dir]`
- Watch a running job from the console: `python monitor.py [seconds]`

## Output layout

```
D:\Projects\YouTube_Downloads\          ← default output root
└── <Video Title>/
    ├── <Video Title>.mp4          ← merged best video + audio
    ├── <Video Title>.jpg          ← thumbnail (converted from WebP)
    ├── <Video Title>.info.json     ← full raw metadata
    └── <Video Title>.description  ← standalone description text
```

## Project layout

| File | Role |
|---|---|
| `main.py` | FastAPI app: routes, Pydantic validation, WS broadcast manager |
| `downloader.py` | `DownloadJob` — yt-dlp options, progress/postprocessor hooks, frame emission |
| `binaries.py` | FFmpeg discovery + auto-download |
| `index.html` | Single-page dark-theme UI (vanilla ES6, no build step) |

## Notes & troubleshooting

- **FFmpeg is not bundled.** If not found on your system PATH, a static build is
  downloaded into `./bin/` on first run (or it falls back to `imageio-ffmpeg`).
  The `bin/` folder is git-ignored.
- **Private / age-restricted / deleted videos** are skipped automatically
  (`ignoreerrors: true`) so channel batches never abort midway.
- **Windows Smart App Control** may block freshly downloaded unsigned
  `ffmpeg.exe` builds ("An Application Control policy has blocked this file").
  The app falls back to the `imageio-ffmpeg` package binary (in
  `requirements.txt`), which carries established file reputation and is
  accepted.
- **"Sign in to confirm you're not a bot"**: YouTube sometimes requires cookies
  for datacenter/VPN IPs. Run from a normal residential connection; if needed,
  export cookies (`yt-dlp --cookies-from-browser chrome`) and add a
  `cookiefile` entry in `_build_opts()` in `downloader.py`.
- Jobs run in a thread pool (yt-dlp is blocking); progress hooks broadcast
  through `asyncio.run_coroutine_threadsafe`, so the event loop never blocks.
- Multiple jobs can run concurrently; each frame carries its `job_id`.
