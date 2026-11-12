"""YouTube Channel Bulk Downloader ??" FastAPI backend.

Routes
------
POST   /api/v1/download   { url, output_dir }  -> dispatch background job
GET    /api/v1/health     server + engine status
GET    /api/v1/jobs       list job history
WS     /ws/progress       live progress frames (JSON)
GET    /                  serves the single-page UI (index.html)
"""

from __future__ import annotations

import asyncio
import logging
import re
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any, Dict, Set

import yt_dlp
from fastapi import BackgroundTasks, FastAPI, HTTPException, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse
from pydantic import BaseModel, Field, field_validator

import binaries
from downloader import DownloadJob

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-7s %(name)s: %(message)s",
)
logger = logging.getLogger("main")

ROOT = Path(__file__).resolve().parent
DEFAULT_OUTPUT_DIR = Path(r"D:\Projects\YouTube_Downloads")
INDEX_HTML = ROOT / "index.html"

_YT_URL_RE = re.compile(r"^(https?://)?(www\.|m\.)?(youtube\.com|youtu\.be)(/|$)", re.IGNORECASE)


# --------------------------------------------------------------------------- #
# WebSocket broadcast manager (thread-safe)                                    #
# --------------------------------------------------------------------------- #
class ConnectionManager:
    """Tracks connected WebSocket clients and broadcasts frames to all of them.

    ``broadcast_threadsafe`` is called from yt-dlp worker threads and
    schedules the coroutine on the server's event loop.
    """

    def __init__(self) -> None:
        self._clients: Set[WebSocket] = set()
        self._loop: asyncio.AbstractEventLoop | None = None

    def bind_loop(self, loop: asyncio.AbstractEventLoop) -> None:
        self._loop = loop

    async def connect(self, websocket: WebSocket) -> None:
        await websocket.accept()
        self._clients.add(websocket)
        logger.info("ws client connected (%d total)", len(self._clients))

    def disconnect(self, websocket: WebSocket) -> None:
        self._clients.discard(websocket)
        logger.info("ws client disconnected (%d total)", len(self._clients))

    async def broadcast(self, frame: Dict[str, Any]) -> None:
        dead: list[WebSocket] = []
        for ws in list(self._clients):
            try:
                await ws.send_json(frame)
            except Exception:
                dead.append(ws)
        for ws in dead:
            self.disconnect(ws)

    def broadcast_threadsafe(self, frame: Dict[str, Any]) -> None:
        if self._loop is None or not self._clients:
            return
        asyncio.run_coroutine_threadsafe(self.broadcast(frame), self._loop)


manager = ConnectionManager()
jobs: Dict[str, Dict[str, Any]] = {}  # job_id -> {"job": DownloadJob, "status": str}


# --------------------------------------------------------------------------- #
# Request models                                                               #
# --------------------------------------------------------------------------- #
class DownloadRequest(BaseModel):
    url: str = Field(
        ..., min_length=1, description="YouTube channel, playlist, or video URL"
    )
    output_dir: str | None = Field(
        default=None,
        description="Root download folder (defaults to ./YouTube_Downloads)",
    )

    @field_validator("url")
    @classmethod
    def _validate_youtube_url(cls, value: str) -> str:
        value = value.strip()
        if not _YT_URL_RE.match(value):
            raise ValueError("must be a valid YouTube URL (youtube.com or youtu.be)")
        return value


# --------------------------------------------------------------------------- #
# App lifecycle                                                                #
# --------------------------------------------------------------------------- #
def _warm_binaries() -> None:
    """Pre-fetch native tools in the background so the first job starts instantly."""
    try:
        logger.info("FFmpeg ready: %s", binaries.ensure_ffmpeg())
    except Exception as exc:
        logger.warning("FFmpeg warm-up failed (will retry on first job): %s", exc)
    try:
        logger.info("Deno ready: %s", binaries.ensure_deno())
    except Exception as exc:
        logger.warning("Deno warm-up failed (will retry on first job): %s", exc)


@asynccontextmanager
async def lifespan(_: FastAPI):
    manager.bind_loop(asyncio.get_running_loop())
    asyncio.create_task(asyncio.to_thread(_warm_binaries))
    yield


app = FastAPI(
    title="YouTube Channel Bulk Downloader",
    version="1.0.0",
    description="Bulk-download a YouTube channel with metadata, thumbnails, "
    "and live WebSocket progress streaming.",
    lifespan=lifespan,
)


# --------------------------------------------------------------------------- #
# Background job runner                                                        #
# --------------------------------------------------------------------------- #
async def _run_job(job: DownloadJob) -> None:
    jobs[job.job_id]["status"] = "running"
    try:
        await asyncio.to_thread(job.run)  # yt-dlp is blocking -> thread pool
    finally:
        jobs[job.job_id]["status"] = job.last_status


# --------------------------------------------------------------------------- #
# Routes                                                                       #
# --------------------------------------------------------------------------- #
@app.post("/api/v1/download", status_code=202)
async def start_download(
    request: DownloadRequest, background_tasks: BackgroundTasks
) -> Dict[str, Any]:
    output_dir = (
        Path(request.output_dir).expanduser()
        if request.output_dir
        else DEFAULT_OUTPUT_DIR
    )
    try:
        output_dir = output_dir.resolve()
        output_dir.mkdir(parents=True, exist_ok=True)
    except OSError as exc:
        raise HTTPException(
            status_code=400, detail=f"Cannot create output directory: {exc}"
        )

    job = DownloadJob(
        url=request.url,
        output_dir=output_dir,
        on_frame=manager.broadcast_threadsafe,
    )
    jobs[job.job_id] = {"job": job, "status": "queued"}
    background_tasks.add_task(_run_job, job)
    logger.info("dispatched job %s -> %s", job.job_id, output_dir)
    return {
        "job_id": job.job_id,
        "status": "queued",
        "output_dir": str(output_dir),
        "ws": "/ws/progress",
    }


@app.get("/api/v1/health")
async def health() -> Dict[str, Any]:
    active = sum(1 for j in jobs.values() if j["status"] in ("queued", "running"))
    return {
        "status": "ok",
        "version": app.version,
        "yt_dlp": yt_dlp.version.__version__,
        "ffmpeg": binaries.ffmpeg_status(),
        "deno": binaries.deno_status(),
        "active_jobs": active,
        "total_jobs": len(jobs),
    }


@app.get("/api/v1/jobs")
async def list_jobs() -> Dict[str, Any]:
    return {
        "jobs": [
            {
                "job_id": job_id,
                "url": entry["job"].url,
                "output_dir": str(entry["job"].output_dir),
                "status": entry["status"],
            }
            for job_id, entry in jobs.items()
        ]
    }


@app.websocket("/ws/progress")
async def ws_progress(websocket: WebSocket) -> None:
    await manager.connect(websocket)
    try:
        while True:  # read loop detects client disconnects
            await websocket.receive_text()
    except WebSocketDisconnect:
        pass
    finally:
        manager.disconnect(websocket)


@app.get("/", include_in_schema=False)
async def index() -> FileResponse:
    return FileResponse(INDEX_HTML)


if __name__ == "__main__":
    import uvicorn

    uvicorn.run("main:app", host="127.0.0.1", port=8000, log_level="info")
