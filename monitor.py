"""Watch a job's live WebSocket frames for a bounded time.

Usage:
    python monitor.py [seconds] [url] [output_dir]

If a url is given, dispatches a new download job first, then prints frames
until the time window ends (the job keeps running on the server either way).
"""

from __future__ import annotations

import asyncio
import json
import sys
import urllib.request

import websockets  # provided by uvicorn[standard]

# Windows console default (cp1252) can't render the log symbols
sys.stdout.reconfigure(encoding="utf-8", errors="replace")

SECONDS = int(sys.argv[1]) if len(sys.argv) > 1 else 120
URL = sys.argv[2] if len(sys.argv) > 2 else None
OUT = sys.argv[3] if len(sys.argv) > 3 else None


async def main() -> None:
    n_logs = n_progress = 0
    async with websockets.connect("ws://127.0.0.1:8000/ws/progress") as ws:
        if URL:
            payload: dict = {"url": URL}
            if OUT:
                payload["output_dir"] = OUT
            req = urllib.request.Request(
                "http://127.0.0.1:8000/api/v1/download",
                data=json.dumps(payload).encode(),
                headers={"Content-Type": "application/json"},
            )
            with urllib.request.urlopen(req) as resp:
                print("dispatched:", resp.status, resp.read().decode())

        loop = asyncio.get_event_loop()
        end = loop.time() + SECONDS
        try:
            while True:
                remaining = end - loop.time()
                if remaining <= 0:
                    break
                raw = await asyncio.wait_for(ws.recv(), timeout=remaining)
                frame = json.loads(raw)
                event = frame.get("event")
                if event == "count":
                    print(
                        f"[count] total={frame.get('total')} "
                        f"already={frame.get('already')} "
                        f"remaining={frame.get('remaining')} "
                        f"channel={frame.get('channel')!r}"
                    )
                elif event == "video_done":
                    print(
                        f"[video_done] {frame.get('index')}/{frame.get('total')} "
                        f"{frame.get('title')!r} -> {frame.get('path')}"
                    )
                elif event == "job":
                    extra = {
                        k: v
                        for k, v in frame.items()
                        if k not in ("event", "job_id")
                    }
                    print(f"[job] {json.dumps(extra)[:200]}")
                elif event == "log":
                    n_logs += 1
                    msg = frame.get("message", "")
                    if (
                        "archive" in msg
                        or "Resume" in msg
                        or frame.get("level") in ("warning", "error", "success")
                    ):
                        print(f"[log:{frame.get('level')}] {msg[:160]}")
                elif event == "progress":
                    n_progress += 1
                    if n_progress % 25 == 1:
                        print(
                            f"[progress] {frame.get('percent')}% "
                            f"{frame.get('speed')} eta {frame.get('eta')} "
                            f"{frame.get('filename')}"[:170]
                        )
        except asyncio.TimeoutError:
            pass
    print(f"\nmonitor window ended — {n_logs} log + {n_progress} progress frames seen; job keeps running on the server")


if __name__ == "__main__":
    asyncio.run(main())
