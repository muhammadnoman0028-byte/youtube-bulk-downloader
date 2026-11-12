"""End-to-end smoke test: POST a job and read live WebSocket frames.

Usage: python smoke_test.py [url] [output_dir]
Defaults to YouTube's first-ever 19-second video (tiny download).
"""

from __future__ import annotations

import asyncio
import json
import sys
import urllib.request

import websockets  # provided by uvicorn[standard]

# Windows console default (cp1252) can't render the log symbols — force UTF-8
sys.stdout.reconfigure(encoding="utf-8", errors="replace")

BASE = "http://127.0.0.1:8000"
URL = sys.argv[1] if len(sys.argv) > 1 else "https://www.youtube.com/watch?v=jNQXAC9IVRw"
OUT = sys.argv[2] if len(sys.argv) > 2 else r"D:\Projects\YouTube_Downloads"


async def main() -> None:
    frames: list[dict] = []
    async with websockets.connect(f"ws://127.0.0.1:8000/ws/progress") as ws:
        payload = json.dumps({"url": URL, "output_dir": OUT}).encode()
        req = urllib.request.Request(
            f"{BASE}/api/v1/download", data=payload,
            headers={"Content-Type": "application/json"},
        )
        with urllib.request.urlopen(req) as resp:
            print("POST /api/v1/download ->", resp.status, resp.read().decode())

        try:
            while True:
                raw = await asyncio.wait_for(ws.recv(), timeout=180)
                frame = json.loads(raw)
                frames.append(frame)
                if frame["event"] == "progress":
                    print(f"  progress  {frame.get('percent')}%  "
                          f"{frame.get('speed')}  eta {frame.get('eta')}  "
                          f"{frame.get('filename')}")
                elif frame["event"] == "log":
                    print(f"  log[{frame.get('level')}]  {frame['message'][:110]}")
                elif frame["event"] in ("video_done", "count", "job"):
                    print(f"  {frame['event']:<11} {json.dumps({k: v for k, v in frame.items() if k not in ('event', 'job_id')})[:110]}")
                if frame["event"] == "job" and frame.get("status") in (
                    "completed", "completed_with_errors", "failed"
                ):
                    break
        except asyncio.TimeoutError:
            print("!! timed out waiting for job completion")

    print(f"\nreceived {len(frames)} frames total")


if __name__ == "__main__":
    asyncio.run(main())
