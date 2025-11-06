# main.py (важные дополнения/замены)

import asyncio
import logging
from fastapi import FastAPI, HTTPException, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse, HTMLResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, HttpUrl
from pathlib import Path
import uvicorn
import os
import json
from typing import Optional, List, Dict, Any
from crawler import AsyncCrawler, setup_logging

setup_logging()

app = FastAPI(title="Async Crawler API")

# --- static dir mount (как раньше) ---
static_dir = Path(__file__).parent / "static"
if not static_dir.exists():
    static_dir.mkdir(parents=True, exist_ok=True)
    (static_dir / "index.html").write_text("<!doctype html><html><body><h3>Placeholder</h3></body></html>", encoding="utf-8")

app.mount("/static", StaticFiles(directory=str(static_dir)), name="static")

@app.get("/", response_class=HTMLResponse)
async def index():
    return (static_dir / "index.html").read_text(encoding="utf-8")

# --- WebSocket manager (simple broadcast) ---
class WebSocketManager:
    def __init__(self):
        self.active: List[WebSocket] = []
        self.lock = asyncio.Lock()

    async def connect(self, ws: WebSocket):
        await ws.accept()
        async with self.lock:
            self.active.append(ws)

    async def disconnect(self, ws: WebSocket):
        async with self.lock:
            if ws in self.active:
                self.active.remove(ws)

    async def broadcast_json(self, data: dict):
        # send to all active websockets; remove closed ones
        to_remove = []
        async with self.lock:
            for ws in self.active:
                try:
                    await ws.send_json(data)
                except Exception:
                    to_remove.append(ws)
            for ws in to_remove:
                if ws in self.active:
                    self.active.remove(ws)

ws_manager = WebSocketManager()

# WS endpoint
@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await ws_manager.connect(websocket)
    try:
        while True:
            # simple ping/pong or receive to keep connection alive
            data = await websocket.receive_text()  # we ignore content; client may send pings
            # optionally respond with ack
            await websocket.send_text("ack")
    except WebSocketDisconnect:
        await ws_manager.disconnect(websocket)
    except Exception:
        await ws_manager.disconnect(websocket)

# --- RUNS / OUTPUT setup (as before) ---
RUNS: Dict[str, Dict[str, Any]] = {}
OUTPUT_DIR = Path("output_async")
OUTPUT_DIR.mkdir(exist_ok=True)

class StartRequest(BaseModel):
    start_url: HttpUrl
    base_url: Optional[HttpUrl] = None
    prefix: Optional[str] = None
    concurrency: Optional[int] = 5
    sleep: Optional[float] = 0.2
    max_pages: Optional[int] = None
    save_html: Optional[bool] = False
    user_agent: Optional[str] = None

# Updated status logger factory — writes log and broadcasts via WS
def status_logger_factory(run_id: str):
    def cb(status: dict):
        lf = OUTPUT_DIR / f"{run_id}.log"
        try:
            with lf.open("a", encoding="utf-8") as f:
                f.write(json.dumps(status, ensure_ascii=False) + "\n")
        except Exception:
            pass
        try:
            payload = {"run_id": run_id, **status}
            # Не блокируем основной поток — отправляем асинхронно
            asyncio.get_event_loop().create_task(ws_manager.broadcast_json(payload))
        except Exception:
            pass
    return cb

# API endpoints (start/stop/status/list/output/logs) — mostly same as before, but use updated factory
@app.post("/api/start")
async def start_crawl(req: StartRequest):
    run_id = str(int(asyncio.get_event_loop().time() * 1000))
    cb = status_logger_factory(run_id)
    crawler = AsyncCrawler(
        start_url=str(req.start_url),
        base_url=str(req.base_url) if req.base_url else None,
        target_prefix=req.prefix,
        concurrency=req.concurrency or 5,
        sleep=req.sleep or 0.2,
        max_pages=req.max_pages,
        save_html=req.save_html or False,
        user_agent=req.user_agent or None,
        status_callback=cb
    )
    loop = asyncio.get_event_loop()
    task = loop.create_task(crawler.crawl())
    RUNS[run_id] = {"crawler": crawler, "task": task, "meta": {"start_url": str(req.start_url)}}
    # Immediately broadcast that run started
    await ws_manager.broadcast_json({"run_id": run_id, "type": "run_started", "start_url": str(req.start_url)})
    return {"run_id": run_id, "status": "started"}

@app.post("/api/stop/{run_id}")
async def stop_crawl(run_id: str):
    entry = RUNS.get(run_id)
    if not entry:
        raise HTTPException(status_code=404, detail="Run not found")
    entry["crawler"].stop()
    await ws_manager.broadcast_json({"run_id": run_id, "type": "stopping"})
    return {"run_id": run_id, "status": "stopping"}

@app.get("/api/status/{run_id}")
async def get_status(run_id: str):
    entry = RUNS.get(run_id)
    if not entry:
        raise HTTPException(status_code=404, detail="Run not found")
    task = entry["task"]
    crawler = entry["crawler"]
    log_path = OUTPUT_DIR / f"{run_id}.log"
    logs = []
    if log_path.exists():
        with log_path.open("r", encoding="utf-8") as f:
            for line in f:
                try:
                    logs.append(json.loads(line.strip()))
                except Exception:
                    pass
    return {
        "run_id": run_id,
        "running": not task.done(),
        "pages_saved": crawler.index,
        "visited": len(crawler.visited),
        "queue": len(crawler.to_visit),
        "logs_tail": logs[-20:],
    }

@app.get("/api/list")
async def list_outputs():
    files = []
    for p in sorted(OUTPUT_DIR.glob("*")):
        files.append({"name": p.name, "size": p.stat().st_size, "path": str(p)})
    return {"files": files}

@app.get("/api/output/{name}")
async def get_output_file(name: str):
    p = OUTPUT_DIR / name
    if not p.exists():
        raise HTTPException(status_code=404, detail="File not found")
    # return file content
    return FileResponse(p, media_type="application/octet-stream", filename=name)

@app.get("/api/logs/{run_id}")
async def get_run_log(run_id: str):
    p = OUTPUT_DIR / f"{run_id}.log"
    if not p.exists():
        raise HTTPException(status_code=404, detail="Log not found")
    return FileResponse(p, media_type="text/plain", filename=f"{run_id}.log")

if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=int(os.environ.get("PORT", 8000)), reload=True)
