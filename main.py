# main.py
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

# Setup logging early
setup_logging()
logger = logging.getLogger("main")

app = FastAPI(title="Async Crawler API")

# --- static dir mount (safety: create placeholder if missing) ---
static_dir = Path(__file__).parent / "static"
if not static_dir.exists():
    static_dir.mkdir(parents=True, exist_ok=True)
    (static_dir / "index.html").write_text(
        "<!doctype html><html><head><meta charset='utf-8'></head><body><h3>Placeholder UI</h3><p>Добавь static/index.html в репозиторий.</p></body></html>",
        encoding="utf-8"
    )

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
        logger.info("WS client connected, total=%d", len(self.active))

    async def disconnect(self, ws: WebSocket):
        async with self.lock:
            try:
                self.active.remove(ws)
            except ValueError:
                pass
        logger.info("WS client disconnected, total=%d", len(self.active))

    async def broadcast_json(self, data: dict):
        """Broadcast JSON to all connected websockets; remove broken ones."""
        to_remove: List[WebSocket] = []
        async with self.lock:
            for ws in list(self.active):
                try:
                    await ws.send_json(data)
                except Exception as e:
                    logger.warning("WS send failed (%s) — will remove client", e)
                    to_remove.append(ws)
            for ws in to_remove:
                try:
                    self.active.remove(ws)
                except ValueError:
                    pass
        logger.debug("Broadcasted event '%s' to %d clients", data.get("type"), len(self.active))


ws_manager = WebSocketManager()


# WS endpoint: accept connections and keep alive; respond to simple text pings from client
@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await ws_manager.connect(websocket)
    try:
        while True:
            try:
                msg = await websocket.receive_text()
                # reply ack for pings; ignore other messages
                try:
                    await websocket.send_text("ack")
                except Exception:
                    # ignore send failures here; client might be dead
                    pass
            except WebSocketDisconnect:
                break
            except Exception:
                # ignore transient errors, keep connection alive
                await asyncio.sleep(0.1)
                continue
    finally:
        await ws_manager.disconnect(websocket)


# --- RUNS / OUTPUT setup ---
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


def status_logger_factory(run_id: str):
    """
    Returns a synchronous callback `cb(status: dict)` which:
     - appends the status JSON to a log file
     - schedules an async broadcast to all WS clients
    """
    def cb(status: dict):
        lf = OUTPUT_DIR / f"{run_id}.log"
        try:
            with lf.open("a", encoding="utf-8") as f:
                f.write(json.dumps(status, ensure_ascii=False) + "\n")
        except Exception:
            logger.exception("Failed to write status log for run %s", run_id)

        # broadcast payload (non-blocking)
        try:
            payload = {"run_id": run_id, **status}
            # schedule broadcast on running loop
            try:
                # if no running loop, asyncio.get_running_loop() will raise
                asyncio.get_running_loop()
                asyncio.create_task(ws_manager.broadcast_json(payload))
            except RuntimeError:
                # fallback: try to use call_soon_threadsafe on default loop
                try:
                    loop = asyncio.get_event_loop()
                    loop.call_soon_threadsafe(asyncio.create_task, ws_manager.broadcast_json(payload))
                except Exception:
                    logger.exception("No loop to schedule WS broadcast for run %s", run_id)
        except Exception:
            logger.exception("Failed to schedule ws broadcast for run %s", run_id)

    return cb


# --- API endpoints ---

@app.post("/api/start")
async def start_crawl(req: StartRequest):
    run_id = str(int(asyncio.get_running_loop().time() * 1000))
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
    loop = asyncio.get_running_loop()
    task = loop.create_task(crawler.crawl())
    RUNS[run_id] = {"crawler": crawler, "task": task, "meta": {"start_url": str(req.start_url)}}
    # Immediately broadcast that run started
    await ws_manager.broadcast_json({"run_id": run_id, "type": "run_started", "start_url": str(req.start_url)})
    logger.info("Started run %s for %s", run_id, req.start_url)
    return {"run_id": run_id, "status": "started"}


@app.post("/api/stop/{run_id}")
async def stop_crawl(run_id: str):
    entry = RUNS.get(run_id)
    if not entry:
        raise HTTPException(status_code=404, detail="Run not found")
    entry["crawler"].stop()
    await ws_manager.broadcast_json({"run_id": run_id, "type": "stopping"})
    logger.info("Stopping run %s", run_id)
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
    return FileResponse(p, media_type="application/octet-stream", filename=name)


@app.get("/api/logs/{run_id}")
async def get_run_log(run_id: str):
    p = OUTPUT_DIR / f"{run_id}.log"
    if not p.exists():
        raise HTTPException(status_code=404, detail="Log not found")
    return FileResponse(p, media_type="text/plain", filename=f"{run_id}.log")


if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=int(os.environ.get("PORT", 8000)), reload=True)
