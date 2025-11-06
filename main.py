# main.py
import asyncio
import logging
import os
import json
import shutil
import tempfile
from pathlib import Path
from typing import Optional, List, Dict, Any

from fastapi import FastAPI, HTTPException, WebSocket, WebSocketDisconnect, Response
from fastapi.responses import FileResponse, HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, HttpUrl
import uvicorn
import zipfile

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


# HEAD on root for uptime checks — return 200 without body
@app.head("/")
async def head_root() -> Response:
    return Response(status_code=200)

@app.get("/", response_class=HTMLResponse)
async def index():
    return (static_dir / "index.html").read_text(encoding="utf-8")


# Simple health endpoints for uptime monitors
@app.head("/health")
async def head_health() -> Response:
    return Response(status_code=200)

@app.get("/health")
async def get_health():
    return JSONResponse({"status": "ok", "uptime_check": True})


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


@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    """
    Keep connection open. Client may send 'ping' text; we reply 'ack'.
    """
    await ws_manager.connect(websocket)
    try:
        while True:
            try:
                msg = await websocket.receive_text()
                try:
                    await websocket.send_text("ack")
                except Exception:
                    pass
            except WebSocketDisconnect:
                break
            except Exception:
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


def status_logger_factory(run_id: str, *,
                          batch_seconds: float = 0.5,
                          batch_size: int = 200):
    """
    Returns a synchronous callback `cb(status: dict)` which:
     - appends the status JSON to a log file
     - pushes the minimal status into a buffer and schedules a batched broadcast to WS clients

    Batched messages will be sent as:
      {"type": "saved_batch", "items": [ {..}, {..} ]}

    Minimizes per-file WS traffic and avoids sending full content over WS.
    """
    lf = OUTPUT_DIR / f"{run_id}.log"
    buffer: List[dict] = []
    buffer_lock = asyncio.Lock()
    flush_handle = None

    async def _flush():
        nonlocal buffer, flush_handle
        async with buffer_lock:
            if not buffer:
                flush_handle = None
                return
            items = buffer
            buffer = []
            flush_handle = None
        # broadcast as one message
        try:
            payload = {"type": "saved_batch", "run_id": run_id, "items": items}
            await ws_manager.broadcast_json(payload)
            logger.debug("Flushed %d buffered events for run %s", len(items), run_id)
        except Exception:
            logger.exception("Failed broadcasting saved_batch for run %s", run_id)

    def schedule_flush(loop: asyncio.AbstractEventLoop, delay: float):
        nonlocal flush_handle
        if flush_handle:
            return
        # schedule an async task using loop.call_later
        def _cb():
            try:
                asyncio.create_task(_flush())
            except Exception:
                # fallback: run_coroutine_threadsafe
                try:
                    asyncio.run(_flush())
                except Exception:
                    logger.exception("Failed scheduling _flush()")
        flush_handle = loop.call_later(delay, _cb)

    def cb(status: dict):
        """
        Synchronous callback expected by crawler; writes log synchronously (append),
        then schedules buffered WS broadcast.
        """
        # write to per-run log (append JSON line)
        try:
            with lf.open("a", encoding="utf-8") as f:
                f.write(json.dumps(status, ensure_ascii=False) + "\n")
        except Exception:
            logger.exception("Failed to write status log for run %s", run_id)

        # buffer minimal info for WS; don't include large fields
        minimal = {
            "type": status.get("type", "event"),
            # expected fields: file, url, title, snippet, branch, index, run-specific meta
            # copy only safe keys
            "file": status.get("file"),
            "url": status.get("url"),
            "title": status.get("title"),
            "snippet": status.get("snippet"),
            "branch": status.get("branch"),
            "index": status.get("index"),
            "ts": status.get("ts") or int(asyncio.get_event_loop().time() * 1000)
        }

        # push into buffer and schedule flush
        try:
            loop = asyncio.get_running_loop()
            # use async lock to mutate buffer
            async def _push_and_maybe_schedule():
                async with buffer_lock:
                    buffer.append(minimal)
                    # if buffer too large, flush immediately
                    if len(buffer) >= batch_size:
                        await _flush()
                        return
                    # else schedule flush if not scheduled
                    loop = asyncio.get_running_loop()
                    schedule_flush(loop, batch_seconds)

            # schedule push task on loop
            asyncio.create_task(_push_and_maybe_schedule())
        except RuntimeError:
            # not in running loop — fallback: use event loop fetched from get_event_loop
            try:
                loop = asyncio.get_event_loop()
                # safe scheduling from other thread
                def _sync_push():
                    async def _inner():
                        async with buffer_lock:
                            buffer.append(minimal)
                            if len(buffer) >= batch_size:
                                await _flush()
                                return
                            schedule_flush(loop, batch_seconds)
                    asyncio.create_task(_inner())
                loop.call_soon_threadsafe(_sync_push)
            except Exception:
                logger.exception("No loop to schedule WS broadcast for run %s", run_id)

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


@app.post("/api/stop_all")
async def stop_all_runs():
    stopped = []
    for run_id, entry in list(RUNS.items()):
        try:
            entry["crawler"].stop()
            stopped.append(run_id)
        except Exception as e:
            logger.exception("Error stopping run %s: %s", run_id, e)
    await ws_manager.broadcast_json({"type": "stopped_all", "runs": stopped})
    logger.info("stop_all called, stopped %d runs", len(stopped))
    return {"stopped": stopped}


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
    """Return only .md files (no .json)."""
    files = []
    for p in sorted(OUTPUT_DIR.rglob("*.md")):
        if p.is_file():
            rel = p.relative_to(OUTPUT_DIR)
            files.append({"name": str(rel), "size": p.stat().st_size, "path": str(p)})
    return {"files": files}


@app.get("/api/dirs")
async def list_dirs():
    """List branches (directories that contain .md files) with counts and sizes."""
    dirs = []
    base = OUTPUT_DIR
    if not base.exists():
        return {"dirs": []}
    # walk directories and check for md files
    for p in sorted(base.rglob("*")):
        if p.is_dir():
            md_files = [f for f in p.glob("**/*.md") if f.is_file()]
            if not md_files:
                continue
            total = sum(f.stat().st_size for f in md_files)
            rel = str(p.relative_to(base))
            dirs.append({"branch": rel, "files": len(md_files), "size": total})
    # Deduplicate by branch (keep unique)
    uniq = {}
    for d in dirs:
        uniq[d["branch"]] = d
    return {"dirs": list(uniq.values())}


@app.get("/api/dir/{dir_path:path}/files")
async def list_files_in_dir(dir_path: str):
    target = (OUTPUT_DIR / dir_path).resolve()
    if not str(target).startswith(str(OUTPUT_DIR.resolve())):
        raise HTTPException(status_code=400, detail="Invalid path")
    if not target.exists() or not target.is_dir():
        raise HTTPException(status_code=404, detail="Directory not found")
    files = []
    for p in sorted(target.glob("**/*.md")):
        if p.is_file():
            rel = p.relative_to(OUTPUT_DIR)
            files.append({"name": str(rel), "size": p.stat().st_size, "path": str(p)})
    return {"files": files}


@app.get("/api/download_dir/{dir_path:path}")
async def download_dir(dir_path: str):
    target = (OUTPUT_DIR / dir_path).resolve()
    if not str(target).startswith(str(OUTPUT_DIR.resolve())):
        raise HTTPException(status_code=400, detail="Invalid path")
    if not target.exists() or not target.is_dir():
        raise HTTPException(status_code=404, detail="Directory not found")

    # create a temporary zip containing only .md files from target
    tmpdir = tempfile.mkdtemp(prefix="crawler-zip-")
    archive_path = os.path.join(tmpdir, "archive.zip")
    try:
        with zipfile.ZipFile(archive_path, "w", compression=zipfile.ZIP_DEFLATED) as zf:
            for p in target.rglob("*.md"):
                if p.is_file():
                    arcname = str(p.relative_to(OUTPUT_DIR))
                    zf.write(p, arcname)
    except Exception:
        logger.exception("Failed to create zip for %s", target)
        # cleanup
        shutil.rmtree(tmpdir, ignore_errors=True)
        raise HTTPException(status_code=500, detail="Failed to create archive")

    # schedule cleanup in 60s
    def _cleanup(pathp, d):
        try:
            if os.path.exists(pathp):
                os.remove(pathp)
            if os.path.isdir(d):
                shutil.rmtree(d, ignore_errors=True)
        except Exception:
            logger.exception("Failed cleaning up temp zip %s", pathp)
    loop = asyncio.get_event_loop()
    loop.call_later(60, _cleanup, archive_path, tmpdir)

    filename = f"{Path(dir_path).name or 'branch'}.zip"
    return FileResponse(archive_path, media_type="application/zip", filename=filename)


@app.get("/api/output/{name:path}")
async def get_output_file(name: str):
    # Allow only .md files to be served
    p = (OUTPUT_DIR / name).resolve()
    if not str(p).startswith(str(OUTPUT_DIR.resolve())):
        raise HTTPException(status_code=400, detail="Invalid path")
    if not p.exists():
        raise HTTPException(status_code=404, detail="File not found")
    if p.suffix.lower() != ".md":
        raise HTTPException(status_code=400, detail="Only .md files are directly downloadable")
    return FileResponse(p, media_type="text/markdown", filename=p.name)


@app.get("/api/logs/{run_id}")
async def get_run_log(run_id: str):
    p = OUTPUT_DIR / f"{run_id}.log"
    if not p.exists():
        raise HTTPException(status_code=404, detail="Log not found")
    return FileResponse(p, media_type="text/plain", filename=f"{run_id}.log")


if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=int(os.environ.get("PORT", 8000)), reload=True)