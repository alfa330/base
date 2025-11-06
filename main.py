# main.py
import asyncio
import logging
from fastapi import FastAPI, HTTPException, BackgroundTasks
from fastapi.responses import FileResponse, HTMLResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, HttpUrl
from pathlib import Path
import uvicorn
import os
import json
from typing import Optional
from crawler import AsyncCrawler, setup_logging

setup_logging()

app = FastAPI(title="Async Crawler API")

# mount static folder
static_dir = Path(__file__).parent / "static"
app.mount("/static", StaticFiles(directory=str(static_dir)), name="static")

# serve single-page app
@app.get("/", response_class=HTMLResponse)
async def index():
    index_file = static_dir / "index.html"
    return index_file.read_text(encoding="utf-8")

# global manager for running crawlers (simple in-memory)
RUNS = {}  # run_id -> {"crawler": AsyncCrawler, "task": asyncio.Task, "meta": {...}}

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
    def cb(status: dict):
        # append to log file file
        lf = OUTPUT_DIR / f"{run_id}.log"
        with lf.open("a", encoding="utf-8") as f:
            f.write(json.dumps(status, ensure_ascii=False) + "\n")
    return cb

@app.post("/api/start")
async def start_crawl(req: StartRequest):
    run_id = str(int(asyncio.get_event_loop().time() * 1000))
    if run_id in RUNS:
        raise HTTPException(status_code=500, detail="Run id conflict (try again)")

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
    return {"run_id": run_id, "status": "started"}

@app.post("/api/stop/{run_id}")
async def stop_crawl(run_id: str):
    entry = RUNS.get(run_id)
    if not entry:
        raise HTTPException(status_code=404, detail="Run not found")
    entry["crawler"].stop()
    # await task cancellation gracefully (we don't await here)
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
    for p in sorted(OUTPUT_DIR.glob("*.json")):
        files.append({"name": p.name, "size": p.stat().st_size, "path": str(p)})
    return {"files": files}

@app.get("/api/output/{name}")
async def get_output_file(name: str):
    p = OUTPUT_DIR / name
    if not p.exists():
        raise HTTPException(status_code=404, detail="File not found")
    return FileResponse(p, media_type="application/json", filename=name)

@app.get("/api/logs/{run_id}")
async def get_run_log(run_id: str):
    p = OUTPUT_DIR / f"{run_id}.log"
    if not p.exists():
        raise HTTPException(status_code=404, detail="Log not found")
    return FileResponse(p, media_type="text/plain", filename=f"{run_id}.log")

if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=int(os.environ.get("PORT", 8000)), reload=True)
