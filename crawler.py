# crawler.py
import asyncio
import aiohttp
import async_timeout
import aiofiles
from aiohttp import ClientError
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse, urlunparse, parse_qsl, urlencode
import urllib.robotparser as robotparser
from slugify import slugify
from tqdm import tqdm
import logging
import time
import os
import json
import hashlib
from pathlib import Path
from collections import deque
from typing import Optional

DEFAULT_USER_AGENT = "Mozilla/5.0 (compatible; ChatGPTBot/1.0; +https://openai.com/)"
DEFAULT_CONCURRENCY = 5
DEFAULT_SLEEP = 0.2
MAX_RAW_HTML_BYTES = 200_000

def setup_logging(level=logging.INFO):
    fmt = "%(asctime)s [%(levelname)s] %(message)s"
    logging.basicConfig(level=level, format=fmt)


def canonicalize_url(url: str, keep_query: bool = False, drop_utm: bool = True) -> str:
    parsed = urlparse(url)
    scheme, netloc, path, params, query, fragment = parsed
    fragment = ""
    if not keep_query:
        query = ""
    else:
        qitems = parse_qsl(query, keep_blank_values=True)
        if drop_utm:
            qitems = [(k, v) for (k, v) in qitems if not k.lower().startswith("utm_")]
        query = urlencode(qitems)
    if path != "/" and path.endswith("/"):
        path = path.rstrip("/")
    return urlunparse((scheme, netloc, path, params, query, fragment))


def is_same_host_or_prefix(url: str, base_netloc: str, target_prefix: Optional[str]) -> bool:
    p = urlparse(url)
    base = urlparse(base_netloc)
    if p.scheme not in ("http", "https"):
        return False
    if p.netloc != base.netloc:
        return False
    if target_prefix:
        return p.path.startswith(target_prefix)
    return True


def make_safe_filename(index: int, url: str, title: str) -> str:
    parsed = urlparse(url)
    path = parsed.path.rstrip("/")
    last = path.split("/")[-1] or "page"
    base = slugify(last) or slugify(title or "") or "page"
    h = hashlib.md5(url.encode("utf-8")).hexdigest()[:6]
    return f"{index:05d}_{base}_{h}.json"


def extract_text(soup: BeautifulSoup) -> tuple[str, str]:
    selectors = ["main", "article", "[role=main]", ".article", ".content", "#content"]
    main = None
    for sel in selectors:
        main = soup.select_one(sel)
        if main:
            break
    if main is None:
        main = soup.body or soup
    title_tag = main.find("h1") or soup.find("h1")
    title = title_tag.get_text(strip=True) if title_tag else ""
    parts = []
    for el in main.find_all(["h1", "h2", "h3", "p", "li"]):
        t = el.get_text(strip=True)
        if t:
            parts.append(t)
    content = "\n\n".join(parts).strip()
    return title, content


class AsyncCrawler:
    def __init__(
        self,
        start_url: str,
        base_url: Optional[str] = None,
        target_prefix: Optional[str] = None,
        output_dir: str = "output_async",
        concurrency: int = DEFAULT_CONCURRENCY,
        sleep: float = DEFAULT_SLEEP,
        max_pages: Optional[int] = None,
        save_html: bool = False,
        timeout: int = 20,
        user_agent: Optional[str] = None,
        status_callback=None,  # callable(status_dict) called on updates
    ):
        # initialize logger early so _init_robots can use it
        self.logger = logging.getLogger("crawler")
        self.start_url = start_url
        self.base_url = base_url or start_url
        parsed = urlparse(self.base_url)
        self.base_netloc = f"{parsed.scheme}://{parsed.netloc}"
        self.target_prefix = target_prefix
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.concurrent = concurrency
        self.semaphore = asyncio.Semaphore(concurrency)
        self.sleep = sleep
        self.max_pages = max_pages
        self.save_html = save_html
        self.timeout = timeout
        self.headers = {"User-Agent": user_agent or DEFAULT_USER_AGENT}
        self.visited: set[str] = set()
        self.to_visit = deque()
        self.visited_file = self.output_dir / "visited.txt"
        self.index = 0
        self.pbar = None
        self.rp = robotparser.RobotFileParser()
        self.status_callback = status_callback
        self._stop_event = asyncio.Event()
        # initialize robots after logger exists
        self._init_robots()

    def _init_robots(self):
        robots_url = urljoin(self.base_netloc, "/robots.txt")
        try:
            self.rp.set_url(robots_url)
            self.rp.read()
            self.logger.info("Loaded robots.txt from %s", robots_url)
        except Exception as e:
            # use logger (initialized in __init__)
            self.logger.warning("Could not read robots.txt (%s)", e)

    def allowed_by_robots(self, url: str) -> bool:
        try:
            return self.rp.can_fetch(self.headers.get("User-Agent", "*"), url)
        except Exception:
            return True

    async def load_resume(self):
        if self.visited_file.exists():
            async with aiofiles.open(self.visited_file, mode="r", encoding="utf-8") as f:
                text = await f.read()
                for line in text.splitlines():
                    if line.strip():
                        self.visited.add(line.strip())
            self.logger.info("Resumed: loaded %d visited URLs", len(self.visited))

    async def persist_visited(self, url: str):
        async with aiofiles.open(self.visited_file, mode="a", encoding="utf-8") as f:
            await f.write(url + "\n")

    async def fetch(self, session: aiohttp.ClientSession, url: str, max_attempts: int = 4) -> tuple[Optional[str], Optional[aiohttp.ClientResponse]]:
        attempt = 0
        backoff = 1.0
        while attempt < max_attempts and not self._stop_event.is_set():
            attempt += 1
            try:
                async with async_timeout.timeout(self.timeout):
                    async with session.get(url, headers=self.headers) as resp:
                        status = resp.status
                        if status in (403, 451):
                            self.logger.error("Forbidden (%s) for %s", status, url)
                            return None, resp
                        if status >= 400:
                            self.logger.warning("Bad status %s for %s", status, url)
                            if status in (429, 500, 502, 503, 504) and attempt < max_attempts:
                                await asyncio.sleep(backoff)
                                backoff *= 2
                                continue
                            return None, resp
                        ctype = resp.headers.get("Content-Type", "")
                        if "text/html" not in ctype:
                            self.logger.info("Skipping non-html %s (Content-Type: %s)", url, ctype)
                            return None, resp
                        text = await resp.text()
                        return text, resp
            except (asyncio.TimeoutError, ClientError) as e:
                self.logger.warning("Fetch error for %s (attempt %d): %s", url, attempt, e)
                if attempt < max_attempts:
                    await asyncio.sleep(backoff)
                    backoff *= 2
                    continue
                return None, None
        return None, None

    async def get_links_from_html(self, html: str, base: str) -> set[str]:
        soup = BeautifulSoup(html, "html.parser")
        urls = set()
        for a in soup.find_all("a", href=True):
            href = a["href"].strip()
            if href.startswith(("mailto:", "tel:", "javascript:")):
                continue
            full = urljoin(base, href)
            full = canonicalize_url(full, keep_query=True, drop_utm=True)
            if is_same_host_or_prefix(full, self.base_netloc, self.target_prefix):
                urls.add(full)
        return urls

    async def parse_and_save(self, url: str, html: str):
        soup = BeautifulSoup(html, "html.parser")
        title, content = extract_text(soup)
        data = {
            "url": url,
            "title": title,
            "content": content,
            "fetched_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
        }
        if self.save_html:
            raw = html
            if isinstance(raw, str) and len(raw) > MAX_RAW_HTML_BYTES:
                raw = raw[:MAX_RAW_HTML_BYTES] + "\n\n<!-- truncated -->"
            data["raw_html"] = raw
        else:
            data["raw_html"] = None

        self.index += 1
        fname = make_safe_filename(self.index, url, title)
        out_path = self.output_dir / fname
        # write json and md asynchronously
        try:
            async with aiofiles.open(out_path, mode="w", encoding="utf-8") as f:
                await f.write(json.dumps(data, ensure_ascii=False, indent=2))
        except Exception:
            self.logger.exception("Failed to write JSON file %s", out_path)

        md_name = fname.replace(".json", ".md")
        md_path = self.output_dir / md_name
        try:
            async with aiofiles.open(md_path, mode="w", encoding="utf-8") as f:
                await f.write(f"# {data.get('title','')}\n\n")
                await f.write(f"Source: {data['url']}\n\n")
                await f.write(data.get("content", ""))
        except Exception:
            self.logger.exception("Failed to write MD file %s", md_path)

        self.logger.info("Saved [%d] %s -> %s", self.index, url, out_path.name)
        if self.pbar:
            self.pbar.update(1)

        # --- realtime push to UI via status_callback ---
        if self.status_callback:
            try:
                MAX_CONTENT_LEN = 20_000
                payload = {
                    "type": "saved",
                    "index": self.index,
                    "file": out_path.name,
                    "url": url,
                    "title": title,
                    "content": (content or "")[:MAX_CONTENT_LEN],
                }
                self.logger.debug("Calling status_callback for %s -> %s", url, out_path.name)
                # status_callback is synchronous by contract here (will schedule async broadcast)
                self.status_callback(payload)
            except Exception:
                self.logger.exception("status_callback failed after saving file")

    async def worker(self, session: aiohttp.ClientSession):
        while not self._stop_event.is_set():
            try:
                url = self.to_visit.popleft()
            except IndexError:
                return
            if self.max_pages and self.index >= self.max_pages:
                return
            if url in self.visited:
                continue
            if not self.allowed_by_robots(url):
                self.logger.info("Disallowed by robots.txt: %s", url)
                self.visited.add(url)
                await self.persist_visited(url)
                continue

            async with self.semaphore:
                await asyncio.sleep(self.sleep)
                html, resp = await self.fetch(session, url)
                if html is None:
                    self.visited.add(url)
                    await self.persist_visited(url)
                    continue

                try:
                    links = await self.get_links_from_html(html, url)
                    for link in links:
                        if link not in self.visited and link not in self.to_visit:
                            self.to_visit.append(link)
                except Exception:
                    self.logger.exception("Error extracting links from %s", url)

                try:
                    await self.parse_and_save(url, html)
                except Exception:
                    self.logger.exception("Error parsing/saving %s", url)

                self.visited.add(url)
                await self.persist_visited(url)

    async def crawl(self):
        await self.load_resume()
        start_norm = canonicalize_url(self.start_url, keep_query=False)
        self.to_visit.append(start_norm)

        timeout = aiohttp.ClientTimeout(total=self.timeout)
        conn = aiohttp.TCPConnector(limit=self.concurrent, ssl=True)
        async with aiohttp.ClientSession(timeout=timeout, connector=conn) as session:
            workers_count = max(2, self.concurrent * 2)
            tasks = [asyncio.create_task(self.worker(session)) for _ in range(workers_count)]
            if self.status_callback:
                # send started event
                try:
                    self.status_callback({"type": "started", "start_url": self.start_url})
                except Exception:
                    self.logger.exception("status_callback failed for started event")
            await asyncio.gather(*tasks)
        if self.status_callback:
            try:
                self.status_callback({"type": "finished", "pages_saved": self.index})
            except Exception:
                self.logger.exception("status_callback failed for finished event")

    def stop(self):
        self._stop_event.set()