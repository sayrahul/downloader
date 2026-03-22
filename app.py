import asyncio
import json
import math
import os
import queue
import random
import re
import threading
import time
from dataclasses import dataclass
from pathlib import Path
from tkinter import filedialog, ttk

import aiofiles
import aiohttp
import customtkinter as ctk


ctk.set_appearance_mode("Dark")
ctk.set_default_color_theme("blue")

BASE_URL = "https://ser3.masahub.cc/myfiless/id/{}.mp4"
DEFAULT_SAVE_DIR = "downloaded_videos"
SETTINGS_FILE = "proventure_settings.json"
MAX_VISIBLE_ROWS = 120
MIN_SPLIT_SIZE = 8 * 1024 * 1024
UI_POLL_MS = 120

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:124.0) Gecko/20100101 Firefox/124.0",
]


@dataclass
class FileProbe:
    exists: bool
    size: int
    supports_ranges: bool
    status: int


class DownloadEngine:
    def __init__(self, app):
        self.app = app
        self.cancel_event = threading.Event()
        self.pause_event = threading.Event()
        self.pause_event.set()
        self.thread = None
        self.loop = None

    def start(self, job):
        self.cancel_event.clear()
        self.pause_event.set()
        self.thread = threading.Thread(target=self._run_thread, args=(job,), daemon=True)
        self.thread.start()

    def pause(self):
        self.pause_event.clear()

    def resume(self):
        self.pause_event.set()

    def cancel(self):
        self.cancel_event.set()
        self.pause_event.set()

    def _run_thread(self, job):
        self.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self.loop)
        try:
            self.loop.run_until_complete(self._run(job))
        finally:
            pending = asyncio.all_tasks(self.loop)
            for task in pending:
                task.cancel()
            if pending:
                self.loop.run_until_complete(asyncio.gather(*pending, return_exceptions=True))
            self.loop.close()
            self.loop = None

    async def _run(self, job):
        save_dir = Path(job["save_dir"])
        save_dir.mkdir(parents=True, exist_ok=True)

        timeout = aiohttp.ClientTimeout(total=None, connect=15, sock_connect=15, sock_read=60)
        connector = aiohttp.TCPConnector(
            limit=0,
            limit_per_host=max(job["workers"] * job["threads"], job["workers"]),
            enable_cleanup_closed=True,
            ssl=False,
            ttl_dns_cache=300,
            use_dns_cache=True,
        )

        async with aiohttp.ClientSession(connector=connector, timeout=timeout, trust_env=False) as session:
            semaphore = asyncio.Semaphore(job["workers"])
            tasks = [
                asyncio.create_task(self._bounded_process(session, semaphore, video_id, job))
                for video_id in job["ids"]
            ]
            results = await asyncio.gather(*tasks, return_exceptions=True)

        cancelled = self.cancel_event.is_set()
        errors = sum(1 for item in results if isinstance(item, Exception))
        self.app.emit_event("run_finished", {"cancelled": cancelled, "errors": errors})

    async def _bounded_process(self, session, semaphore, video_id, job):
        async with semaphore:
            if self.cancel_event.is_set():
                return
            await self._process_video(session, video_id, job)

    async def _process_video(self, session, video_id, job):
        await self._wait_if_paused()
        if self.cancel_event.is_set():
            return

        url = job["base_url"].format(video_id)
        save_dir = Path(job["save_dir"])
        final_path = save_dir / f"video_{video_id}.mp4"
        tmp_path = save_dir / f"video_{video_id}.mp4.tmp"
        headers = {"User-Agent": random.choice(USER_AGENTS)}
        proxy = self._pick_proxy(job["proxies"])

        try:
            probe = await self._probe_file(session, url, headers, proxy)

            if probe.status == 404 or not probe.exists:
                self.app.emit_event("item_done", {"video_id": video_id, "result": "missing", "status": "Not Found", "size_mb": "-"})
                return

            size_mb = self._size_label(probe.size)

            if probe.size and probe.size > job["max_bytes"]:
                self.app.emit_event(
                    "item_done",
                    {"video_id": video_id, "result": "skipped", "status": "Skipped: over limit", "size_mb": size_mb},
                )
                return

            if final_path.exists() and (probe.size == 0 or final_path.stat().st_size == probe.size):
                self.app.emit_event(
                    "item_done",
                    {"video_id": video_id, "result": "skipped", "status": "Skipped: already downloaded", "size_mb": size_mb},
                )
                return

            self._cleanup_partial_files(save_dir, video_id)
            self.app.emit_event("item_status", {"video_id": video_id, "status": "Queued for download", "size_mb": size_mb})

            if probe.supports_ranges and probe.size >= MIN_SPLIT_SIZE and job["threads"] > 1:
                await self._download_multi_part(session, url, final_path, probe.size, headers, proxy, job, video_id)
            else:
                await self._download_single(session, url, tmp_path, headers, proxy, job, video_id)
                if self.cancel_event.is_set():
                    tmp_path.unlink(missing_ok=True)
                    return
                os.replace(tmp_path, final_path)

            if not self.cancel_event.is_set():
                self.app.emit_event(
                    "item_done",
                    {"video_id": video_id, "result": "success", "status": "Complete", "size_mb": size_mb},
                )
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            self._cleanup_partial_files(save_dir, video_id)
            self.app.emit_event(
                "item_done",
                {"video_id": video_id, "result": "failed", "status": f"Failed: {self._short_error(exc)}", "size_mb": "Error"},
            )

    async def _probe_file(self, session, url, headers, proxy):
        methods = ("HEAD", "GET")
        for method in methods:
            try:
                response = await self._request_with_retries(
                    session,
                    method,
                    url,
                    headers=headers,
                    proxy=proxy,
                    allow_redirects=True,
                    read_body=(method == "GET"),
                )
                async with response:
                    status = response.status
                    if status == 404:
                        return FileProbe(False, 0, False, status)
                    if status >= 400:
                        continue

                    size = int(response.headers.get("Content-Length", 0) or 0)
                    supports_ranges = "bytes" in response.headers.get("Accept-Ranges", "").lower()
                    return FileProbe(True, size, supports_ranges, status)
            except Exception:
                continue
        return FileProbe(False, 0, False, 0)

    async def _download_single(self, session, url, tmp_path, headers, proxy, job, video_id):
        self.app.emit_event("item_status", {"video_id": video_id, "status": "Downloading", "size_mb": "-"})
        async with aiofiles.open(tmp_path, "wb") as file_obj:
            response = await self._request_with_retries(
                session,
                "GET",
                url,
                headers=headers,
                proxy=proxy,
                allow_redirects=True,
            )
            async with response:
                if response.status >= 400:
                    raise aiohttp.ClientResponseError(
                        response.request_info,
                        response.history,
                        status=response.status,
                        message=f"HTTP {response.status}",
                        headers=response.headers,
                    )
                async for chunk in response.content.iter_chunked(job["chunk_size"]):
                    await self._wait_if_paused()
                    if self.cancel_event.is_set():
                        return
                    await file_obj.write(chunk)
                    self.app.emit_event("bytes", {"count": len(chunk)})

    async def _download_multi_part(self, session, url, final_path, total_size, headers, proxy, job, video_id):
        self.app.emit_event(
            "item_status",
            {"video_id": video_id, "status": f"Multipart x{job['threads']}", "size_mb": self._size_label(total_size)},
        )
        chunk_size = math.ceil(total_size / job["threads"])
        part_paths = []
        tasks = []

        for index in range(job["threads"]):
            start = index * chunk_size
            end = min(total_size - 1, start + chunk_size - 1)
            if start > end:
                break
            part_path = final_path.with_suffix(final_path.suffix + f".part{index}")
            part_paths.append(part_path)
            tasks.append(
                asyncio.create_task(
                    self._download_part(session, url, start, end, part_path, headers.copy(), proxy, job)
                )
            )

        results = await asyncio.gather(*tasks, return_exceptions=True)
        failures = [result for result in results if isinstance(result, Exception)]
        if failures or self.cancel_event.is_set():
            for task in tasks:
                task.cancel()
            self._cleanup_paths(part_paths)
            if failures:
                raise failures[0]
            return

        self.app.emit_event("item_status", {"video_id": video_id, "status": "Merging chunks", "size_mb": self._size_label(total_size)})
        async with aiofiles.open(final_path, "wb") as output_file:
            for part_path in part_paths:
                async with aiofiles.open(part_path, "rb") as part_file:
                    while True:
                        data = await part_file.read(job["chunk_size"])
                        if not data:
                            break
                        await output_file.write(data)
                part_path.unlink(missing_ok=True)

    async def _download_part(self, session, url, start, end, part_path, headers, proxy, job):
        headers["Range"] = f"bytes={start}-{end}"
        response = await self._request_with_retries(
            session,
            "GET",
            url,
            headers=headers,
            proxy=proxy,
            allow_redirects=True,
        )
        async with response:
            if response.status not in (200, 206):
                raise aiohttp.ClientResponseError(
                    response.request_info,
                    response.history,
                    status=response.status,
                    message=f"HTTP {response.status}",
                    headers=response.headers,
                )
            async with aiofiles.open(part_path, "wb") as file_obj:
                async for chunk in response.content.iter_chunked(job["chunk_size"]):
                    await self._wait_if_paused()
                    if self.cancel_event.is_set():
                        return
                    await file_obj.write(chunk)
                    self.app.emit_event("bytes", {"count": len(chunk)})

    async def _request_with_retries(self, session, method, url, headers, proxy, allow_redirects, read_body=False):
        last_error = None
        for attempt in range(3):
            if self.cancel_event.is_set():
                raise asyncio.CancelledError()
            try:
                response = await session.request(
                    method,
                    url,
                    headers=headers,
                    proxy=proxy,
                    ssl=False,
                    allow_redirects=allow_redirects,
                )
                if read_body and response.status < 400:
                    await response.content.read(1)
                if response.status in (429, 500, 502, 503, 504) and attempt < 2:
                    response.release()
                    await asyncio.sleep(1 + attempt)
                    continue
                return response
            except (aiohttp.ClientError, asyncio.TimeoutError) as exc:
                last_error = exc
                if attempt < 2:
                    await asyncio.sleep(1 + attempt)
                    continue
                raise
        if last_error:
            raise last_error
        raise RuntimeError("Request failed without an error")

    async def _wait_if_paused(self):
        while not self.pause_event.is_set():
            if self.cancel_event.is_set():
                return
            await asyncio.sleep(0.2)

    def _pick_proxy(self, proxies):
        if not proxies:
            return None
        proxy = random.choice(proxies)
        if not proxy.startswith(("http://", "https://", "socks5://")):
            proxy = f"http://{proxy}"
        return proxy

    def _cleanup_partial_files(self, save_dir, video_id):
        partials = list(Path(save_dir).glob(f"video_{video_id}.mp4.part*"))
        partials.append(Path(save_dir) / f"video_{video_id}.mp4.tmp")
        self._cleanup_paths(partials)

    def _cleanup_paths(self, paths):
        for path in paths:
            try:
                Path(path).unlink(missing_ok=True)
            except OSError:
                pass

    def _size_label(self, size_bytes):
        if not size_bytes:
            return "-"
        return f"{size_bytes / (1024 * 1024):.1f}"

    def _short_error(self, exc):
        text = str(exc).strip() or exc.__class__.__name__
        return text[:80]


class ProventureHyperFetcher(ctk.CTk):
    def __init__(self):
        super().__init__()
        self.title("Proventure Asset Fetcher - V5 Turbo")
        self.geometry("1180x860")
        self.minsize(1040, 760)
        self.grid_columnconfigure(0, weight=1)
        self.grid_rowconfigure(3, weight=1)

        self.event_queue = queue.Queue()
        self.engine = DownloadEngine(self)

        self.silent_mode = ctk.BooleanVar(value=False)
        self.range_mode = ctk.StringVar(value="sequence")

        self.proxies = []
        self.status_items = {}
        self.run_started_at = None
        self.bytes_downloaded = 0
        self.total_videos = 0
        self.processed_count = 0
        self.success_count = 0
        self.skipped_count = 0
        self.failed_count = 0
        self.missing_count = 0
        self.active = False
        self.is_paused = False

        self.setup_ui()
        self.load_settings()
        self.after(UI_POLL_MS, self.process_ui_events)

    def setup_ui(self):
        header = ctk.CTkFrame(self, fg_color="transparent")
        header.grid(row=0, column=0, padx=20, pady=(18, 8), sticky="ew")
        header.grid_columnconfigure(0, weight=1)
        header.grid_columnconfigure(1, weight=0)

        title_frame = ctk.CTkFrame(header, fg_color="transparent")
        title_frame.grid(row=0, column=0, sticky="w")
        ctk.CTkLabel(title_frame, text="Rocket Asset Engine", font=ctk.CTkFont(size=30, weight="bold")).pack(anchor="w")
        ctk.CTkLabel(
            title_frame,
            text="Safer UI thread, stronger retries, faster multipart downloads, and better run visibility.",
            text_color="#93a2b7",
            font=ctk.CTkFont(size=13),
        ).pack(anchor="w", pady=(2, 0))

        action_frame = ctk.CTkFrame(header, fg_color="transparent")
        action_frame.grid(row=0, column=1, sticky="e")
        self.proxy_btn = ctk.CTkButton(action_frame, text="Load Proxies", width=120, command=self.load_proxies)
        self.proxy_btn.pack(side="left", padx=(0, 10))
        self.folder_btn = ctk.CTkButton(action_frame, text="Output Folder", width=120, fg_color="transparent", border_width=1, command=self.select_output_dir)
        self.folder_btn.pack(side="left")

        cards = ctk.CTkFrame(self, fg_color="transparent")
        cards.grid(row=1, column=0, padx=20, pady=6, sticky="ew")
        cards.grid_columnconfigure((0, 1), weight=1)

        target_card = ctk.CTkFrame(cards, corner_radius=14)
        target_card.grid(row=0, column=0, padx=(0, 10), sticky="nsew")
        target_card.grid_columnconfigure(0, weight=1)
        ctk.CTkLabel(target_card, text="Target Setup", font=ctk.CTkFont(size=16, weight="bold")).grid(row=0, column=0, padx=18, pady=(16, 8), sticky="w")

        link_frame = ctk.CTkFrame(target_card, fg_color="transparent")
        link_frame.grid(row=1, column=0, padx=18, pady=(0, 8), sticky="ew")
        link_frame.grid_columnconfigure(0, weight=1)
        self.link_entry = ctk.CTkEntry(link_frame, placeholder_text="Paste direct URL or template...")
        self.link_entry.grid(row=0, column=0, sticky="ew", padx=(0, 10))
        ctk.CTkButton(link_frame, text="Extract", width=88, command=self.extract_id).grid(row=0, column=1)

        range_frame = ctk.CTkFrame(target_card, fg_color="transparent")
        range_frame.grid(row=2, column=0, padx=18, pady=(0, 10), sticky="ew")
        for column in range(6):
            range_frame.grid_columnconfigure(column, weight=1 if column in (1, 3, 5) else 0)

        ctk.CTkLabel(range_frame, text="Start ID").grid(row=0, column=0, sticky="w")
        self.start_entry = ctk.CTkEntry(range_frame, width=120)
        self.start_entry.grid(row=0, column=1, sticky="ew", padx=(6, 16))

        ctk.CTkLabel(range_frame, text="End ID").grid(row=0, column=2, sticky="w")
        self.end_entry = ctk.CTkEntry(range_frame, width=120)
        self.end_entry.grid(row=0, column=3, sticky="ew", padx=(6, 16))

        ctk.CTkLabel(range_frame, text="Step").grid(row=0, column=4, sticky="w")
        self.step_entry = ctk.CTkEntry(range_frame, width=80)
        self.step_entry.grid(row=0, column=5, sticky="ew", padx=(6, 0))

        mode_frame = ctk.CTkFrame(target_card, fg_color="transparent")
        mode_frame.grid(row=3, column=0, padx=18, pady=(0, 16), sticky="ew")
        mode_frame.grid_columnconfigure(1, weight=1)
        ctk.CTkLabel(mode_frame, text="Range Mode").grid(row=0, column=0, sticky="w")
        self.mode_menu = ctk.CTkOptionMenu(mode_frame, values=["sequence", "reverse"], variable=self.range_mode, command=lambda _value: self.update_preview_label())
        self.mode_menu.grid(row=0, column=1, sticky="w", padx=(10, 0))
        self.preview_label = ctk.CTkLabel(mode_frame, text="Ready", text_color="#93a2b7")
        self.preview_label.grid(row=0, column=2, sticky="e")

        engine_card = ctk.CTkFrame(cards, corner_radius=14, fg_color="#2f1a1d")
        engine_card.grid(row=0, column=1, padx=(10, 0), sticky="nsew")
        for column in range(4):
            engine_card.grid_columnconfigure(column, weight=1)

        ctk.CTkLabel(engine_card, text="Performance Controls", text_color="#ff8d8d", font=ctk.CTkFont(size=16, weight="bold")).grid(
            row=0, column=0, columnspan=4, padx=18, pady=(16, 8), sticky="w"
        )

        ctk.CTkLabel(engine_card, text="Max Size (MB)").grid(row=1, column=0, padx=(18, 8), pady=8, sticky="w")
        self.size_entry = ctk.CTkEntry(engine_card, width=90)
        self.size_entry.grid(row=1, column=1, padx=(0, 10), pady=8, sticky="ew")

        ctk.CTkLabel(engine_card, text="Connections").grid(row=1, column=2, padx=(10, 8), pady=8, sticky="w")
        self.workers_entry = ctk.CTkEntry(engine_card, width=90)
        self.workers_entry.grid(row=1, column=3, padx=(0, 18), pady=8, sticky="ew")

        ctk.CTkLabel(engine_card, text="Threads / File").grid(row=2, column=0, padx=(18, 8), pady=8, sticky="w")
        self.threads_entry = ctk.CTkEntry(engine_card, width=90)
        self.threads_entry.grid(row=2, column=1, padx=(0, 10), pady=8, sticky="ew")

        ctk.CTkLabel(engine_card, text="Chunk KB").grid(row=2, column=2, padx=(10, 8), pady=8, sticky="w")
        self.chunk_entry = ctk.CTkEntry(engine_card, width=90)
        self.chunk_entry.grid(row=2, column=3, padx=(0, 18), pady=8, sticky="ew")

        self.silent_chk = ctk.CTkCheckBox(engine_card, text="Silent UI mode", variable=self.silent_mode)
        self.silent_chk.grid(row=3, column=0, columnspan=2, padx=18, pady=(6, 16), sticky="w")
        self.cleanup_switch = ctk.CTkSwitch(engine_card, text="Clean stale parts before run")
        self.cleanup_switch.select()
        self.cleanup_switch.grid(row=3, column=2, columnspan=2, padx=18, pady=(6, 16), sticky="e")

        dashboard = ctk.CTkFrame(self, corner_radius=14, fg_color="#1b1f24")
        dashboard.grid(row=2, column=0, padx=20, pady=10, sticky="ew")
        dashboard.grid_columnconfigure((0, 1, 2), weight=1)

        buttons = ctk.CTkFrame(dashboard, fg_color="transparent")
        buttons.grid(row=0, column=0, columnspan=3, padx=18, pady=(16, 10), sticky="ew")
        buttons.grid_columnconfigure((0, 1, 2), weight=1)

        self.start_btn = ctk.CTkButton(
            buttons,
            text="Start Turbo Run",
            height=46,
            fg_color="#d13b35",
            hover_color="#9f2622",
            font=ctk.CTkFont(size=15, weight="bold"),
            command=self.start_process,
        )
        self.start_btn.grid(row=0, column=0, padx=(0, 10), sticky="ew")
        self.pause_btn = ctk.CTkButton(buttons, text="Pause", height=46, state="disabled", command=self.toggle_pause)
        self.pause_btn.grid(row=0, column=1, padx=10, sticky="ew")
        self.cancel_btn = ctk.CTkButton(
            buttons,
            text="Cancel",
            height=46,
            fg_color="transparent",
            border_width=1,
            border_color="#ef6d6d",
            text_color="#ef6d6d",
            state="disabled",
            command=self.cancel_process,
        )
        self.cancel_btn.grid(row=0, column=2, padx=(10, 0), sticky="ew")

        stats = ctk.CTkFrame(dashboard, fg_color="transparent")
        stats.grid(row=1, column=0, columnspan=3, padx=18, pady=(0, 8), sticky="ew")
        stats.grid_columnconfigure((0, 1, 2, 3, 4), weight=1)

        self.stat_total = ctk.CTkLabel(stats, text="Queue: 0", font=ctk.CTkFont(size=13, weight="bold"))
        self.stat_total.grid(row=0, column=0, sticky="w")
        self.stat_success = ctk.CTkLabel(stats, text="Done: 0", text_color="#4ade80", font=ctk.CTkFont(size=13, weight="bold"))
        self.stat_success.grid(row=0, column=1)
        self.stat_skipped = ctk.CTkLabel(stats, text="Skipped: 0", text_color="#cbd5e1", font=ctk.CTkFont(size=13, weight="bold"))
        self.stat_skipped.grid(row=0, column=2)
        self.stat_failed = ctk.CTkLabel(stats, text="Failed: 0", text_color="#f87171", font=ctk.CTkFont(size=13, weight="bold"))
        self.stat_failed.grid(row=0, column=3)
        self.stat_speed = ctk.CTkLabel(stats, text="Speed: 0 MB/s", text_color="#7dd3fc", font=ctk.CTkFont(size=13, weight="bold"))
        self.stat_speed.grid(row=0, column=4, sticky="e")

        progress = ctk.CTkFrame(dashboard, fg_color="transparent")
        progress.grid(row=2, column=0, columnspan=3, padx=18, pady=(0, 10), sticky="ew")
        progress.grid_columnconfigure(0, weight=1)

        self.progress_label = ctk.CTkLabel(progress, text="Batch Progress: 0.0% (0 / 0)")
        self.progress_label.grid(row=0, column=0, sticky="w")
        self.eta_label = ctk.CTkLabel(progress, text="ETA: --", text_color="#93a2b7")
        self.eta_label.grid(row=0, column=1, sticky="e")

        self.progress_bar = ctk.CTkProgressBar(progress, height=13, progress_color="#d13b35")
        self.progress_bar.grid(row=1, column=0, columnspan=2, sticky="ew", pady=(6, 0))
        self.progress_bar.set(0)

        self.run_status = ctk.CTkLabel(dashboard, text="Idle", text_color="#93a2b7")
        self.run_status.grid(row=3, column=0, columnspan=3, padx=18, pady=(0, 16), sticky="w")

        grid_frame = ctk.CTkFrame(self, corner_radius=14)
        grid_frame.grid(row=3, column=0, padx=20, pady=(0, 20), sticky="nsew")
        grid_frame.grid_rowconfigure(0, weight=1)
        grid_frame.grid_columnconfigure(0, weight=1)

        style = ttk.Style()
        style.theme_use("default")
        style.configure("Treeview", background="#202328", foreground="#dce4ee", fieldbackground="#202328", rowheight=30, borderwidth=0)
        style.configure("Treeview.Heading", background="#292d34", foreground="#ffffff", font=("Segoe UI", 10, "bold"))
        style.map("Treeview", background=[("selected", "#d13b35")])

        self.tree = ttk.Treeview(grid_frame, columns=("ID", "Status", "Size"), show="headings", selectmode="browse")
        self.tree.heading("ID", text="Video ID")
        self.tree.heading("Status", text="Status")
        self.tree.heading("Size", text="Size (MB)")
        self.tree.column("ID", width=140, anchor="center")
        self.tree.column("Status", width=700, anchor="w")
        self.tree.column("Size", width=120, anchor="center")
        self.tree.grid(row=0, column=0, sticky="nsew", padx=(2, 0), pady=2)

        scrollbar = ttk.Scrollbar(grid_frame, orient="vertical", command=self.tree.yview)
        scrollbar.grid(row=0, column=1, sticky="ns", pady=2)
        self.tree.configure(yscrollcommand=scrollbar.set)

        for entry in (self.start_entry, self.end_entry, self.step_entry):
            entry.bind("<KeyRelease>", lambda _event: self.update_preview_label())

    def emit_event(self, event_type, payload):
        self.event_queue.put((event_type, payload))

    def process_ui_events(self):
        processed = 0
        while processed < 200:
            try:
                event_type, payload = self.event_queue.get_nowait()
            except queue.Empty:
                break
            self.handle_event(event_type, payload)
            processed += 1
        if self.active:
            self.refresh_runtime_metrics()
        self.after(UI_POLL_MS, self.process_ui_events)

    def handle_event(self, event_type, payload):
        if event_type == "item_status":
            self.update_grid(payload["video_id"], payload["status"], payload.get("size_mb", "-"))
        elif event_type == "item_done":
            self.update_grid(payload["video_id"], payload["status"], payload.get("size_mb", "-"))
            self.update_stats(payload["result"])
        elif event_type == "bytes":
            self.bytes_downloaded += payload["count"]
        elif event_type == "run_finished":
            self.finish_run(payload["cancelled"], payload["errors"])

    def load_settings(self):
        settings = {}
        if Path(SETTINGS_FILE).exists():
            try:
                settings = json.loads(Path(SETTINGS_FILE).read_text(encoding="utf-8"))
            except (json.JSONDecodeError, OSError):
                settings = {}

        self.link_entry.insert(0, settings.get("last_url", BASE_URL.format(68000)))
        self.start_entry.insert(0, str(settings.get("start_id", 68000)))
        self.end_entry.insert(0, str(settings.get("end_id", 68010)))
        self.step_entry.insert(0, str(settings.get("step", 1)))
        self.size_entry.insert(0, str(settings.get("max_size", 200)))
        self.workers_entry.insert(0, str(settings.get("workers", 12)))
        self.threads_entry.insert(0, str(settings.get("threads", 6)))
        self.chunk_entry.insert(0, str(settings.get("chunk_kb", 1024)))
        self.range_mode.set(settings.get("range_mode", "sequence"))
        self.silent_mode.set(bool(settings.get("silent_mode", False)))
        self.output_dir = settings.get("output_dir", DEFAULT_SAVE_DIR)
        self.update_preview_label()

    def save_settings(self):
        data = {
            "last_url": self.link_entry.get().strip() or BASE_URL.format(68000),
            "start_id": self.start_entry.get().strip() or "68000",
            "end_id": self.end_entry.get().strip() or "68010",
            "step": self.step_entry.get().strip() or "1",
            "max_size": self.size_entry.get().strip() or "200",
            "workers": self.workers_entry.get().strip() or "12",
            "threads": self.threads_entry.get().strip() or "6",
            "chunk_kb": self.chunk_entry.get().strip() or "1024",
            "range_mode": self.range_mode.get(),
            "silent_mode": self.silent_mode.get(),
            "output_dir": self.output_dir,
        }
        Path(SETTINGS_FILE).write_text(json.dumps(data, indent=2), encoding="utf-8")

    def extract_id(self):
        url = self.link_entry.get().strip()
        match = re.search(r"id/(\d+)\.mp4", url)
        if not match:
            self.run_status.configure(text="Could not extract an ID from that link.", text_color="#fbbf24")
            return

        extracted_id = int(match.group(1))
        self.start_entry.delete(0, "end")
        self.start_entry.insert(0, str(extracted_id))
        if not self.end_entry.get().strip():
            self.end_entry.insert(0, str(extracted_id))
        self.update_preview_label()
        self.update_grid("SYS", f"Extracted start ID {extracted_id}", "-")

    def load_proxies(self):
        file_path = filedialog.askopenfilename(filetypes=[("Text files", "*.txt"), ("All files", "*.*")])
        if not file_path:
            return
        try:
            with open(file_path, "r", encoding="utf-8") as file_obj:
                self.proxies = [line.strip() for line in file_obj if line.strip()]
            self.proxy_btn.configure(text=f"Proxies: {len(self.proxies)}", fg_color="#1f6aa5")
            self.run_status.configure(text=f"Loaded {len(self.proxies)} proxies.", text_color="#93c5fd")
        except OSError as exc:
            self.run_status.configure(text=f"Failed to load proxies: {exc}", text_color="#f87171")

    def select_output_dir(self):
        selected = filedialog.askdirectory(initialdir=str(Path(self.output_dir).resolve()))
        if not selected:
            return
        self.output_dir = selected
        self.run_status.configure(text=f"Output folder set to {selected}", text_color="#93c5fd")

    def build_job(self):
        start_id = int(self.start_entry.get().strip())
        end_id = int(self.end_entry.get().strip())
        step = int(self.step_entry.get().strip() or "1")
        if step <= 0:
            raise ValueError("Step must be greater than zero.")
        if start_id > end_id:
            raise ValueError("Start ID must be less than or equal to End ID.")

        ids = list(range(start_id, end_id + 1, step))
        if self.range_mode.get() == "reverse":
            ids.reverse()

        max_size_mb = float(self.size_entry.get().strip() or "200")
        workers = int(self.workers_entry.get().strip() or "12")
        threads = int(self.threads_entry.get().strip() or "6")
        chunk_kb = int(self.chunk_entry.get().strip() or "1024")

        if workers < 1 or threads < 1 or chunk_kb < 64:
            raise ValueError("Connections, threads, and chunk size must be positive.")

        base_url = self.link_entry.get().strip() or BASE_URL.format(start_id)
        if "{}" not in base_url:
            probe_match = re.search(r"id/\d+\.mp4", base_url)
            if probe_match:
                base_url = re.sub(r"id/\d+\.mp4", "id/{}.mp4", base_url)
            else:
                raise ValueError("Link must contain a numeric ID or a {} placeholder.")

        return {
            "ids": ids,
            "max_bytes": int(max_size_mb * 1024 * 1024),
            "workers": workers,
            "threads": threads,
            "chunk_size": chunk_kb * 1024,
            "base_url": base_url,
            "proxies": list(self.proxies),
            "save_dir": self.output_dir,
        }

    def start_process(self):
        try:
            job = self.build_job()
        except ValueError as exc:
            self.run_status.configure(text=str(exc), text_color="#fbbf24")
            self.update_grid("SYS", f"Invalid input: {exc}", "-")
            return

        if self.cleanup_switch.get():
            self.cleanup_stale_partials()

        self.save_settings()
        self.reset_run_state(job["ids"])
        self.active = True
        self.start_btn.configure(state="disabled", text="Turbo Running")
        self.pause_btn.configure(state="normal", text="Pause")
        self.cancel_btn.configure(state="normal")
        self.run_status.configure(text="Run started. Downloader is warming up...", text_color="#7dd3fc")
        self.engine.start(job)

    def reset_run_state(self, ids):
        self.total_videos = len(ids)
        self.processed_count = 0
        self.success_count = 0
        self.skipped_count = 0
        self.failed_count = 0
        self.missing_count = 0
        self.bytes_downloaded = 0
        self.is_paused = False
        self.run_started_at = time.time()
        self.status_items.clear()
        self.tree.delete(*self.tree.get_children())
        self.stat_total.configure(text=f"Queue: {self.total_videos}")
        self.stat_success.configure(text="Done: 0")
        self.stat_skipped.configure(text="Skipped: 0")
        self.stat_failed.configure(text="Failed: 0")
        self.stat_speed.configure(text="Speed: 0 MB/s")
        self.progress_label.configure(text=f"Batch Progress: 0.0% (0 / {self.total_videos})")
        self.progress_bar.set(0)
        self.eta_label.configure(text="ETA: --")
        self.update_preview_label()

    def toggle_pause(self):
        if not self.active:
            return
        self.is_paused = not self.is_paused
        if self.is_paused:
            self.engine.pause()
            self.pause_btn.configure(text="Resume", fg_color="#1f6aa5")
            self.run_status.configure(text="Paused. Downloads will continue when resumed.", text_color="#fbbf24")
        else:
            self.engine.resume()
            self.pause_btn.configure(text="Pause", fg_color=ctk.ThemeManager.theme["CTkButton"]["fg_color"])
            self.run_status.configure(text="Resumed.", text_color="#7dd3fc")

    def cancel_process(self):
        if not self.active:
            return
        self.engine.cancel()
        self.run_status.configure(text="Cancelling active downloads...", text_color="#f87171")

    def finish_run(self, cancelled, errors):
        self.active = False
        self.start_btn.configure(state="normal", text="Start Turbo Run")
        self.pause_btn.configure(state="disabled", text="Pause", fg_color=ctk.ThemeManager.theme["CTkButton"]["fg_color"])
        self.cancel_btn.configure(state="disabled")
        duration = max(time.time() - self.run_started_at, 0.1) if self.run_started_at else 0.1
        avg_speed = self.bytes_downloaded / duration / (1024 * 1024)
        if cancelled:
            status = f"Run cancelled. Done {self.success_count}, skipped {self.skipped_count}, failed {self.failed_count}. Avg speed {avg_speed:.2f} MB/s."
            color = "#fbbf24"
        else:
            status = f"Run complete. Done {self.success_count}, skipped {self.skipped_count}, failed {self.failed_count}. Avg speed {avg_speed:.2f} MB/s."
            if errors:
                status += f" Internal task errors: {errors}."
            color = "#4ade80" if self.failed_count == 0 else "#fbbf24"
        self.run_status.configure(text=status, text_color=color)
        self.refresh_runtime_metrics(force=True)

    def update_stats(self, result_type):
        self.processed_count += 1
        if result_type == "success":
            self.success_count += 1
        elif result_type == "skipped":
            self.skipped_count += 1
        elif result_type == "missing":
            self.failed_count += 1
            self.missing_count += 1
        elif result_type == "failed":
            self.failed_count += 1

        if self.silent_mode.get() and self.processed_count % 6 != 0 and self.processed_count != self.total_videos:
            return

        self.stat_success.configure(text=f"Done: {self.success_count}")
        self.stat_skipped.configure(text=f"Skipped: {self.skipped_count}")
        self.stat_failed.configure(text=f"Failed: {self.failed_count}")
        progress = self.processed_count / self.total_videos if self.total_videos else 0
        self.progress_bar.set(progress)
        self.progress_label.configure(text=f"Batch Progress: {progress * 100:.1f}% ({self.processed_count} / {self.total_videos})")
        self.refresh_runtime_metrics()

    def refresh_runtime_metrics(self, force=False):
        if not self.run_started_at:
            return
        elapsed = max(time.time() - self.run_started_at, 0.1)
        speed = self.bytes_downloaded / elapsed / (1024 * 1024)
        self.stat_speed.configure(text=f"Speed: {speed:.2f} MB/s")

        if self.processed_count and self.total_videos and speed > 0:
            remaining = max(self.total_videos - self.processed_count, 0)
            avg_item_time = elapsed / self.processed_count
            eta_seconds = remaining * avg_item_time
            self.eta_label.configure(text=f"ETA: {self.format_eta(eta_seconds)}")
        elif force or self.active:
            self.eta_label.configure(text="ETA: --")

    def format_eta(self, seconds):
        seconds = int(max(seconds, 0))
        minutes, sec = divmod(seconds, 60)
        hours, minutes = divmod(minutes, 60)
        if hours:
            return f"{hours}h {minutes}m {sec}s"
        if minutes:
            return f"{minutes}m {sec}s"
        return f"{sec}s"

    def update_grid(self, vid_id, status, size="-"):
        if self.silent_mode.get() and str(vid_id) != "SYS":
            return

        key = str(vid_id)
        item_id = self.status_items.get(key)
        if item_id and self.tree.exists(item_id):
            self.tree.item(item_id, values=(vid_id, status, size))
        else:
            item_id = self.tree.insert("", "end", values=(vid_id, status, size))
            self.status_items[key] = item_id

        children = self.tree.get_children()
        if len(children) > MAX_VISIBLE_ROWS:
            oldest = children[0]
            values = self.tree.item(oldest, "values")
            if values:
                self.status_items.pop(str(values[0]), None)
            self.tree.delete(oldest)
        self.tree.yview_moveto(1.0)

    def cleanup_stale_partials(self):
        save_dir = Path(self.output_dir)
        if not save_dir.exists():
            return
        removed = 0
        for path in save_dir.glob("*.part*"):
            try:
                path.unlink()
                removed += 1
            except OSError:
                pass
        for path in save_dir.glob("*.tmp"):
            try:
                path.unlink()
                removed += 1
            except OSError:
                pass
        if removed:
            self.update_grid("SYS", f"Removed {removed} stale partial files", "-")

    def update_preview_label(self):
        start = self.start_entry.get().strip()
        end = self.end_entry.get().strip()
        step = self.step_entry.get().strip() or "1"
        if not start or not end:
            self.preview_label.configure(text="Ready")
            return
        try:
            start_id = int(start)
            end_id = int(end)
            step_value = max(int(step), 1)
            count = len(range(start_id, end_id + 1, step_value)) if start_id <= end_id else 0
            order = "reverse" if self.range_mode.get() == "reverse" else "forward"
            self.preview_label.configure(text=f"{count} IDs, {order}")
        except ValueError:
            self.preview_label.configure(text="Check IDs")


if __name__ == "__main__":
    app = ProventureHyperFetcher()
    app.mainloop()
