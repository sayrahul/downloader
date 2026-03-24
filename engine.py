import asyncio
import math
import os
import random
import threading
import time
from dataclasses import asdict
from datetime import datetime
from pathlib import Path

import aiofiles
import aiohttp

from models import DownloadJob, DownloadResult, FileProbe, HistoryRecord, ProxyEndpoint, RunContext, RuntimeStats, TuningProfile


MIN_SPLIT_SIZE = 8 * 1024 * 1024
MAX_INSPECTOR_NOTE = 120
RUNTIME_EMIT_INTERVAL = 0.4
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:124.0) Gecko/20100101 Firefox/124.0",
]
RETRYABLE_STATUSES = {408, 409, 425, 429, 500, 502, 503, 504}
PROBE_MAX_RETRIES = 1
DOWNLOAD_MAX_RETRIES = 2


class DownloadEngine:
    def __init__(self, app, storage):
        self.app = app
        self.storage = storage
        self.cancel_event = threading.Event()
        self.pause_event = threading.Event()
        self.pause_event.set()
        self.thread = None
        self.loop = None
        self.stats = RuntimeStats()
        self.run_context = RunContext(status_cache=self.storage.load_known_status_cache())
        self.proxy_pool = []
        self.settings_snapshot = None
        self.current_run_label = ""
        self.result_batch = []
        self.batch_size = 25
        self.tuning_profile = TuningProfile(workers=1, threads=1, chunk_kb=1024, reason="default")
        self.job_ids = []
        self.remaining_ids = set()
        self.last_runtime_emit = 0.0

    def start(self, job: DownloadJob, settings_snapshot):
        self.cancel_event.clear()
        self.pause_event.set()
        self.settings_snapshot = settings_snapshot
        self.current_run_label = job.run_label
        self.proxy_pool = [ProxyEndpoint(raw=value) for value in getattr(self.app, "proxies", [])]
        self.result_batch = []
        self.job_ids = list(job.ids)
        self.remaining_ids = set(job.ids)
        self.last_runtime_emit = 0.0

        self.tuning_profile = self._build_tuning_profile(job)
        job.workers = self.tuning_profile.workers
        job.threads = self.tuning_profile.threads
        job.chunk_size = self.tuning_profile.chunk_kb * 1024

        self.stats = RuntimeStats(
            total=len(job.ids),
            queued_left=len(job.ids),
            current_workers=job.workers,
            current_threads=job.threads,
            chunk_kb=self.tuning_profile.chunk_kb,
            started_at=time.time(),
        )
        run_label = f"{job.ids[0]}-{job.ids[-1]}" if job.ids else "empty"
        run_id = self.storage.record_run_start(run_label, len(job.ids), settings_snapshot)
        self.storage.save_active_run(run_id, job.ids, settings_snapshot)
        self.run_context = RunContext(
            run_id=run_id,
            status_cache=self.storage.load_known_status_cache(),
        )
        self.app.emit_event("diagnostic", {"message": f"Tuning: {self.tuning_profile.reason}"})
        self.thread = threading.Thread(target=self._run_thread, args=(job,), daemon=True)
        self.thread.start()

    def pause(self):
        self.pause_event.clear()

    def resume(self):
        self.pause_event.set()

    def cancel(self):
        self.cancel_event.set()
        self.pause_event.set()

    def _run_thread(self, job: DownloadJob):
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

    async def _run(self, job: DownloadJob):
        save_dir = Path(job.save_dir)
        save_dir.mkdir(parents=True, exist_ok=True)

        timeout = aiohttp.ClientTimeout(
            total=None,
            connect=job.timeout_seconds,
            sock_connect=job.timeout_seconds,
            sock_read=max(job.timeout_seconds, 60),
        )
        connector = aiohttp.TCPConnector(
            limit=max(job.workers * max(job.threads, 1), job.workers),
            limit_per_host=max(job.workers * max(job.threads, 1), job.workers),
            enable_cleanup_closed=True,
            ssl=False,
            ttl_dns_cache=300,
            use_dns_cache=True,
        )

        async with aiohttp.ClientSession(connector=connector, timeout=timeout, trust_env=False) as session:
            job_queue = asyncio.Queue()
            for video_id in job.ids:
                await job_queue.put(video_id)
            worker_count = max(1, min(job.workers, len(job.ids) or 1))
            tasks = [asyncio.create_task(self._worker_loop(session, job_queue, job)) for _ in range(worker_count)]
            results = await asyncio.gather(*tasks, return_exceptions=True)

        self._flush_result_batch()
        errors = sum(1 for item in results if isinstance(item, Exception))
        await self._finish_run(errors)

    async def _worker_loop(self, session, job_queue: asyncio.Queue, job: DownloadJob):
        while not self.cancel_event.is_set():
            try:
                video_id = job_queue.get_nowait()
            except asyncio.QueueEmpty:
                return

            self.stats.active_files += 1
            self.stats.queued_left = max(job_queue.qsize(), 0)
            self._emit_runtime(force=True)
            try:
                result = await self._process_video(session, video_id, job)
                if result:
                    self._apply_result(result)
            finally:
                job_queue.task_done()
                self.stats.active_files = max(self.stats.active_files - 1, 0)
                self.stats.queued_left = max(job_queue.qsize(), 0)
                self._emit_runtime(force=True)

    async def _process_video(self, session, video_id: int, job: DownloadJob):
        await self._wait_if_paused()
        if self.cancel_event.is_set():
            return None

        cached_result = self.run_context.status_cache.get(video_id)
        if job.retry_failed_only and cached_result in {"success", "skipped"}:
            return DownloadResult(video_id, "skipped", "Skipped: already good in history", "-", 0)
        if job.skip_known_missing and cached_result == "missing":
            return DownloadResult(video_id, "skipped", "Skipped: known missing", "-", 0)

        url = job.base_url.format(video_id)
        save_dir = Path(job.save_dir)
        final_path = save_dir / f"video_{video_id}.mp4"
        tmp_path = save_dir / f"video_{video_id}.mp4.tmp"
        headers = {"User-Agent": random.choice(USER_AGENTS)}
        proxy = self._pick_proxy()

        probe, retries_used = await self._probe_file(session, url, headers, proxy, job)
        size_mb = self._size_label(probe.size)

        if probe.status == 404 or not probe.exists:
            return DownloadResult(video_id, "missing", "Not Found", "-", retries_used, http_status=probe.status)
        if probe.size and probe.size > job.max_bytes:
            return DownloadResult(video_id, "skipped", "Skipped: over limit", size_mb, retries_used, http_status=probe.status)
        if probe.content_type and "html" in probe.content_type.lower():
            return DownloadResult(video_id, "failed", "Failed: server returned HTML", size_mb, retries_used, http_status=probe.status, content_type=probe.content_type)
        if final_path.exists() and (probe.size == 0 or final_path.stat().st_size == probe.size):
            return DownloadResult(video_id, "skipped", "Skipped: already downloaded", size_mb, retries_used, http_status=probe.status)
        if tmp_path.exists() and probe.size and tmp_path.stat().st_size == probe.size:
            os.replace(tmp_path, final_path)
            return DownloadResult(video_id, "success", "Recovered temp file", size_mb, retries_used, probe.size, probe.status, probe.content_type)

        try:
            if probe.supports_ranges and probe.size >= MIN_SPLIT_SIZE and job.threads > 1:
                bytes_written, extra_retries = await self._download_multi_part(
                    session, url, final_path, probe.size, headers, proxy, job, video_id
                )
            else:
                bytes_written, extra_retries = await self._download_single(
                    session, url, tmp_path, probe.size, headers, proxy, job, video_id
                )
                if self.cancel_event.is_set():
                    tmp_path.unlink(missing_ok=True)
                    return None
                if job.verify_downloads:
                    self._validate_download(tmp_path, probe.size, probe.content_type)
                os.replace(tmp_path, final_path)
            if job.verify_downloads and final_path.exists():
                self._validate_download(final_path, probe.size, probe.content_type)
            return DownloadResult(
                video_id,
                "success",
                "Complete",
                size_mb,
                retries_used + extra_retries,
                bytes_written,
                probe.status,
                probe.content_type,
            )
        except Exception as exc:
            self._cleanup_partial_files(save_dir, video_id)
            return DownloadResult(
                video_id,
                "failed",
                f"Failed: {self._short_error(exc)}",
                "Error",
                retries_used,
                http_status=probe.status,
                content_type=probe.content_type,
            )

    async def _probe_file(self, session, url, headers, proxy, job):
        retries = 0
        for method in ("HEAD", "GET"):
            try:
                response, used = await self._request_with_retries(
                    session,
                    method,
                    url,
                    headers=headers,
                    proxy=proxy,
                    allow_redirects=True,
                    read_body=(method == "GET"),
                    max_retries=PROBE_MAX_RETRIES,
                    unsafe_ssl=job.unsafe_ssl,
                )
                retries += used
                async with response:
                    if response.status == 404:
                        return FileProbe(False, 0, False, response.status), retries
                    if response.status >= 400:
                        if response.status in (401, 403):
                            content_type = response.headers.get("Content-Type", "")
                            size = int(response.headers.get("Content-Length", 0) or 0)
                            return FileProbe(True, size, False, response.status, content_type), retries
                        continue
                    size = int(response.headers.get("Content-Length", 0) or 0)
                    supports_ranges = "bytes" in response.headers.get("Accept-Ranges", "").lower()
                    content_type = response.headers.get("Content-Type", "")
                    return FileProbe(True, size, supports_ranges, response.status, content_type), retries
            except (aiohttp.ClientError, asyncio.TimeoutError):
                retries += 1
                continue
        return FileProbe(False, 0, False, 0, ""), retries

    async def _download_single(self, session, url, tmp_path, expected_size, headers, proxy, job, video_id):
        existing_size = tmp_path.stat().st_size if tmp_path.exists() else 0
        retries = 0
        if expected_size and existing_size and existing_size < expected_size:
            headers = headers.copy()
            headers["Range"] = f"bytes={existing_size}-"
        else:
            existing_size = 0
        mode = "ab" if existing_size else "wb"

        async with aiofiles.open(tmp_path, mode) as file_obj:
            response, used = await self._request_with_retries(
                session,
                "GET",
                url,
                headers=headers,
                proxy=proxy,
                allow_redirects=True,
                max_retries=DOWNLOAD_MAX_RETRIES,
                unsafe_ssl=job.unsafe_ssl,
            )
            retries += used
            async with response:
                if response.status >= 400:
                    raise aiohttp.ClientResponseError(response.request_info, response.history, status=response.status)
                bytes_written = existing_size
                async for chunk in response.content.iter_chunked(job.chunk_size):
                    await self._wait_if_paused()
                    if self.cancel_event.is_set():
                        return bytes_written, retries
                    await file_obj.write(chunk)
                    bytes_written += len(chunk)
                    self.stats.bytes_downloaded += len(chunk)
                    self._emit_runtime()
                return bytes_written, retries

    async def _download_multi_part(self, session, url, final_path, total_size, headers, proxy, job, video_id):
        part_size = math.ceil(total_size / job.threads)
        part_paths = []
        tasks = []
        for index in range(job.threads):
            start = index * part_size
            end = min(total_size - 1, start + part_size - 1)
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
        failures = [item for item in results if isinstance(item, Exception)]
        if failures or self.cancel_event.is_set():
            self._cleanup_paths(part_paths)
            if failures:
                raise failures[0]
            return 0, 0

        total_written = 0
        total_retries = 0
        async with aiofiles.open(final_path, "wb") as output_file:
            for part_path, result in zip(part_paths, results):
                bytes_written, retries = result
                total_written += bytes_written
                total_retries += retries
                async with aiofiles.open(part_path, "rb") as part_file:
                    while True:
                        data = await part_file.read(job.chunk_size)
                        if not data:
                            break
                        await output_file.write(data)
                part_path.unlink(missing_ok=True)
        return total_written, total_retries

    async def _download_part(self, session, url, start, end, part_path, headers, proxy, job):
        existing_size = part_path.stat().st_size if part_path.exists() else 0
        retries = 0
        range_start = start + existing_size
        if range_start > end:
            return existing_size, retries
        headers["Range"] = f"bytes={range_start}-{end}"
        async with aiofiles.open(part_path, "ab" if existing_size else "wb") as file_obj:
            response, used = await self._request_with_retries(
                session,
                "GET",
                url,
                headers=headers,
                proxy=proxy,
                allow_redirects=True,
                max_retries=DOWNLOAD_MAX_RETRIES,
                unsafe_ssl=job.unsafe_ssl,
            )
            retries += used
            async with response:
                if response.status not in (200, 206):
                    raise aiohttp.ClientResponseError(response.request_info, response.history, status=response.status)
                bytes_written = existing_size
                async for chunk in response.content.iter_chunked(job.chunk_size):
                    await self._wait_if_paused()
                    if self.cancel_event.is_set():
                        return bytes_written, retries
                    await file_obj.write(chunk)
                    bytes_written += len(chunk)
                    self.stats.bytes_downloaded += len(chunk)
                    self._emit_runtime()
                return bytes_written, retries

    async def _request_with_retries(self, session, method, url, headers, proxy, allow_redirects, max_retries, unsafe_ssl, read_body=False):
        last_error = None
        retry_count = 0
        for attempt in range(max_retries):
            if self.cancel_event.is_set():
                raise asyncio.CancelledError()
            try:
                request_kwargs = {
                    "method": method,
                    "url": url,
                    "headers": headers,
                    "proxy": proxy,
                    "allow_redirects": allow_redirects,
                }
                if unsafe_ssl:
                    request_kwargs["ssl"] = False
                else:
                    request_kwargs["ssl"] = False
                response = await session.request(**request_kwargs)
                if read_body and response.status < 400:
                    await response.content.read(1)
                if response.status == 404:
                    return response, retry_count
                if response.status in RETRYABLE_STATUSES and attempt < (max_retries - 1):
                    response.release()
                    retry_count += 1
                    self._note_retry(proxy)
                    await asyncio.sleep(self._backoff_delay(attempt, response.status))
                    continue
                self._report_proxy(proxy, True)
                return response, retry_count
            except (aiohttp.ClientError, asyncio.TimeoutError) as exc:
                last_error = exc
                retry_count += 1
                self._note_retry(proxy)
                self._report_proxy(proxy, False)
                if attempt < (max_retries - 1):
                    await asyncio.sleep(self._backoff_delay(attempt, 0))
                    continue
                raise
        if last_error:
            raise last_error
        raise RuntimeError("Request failed without an error")

    async def _finish_run(self, errors: int):
        self._flush_result_batch()
        self.storage.clear_active_run()
        finished_at = datetime.now().isoformat(timespec="seconds")
        elapsed = max(time.time() - self.stats.started_at, 0.1)
        avg_speed = self.stats.bytes_downloaded / elapsed / (1024 * 1024)
        record = HistoryRecord(
            started_at=datetime.fromtimestamp(self.stats.started_at).isoformat(timespec="seconds"),
            finished_at=finished_at,
            run_label=self.current_run_label,
            total=self.stats.total,
            success=self.stats.success,
            skipped=self.stats.skipped,
            failed=self.stats.failed,
            missing=self.stats.missing,
            retries=self.stats.retries,
            bytes_downloaded=self.stats.bytes_downloaded,
            avg_speed_mbps=avg_speed,
            notes="cancelled" if self.cancel_event.is_set() else f"errors={errors}; tuning={self.tuning_profile.reason}",
        )
        if self.run_context.run_id is not None:
            self.storage.record_run_finish(self.run_context.run_id, record, self.settings_snapshot)
        self._emit_runtime(force=True)
        self.app.emit_event("run_finished", {"cancelled": self.cancel_event.is_set(), "errors": errors})

    def _apply_result(self, result: DownloadResult):
        self.stats.processed += 1
        self.stats.queued_left = max(self.stats.total - self.stats.processed, 0)
        if result.result == "success":
            self.stats.success += 1
        elif result.result == "skipped":
            self.stats.skipped += 1
        elif result.result == "missing":
            self.stats.failed += 1
            self.stats.missing += 1
        else:
            self.stats.failed += 1

        self.run_context.status_cache[result.video_id] = result.result
        self.remaining_ids.discard(result.video_id)
        self.result_batch.append(
            (
                self.run_context.run_id or 0,
                result.video_id,
                result.result,
                result.status,
                result.size_mb,
                result.retries,
                result.http_status,
                result.content_type,
                self._now(),
            )
        )
        if len(self.result_batch) >= self.batch_size:
            self._flush_result_batch()

        self._emit_runtime(force=True)

    def _flush_result_batch(self):
        if self.run_context.run_id is None or not self.result_batch:
            return
        self.storage.record_items(self.result_batch)
        self.storage.update_active_run_ids([video_id for video_id in self.job_ids if video_id in self.remaining_ids])
        self.result_batch.clear()

    def _emit_runtime(self, force: bool = False):
        now = time.monotonic()
        if not force and (now - self.last_runtime_emit) < RUNTIME_EMIT_INTERVAL:
            return
        self.last_runtime_emit = now
        self.app.emit_event("runtime", asdict(self.stats))

    async def _wait_if_paused(self):
        while not self.pause_event.is_set():
            if self.cancel_event.is_set():
                return
            await asyncio.sleep(0.2)

    def _build_tuning_profile(self, job: DownloadJob) -> TuningProfile:
        workers = max(1, job.workers)
        threads = max(1, job.threads)
        chunk_kb = max(64, job.chunk_size // 1024)
        total = len(job.ids)
        preset = (job.performance_preset or "balanced").lower()

        preset_targets = {
            "safe": (4, 1, 512),
            "balanced": (8, 2, 1024),
            "fast": (10, 3, 1024),
            "turbo": (12, 4, 2048),
        }
        target_workers, target_threads, target_chunk = preset_targets.get(preset, preset_targets["balanced"])

        if job.auto_tune:
            max_workers = 12 if not self.proxy_pool else 16
            max_lanes = 24 if not self.proxy_pool else 48
            workers = min(max(workers, target_workers), max(2, min(total or 1, max_workers)))
            threads = min(max(threads, target_threads), max(1, max_lanes // max(workers, 1)))
            threads = min(threads, 4 if not self.proxy_pool else 6)
            if total <= 20:
                workers = min(workers, 6)
            if total <= 5:
                threads = min(threads, 2)
            chunk_kb = max(chunk_kb, target_chunk)
            reason = f"{preset} preset + auto tune -> {workers} files, {threads} lanes, {chunk_kb} KB blocks"
        else:
            reason = f"manual setup -> {workers} files, {threads} lanes, {chunk_kb} KB blocks"
        return TuningProfile(workers=workers, threads=threads, chunk_kb=chunk_kb, reason=reason)

    def _pick_proxy(self):
        now = time.time()
        ready = [proxy for proxy in self.proxy_pool if proxy.cooldown_until <= now]
        if not ready:
            return None
        ready.sort(key=lambda item: item.score, reverse=True)
        raw = ready[0].raw
        if not raw.startswith(("http://", "https://", "socks5://")):
            raw = f"http://{raw}"
        return raw

    def _report_proxy(self, proxy, success: bool):
        if not proxy:
            return
        raw = proxy.replace("http://", "").replace("https://", "").replace("socks5://", "")
        for endpoint in self.proxy_pool:
            if endpoint.raw == raw:
                if success:
                    endpoint.successes += 1
                else:
                    endpoint.failures += 1
                    endpoint.cooldown_until = time.time() + min(60, 5 + endpoint.failures * 4)
                break

    def _note_retry(self, proxy):
        self.stats.retries += 1
        self._emit_runtime()

    def _validate_download(self, path: Path, expected_size: int, content_type: str):
        if expected_size and path.stat().st_size != expected_size:
            raise ValueError(f"size mismatch for {path.name}")
        if content_type and "html" in content_type.lower():
            raise ValueError(f"unexpected content type {content_type}")
        if path.suffix.lower() == ".mp4" and path.stat().st_size >= 8:
            with path.open("rb") as file_obj:
                header = file_obj.read(32)
            if b"ftyp" not in header:
                raise ValueError("missing mp4 header")

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

    def _backoff_delay(self, attempt: int, status_code: int) -> float:
        base = 0.35 + (attempt * 0.45)
        if status_code == 429:
            base += 0.75
        return base + random.uniform(0.05, 0.2)

    def _short_error(self, exc):
        text = str(exc).strip() or exc.__class__.__name__
        return text[:MAX_INSPECTOR_NOTE]

    def _now(self) -> str:
        return datetime.now().isoformat(timespec="seconds")
