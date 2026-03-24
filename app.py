import hashlib
import queue
import re
import time
from pathlib import Path
from tkinter import filedialog, ttk
from urllib.parse import urlparse

import customtkinter as ctk

from engine import DownloadEngine
from models import AppSettings, DownloadJob
from storage import AppStorage


ctk.set_appearance_mode("Dark")
ctk.set_default_color_theme("blue")

UI_POLL_MS = 100
MAX_INSPECTOR_LINES = 500
MAX_EVENTS_PER_TICK = 200
PROFILE_LABELS = {
    "max_size_mb": "Largest file to keep (MB)",
    "workers": "Files at the same time",
    "threads": "Speed boost per file",
    "chunk_kb": "Data block size (KB)",
    "timeout_seconds": "Wait time before retry (sec)",
}
PRESET_DEFAULTS = {
    "safe": {"workers": 4, "threads": 1, "chunk_kb": 512, "timeout_seconds": 25},
    "balanced": {"workers": 8, "threads": 2, "chunk_kb": 1024, "timeout_seconds": 20},
    "fast": {"workers": 10, "threads": 3, "chunk_kb": 1024, "timeout_seconds": 18},
    "turbo": {"workers": 12, "threads": 4, "chunk_kb": 2048, "timeout_seconds": 15},
}


class ProventureStudio(ctk.CTk):
    def __init__(self):
        super().__init__()
        self.title("Downloader Studio Pro")
        self.geometry("1280x760")
        self.minsize(1024, 680)
        self.grid_columnconfigure(0, weight=1)
        self.grid_rowconfigure(1, weight=1)

        self.root_dir = Path(__file__).resolve().parent
        self.storage = AppStorage(self.root_dir)
        self.settings = self.storage.load_settings()
        self.engine = DownloadEngine(self, self.storage)
        self.event_queue = queue.Queue()

        self.proxies = []
        self.active = False
        self.is_paused = False
        self.current_stats = {}
        self.inspector_lines = []
        self.pending_resume_ids = None

        self.setup_ui()
        self.apply_settings_to_form()
        self.refresh_history()
        self.refresh_resume_button()
        self.after(UI_POLL_MS, self.process_ui_events)

    def setup_ui(self):
        header = ctk.CTkFrame(self, fg_color="transparent")
        header.grid(row=0, column=0, padx=18, pady=(14, 6), sticky="ew")
        header.grid_columnconfigure(0, weight=1)

        ctk.CTkLabel(header, text="Downloader Studio Pro", font=ctk.CTkFont(size=24, weight="bold")).grid(row=0, column=0, sticky="w")
        ctk.CTkLabel(
            header,
            text="Preset-driven speed, safer recovery, live diagnostics, searchable history, and better restart support.",
            text_color="#93a2b7",
            font=ctk.CTkFont(size=12),
        ).grid(row=1, column=0, sticky="w", pady=(2, 0))

        top_actions = ctk.CTkFrame(header, fg_color="transparent")
        top_actions.grid(row=0, column=1, rowspan=2, sticky="e")
        ctk.CTkButton(top_actions, text="Load Proxies", command=self.load_proxies, width=110, height=34).pack(side="left", padx=(0, 8))
        ctk.CTkButton(top_actions, text="Output Folder", command=self.select_output_dir, width=110, height=34, fg_color="transparent", border_width=1).pack(side="left", padx=(0, 8))
        self.resume_btn = ctk.CTkButton(
            top_actions,
            text="Resume Last Run",
            command=self.resume_last_run,
            width=130,
            height=34,
            fg_color="#345b7b",
            hover_color="#24425a",
        )
        self.resume_btn.pack(side="left")

        self.tabs = ctk.CTkTabview(self, segmented_button_selected_color="#c3423f")
        self.tabs.grid(row=1, column=0, padx=18, pady=(0, 8), sticky="nsew")
        self.tabs.add("Control Center")
        self.tabs.add("Run History")

        self.setup_control_tab()
        self.setup_history_tab()

        self.run_status = ctk.CTkLabel(self, text="Idle", text_color="#93a2b7")
        self.run_status.grid(row=2, column=0, padx=22, pady=(0, 10), sticky="w")

    def setup_control_tab(self):
        tab = self.tabs.tab("Control Center")
        tab.grid_columnconfigure(0, weight=1)
        tab.grid_columnconfigure(1, weight=1)
        tab.grid_rowconfigure(1, weight=1)

        config_left = ctk.CTkFrame(tab, corner_radius=14)
        config_left.grid(row=0, column=0, padx=(0, 8), pady=(8, 6), sticky="new")
        config_left.grid_columnconfigure(1, weight=1)
        config_left.grid_columnconfigure(2, weight=0)

        ctk.CTkLabel(config_left, text="Target Builder", font=ctk.CTkFont(size=16, weight="bold")).grid(row=0, column=0, columnspan=3, padx=16, pady=(14, 8), sticky="w")
        self.link_entry = self._labeled_entry(config_left, 1, "URL / Template")
        self.start_entry = self._labeled_entry(config_left, 2, "Start ID")
        self.end_entry = self._labeled_entry(config_left, 3, "End ID")
        self.step_entry = self._labeled_entry(config_left, 4, "Step")

        ctk.CTkButton(config_left, text="Extract ID", command=self.extract_id, width=104, height=34).grid(row=1, column=2, padx=(8, 16), pady=6)
        self.range_mode = ctk.CTkOptionMenu(config_left, values=["sequence", "reverse"], command=self.on_range_change, height=34)
        self.range_mode.grid(row=5, column=1, padx=(0, 16), pady=6, sticky="w")
        ctk.CTkLabel(config_left, text="Range Order").grid(row=5, column=0, padx=16, pady=6, sticky="w")
        self.preview_label = ctk.CTkLabel(config_left, text="Ready", text_color="#93a2b7")
        self.preview_label.grid(row=5, column=2, padx=16, pady=6, sticky="e")

        config_right = ctk.CTkFrame(tab, corner_radius=14, fg_color="#2c191d")
        config_right.grid(row=0, column=1, padx=(8, 0), pady=(8, 6), sticky="new")
        config_right.grid_columnconfigure(1, weight=1)
        config_right.grid_columnconfigure(2, weight=0)

        ctk.CTkLabel(config_right, text="Download Setup", font=ctk.CTkFont(size=16, weight="bold"), text_color="#ff9d9d").grid(
            row=0, column=0, columnspan=3, padx=16, pady=(14, 8), sticky="w"
        )
        ctk.CTkLabel(config_right, text="Preset").grid(row=1, column=0, padx=16, pady=6, sticky="w")
        self.preset_menu = ctk.CTkOptionMenu(config_right, values=["safe", "balanced", "fast", "turbo"], command=self.on_preset_change, height=34)
        self.preset_menu.grid(row=1, column=1, padx=(0, 10), pady=6, sticky="ew")
        self.apply_preset_btn = ctk.CTkButton(config_right, text="Apply Preset", command=self.apply_selected_preset, width=104, height=34)
        self.apply_preset_btn.grid(row=1, column=2, padx=(0, 16), pady=6, sticky="e")
        self.profile_summary = ctk.CTkLabel(config_right, text="Balanced everyday setup.", text_color="#f5c2c2")
        self.profile_summary.grid(row=2, column=0, columnspan=3, padx=16, pady=(0, 6), sticky="w")
        self.size_entry = self._labeled_entry(config_right, 3, PROFILE_LABELS["max_size_mb"])
        self.workers_entry = self._labeled_entry(config_right, 4, PROFILE_LABELS["workers"])
        self.threads_entry = self._labeled_entry(config_right, 5, PROFILE_LABELS["threads"])
        self.chunk_entry = self._labeled_entry(config_right, 6, PROFILE_LABELS["chunk_kb"])
        self.timeout_entry = self._labeled_entry(config_right, 7, PROFILE_LABELS["timeout_seconds"])

        dashboard = ctk.CTkFrame(tab, corner_radius=14, fg_color="#191f24")
        dashboard.grid(row=1, column=0, columnspan=2, pady=(2, 8), sticky="nsew")
        dashboard.grid_columnconfigure(0, weight=1)

        actions = ctk.CTkFrame(dashboard, fg_color="transparent")
        actions.grid(row=0, column=0, padx=16, pady=(14, 8), sticky="ew")
        actions.grid_columnconfigure((0, 1, 2), weight=1)
        self.start_btn = ctk.CTkButton(actions, text="Launch Run", command=self.start_process, height=42, fg_color="#d13b35", hover_color="#9f2622")
        self.start_btn.grid(row=0, column=0, padx=(0, 8), sticky="ew")
        self.pause_btn = ctk.CTkButton(actions, text="Pause", command=self.toggle_pause, state="disabled", height=42)
        self.pause_btn.grid(row=0, column=1, padx=8, sticky="ew")
        self.cancel_btn = ctk.CTkButton(actions, text="Cancel", command=self.cancel_process, state="disabled", height=42, fg_color="transparent", border_width=1)
        self.cancel_btn.grid(row=0, column=2, padx=(8, 0), sticky="ew")

        stats = ctk.CTkFrame(dashboard, fg_color="transparent")
        stats.grid(row=1, column=0, padx=16, pady=(0, 6), sticky="ew")
        stats.grid_columnconfigure((0, 1, 2, 3, 4, 5, 6, 7, 8), weight=1)
        self.labels = {
            "queue": ctk.CTkLabel(stats, text="Queue: 0"),
            "done": ctk.CTkLabel(stats, text="Done: 0", text_color="#4ade80"),
            "skipped": ctk.CTkLabel(stats, text="Skipped: 0", text_color="#cbd5e1"),
            "missing": ctk.CTkLabel(stats, text="Missing: 0", text_color="#fca5a5"),
            "failed": ctk.CTkLabel(stats, text="Failed: 0", text_color="#f87171"),
            "speed": ctk.CTkLabel(stats, text="Speed: 0 MB/s", text_color="#7dd3fc"),
            "retries": ctk.CTkLabel(stats, text="Retries: 0", text_color="#fbbf24"),
            "active": ctk.CTkLabel(stats, text="Working: 0", text_color="#f5d17a"),
            "profile": ctk.CTkLabel(stats, text="Mode: --", text_color="#b8b8ff"),
        }
        for idx, key in enumerate(("queue", "done", "skipped", "missing", "failed", "speed", "retries", "active", "profile")):
            self.labels[key].grid(row=0, column=idx, sticky="w" if idx == 0 else "e")

        progress = ctk.CTkFrame(dashboard, fg_color="transparent")
        progress.grid(row=2, column=0, padx=16, pady=(0, 6), sticky="new")
        progress.grid_columnconfigure(0, weight=1)
        self.progress_label = ctk.CTkLabel(progress, text="Progress: 0.0% (0 / 0)")
        self.progress_label.grid(row=0, column=0, sticky="w")
        self.eta_label = ctk.CTkLabel(progress, text="ETA: --", text_color="#93a2b7")
        self.eta_label.grid(row=0, column=1, sticky="e")
        self.progress_bar = ctk.CTkProgressBar(progress, height=18, progress_color="#d13b35", fg_color="#3e4650")
        self.progress_bar.grid(row=1, column=0, columnspan=2, sticky="ew", pady=(8, 14))
        self.progress_bar.set(0)

        for entry in (self.link_entry, self.start_entry, self.end_entry, self.step_entry):
            entry.bind("<KeyRelease>", self.on_manual_input_change)

    def setup_history_tab(self):
        tab = self.tabs.tab("Run History")
        tab.grid_columnconfigure(0, weight=1)
        tab.grid_rowconfigure(1, weight=1)
        tab.grid_rowconfigure(2, weight=1)

        tools = ctk.CTkFrame(tab, fg_color="transparent")
        tools.grid(row=0, column=0, sticky="ew", pady=(10, 8))
        tools.grid_columnconfigure(1, weight=1)
        ctk.CTkButton(tools, text="Refresh History", command=self.refresh_history, width=120).grid(row=0, column=0, padx=(0, 10))
        self.history_search = ctk.CTkEntry(tools, placeholder_text="Search by run label, notes, or settings")
        self.history_search.grid(row=0, column=1, sticky="ew", padx=(0, 10))
        ctk.CTkButton(tools, text="Search", command=self.refresh_history, width=90).grid(row=0, column=2)

        self.history_tree = ttk.Treeview(tab, columns=("Started", "Label", "Total", "Done", "Skipped", "Failed", "Speed"), show="headings")
        self.history_tree.grid(row=1, column=0, sticky="nsew")
        self.history_tree.bind("<<TreeviewSelect>>", self.show_history_detail)
        for title, width in (("Started", 180), ("Label", 340), ("Total", 80), ("Done", 80), ("Skipped", 80), ("Failed", 80), ("Speed", 110)):
            self.history_tree.heading(title, text=title)
            self.history_tree.column(title, width=width, anchor="center" if title != "Label" else "w")
        self.history_detail = ctk.CTkTextbox(tab, height=220)
        self.history_detail.grid(row=2, column=0, sticky="nsew", pady=(10, 0))
        self.history_detail.insert("1.0", "Select a run to inspect its recent items.\n")

    def _labeled_entry(self, parent, row, label):
        ctk.CTkLabel(parent, text=label).grid(row=row, column=0, padx=16, pady=6, sticky="w")
        entry = ctk.CTkEntry(parent, height=34)
        entry.grid(row=row, column=1, padx=(0, 16), pady=6, sticky="ew")
        return entry

    def _set_entry(self, entry, value):
        entry.delete(0, "end")
        entry.insert(0, str(value))

    def apply_settings_to_form(self):
        self._set_entry(self.link_entry, self.settings.base_url)
        self._set_entry(self.start_entry, self.settings.start_id)
        self._set_entry(self.end_entry, self.settings.end_id)
        self._set_entry(self.step_entry, self.settings.step)
        self._set_entry(self.size_entry, self.settings.max_size_mb)
        self._set_entry(self.workers_entry, self.settings.workers)
        self._set_entry(self.threads_entry, self.settings.threads)
        self._set_entry(self.chunk_entry, self.settings.chunk_kb)
        self._set_entry(self.timeout_entry, self.settings.timeout_seconds)
        self.range_mode.set(self.settings.range_mode)
        self.preset_menu.set(self.settings.performance_preset)
        self.update_profile_summary()
        self.update_preview()
        self.log_inspector(f"Output directory: {self.settings.output_dir}")

    def collect_settings(self) -> AppSettings:
        return AppSettings(
            base_url=self.link_entry.get().strip(),
            output_dir=self.settings.output_dir,
            start_id=int(self.start_entry.get().strip()),
            end_id=int(self.end_entry.get().strip()),
            step=int(self.step_entry.get().strip() or "1"),
            range_mode=self.range_mode.get(),
            performance_preset=self.preset_menu.get(),
            max_size_mb=float(self.size_entry.get().strip() or "200"),
            workers=int(self.workers_entry.get().strip() or "12"),
            threads=int(self.threads_entry.get().strip() or "6"),
            chunk_kb=int(self.chunk_entry.get().strip() or "1024"),
            auto_tune=True,
            verify_downloads=True,
            skip_known_missing=False,
            retry_failed_only=False,
            silent_mode=False,
            cleanup_before_run=False,
            unsafe_ssl=True,
            timeout_seconds=int(self.timeout_entry.get().strip() or "20"),
            history_limit=max(10, self.settings.history_limit),
        )

    def build_job(self, settings: AppSettings) -> DownloadJob:
        if settings.step <= 0:
            raise ValueError("Step must be greater than zero.")
        if settings.start_id > settings.end_id:
            raise ValueError("Start ID must be less than or equal to End ID.")
        if settings.max_size_mb <= 0:
            raise ValueError(f"{PROFILE_LABELS['max_size_mb']} must be greater than zero.")
        if settings.workers < 1:
            raise ValueError(f"{PROFILE_LABELS['workers']} must be at least 1.")
        if settings.threads < 1:
            raise ValueError(f"{PROFILE_LABELS['threads']} must be at least 1.")
        if settings.chunk_kb < 64:
            raise ValueError(f"{PROFILE_LABELS['chunk_kb']} must be at least 64.")
        if settings.timeout_seconds < 5:
            raise ValueError(f"{PROFILE_LABELS['timeout_seconds']} must be at least 5.")

        base_url = settings.base_url
        if "{}" not in base_url:
            if re.search(r"id/\d+\.mp4", base_url):
                base_url = re.sub(r"id/\d+\.mp4", "id/{}.mp4", base_url)
            else:
                raise ValueError("URL must contain a numeric ID or a {} placeholder.")

        if self.pending_resume_ids:
            ids = list(self.pending_resume_ids)
        else:
            ids = list(range(settings.start_id, settings.end_id + 1, settings.step))
            if settings.range_mode == "reverse":
                ids.reverse()
        return DownloadJob(
            ids=ids,
            base_url=base_url,
            save_dir=settings.output_dir,
            max_bytes=int(settings.max_size_mb * 1024 * 1024),
            workers=settings.workers,
            threads=settings.threads,
            chunk_size=settings.chunk_kb * 1024,
            timeout_seconds=settings.timeout_seconds,
            performance_preset=settings.performance_preset,
            auto_tune=settings.auto_tune,
            verify_downloads=settings.verify_downloads,
            skip_known_missing=settings.skip_known_missing,
            retry_failed_only=settings.retry_failed_only,
            silent_mode=settings.silent_mode,
            clean_before_run=settings.cleanup_before_run,
            unsafe_ssl=settings.unsafe_ssl,
            run_label=f"{self._source_key(base_url)}:{ids[0]}-{ids[-1]}" if ids else f"{self._source_key(base_url)}:empty",
        )

    def on_preset_change(self, _value):
        self.update_profile_summary()

    def apply_selected_preset(self):
        preset = PRESET_DEFAULTS.get(self.preset_menu.get(), PRESET_DEFAULTS["balanced"])
        self._set_entry(self.workers_entry, preset["workers"])
        self._set_entry(self.threads_entry, preset["threads"])
        self._set_entry(self.chunk_entry, preset["chunk_kb"])
        self._set_entry(self.timeout_entry, preset["timeout_seconds"])
        self.update_profile_summary()
        self.run_status.configure(text=f"Applied {self.preset_menu.get()} preset.", text_color="#93c5fd")

    def update_profile_summary(self):
        preset = self.preset_menu.get() if hasattr(self, "preset_menu") else "balanced"
        summaries = {
            "safe": "Low pressure and high stability for fragile sources.",
            "balanced": "Good default for most runs.",
            "fast": "Higher speed with moderate extra load.",
            "turbo": "High speed mode. Best for stronger sources.",
        }
        auto_tune = True
        extra = " Auto tune on." if auto_tune else " Manual values in use."
        if hasattr(self, "profile_summary"):
            self.profile_summary.configure(text=summaries.get(preset, summaries["balanced"]) + extra)

    def start_process(self):
        try:
            self.settings = self.collect_settings()
            job = self.build_job(self.settings)
        except ValueError as exc:
            self.run_status.configure(text=str(exc), text_color="#fbbf24")
            self.log_inspector(f"Validation error: {exc}")
            return
        except Exception as exc:
            self.run_status.configure(text=f"Failed to prepare run: {exc}", text_color="#f87171")
            self.log_inspector(f"Unexpected setup error: {exc}")
            return

        self.storage.save_settings(self.settings)
        if self.settings.cleanup_before_run:
            self.cleanup_stale_partials()

        self.active = True
        self.is_paused = False
        self.current_stats = {
            "total": len(job.ids),
            "processed": 0,
            "success": 0,
            "skipped": 0,
            "failed": 0,
            "missing": 0,
            "retries": 0,
            "bytes_downloaded": 0,
            "started_at": time.time(),
            "active_files": 0,
            "queued_left": len(job.ids),
            "current_workers": job.workers,
            "current_threads": job.threads,
            "chunk_kb": job.chunk_size // 1024,
        }
        self.start_btn.configure(state="disabled")
        self.pause_btn.configure(state="normal", text="Pause")
        self.cancel_btn.configure(state="normal")
        self.labels["queue"].configure(text=f"Queue: {len(job.ids)}")
        self.labels["profile"].configure(text=f"Mode: {self.settings.performance_preset}")
        self.progress_bar.set(0)
        self.progress_label.configure(text=f"Progress: 0.0% (0 / {len(job.ids)})")
        self.eta_label.configure(text="ETA: --")
        self.run_status.configure(text="Run started.", text_color="#7dd3fc")
        self.log_inspector(
            f"Run started with {len(job.ids)} IDs, preset {job.performance_preset}, auto tune {'on' if job.auto_tune else 'off'}."
        )
        self.engine.start(job, self.settings)
        self.pending_resume_ids = None
        self.refresh_resume_button()

    def resume_last_run(self):
        active_run = self.storage.load_active_run()
        if not active_run or not active_run.ids:
            self.run_status.configure(text="No interrupted run found.", text_color="#fbbf24")
            return
        settings = AppSettings()
        for key, value in active_run.settings.items():
            if hasattr(settings, key):
                setattr(settings, key, value)
        settings.start_id = min(active_run.ids)
        settings.end_id = max(active_run.ids)
        settings.range_mode = "sequence"
        self.settings = settings
        self.pending_resume_ids = list(active_run.ids)
        self.apply_settings_to_form()
        self.run_status.configure(text="Recovered the last interrupted setup. Launch when ready.", text_color="#93c5fd")
        self.log_inspector(f"Recovered last run with {len(active_run.ids)} queued IDs.")

    def refresh_resume_button(self):
        state = "normal" if self.storage.load_active_run() else "disabled"
        self.resume_btn.configure(state=state)

    def toggle_pause(self):
        if not self.active:
            return
        self.is_paused = not self.is_paused
        if self.is_paused:
            self.engine.pause()
            self.pause_btn.configure(text="Resume")
            self.run_status.configure(text="Paused.", text_color="#fbbf24")
        else:
            self.engine.resume()
            self.pause_btn.configure(text="Pause")
            self.run_status.configure(text="Resumed.", text_color="#7dd3fc")

    def cancel_process(self):
        if not self.active:
            return
        self.engine.cancel()
        self.run_status.configure(text="Cancelling run...", text_color="#f87171")

    def emit_event(self, event_type, payload):
        self.event_queue.put((event_type, payload))

    def process_ui_events(self):
        processed_count = 0
        runtime_dirty = False
        while True:
            try:
                event_type, payload = self.event_queue.get_nowait()
            except queue.Empty:
                break
            runtime_dirty = self.handle_event(event_type, payload) or runtime_dirty
            processed_count += 1
            if processed_count >= MAX_EVENTS_PER_TICK:
                break
        if runtime_dirty:
            self.refresh_runtime()
        self.after(UI_POLL_MS, self.process_ui_events)

    def handle_event(self, event_type, payload):
        if event_type == "runtime":
            self.current_stats = payload
            return True
        elif event_type == "run_finished":
            self.finish_run(payload["cancelled"], payload["errors"])
            return False
        elif event_type == "diagnostic":
            self.log_inspector(payload.get("message", ""))
            return False
        return False

    def refresh_runtime(self):
        stats = self.current_stats
        total = stats.get("total", 0)
        processed = stats.get("processed", 0)
        success = stats.get("success", 0)
        skipped = stats.get("skipped", 0)
        failed = stats.get("failed", 0)
        missing = stats.get("missing", 0)
        retries = stats.get("retries", 0)
        bytes_downloaded = stats.get("bytes_downloaded", 0)
        started_at = stats.get("started_at", time.time())
        active_files = stats.get("active_files", 0)
        queued_left = stats.get("queued_left", max(total - processed, 0))
        current_workers = stats.get("current_workers", 0)
        current_threads = stats.get("current_threads", 0)
        chunk_kb = stats.get("chunk_kb", 0)

        elapsed = max(time.time() - started_at, 0.1)
        speed = bytes_downloaded / elapsed / (1024 * 1024)
        progress = (processed / total) if total else 0
        eta = ((total - processed) * (elapsed / processed)) if processed else 0

        self.labels["queue"].configure(text=f"Queue: {queued_left}")
        self.labels["done"].configure(text=f"Done: {success}")
        self.labels["skipped"].configure(text=f"Skipped: {skipped}")
        self.labels["missing"].configure(text=f"Missing: {missing}")
        self.labels["failed"].configure(text=f"Failed: {max(failed - missing, 0)}")
        self.labels["speed"].configure(text=f"Speed: {speed:.2f} MB/s")
        self.labels["retries"].configure(text=f"Retries: {retries}")
        self.labels["active"].configure(text=f"Working: {active_files}")
        self.labels["profile"].configure(text=f"Mode: {current_workers}x{current_threads} @ {chunk_kb}KB")
        self.progress_bar.set(progress)
        self.progress_label.configure(text=f"Progress: {progress * 100:.1f}% ({processed} / {total})")
        self.eta_label.configure(text=f"ETA: {self.format_eta(eta) if processed else '--'}")

    def finish_run(self, cancelled, errors):
        self.active = False
        self.start_btn.configure(state="normal")
        self.pause_btn.configure(state="disabled", text="Pause")
        self.cancel_btn.configure(state="disabled")
        self.refresh_resume_button()
        stats = self.current_stats
        success = stats.get("success", 0)
        missing = stats.get("missing", 0)
        failed = max(stats.get("failed", 0) - missing, 0)
        if cancelled:
            text = "Run cancelled."
            color = "#fbbf24"
        elif success == 0 and missing > 0 and failed == 0:
            text = "Run complete, but no downloadable videos were found in this range. Check the URL template or IDs."
            color = "#fbbf24"
        else:
            text = "Run complete."
            color = "#4ade80"
        if errors:
            text += f" Background errors: {errors}."
        self.run_status.configure(text=text, text_color=color)
        self.refresh_history()
        self.log_inspector(text)

    def refresh_history(self):
        self.history_tree.delete(*self.history_tree.get_children())
        for row in self.storage.load_recent_runs(limit=self.settings.history_limit, search=self.history_search.get().strip()):
            self.history_tree.insert(
                "",
                "end",
                values=(row["started_at"], row["run_label"], row["total"], row["success"], row["skipped"], row["failed"], f"{row['avg_speed_mbps']:.2f}"),
            )

    def show_history_detail(self, _event=None):
        selection = self.history_tree.selection()
        if not selection:
            return
        values = self.history_tree.item(selection[0], "values")
        if not values:
            return
        run_label = values[1]
        rows = self.storage.load_run_items(run_label, limit=150)
        lines = [f"Run: {run_label}", ""]
        for row in rows:
            lines.append(
                f"ID {row['video_id']} | {row['result']} | {row['status']} | size={row['size_mb']} | retries={row['retries']}"
            )
        if len(lines) == 2:
            lines.append("No stored item details yet.")
        self.history_detail.delete("1.0", "end")
        self.history_detail.insert("1.0", "\n".join(lines))

    def extract_id(self):
        match = re.search(r"id/(\d+)\.mp4", self.link_entry.get().strip())
        if not match:
            self.run_status.configure(text="Could not extract an ID from the URL.", text_color="#fbbf24")
            return
        video_id = match.group(1)
        self._set_entry(self.start_entry, video_id)
        self._set_entry(self.end_entry, video_id)
        self.pending_resume_ids = None
        self.update_preview()

    def update_preview(self):
        try:
            if self.pending_resume_ids:
                count = len(self.pending_resume_ids)
                self.preview_label.configure(text=f"{count} pending IDs, resume")
            else:
                start_id = int(self.start_entry.get().strip())
                end_id = int(self.end_entry.get().strip())
                step = max(int(self.step_entry.get().strip() or "1"), 1)
                count = len(range(start_id, end_id + 1, step)) if start_id <= end_id else 0
                self.preview_label.configure(text=f"{count} IDs, {self.range_mode.get()}")
            self.update_profile_summary()
        except ValueError:
            self.preview_label.configure(text="Check IDs")

    def on_manual_input_change(self, _event=None):
        self.pending_resume_ids = None
        self.update_preview()

    def on_range_change(self, _value):
        self.pending_resume_ids = None
        self.update_preview()

    def load_proxies(self):
        file_path = filedialog.askopenfilename(filetypes=[("Text files", "*.txt"), ("All files", "*.*")])
        if not file_path:
            return
        try:
            with open(file_path, "r", encoding="utf-8") as file_obj:
                candidates = [line.strip() for line in file_obj if line.strip()]
            self.proxies = [item for item in candidates if ":" in item]
            rejected = len(candidates) - len(self.proxies)
            self.run_status.configure(text=f"Loaded {len(self.proxies)} proxies.", text_color="#93c5fd")
            self.log_inspector(f"Loaded proxies from {file_path}. Rejected {rejected} invalid lines.")
        except OSError as exc:
            self.run_status.configure(text=f"Proxy load failed: {exc}", text_color="#f87171")
            self.log_inspector(f"Proxy load failed: {exc}")

    def select_output_dir(self):
        selected = filedialog.askdirectory(initialdir=str(Path(self.settings.output_dir).resolve()))
        if not selected:
            return
        self.settings.output_dir = selected
        self.run_status.configure(text=f"Output directory set to {selected}", text_color="#93c5fd")
        self.log_inspector(f"Output directory changed to {selected}")

    def cleanup_stale_partials(self):
        output_dir = Path(self.settings.output_dir)
        if not output_dir.exists():
            return
        removed = 0
        for pattern in ("*.part*", "*.tmp"):
            for path in output_dir.glob(pattern):
                try:
                    path.unlink()
                    removed += 1
                except OSError:
                    pass
        if removed:
            self.log_inspector(f"Removed {removed} stale partial files.")

    def log_inspector(self, message):
        if not message:
            return
        timestamp = time.strftime("%H:%M:%S")
        line = f"[{timestamp}] {message}"
        self.inspector_lines.append(line)
        self.inspector_lines = self.inspector_lines[-MAX_INSPECTOR_LINES:]

    def format_eta(self, seconds):
        seconds = int(max(seconds, 0))
        minutes, sec = divmod(seconds, 60)
        hours, minutes = divmod(minutes, 60)
        if hours:
            return f"{hours}h {minutes}m {sec}s"
        if minutes:
            return f"{minutes}m {sec}s"
        return f"{sec}s"

    def _source_key(self, base_url: str) -> str:
        parsed = urlparse(base_url)
        host = (parsed.netloc or "source").lower().replace(".", "_").replace(":", "_")
        digest = hashlib.sha1(base_url.encode("utf-8")).hexdigest()[:8]
        return f"{host}_{digest}"


if __name__ == "__main__":
    app = ProventureStudio()
    app.mainloop()
