import queue
import re
import time
from pathlib import Path
from tkinter import filedialog, ttk

import customtkinter as ctk

from engine import DownloadEngine
from models import AppSettings, DownloadJob
from storage import AppStorage


ctk.set_appearance_mode("Dark")
ctk.set_default_color_theme("blue")

UI_POLL_MS = 120
MAX_VISIBLE_ROWS = 160


class ProventureStudio(ctk.CTk):
    def __init__(self):
        super().__init__()
        self.title("Downloader Studio Pro")
        self.geometry("1260x900")
        self.minsize(1100, 780)
        self.grid_columnconfigure(0, weight=1)
        self.grid_rowconfigure(2, weight=1)

        self.root_dir = Path(__file__).resolve().parent
        self.storage = AppStorage(self.root_dir)
        self.settings = self.storage.load_settings()
        self.engine = DownloadEngine(self, self.storage)
        self.event_queue = queue.Queue()

        self.proxies = []
        self.tree_items = {}
        self.active = False
        self.is_paused = False
        self.current_stats = {}

        self.setup_ui()
        self.apply_settings_to_form()
        self.refresh_history()
        self.after(UI_POLL_MS, self.process_ui_events)

    def setup_ui(self):
        header = ctk.CTkFrame(self, fg_color="transparent")
        header.grid(row=0, column=0, padx=20, pady=(18, 8), sticky="ew")
        header.grid_columnconfigure(0, weight=1)

        ctk.CTkLabel(header, text="Downloader Studio Pro", font=ctk.CTkFont(size=30, weight="bold")).grid(row=0, column=0, sticky="w")
        ctk.CTkLabel(
            header,
            text="Resumable downloads, persistent run history, better diagnostics, and a safer high-speed engine.",
            text_color="#93a2b7",
            font=ctk.CTkFont(size=13),
        ).grid(row=1, column=0, sticky="w", pady=(2, 0))

        top_actions = ctk.CTkFrame(header, fg_color="transparent")
        top_actions.grid(row=0, column=1, rowspan=2, sticky="e")
        ctk.CTkButton(top_actions, text="Load Proxies", command=self.load_proxies, width=120).pack(side="left", padx=(0, 10))
        ctk.CTkButton(top_actions, text="Output Folder", command=self.select_output_dir, width=120, fg_color="transparent", border_width=1).pack(side="left")

        self.tabs = ctk.CTkTabview(self, segmented_button_selected_color="#c3423f")
        self.tabs.grid(row=1, column=0, padx=20, pady=(0, 10), sticky="nsew")
        self.tabs.add("Control Center")
        self.tabs.add("Run History")
        self.tabs.add("Inspector")

        self.setup_control_tab()
        self.setup_history_tab()
        self.setup_inspector_tab()

        self.run_status = ctk.CTkLabel(self, text="Idle", text_color="#93a2b7")
        self.run_status.grid(row=3, column=0, padx=24, pady=(0, 14), sticky="w")

    def setup_control_tab(self):
        tab = self.tabs.tab("Control Center")
        tab.grid_columnconfigure(0, weight=1)
        tab.grid_columnconfigure(1, weight=1)
        tab.grid_rowconfigure(1, weight=1)

        config_left = ctk.CTkFrame(tab, corner_radius=14)
        config_left.grid(row=0, column=0, padx=(0, 10), pady=10, sticky="nsew")
        config_left.grid_columnconfigure(1, weight=1)

        ctk.CTkLabel(config_left, text="Target Builder", font=ctk.CTkFont(size=17, weight="bold")).grid(row=0, column=0, columnspan=2, padx=18, pady=(16, 12), sticky="w")
        self.link_entry = self._labeled_entry(config_left, 1, "URL / Template")
        self.start_entry = self._labeled_entry(config_left, 2, "Start ID")
        self.end_entry = self._labeled_entry(config_left, 3, "End ID")
        self.step_entry = self._labeled_entry(config_left, 4, "Step")

        ctk.CTkButton(config_left, text="Extract ID", command=self.extract_id, width=110).grid(row=1, column=2, padx=(10, 18), pady=8)
        self.range_mode = ctk.CTkOptionMenu(config_left, values=["sequence", "reverse"], command=lambda _v: self.update_preview())
        self.range_mode.grid(row=5, column=1, padx=18, pady=8, sticky="w")
        ctk.CTkLabel(config_left, text="Range Mode").grid(row=5, column=0, padx=18, pady=8, sticky="w")
        self.preview_label = ctk.CTkLabel(config_left, text="Ready", text_color="#93a2b7")
        self.preview_label.grid(row=5, column=2, padx=18, pady=8, sticky="e")

        config_right = ctk.CTkFrame(tab, corner_radius=14, fg_color="#2c191d")
        config_right.grid(row=0, column=1, padx=(10, 0), pady=10, sticky="nsew")
        config_right.grid_columnconfigure(1, weight=1)

        ctk.CTkLabel(config_right, text="Performance Profile", font=ctk.CTkFont(size=17, weight="bold"), text_color="#ff9d9d").grid(
            row=0, column=0, columnspan=2, padx=18, pady=(16, 12), sticky="w"
        )
        self.size_entry = self._labeled_entry(config_right, 1, "Max Size (MB)")
        self.workers_entry = self._labeled_entry(config_right, 2, "Connections")
        self.threads_entry = self._labeled_entry(config_right, 3, "Threads / File")
        self.chunk_entry = self._labeled_entry(config_right, 4, "Chunk KB")
        self.timeout_entry = self._labeled_entry(config_right, 5, "Timeout Sec")
        self.silent_var = ctk.BooleanVar(value=False)
        self.cleanup_var = ctk.BooleanVar(value=True)
        ctk.CTkCheckBox(config_right, text="Silent UI mode", variable=self.silent_var).grid(row=6, column=0, padx=18, pady=(8, 16), sticky="w")
        ctk.CTkCheckBox(config_right, text="Clean stale partials", variable=self.cleanup_var).grid(row=6, column=1, padx=18, pady=(8, 16), sticky="e")

        dashboard = ctk.CTkFrame(tab, corner_radius=14, fg_color="#191f24")
        dashboard.grid(row=1, column=0, columnspan=2, pady=(4, 10), sticky="nsew")
        dashboard.grid_columnconfigure(0, weight=1)
        dashboard.grid_rowconfigure(2, weight=1)

        actions = ctk.CTkFrame(dashboard, fg_color="transparent")
        actions.grid(row=0, column=0, padx=18, pady=(16, 10), sticky="ew")
        actions.grid_columnconfigure((0, 1, 2), weight=1)
        self.start_btn = ctk.CTkButton(actions, text="Launch Run", command=self.start_process, height=46, fg_color="#d13b35", hover_color="#9f2622")
        self.start_btn.grid(row=0, column=0, padx=(0, 10), sticky="ew")
        self.pause_btn = ctk.CTkButton(actions, text="Pause", command=self.toggle_pause, state="disabled", height=46)
        self.pause_btn.grid(row=0, column=1, padx=10, sticky="ew")
        self.cancel_btn = ctk.CTkButton(actions, text="Cancel", command=self.cancel_process, state="disabled", height=46, fg_color="transparent", border_width=1)
        self.cancel_btn.grid(row=0, column=2, padx=(10, 0), sticky="ew")

        stats = ctk.CTkFrame(dashboard, fg_color="transparent")
        stats.grid(row=1, column=0, padx=18, pady=(0, 8), sticky="ew")
        stats.grid_columnconfigure((0, 1, 2, 3, 4, 5), weight=1)
        self.labels = {
            "queue": ctk.CTkLabel(stats, text="Queue: 0"),
            "done": ctk.CTkLabel(stats, text="Done: 0", text_color="#4ade80"),
            "skipped": ctk.CTkLabel(stats, text="Skipped: 0", text_color="#cbd5e1"),
            "failed": ctk.CTkLabel(stats, text="Failed: 0", text_color="#f87171"),
            "speed": ctk.CTkLabel(stats, text="Speed: 0 MB/s", text_color="#7dd3fc"),
            "retries": ctk.CTkLabel(stats, text="Retries: 0", text_color="#fbbf24"),
        }
        for idx, key in enumerate(("queue", "done", "skipped", "failed", "speed", "retries")):
            self.labels[key].grid(row=0, column=idx, sticky="w" if idx == 0 else "e")

        progress = ctk.CTkFrame(dashboard, fg_color="transparent")
        progress.grid(row=2, column=0, padx=18, pady=(0, 8), sticky="new")
        progress.grid_columnconfigure(0, weight=1)
        self.progress_label = ctk.CTkLabel(progress, text="Progress: 0.0% (0 / 0)")
        self.progress_label.grid(row=0, column=0, sticky="w")
        self.eta_label = ctk.CTkLabel(progress, text="ETA: --", text_color="#93a2b7")
        self.eta_label.grid(row=0, column=1, sticky="e")
        self.progress_bar = ctk.CTkProgressBar(progress, height=13, progress_color="#d13b35")
        self.progress_bar.grid(row=1, column=0, columnspan=2, sticky="ew", pady=(6, 10))
        self.progress_bar.set(0)

        self.tree = ttk.Treeview(dashboard, columns=("ID", "Status", "Size", "Retries"), show="headings")
        self.tree.grid(row=3, column=0, sticky="nsew", padx=18, pady=(0, 18))
        for title, width, anchor in (("ID", 120, "center"), ("Status", 700, "w"), ("Size", 110, "center"), ("Retries", 90, "center")):
            self.tree.heading(title, text=title)
            self.tree.column(title, width=width, anchor=anchor)
        dashboard.grid_rowconfigure(3, weight=1)

        for entry in (self.start_entry, self.end_entry, self.step_entry):
            entry.bind("<KeyRelease>", lambda _event: self.update_preview())

    def setup_history_tab(self):
        tab = self.tabs.tab("Run History")
        tab.grid_columnconfigure(0, weight=1)
        tab.grid_rowconfigure(1, weight=1)

        tools = ctk.CTkFrame(tab, fg_color="transparent")
        tools.grid(row=0, column=0, sticky="ew", pady=(10, 8))
        ctk.CTkButton(tools, text="Refresh History", command=self.refresh_history, width=120).pack(side="left")

        self.history_tree = ttk.Treeview(tab, columns=("Started", "Label", "Total", "Done", "Skipped", "Failed", "Speed"), show="headings")
        self.history_tree.grid(row=1, column=0, sticky="nsew")
        for title, width in (("Started", 180), ("Label", 340), ("Total", 80), ("Done", 80), ("Skipped", 80), ("Failed", 80), ("Speed", 110)):
            self.history_tree.heading(title, text=title)
            self.history_tree.column(title, width=width, anchor="center" if title != "Label" else "w")

    def setup_inspector_tab(self):
        tab = self.tabs.tab("Inspector")
        tab.grid_columnconfigure(0, weight=1)
        tab.grid_rowconfigure(0, weight=1)
        self.inspector = ctk.CTkTextbox(tab)
        self.inspector.grid(row=0, column=0, sticky="nsew", pady=10)
        self.inspector.insert("1.0", "Inspector ready.\n")

    def _labeled_entry(self, parent, row, label):
        ctk.CTkLabel(parent, text=label).grid(row=row, column=0, padx=18, pady=8, sticky="w")
        entry = ctk.CTkEntry(parent)
        entry.grid(row=row, column=1, padx=(0, 18), pady=8, sticky="ew")
        return entry

    def apply_settings_to_form(self):
        self.link_entry.insert(0, self.settings.base_url)
        self.start_entry.insert(0, str(self.settings.start_id))
        self.end_entry.insert(0, str(self.settings.end_id))
        self.step_entry.insert(0, str(self.settings.step))
        self.size_entry.insert(0, str(self.settings.max_size_mb))
        self.workers_entry.insert(0, str(self.settings.workers))
        self.threads_entry.insert(0, str(self.settings.threads))
        self.chunk_entry.insert(0, str(self.settings.chunk_kb))
        self.timeout_entry.insert(0, str(self.settings.timeout_seconds))
        self.range_mode.set(self.settings.range_mode)
        self.silent_var.set(self.settings.silent_mode)
        self.cleanup_var.set(self.settings.cleanup_before_run)
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
            max_size_mb=float(self.size_entry.get().strip() or "200"),
            workers=int(self.workers_entry.get().strip() or "12"),
            threads=int(self.threads_entry.get().strip() or "6"),
            chunk_kb=int(self.chunk_entry.get().strip() or "1024"),
            silent_mode=self.silent_var.get(),
            cleanup_before_run=self.cleanup_var.get(),
            timeout_seconds=int(self.timeout_entry.get().strip() or "20"),
        )

    def build_job(self, settings: AppSettings) -> DownloadJob:
        if settings.step <= 0:
            raise ValueError("Step must be greater than zero.")
        if settings.start_id > settings.end_id:
            raise ValueError("Start ID must be less than or equal to End ID.")
        if settings.workers < 1 or settings.threads < 1 or settings.chunk_kb < 64:
            raise ValueError("Connections, threads, and chunk KB must be positive.")

        base_url = settings.base_url
        if "{}" not in base_url:
            if re.search(r"id/\d+\.mp4", base_url):
                base_url = re.sub(r"id/\d+\.mp4", "id/{}.mp4", base_url)
            else:
                raise ValueError("URL must contain a numeric ID or a {} placeholder.")

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
            silent_mode=settings.silent_mode,
            clean_before_run=settings.cleanup_before_run,
            run_label=f"{ids[0]}-{ids[-1]}" if ids else "empty",
        )

    def start_process(self):
        try:
            self.settings = self.collect_settings()
            job = self.build_job(self.settings)
        except ValueError as exc:
            self.run_status.configure(text=str(exc), text_color="#fbbf24")
            self.log_inspector(f"Validation error: {exc}")
            return

        self.storage.save_settings(self.settings)
        if self.settings.cleanup_before_run:
            self.cleanup_stale_partials()

        self.active = True
        self.is_paused = False
        self.tree.delete(*self.tree.get_children())
        self.tree_items.clear()
        self.start_btn.configure(state="disabled")
        self.pause_btn.configure(state="normal", text="Pause")
        self.cancel_btn.configure(state="normal")
        self.labels["queue"].configure(text=f"Queue: {len(job.ids)}")
        self.progress_bar.set(0)
        self.progress_label.configure(text=f"Progress: 0.0% (0 / {len(job.ids)})")
        self.eta_label.configure(text="ETA: --")
        self.run_status.configure(text="Run started.", text_color="#7dd3fc")
        self.log_inspector(f"Run started with {len(job.ids)} IDs, {job.workers} workers, {job.threads} threads/file.")
        self.engine.start(job, self.settings)

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
        while True:
            try:
                event_type, payload = self.event_queue.get_nowait()
            except queue.Empty:
                break
            self.handle_event(event_type, payload)
        self.after(UI_POLL_MS, self.process_ui_events)

    def handle_event(self, event_type, payload):
        if event_type == "item_status":
            self.update_grid(payload["video_id"], payload["status"], payload.get("size_mb", "-"), "-")
        elif event_type == "item_done":
            self.update_grid(payload["video_id"], payload["status"], payload.get("size_mb", "-"), payload.get("retries", 0))
        elif event_type == "runtime":
            self.current_stats = payload
            self.refresh_runtime()
        elif event_type == "bytes":
            pass
        elif event_type == "run_finished":
            self.finish_run(payload["cancelled"], payload["errors"])

    def refresh_runtime(self):
        stats = self.current_stats
        total = stats.get("total", 0)
        processed = stats.get("processed", 0)
        success = stats.get("success", 0)
        skipped = stats.get("skipped", 0)
        failed = stats.get("failed", 0)
        retries = stats.get("retries", 0)
        bytes_downloaded = stats.get("bytes_downloaded", 0)
        started_at = stats.get("started_at", time.time())

        elapsed = max(time.time() - started_at, 0.1)
        speed = bytes_downloaded / elapsed / (1024 * 1024)
        progress = (processed / total) if total else 0
        eta = ((total - processed) * (elapsed / processed)) if processed else 0

        self.labels["done"].configure(text=f"Done: {success}")
        self.labels["skipped"].configure(text=f"Skipped: {skipped}")
        self.labels["failed"].configure(text=f"Failed: {failed}")
        self.labels["speed"].configure(text=f"Speed: {speed:.2f} MB/s")
        self.labels["retries"].configure(text=f"Retries: {retries}")
        self.progress_bar.set(progress)
        self.progress_label.configure(text=f"Progress: {progress * 100:.1f}% ({processed} / {total})")
        self.eta_label.configure(text=f"ETA: {self.format_eta(eta) if processed else '--'}")

    def finish_run(self, cancelled, errors):
        self.active = False
        self.start_btn.configure(state="normal")
        self.pause_btn.configure(state="disabled", text="Pause")
        self.cancel_btn.configure(state="disabled")
        if cancelled:
            text = "Run cancelled."
            color = "#fbbf24"
        else:
            text = "Run complete."
            color = "#4ade80"
        if errors:
            text += f" Background errors: {errors}."
        self.run_status.configure(text=text, text_color=color)
        self.refresh_history()
        self.log_inspector(text)

    def update_grid(self, vid_id, status, size, retries):
        if self.silent_var.get() and str(vid_id) != "SYS":
            return
        key = str(vid_id)
        item = self.tree_items.get(key)
        values = (vid_id, status, size, retries)
        if item and self.tree.exists(item):
            self.tree.item(item, values=values)
        else:
            self.tree_items[key] = self.tree.insert("", "end", values=values)
        children = self.tree.get_children()
        if len(children) > MAX_VISIBLE_ROWS:
            oldest = children[0]
            old_values = self.tree.item(oldest, "values")
            if old_values:
                self.tree_items.pop(str(old_values[0]), None)
            self.tree.delete(oldest)

    def refresh_history(self):
        self.history_tree.delete(*self.history_tree.get_children())
        for row in self.storage.load_recent_runs():
            self.history_tree.insert(
                "",
                "end",
                values=(row["started_at"], row["run_label"], row["total"], row["success"], row["skipped"], row["failed"], f"{row['avg_speed_mbps']:.2f}"),
            )

    def extract_id(self):
        match = re.search(r"id/(\d+)\.mp4", self.link_entry.get().strip())
        if not match:
            self.run_status.configure(text="Could not extract an ID from the URL.", text_color="#fbbf24")
            return
        video_id = match.group(1)
        self.start_entry.delete(0, "end")
        self.start_entry.insert(0, video_id)
        self.end_entry.delete(0, "end")
        self.end_entry.insert(0, video_id)
        self.update_preview()

    def update_preview(self):
        try:
            start_id = int(self.start_entry.get().strip())
            end_id = int(self.end_entry.get().strip())
            step = max(int(self.step_entry.get().strip() or "1"), 1)
            count = len(range(start_id, end_id + 1, step)) if start_id <= end_id else 0
            self.preview_label.configure(text=f"{count} IDs, {self.range_mode.get()}")
        except ValueError:
            self.preview_label.configure(text="Check IDs")

    def load_proxies(self):
        file_path = filedialog.askopenfilename(filetypes=[("Text files", "*.txt"), ("All files", "*.*")])
        if not file_path:
            return
        with open(file_path, "r", encoding="utf-8") as file_obj:
            self.proxies = [line.strip() for line in file_obj if line.strip()]
        self.run_status.configure(text=f"Loaded {len(self.proxies)} proxies.", text_color="#93c5fd")
        self.log_inspector(f"Loaded proxies from {file_path}")

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
        timestamp = time.strftime("%H:%M:%S")
        self.inspector.insert("end", f"[{timestamp}] {message}\n")
        self.inspector.see("end")

    def format_eta(self, seconds):
        seconds = int(max(seconds, 0))
        minutes, sec = divmod(seconds, 60)
        hours, minutes = divmod(minutes, 60)
        if hours:
            return f"{hours}h {minutes}m {sec}s"
        if minutes:
            return f"{minutes}m {sec}s"
        return f"{sec}s"


if __name__ == "__main__":
    app = ProventureStudio()
    app.mainloop()
