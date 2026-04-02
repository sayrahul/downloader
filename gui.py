import asyncio
import hashlib
import queue
import re
import threading
import time
from collections import defaultdict
from datetime import datetime
from pathlib import Path

import flet as ft

from engine import DownloadEngine
from models import AppSettings, DownloadJob
from storage import AppStorage

PRESET_DEFAULTS = {
    "safe": {"workers": 4, "threads": 1, "chunk_kb": 512, "timeout_seconds": 25},
    "balanced": {"workers": 8, "threads": 2, "chunk_kb": 1024, "timeout_seconds": 20},
    "fast": {"workers": 10, "threads": 3, "chunk_kb": 1024, "timeout_seconds": 18},
    "turbo": {"workers": 12, "threads": 4, "chunk_kb": 2048, "timeout_seconds": 15},
}
VALID_RANGE_MODES = {"sequence", "reverse"}
POLL_SECONDS = 0.12
MAX_CONSOLE_LINES = 250

COLOR_BG = "#020617"
COLOR_SURFACE = "#0F172A"
COLOR_ACCENT = "#0EA5E9"
COLOR_SUCCESS = "#10B981"
COLOR_ERROR = "#F43F5E"
COLOR_WARNING = "#F59E0B"
COLOR_TEXT_PRIMARY = "#F8FAFC"
COLOR_TEXT_SECONDARY = "#64748B"
COLOR_BORDER = "#1E293B"


class DownloaderApp:
    def __init__(self, page: ft.Page):
        self.page = page
        self.db = AppStorage(Path("."))
        self.settings = self._normalize_settings(self.db.load_settings())
        self.engine = DownloadEngine(self, self.db)
        self.event_queue = queue.Queue()
        self.proxies = []
        self.nav_index = 0
        self.is_running = False
        self.is_paused = False
        self.output_path = self.settings.output_dir
        self.current_stats = {}
        self.ui_ready = False

        self.txt_queue = ft.Text("0", size=32, weight=ft.FontWeight.BOLD, color=COLOR_TEXT_PRIMARY)
        self.txt_speed = ft.Text("0.00 MB/s", size=32, weight=ft.FontWeight.BOLD, color=COLOR_ACCENT)
        self.txt_done = ft.Text("0", size=32, weight=ft.FontWeight.BOLD, color=COLOR_SUCCESS)
        self.txt_failed = ft.Text("0", size=32, weight=ft.FontWeight.BOLD, color=COLOR_ERROR)
        self.txt_progress = ft.Text("Ready to initialize...", weight=ft.FontWeight.BOLD, size=14, color=COLOR_TEXT_PRIMARY)
        self.txt_eta = ft.Text("ETA: --", color=COLOR_TEXT_SECONDARY)
        self.txt_profile = ft.Text("", size=12, color=COLOR_ACCENT, selectable=True)
        self.txt_preview = ft.Text("Ready", size=12, color=COLOR_TEXT_SECONDARY, text_align=ft.TextAlign.RIGHT)
        self.txt_output = ft.Text(self.output_path, size=12, color=COLOR_TEXT_SECONDARY, selectable=True)
        self.txt_status = ft.Text("SYSTEM READY", size=12, weight=ft.FontWeight.BOLD, color=COLOR_SUCCESS)
        self.txt_dup_files = ft.Text("0", size=32, weight=ft.FontWeight.BOLD, color=COLOR_TEXT_PRIMARY)
        self.txt_dup_size_groups = ft.Text("0", size=32, weight=ft.FontWeight.BOLD, color=COLOR_WARNING)
        self.txt_dup_exact_groups = ft.Text("0", size=32, weight=ft.FontWeight.BOLD, color=COLOR_ACCENT)
        self.txt_dup_removable = ft.Text("0", size=32, weight=ft.FontWeight.BOLD, color=COLOR_ERROR)
        self.txt_dup_status = ft.Text("Ready to scan current output folder.", size=13, color=COLOR_TEXT_SECONDARY)
        self.prog_bar = ft.ProgressBar(value=0, height=8, color=COLOR_ACCENT, bgcolor=COLOR_BG, border_radius=4)
        self.console = ft.ListView(expand=True, spacing=4, padding=10, auto_scroll=True)
        self.dup_results = {"files": [], "same_size_groups": [], "exact_groups": [], "removable": []}
        self.dup_busy = False
        self.dup_list = ft.ListView(expand=True, spacing=8, padding=5, auto_scroll=False)
        self.history_search = self._create_input("Search history")
        self.history_search.height = 46
        self.history_search.on_submit = self.refresh_history_view
        self.history_detail = ft.ListView(expand=True, spacing=6, padding=5, auto_scroll=False)

        self.in_url = self._create_input("Template URL")
        self.in_start = self._create_input("Range Start")
        self.in_end = self._create_input("Range End")
        self.in_step = self._create_input("Increment")
        self.in_size = self._create_input("Size Limit (MB)")
        self.in_step.width = 140
        self.dropdown_preset = self._create_dropdown("Preset Mode", ["safe", "balanced", "fast", "turbo"], self.on_preset_change)
        self.dropdown_range = self._create_dropdown("Order Mode", ["sequence", "reverse"], self.on_form_change)
        self.dropdown_range.width = 190
        self.file_picker = ft.FilePicker()
        self.file_picker.on_result = self.on_file_result

        self.rail = None
        self.content_area = None
        self.start_button = None
        self.pause_button = None
        self.stop_button = None
        self.dup_scan_button = None
        self.dup_move_button = None
        self.dup_delete_button = None
        self.settings_save_button = None
        self.settings_reset_button = None

        self.setup_page()
        self.build_ui()
        self.apply_settings_to_form()
        self.show_dashboard()
        self.log("STUDIO PRO KERNEL ONLINE", COLOR_SUCCESS)
        self.log(f"Output directory: {self.output_path}", COLOR_TEXT_SECONDARY)
        self.ui_ready = True
        self._safe_update()
        self.page.run_task(self._event_pump)

    def setup_page(self):
        self.page.title = "Downloader Studio Pro 4.0"
        self.page.theme_mode = ft.ThemeMode.DARK
        self.page.bgcolor = COLOR_BG
        self.page.padding = 0
        self.page.window_width = 1350
        self.page.window_height = 920
        self.page.theme = ft.Theme(font_family="Segoe UI")

    def _create_input(self, label):
        return ft.TextField(
            label=label,
            text_size=13,
            height=50,
            color=COLOR_TEXT_PRIMARY,
            bgcolor=COLOR_SURFACE,
            border_color=COLOR_BORDER,
            focused_border_color=COLOR_ACCENT,
            label_style=ft.TextStyle(color=COLOR_TEXT_SECONDARY),
            border_radius=10,
            on_change=self.on_form_change,
        )

    def _create_dropdown(self, label, values, handler):
        return ft.Dropdown(
            label=label,
            options=[ft.dropdown.Option(v) for v in values],
            border_color=COLOR_BORDER,
            focused_border_color=COLOR_ACCENT,
            border_radius=10,
            text_style=ft.TextStyle(color=COLOR_TEXT_PRIMARY),
            label_style=ft.TextStyle(color=COLOR_TEXT_SECONDARY),
            bgcolor=COLOR_SURFACE,
            color=COLOR_TEXT_PRIMARY,
            height=50,
            on_select=handler,
        )

    def _card(self, title, value_ref, icon):
        return ft.Container(
            content=ft.Column(
                [
                    ft.Row(
                        [ft.Text(title, size=11, weight=ft.FontWeight.BOLD, color=COLOR_TEXT_SECONDARY), ft.Icon(icon, size=18, color=COLOR_TEXT_SECONDARY)],
                        alignment=ft.MainAxisAlignment.SPACE_BETWEEN,
                    ),
                    value_ref,
                ],
                spacing=8,
            ),
            bgcolor=COLOR_SURFACE,
            padding=20,
            border_radius=16,
            border=ft.Border.all(1, COLOR_BORDER),
            expand=True,
        )

    def build_ui(self):
        self.page.services.append(self.file_picker)
        self.rail = ft.NavigationRail(
            selected_index=0,
            label_type=ft.NavigationRailLabelType.ALL,
            min_width=96,
            min_extended_width=180,
            bgcolor=COLOR_SURFACE,
            indicator_color=COLOR_BORDER,
            destinations=[
                ft.NavigationRailDestination(icon=ft.Icons.DASHBOARD_OUTLINED, selected_icon=ft.Icons.DASHBOARD, label="Dashboard"),
                ft.NavigationRailDestination(icon=ft.Icons.CONTENT_COPY_OUTLINED, selected_icon=ft.Icons.CONTENT_COPY, label="Duplicates"),
                ft.NavigationRailDestination(icon=ft.Icons.HISTORY_OUTLINED, selected_icon=ft.Icons.HISTORY, label="Operations"),
                ft.NavigationRailDestination(icon=ft.Icons.SETTINGS_OUTLINED, selected_icon=ft.Icons.SETTINGS, label="Settings"),
            ],
            on_change=self.on_nav_change,
        )
        self.content_area = ft.Container(expand=True, padding=24)
        self.page.add(ft.Row([self.rail, ft.VerticalDivider(width=1, color=COLOR_BORDER), self.content_area], expand=True))

    def show_dashboard(self):
        self.start_button = ft.Button(
            "Launch Operation",
            icon=ft.Icons.PLAY_ARROW,
            color=COLOR_BG,
            bgcolor=COLOR_ACCENT,
            height=50,
            expand=True,
            on_click=self.start_process,
        )
        self.pause_button = ft.IconButton(
            icon=ft.Icons.PAUSE,
            icon_color=COLOR_TEXT_PRIMARY,
            bgcolor=COLOR_BORDER,
            height=50,
            on_click=self.toggle_pause,
        )
        self.stop_button = ft.IconButton(
            icon=ft.Icons.STOP,
            icon_color=COLOR_ERROR,
            bgcolor=COLOR_BORDER,
            height=50,
            on_click=self.stop_process,
        )

        metrics = ft.Row(
            [
                self._card("ACTIVE QUEUE", self.txt_queue, ft.Icons.SEARCH),
                self._card("PEAK SPEED", self.txt_speed, ft.Icons.SPEED),
                self._card("COMPLETED", self.txt_done, ft.Icons.CHECK_CIRCLE_OUTLINE),
                self._card("FAILED", self.txt_failed, ft.Icons.ERROR_OUTLINE),
            ],
            spacing=16,
        )

        left_panel = ft.Container(
            content=ft.Column(
                [
                    ft.Text("Target Parameters", weight=ft.FontWeight.BOLD, size=18, color=COLOR_TEXT_PRIMARY),
                    self.in_url,
                    ft.Row(
                        [
                            ft.Container(content=self.in_start, expand=True),
                            ft.Container(content=self.in_end, expand=True),
                            ft.Container(content=self.in_step, width=120),
                        ],
                        spacing=10,
                    ),
                    ft.Row(
                        [
                            ft.Container(content=self.dropdown_range, width=190),
                            self.txt_preview,
                        ],
                        spacing=10,
                    ),
                    ft.Button(
                        "Detect Sequence ID",
                        icon=ft.Icons.BOLT,
                        color=COLOR_TEXT_PRIMARY,
                        bgcolor=COLOR_BORDER,
                        height=45,
                        on_click=self.detect_sequence_id,
                    ),
                ],
                spacing=15,
            ),
            bgcolor=COLOR_SURFACE,
            padding=25,
            border_radius=16,
            border=ft.Border.all(1, COLOR_BORDER),
            expand=3,
        )

        right_panel = ft.Container(
            content=ft.Column(
                [
                    ft.Text("Engine Optimization", weight=ft.FontWeight.BOLD, size=18, color=COLOR_TEXT_PRIMARY),
                    self.dropdown_preset,
                    self.in_size,
                    self.txt_profile,
                    ft.Row([self.start_button, self.pause_button, self.stop_button], spacing=10),
                    ft.TextButton("Set Output Folder", icon=ft.Icons.FOLDER_OPEN, icon_color=COLOR_ACCENT, on_click=self.pick_output_folder),
                    self.txt_output,
                    self.txt_status,
                ],
                spacing=15,
            ),
            bgcolor=COLOR_SURFACE,
            padding=25,
            border_radius=16,
            border=ft.Border.all(1, COLOR_BORDER),
            expand=2,
        )

        progress_panel = ft.Container(
            content=ft.Column(
                [
                    ft.Row([self.txt_progress, self.txt_eta], alignment=ft.MainAxisAlignment.SPACE_BETWEEN),
                    self.prog_bar,
                ],
                spacing=15,
            ),
            bgcolor=COLOR_SURFACE,
            padding=25,
            border_radius=16,
            border=ft.Border.all(1, COLOR_BORDER),
        )

        console_panel = ft.Container(
            content=ft.Column(
                [
                    ft.Text("> SYSTEM CONSOLE", size=10, weight=ft.FontWeight.BOLD, color=COLOR_TEXT_SECONDARY),
                    ft.Container(content=self.console, bgcolor=COLOR_BG, border_radius=10, height=180),
                ],
                spacing=12,
            ),
            bgcolor=COLOR_SURFACE,
            padding=15,
            border_radius=16,
            border=ft.Border.all(1, COLOR_BORDER),
        )

        self.content_area.content = ft.Column(
            [
                metrics,
                ft.Row([left_panel, right_panel], spacing=20, vertical_alignment=ft.CrossAxisAlignment.START),
                progress_panel,
                console_panel,
            ],
            spacing=20,
            scroll=ft.ScrollMode.AUTO,
        )
        self.refresh_button_states()
        self._safe_update()

    def show_history(self):
        rows = []
        runs = self.db.load_recent_runs(
            limit=max(20, self.settings.history_limit),
            search=self.history_search.value.strip() if self.history_search.value else "",
        )
        for row in runs:
            run_label = str(row["run_label"])
            rows.append(
                ft.DataRow(
                    cells=[
                        ft.DataCell(ft.Text(str(row["started_at"]).replace("T", " "), size=12, color=COLOR_TEXT_PRIMARY)),
                        ft.DataCell(
                            ft.TextButton(
                                content=ft.Text(run_label, weight=ft.FontWeight.BOLD, color=COLOR_ACCENT),
                                on_click=lambda _e, label=run_label: self.show_history_detail(label),
                            )
                        ),
                        ft.DataCell(ft.Text(str(row["total"]))),
                        ft.DataCell(ft.Text(str(row["success"]), color=COLOR_SUCCESS)),
                        ft.DataCell(ft.Text(str(row["skipped"]), color=COLOR_TEXT_SECONDARY)),
                        ft.DataCell(ft.Text(str(row["failed"]), color=COLOR_ERROR)),
                        ft.DataCell(ft.Text(f"{row['avg_speed_mbps']:.2f} MB/s", size=12, color=COLOR_TEXT_SECONDARY)),
                    ]
                )
            )
        table = ft.DataTable(
            columns=[ft.DataColumn(ft.Text(v, color=COLOR_TEXT_PRIMARY)) for v in ["Started", "Label", "Total", "Done", "Skipped", "Failed", "Avg Speed"]],
            rows=rows,
            heading_row_color=COLOR_BORDER,
            expand=True,
        )
        self.history_detail.controls = [
            ft.Text("Select a run label below to inspect recent stored items.", color=COLOR_TEXT_SECONDARY)
        ]
        self.content_area.content = ft.Column(
            [
                ft.Row([ft.Text("Operation History", size=24, weight=ft.FontWeight.BOLD, color=COLOR_TEXT_PRIMARY), ft.IconButton(icon=ft.Icons.REFRESH, on_click=self.refresh_history_view)], alignment=ft.MainAxisAlignment.SPACE_BETWEEN),
                ft.Container(
                    content=ft.Column(
                        [
                            ft.Text("Search past runs, then click a run label below to inspect duplicate-safe results and item history.", size=12, color=COLOR_TEXT_SECONDARY),
                            ft.Row(
                                [
                                    ft.Container(content=self.history_search, expand=True),
                                    ft.Button("Search", icon=ft.Icons.SEARCH, bgcolor=COLOR_ACCENT, color=COLOR_BG, height=46, on_click=self.refresh_history_view),
                                    ft.Button("Clear", icon=ft.Icons.CLEAR, bgcolor=COLOR_BORDER, color=COLOR_TEXT_PRIMARY, height=46, on_click=self.clear_history_search),
                                ],
                                spacing=10,
                            ),
                            ft.Row([table], scroll=ft.ScrollMode.ALWAYS, expand=True),
                        ],
                        spacing=15,
                        expand=True,
                    ),
                    bgcolor=COLOR_SURFACE,
                    padding=20,
                    border_radius=16,
                    border=ft.Border.all(1, COLOR_BORDER),
                    expand=True,
                ),
                ft.Container(
                    content=ft.Column(
                        [
                            ft.Text("Run Detail", size=18, weight=ft.FontWeight.BOLD, color=COLOR_TEXT_PRIMARY),
                            ft.Container(content=self.history_detail, bgcolor=COLOR_BG, border_radius=10, height=230, padding=10),
                        ],
                        spacing=12,
                    ),
                    bgcolor=COLOR_SURFACE,
                    padding=18,
                    border_radius=16,
                    border=ft.Border.all(1, COLOR_BORDER),
                ),
            ],
            spacing=20,
            expand=True,
        )
        if runs:
            self.show_history_detail(str(runs[0]["run_label"]))
        self._safe_update()

    def show_duplicates(self):
        self.dup_scan_button = ft.Button(
            "Scan Output Folder",
            icon=ft.Icons.SEARCH,
            color=COLOR_BG,
            bgcolor=COLOR_ACCENT,
            height=46,
            on_click=self.start_duplicate_scan,
        )
        self.dup_move_button = ft.Button(
            "Move Exact Duplicates To Review",
            icon=ft.Icons.DRIVE_FILE_MOVE_OUTLINE,
            color=COLOR_TEXT_PRIMARY,
            bgcolor=COLOR_BORDER,
            height=46,
            on_click=self.move_duplicate_files,
            disabled=self.dup_busy or not self.dup_results["removable"],
        )
        self.dup_delete_button = ft.Button(
            "Smart Delete Exact Duplicates",
            icon=ft.Icons.DELETE_SWEEP_OUTLINED,
            color=COLOR_TEXT_PRIMARY,
            bgcolor=COLOR_ERROR,
            height=46,
            on_click=self.delete_duplicate_files,
            disabled=self.dup_busy or not self.dup_results["removable"],
        )

        metrics = ft.Row(
            [
                self._card("FILES SCANNED", self.txt_dup_files, ft.Icons.FOLDER),
                self._card("SAME SIZE GROUPS", self.txt_dup_size_groups, ft.Icons.STRAIGHTEN),
                self._card("EXACT DUP GROUPS", self.txt_dup_exact_groups, ft.Icons.FACT_CHECK),
                self._card("REMOVABLE COPIES", self.txt_dup_removable, ft.Icons.DELETE_OUTLINE),
            ],
            spacing=16,
        )

        controls = ft.Container(
            content=ft.Column(
                [
                    ft.Text("Duplicate Video Manager", size=22, weight=ft.FontWeight.BOLD, color=COLOR_TEXT_PRIMARY),
                    ft.Text(
                        "Scan the current output folder to find same-size files and verify exact duplicates by SHA-256 hash. Exact duplicate copies can be moved into a safe review folder or permanently removed with Smart Delete.",
                        size=13,
                        color=COLOR_TEXT_SECONDARY,
                    ),
                    ft.Row([self.dup_scan_button, self.dup_move_button, self.dup_delete_button], spacing=12, wrap=True),
                    ft.Text(f"Current folder: {self.output_path}", size=12, color=COLOR_TEXT_SECONDARY, selectable=True),
                    self.txt_dup_status,
                ],
                spacing=14,
            ),
            bgcolor=COLOR_SURFACE,
            padding=24,
            border_radius=16,
            border=ft.Border.all(1, COLOR_BORDER),
        )

        results = ft.Container(
            content=ft.Column(
                [
                    ft.Text("Review Queue", size=18, weight=ft.FontWeight.BOLD, color=COLOR_TEXT_PRIMARY),
                    ft.Text(
                        "Groups show one kept file and the extra copies that can be removed or moved.",
                        size=12,
                        color=COLOR_TEXT_SECONDARY,
                    ),
                    ft.Container(content=self.dup_list, bgcolor=COLOR_BG, border_radius=10, height=420, padding=10),
                ],
                spacing=12,
            ),
            bgcolor=COLOR_SURFACE,
            padding=18,
            border_radius=16,
            border=ft.Border.all(1, COLOR_BORDER),
        )

        self.content_area.content = ft.Column(
            [metrics, controls, results],
            spacing=20,
            scroll=ft.ScrollMode.AUTO,
        )
        self.refresh_duplicate_metrics()
        self._safe_update()

    def show_settings(self):
        self.settings_save_button = ft.Button(
            "Save Current Form",
            icon=ft.Icons.SAVE_OUTLINED,
            bgcolor=COLOR_ACCENT,
            color=COLOR_BG,
            height=46,
            on_click=self.save_settings_from_form,
        )
        self.settings_reset_button = ft.Button(
            "Reload Saved Settings",
            icon=ft.Icons.RESTART_ALT,
            bgcolor=COLOR_BORDER,
            color=COLOR_TEXT_PRIMARY,
            height=46,
            on_click=self.reload_saved_settings,
        )
        lines = [
            f"Base URL: {self.settings.base_url}",
            f"Output directory: {self.output_path}",
            f"Range: {self.settings.start_id} to {self.settings.end_id} step {self.settings.step}",
            f"Preset: {self.settings.performance_preset}",
            f"Workers / Threads: {self.settings.workers} / {self.settings.threads}",
            f"Chunk size: {self.settings.chunk_kb} KB",
            f"Timeout: {self.settings.timeout_seconds} sec",
            f"Auto tune: {self.settings.auto_tune}",
        ]
        self.content_area.content = ft.Container(
            content=ft.Column(
                [
                    ft.Text("Settings Snapshot", size=24, weight=ft.FontWeight.BOLD, color=COLOR_TEXT_PRIMARY),
                    ft.Text("Save the current dashboard form as defaults or reload the last saved configuration.", size=12, color=COLOR_TEXT_SECONDARY),
                    ft.Row([self.settings_save_button, self.settings_reset_button], spacing=12),
                    ft.Container(content=ft.Column([ft.Text(line, color=COLOR_TEXT_PRIMARY, size=14) for line in lines], spacing=12), bgcolor=COLOR_SURFACE, padding=24, border_radius=16, border=ft.Border.all(1, COLOR_BORDER)),
                ],
                spacing=20,
                expand=True,
            ),
            expand=True,
        )
        self._safe_update()

    def emit_event(self, event_type, data):
        self.event_queue.put((event_type, data))

    async def _event_pump(self):
        while True:
            dirty = False
            while True:
                try:
                    event_type, payload = self.event_queue.get_nowait()
                except queue.Empty:
                    break
                dirty = self.handle_event(event_type, payload) or dirty
            if dirty:
                self._safe_update()
            await asyncio.sleep(POLL_SECONDS)

    def handle_event(self, event_type, payload):
        if event_type == "runtime":
            self.current_stats = payload
            self.refresh_runtime()
            return True
        if event_type == "diagnostic":
            self.log(payload.get("message", "N/A"), COLOR_ACCENT)
            return True
        if event_type == "run_finished":
            self.finish_run(payload.get("cancelled", False), payload.get("errors", 0))
            return True
        if event_type == "duplicate_scan_complete":
            self.finish_duplicate_scan(payload)
            return True
        if event_type == "duplicate_scan_error":
            self.dup_busy = False
            self.txt_dup_status.value = payload.get("message", "Duplicate scan failed.")
            self.txt_dup_status.color = COLOR_ERROR
            self.refresh_duplicate_metrics()
            self.log(self.txt_dup_status.value, COLOR_ERROR)
            return True
        return False

    def refresh_runtime(self):
        stats = self.current_stats
        total = stats.get("total", 0)
        processed = stats.get("processed", 0)
        missing = stats.get("missing", 0)
        failed = max(stats.get("failed", 0) - missing, 0)
        elapsed = max(time.time() - stats.get("started_at", time.time()), 0.1)
        speed = stats.get("bytes_downloaded", 0) / elapsed / (1024 * 1024)
        progress = (processed / total) if total else 0
        eta = ((total - processed) * (elapsed / processed)) if processed else 0
        self.txt_queue.value = str(stats.get("queued_left", max(total - processed, 0)))
        self.txt_speed.value = f"{speed:.2f} MB/s"
        self.txt_done.value = str(stats.get("success", 0))
        self.txt_failed.value = str(failed)
        self.prog_bar.value = progress
        self.txt_progress.value = f"Progress: {progress * 100:.1f}% ({processed} / {total})"
        self.txt_eta.value = f"ETA: {self.format_eta(eta) if processed else '--'}"
        self.txt_status.value = "PAUSED" if self.is_paused else "OPERATING..."
        self.txt_status.color = COLOR_WARNING if self.is_paused else COLOR_ACCENT

    def start_process(self, _event):
        try:
            settings = self.collect_settings()
            job = self.build_job(settings)
        except ValueError as exc:
            self.set_status(str(exc), COLOR_WARNING)
            self.log(f"Validation error: {exc}", COLOR_WARNING)
            return
        except Exception as exc:
            self.set_status(f"Failed to prepare run: {exc}", COLOR_ERROR)
            self.log(f"Unexpected setup error: {exc}", COLOR_ERROR)
            return

        Path(settings.output_dir).mkdir(parents=True, exist_ok=True)
        self.db.save_settings(settings)
        self.settings = settings
        self.output_path = settings.output_dir
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
            "queued_left": len(job.ids),
        }
        self.is_running = True
        self.is_paused = False
        self.refresh_runtime()
        self.refresh_button_states()
        self.log(f"Run started with {len(job.ids)} IDs using {job.performance_preset} preset.", COLOR_SUCCESS)
        self.engine.start(job, settings)
        self._safe_update()

    def finish_run(self, cancelled, errors):
        self.is_running = False
        self.is_paused = False
        self.refresh_button_states()
        success = self.current_stats.get("success", 0)
        missing = self.current_stats.get("missing", 0)
        failed = max(self.current_stats.get("failed", 0) - missing, 0)
        if cancelled:
            message, color = "Run cancelled.", COLOR_WARNING
        elif success == 0 and missing > 0 and failed == 0:
            message, color = "Run complete, but no downloadable videos were found in this range.", COLOR_WARNING
        else:
            message, color = "Run complete.", COLOR_SUCCESS
        if errors:
            message += f" Background errors: {errors}."
        self.set_status(message, color)
        self.log(message, color)
        if self.nav_index == 2:
            self.show_history()

    def on_file_result(self, e):
        if e.path:
            self.output_path = e.path
            self.txt_output.value = e.path
            self.set_status("Output folder updated.", COLOR_ACCENT)
            self.txt_dup_status.value = f"Current duplicate scan folder updated to: {e.path}"
            self.txt_dup_status.color = COLOR_TEXT_SECONDARY
            self.log(f"Output folder set to: {e.path}", COLOR_ACCENT)
            self._safe_update()

    def refresh_history_view(self, _event=None):
        if self.nav_index == 2:
            self.show_history()

    def clear_history_search(self, _event):
        self.history_search.value = ""
        self.refresh_history_view()

    def show_history_detail(self, run_label: str):
        rows = self.db.load_run_items(run_label, limit=150)
        self.history_detail.controls.clear()
        self.history_detail.controls.append(
            ft.Text(f"Run: {run_label}", weight=ft.FontWeight.BOLD, color=COLOR_ACCENT)
        )
        if not rows:
            self.history_detail.controls.append(
                ft.Text("No stored item details yet.", color=COLOR_TEXT_SECONDARY)
            )
            self._safe_update()
            return
        for row in rows:
            self.history_detail.controls.append(
                ft.Container(
                    content=ft.Column(
                        [
                            ft.Text(
                                f"ID {row['video_id']} | {row['result']} | retries={row['retries']}",
                                size=12,
                                weight=ft.FontWeight.BOLD,
                                color=COLOR_TEXT_PRIMARY,
                            ),
                            ft.Text(
                                f"Status: {row['status']} | size={row['size_mb']} | http={row['http_status']}",
                                size=11,
                                color=COLOR_TEXT_SECONDARY,
                            ),
                            ft.Text(
                                f"Updated: {row['updated_at']}",
                                size=11,
                                color=COLOR_TEXT_SECONDARY,
                            ),
                        ],
                        spacing=3,
                    ),
                    bgcolor=COLOR_SURFACE,
                    padding=10,
                    border_radius=10,
                    border=ft.Border.all(1, COLOR_BORDER),
                )
            )
        self._safe_update()

    def save_settings_from_form(self, _event):
        try:
            self.settings = self.collect_settings()
            self.db.save_settings(self.settings)
            self.output_path = self.settings.output_dir
            self.txt_output.value = self.output_path
            self.set_status("Current form saved as default settings.", COLOR_SUCCESS)
            self.log("Current form saved as default settings.", COLOR_SUCCESS)
            if self.nav_index == 3:
                self.show_settings()
        except Exception as exc:
            self.set_status(f"Could not save settings: {exc}", COLOR_ERROR)
            self.log(f"Could not save settings: {exc}", COLOR_ERROR)

    def reload_saved_settings(self, _event):
        self.settings = self._normalize_settings(self.db.load_settings())
        self.output_path = self.settings.output_dir
        self.apply_settings_to_form()
        self.set_status("Reloaded saved settings.", COLOR_ACCENT)
        self.log("Reloaded saved settings.", COLOR_ACCENT)
        if self.nav_index == 3:
            self.show_settings()

    def pick_output_folder(self, _event):
        initial_dir = str(Path(self.output_path).resolve()) if self.output_path else None
        self.file_picker.get_directory_path(dialog_title="Choose output folder", initial_directory=initial_dir)

    def toggle_pause(self, _event):
        if not self.is_running:
            return
        if self.is_paused:
            self.engine.resume()
            self.is_paused = False
            self.log("Kernel resumed.", COLOR_ACCENT)
        else:
            self.engine.pause()
            self.is_paused = True
            self.log("Kernel paused.", COLOR_WARNING)
        self.refresh_button_states()
        self.refresh_runtime()
        self._safe_update()

    def stop_process(self, _event):
        if not self.is_running:
            return
        self.engine.cancel()
        self.set_status("Cancelling run...", COLOR_ERROR)
        self.log("Kernel stop signal sent.", COLOR_ERROR)
        self._safe_update()

    def on_nav_change(self, e):
        self.nav_index = e.control.selected_index
        if self.nav_index == 0:
            self.show_dashboard()
        elif self.nav_index == 1:
            self.show_duplicates()
        elif self.nav_index == 2:
            self.show_history()
        else:
            self.show_settings()

    def on_preset_change(self, _event):
        self.update_profile_summary()
        self._safe_update()

    def on_form_change(self, _event):
        self.update_preview()
        self._safe_update()

    def detect_sequence_id(self, _event):
        url = self.in_url.value.strip()
        match = re.search(r"id/(\d+)\.mp4", url)
        if not match:
            self.set_status("Could not extract an ID from the URL.", COLOR_WARNING)
            self.log("Could not extract an ID from the URL.", COLOR_WARNING)
            self._safe_update()
            return
        video_id = match.group(1)
        self.in_start.value = video_id
        self.in_end.value = video_id
        if "{}" not in url:
            self.in_url.value = re.sub(r"id/\d+\.mp4", "id/{}.mp4", url)
        self.update_preview()
        self.set_status("Detected sequence ID from URL.", COLOR_ACCENT)
        self._safe_update()

    def start_duplicate_scan(self, _event):
        if self.dup_busy:
            return
        target_dir = Path(self.output_path)
        self.dup_busy = True
        self.txt_dup_status.value = f"Scanning {target_dir} for duplicate videos..."
        self.txt_dup_status.color = COLOR_ACCENT
        self.refresh_duplicate_metrics()
        self._safe_update()
        threading.Thread(target=self._scan_duplicates_worker, args=(target_dir,), daemon=True).start()

    def _scan_duplicates_worker(self, target_dir: Path):
        try:
            results = self._scan_duplicate_groups(target_dir)
            self.emit_event("duplicate_scan_complete", results)
        except Exception as exc:
            self.emit_event("duplicate_scan_error", {"message": f"Duplicate scan error: {exc}"})

    def _scan_duplicate_groups(self, target_dir: Path):
        if not target_dir.exists():
            raise FileNotFoundError(f"Folder not found: {target_dir}")

        files = [path for path in target_dir.rglob("*") if path.is_file() and path.suffix.lower() in {".mp4", ".mkv", ".mov", ".avi", ".webm"}]
        size_map = defaultdict(list)
        for path in files:
            try:
                size_map[path.stat().st_size].append(path)
            except OSError:
                continue

        same_size_groups = []
        exact_groups = []
        removable = []
        for size_bytes, paths in size_map.items():
            if len(paths) < 2:
                continue
            same_size_groups.append({"size": size_bytes, "paths": [str(p) for p in sorted(paths)]})
            hash_map = defaultdict(list)
            for path in paths:
                file_hash = self._hash_file(path)
                hash_map[file_hash].append(path)
            for dup_paths in hash_map.values():
                if len(dup_paths) < 2:
                    continue
                ordered = sorted(dup_paths, key=self._duplicate_keep_sort_key)
                keep = ordered[0]
                extras = ordered[1:]
                group = {
                    "size": keep.stat().st_size,
                    "keep": str(keep),
                    "duplicates": [str(p) for p in extras],
                }
                exact_groups.append(group)
                removable.extend(group["duplicates"])

        return {
            "files": [str(p) for p in files],
            "same_size_groups": same_size_groups,
            "exact_groups": exact_groups,
            "removable": removable,
        }

    def _hash_file(self, path: Path):
        digest = hashlib.sha256()
        with path.open("rb") as file_obj:
            while True:
                chunk = file_obj.read(1024 * 1024)
                if not chunk:
                    break
                digest.update(chunk)
        return digest.hexdigest()

    def _duplicate_keep_sort_key(self, path: Path):
        stat = path.stat()
        return (stat.st_mtime, len(str(path)), str(path).lower())

    def finish_duplicate_scan(self, payload):
        self.dup_busy = False
        self.dup_results = payload
        exact_groups = payload.get("exact_groups", [])
        removable = payload.get("removable", [])
        same_size_groups = payload.get("same_size_groups", [])
        self.txt_dup_status.value = (
            f"Scan complete. Found {len(same_size_groups)} same-size groups, "
            f"{len(exact_groups)} exact duplicate groups, and {len(removable)} removable duplicate files."
        )
        self.txt_dup_status.color = COLOR_SUCCESS if exact_groups else COLOR_TEXT_SECONDARY
        self.refresh_duplicate_metrics()
        self.render_duplicate_results()
        self.log(self.txt_dup_status.value, COLOR_ACCENT if exact_groups else COLOR_TEXT_SECONDARY)

    def refresh_duplicate_metrics(self):
        self.txt_dup_files.value = str(len(self.dup_results.get("files", [])))
        self.txt_dup_size_groups.value = str(len(self.dup_results.get("same_size_groups", [])))
        self.txt_dup_exact_groups.value = str(len(self.dup_results.get("exact_groups", [])))
        self.txt_dup_removable.value = str(len(self.dup_results.get("removable", [])))
        has_removable = bool(self.dup_results.get("removable"))
        if self.dup_scan_button is not None:
            self.dup_scan_button.disabled = self.dup_busy
        if self.dup_move_button is not None:
            self.dup_move_button.disabled = self.dup_busy or not has_removable
        if self.dup_delete_button is not None:
            self.dup_delete_button.disabled = self.dup_busy or not has_removable

    def render_duplicate_results(self):
        self.dup_list.controls.clear()
        exact_groups = self.dup_results.get("exact_groups", [])
        if not exact_groups:
            self.dup_list.controls.append(
                ft.Text("No exact duplicate videos found in the current output folder.", color=COLOR_TEXT_SECONDARY)
            )
            return
        for index, group in enumerate(exact_groups[:50], start=1):
            dup_count = len(group["duplicates"])
            size_mb = group["size"] / (1024 * 1024) if group["size"] else 0
            self.dup_list.controls.append(
                ft.Container(
                    content=ft.Column(
                        [
                            ft.Text(f"Group {index}: {dup_count} removable copies | {size_mb:.2f} MB", weight=ft.FontWeight.BOLD, color=COLOR_TEXT_PRIMARY),
                            ft.Text(f"Keep: {group['keep']}", size=12, color=COLOR_SUCCESS, selectable=True),
                            ft.Text("Duplicates:", size=12, color=COLOR_WARNING),
                            ft.Column(
                                [ft.Text(path, size=12, color=COLOR_TEXT_SECONDARY, selectable=True) for path in group["duplicates"]],
                                spacing=4,
                            ),
                        ],
                        spacing=6,
                    ),
                    bgcolor=COLOR_SURFACE,
                    padding=14,
                    border_radius=12,
                    border=ft.Border.all(1, COLOR_BORDER),
                )
            )

    def move_duplicate_files(self, _event):
        removable = self.dup_results.get("removable", [])
        if not removable:
            self.txt_dup_status.value = "No exact duplicate copies are queued for review."
            self.txt_dup_status.color = COLOR_WARNING
            self._safe_update()
            return
        review_dir = Path(self.output_path) / "_duplicate_review"
        review_dir.mkdir(parents=True, exist_ok=True)
        moved = 0
        for raw_path in removable:
            source = Path(raw_path)
            if not source.exists():
                continue
            target = review_dir / source.name
            counter = 1
            while target.exists():
                target = review_dir / f"{source.stem}_{counter}{source.suffix}"
                counter += 1
            source.replace(target)
            moved += 1
        self.txt_dup_status.value = f"Moved {moved} duplicate files into {review_dir} for safe review."
        self.txt_dup_status.color = COLOR_SUCCESS
        self.log(self.txt_dup_status.value, COLOR_SUCCESS)
        self.start_duplicate_scan(None)

    def delete_duplicate_files(self, _event):
        removable = self.dup_results.get("removable", [])
        if not removable:
            self.txt_dup_status.value = "No exact duplicate copies are queued for deletion."
            self.txt_dup_status.color = COLOR_WARNING
            self._safe_update()
            return

        deleted = 0
        failed = 0
        for raw_path in removable:
            source = Path(raw_path)
            if not source.exists():
                continue
            try:
                source.unlink()
                deleted += 1
            except OSError:
                failed += 1

        if failed:
            self.txt_dup_status.value = f"Smart Delete removed {deleted} duplicates. {failed} files could not be deleted."
            self.txt_dup_status.color = COLOR_WARNING
        else:
            self.txt_dup_status.value = f"Smart Delete removed {deleted} verified duplicate files and kept one best copy per group."
            self.txt_dup_status.color = COLOR_SUCCESS
        self.log(self.txt_dup_status.value, COLOR_ERROR if deleted else COLOR_WARNING)
        self.start_duplicate_scan(None)

    def collect_settings(self):
        start_id = self._safe_int(self.in_start.value, None)
        end_id = self._safe_int(self.in_end.value, None)
        step = self._safe_int(self.in_step.value, None)
        max_size_mb = self._safe_float(self.in_size.value or "0", 0.0)
        base_url = self.in_url.value.strip()
        preset = (self.dropdown_preset.value or "balanced").lower()
        range_mode = (self.dropdown_range.value or "sequence").lower()
        if not base_url:
            raise ValueError("URL / Template is required.")
        if start_id is None or end_id is None:
            raise ValueError("Start ID and End ID must be numbers.")
        if step is None or step <= 0:
            raise ValueError("Step must be greater than zero.")
        if start_id > end_id:
            raise ValueError("Start ID must be less than or equal to End ID.")
        if max_size_mb < 0:
            raise ValueError("Size limit must be 0 or more.")
        if preset not in PRESET_DEFAULTS:
            preset = "balanced"
        if range_mode not in VALID_RANGE_MODES:
            range_mode = "sequence"
        p = PRESET_DEFAULTS[preset]
        return AppSettings(
            base_url=base_url,
            output_dir=self.output_path or self.settings.output_dir,
            start_id=start_id,
            end_id=end_id,
            step=step,
            range_mode=range_mode,
            performance_preset=preset,
            max_size_mb=max_size_mb,
            workers=p["workers"],
            threads=p["threads"],
            chunk_kb=p["chunk_kb"],
            timeout_seconds=p["timeout_seconds"],
            auto_tune=True,
            verify_downloads=False,
            skip_known_missing=False,
            retry_failed_only=False,
            silent_mode=False,
            cleanup_before_run=False,
            unsafe_ssl=True,
            history_limit=max(10, self.settings.history_limit),
        )

    def build_job(self, settings):
        base_url = settings.base_url.strip()
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
            max_bytes=int(settings.max_size_mb * 1024 * 1024) if settings.max_size_mb > 0 else 0,
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
            run_label=f"Run_{datetime.now().strftime('%Y%m%d_%H%M%S')}",
        )

    def apply_settings_to_form(self):
        self.in_url.value = self.settings.base_url
        self.in_start.value = str(self.settings.start_id)
        self.in_end.value = str(self.settings.end_id)
        self.in_step.value = str(self.settings.step)
        self.in_size.value = str(self.settings.max_size_mb)
        self.dropdown_range.value = self.settings.range_mode
        self.dropdown_preset.value = self.settings.performance_preset
        self.txt_output.value = self.output_path
        self.update_profile_summary()
        self.update_preview()
        self.set_status("SYSTEM READY", COLOR_SUCCESS)

    def update_profile_summary(self):
        preset = (self.dropdown_preset.value or "balanced").lower()
        p = PRESET_DEFAULTS.get(preset, PRESET_DEFAULTS["balanced"])
        messages = {
            "safe": "Low pressure and high stability for fragile sources.",
            "balanced": "Good default for most runs.",
            "fast": "Higher speed with moderate extra load.",
            "turbo": "High speed mode for stronger sources.",
        }
        self.txt_profile.value = f"{messages[preset]} {p['workers']} workers, {p['threads']} lanes, {p['chunk_kb']} KB blocks."

    def update_preview(self):
        start_id = self._safe_int(self.in_start.value, None)
        end_id = self._safe_int(self.in_end.value, None)
        step = self._safe_int(self.in_step.value, None)
        if start_id is None or end_id is None or step is None or step <= 0:
            self.txt_preview.value = "Check IDs"
            return
        count = len(range(start_id, end_id + 1, step)) if start_id <= end_id else 0
        self.txt_preview.value = f"{count} IDs, {self.dropdown_range.value or 'sequence'}"

    def refresh_button_states(self):
        if self.start_button is not None:
            self.start_button.disabled = self.is_running
        if self.pause_button is not None:
            self.pause_button.disabled = not self.is_running
            self.pause_button.icon = ft.Icons.PLAY_ARROW if self.is_paused else ft.Icons.PAUSE
        if self.stop_button is not None:
            self.stop_button.disabled = not self.is_running

    def set_status(self, message, color):
        self.txt_status.value = message
        self.txt_status.color = color

    def log_inspector(self, msg):
        self.log(msg, COLOR_TEXT_SECONDARY)

    def log(self, msg, color=COLOR_SUCCESS):
        self.console.controls.append(ft.Text(f"[{time.strftime('%H:%M:%S')}] {msg}", font_family="Consolas", size=12, color=color))
        if len(self.console.controls) > MAX_CONSOLE_LINES:
            self.console.controls = self.console.controls[-MAX_CONSOLE_LINES:]
        if self.ui_ready:
            self._safe_update()

    def _safe_update(self):
        if not self.ui_ready:
            return
        try:
            self.page.update()
        except Exception:
            pass

    def _normalize_settings(self, settings):
        out = AppSettings()
        out.base_url = str(settings.base_url or out.base_url).strip() or out.base_url
        out.output_dir = str(settings.output_dir or out.output_dir).strip() or out.output_dir
        out.start_id = self._safe_int(settings.start_id, out.start_id)
        out.end_id = self._safe_int(settings.end_id, out.end_id)
        out.step = max(1, self._safe_int(settings.step, out.step))
        out.range_mode = settings.range_mode if settings.range_mode in VALID_RANGE_MODES else out.range_mode
        out.performance_preset = settings.performance_preset if settings.performance_preset in PRESET_DEFAULTS else out.performance_preset
        out.max_size_mb = max(0.0, self._safe_float(settings.max_size_mb, out.max_size_mb))
        out.workers = max(1, self._safe_int(settings.workers, out.workers))
        out.threads = max(1, self._safe_int(settings.threads, out.threads))
        out.chunk_kb = max(64, self._safe_int(settings.chunk_kb, out.chunk_kb))
        out.timeout_seconds = max(5, self._safe_int(settings.timeout_seconds, out.timeout_seconds))
        out.auto_tune = bool(settings.auto_tune)
        out.verify_downloads = bool(settings.verify_downloads)
        out.skip_known_missing = bool(settings.skip_known_missing)
        out.retry_failed_only = bool(settings.retry_failed_only)
        out.silent_mode = bool(settings.silent_mode)
        out.cleanup_before_run = bool(settings.cleanup_before_run)
        out.unsafe_ssl = bool(settings.unsafe_ssl)
        out.history_limit = max(10, self._safe_int(settings.history_limit, out.history_limit))
        return out

    def _safe_int(self, value, fallback):
        try:
            return int(value)
        except (TypeError, ValueError):
            return fallback

    def _safe_float(self, value, fallback):
        try:
            return float(value)
        except (TypeError, ValueError):
            return fallback

    def format_eta(self, seconds):
        seconds = int(max(seconds, 0))
        minutes, sec = divmod(seconds, 60)
        hours, minutes = divmod(minutes, 60)
        if hours:
            return f"{hours}h {minutes}m {sec}s"
        if minutes:
            return f"{minutes}m {sec}s"
        return f"{sec}s"


def main(page: ft.Page):
    DownloaderApp(page)


if __name__ == "__main__":
    ft.run(main)
