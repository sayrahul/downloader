import json
import sqlite3
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path

from models import AppSettings, HistoryRecord


SETTINGS_FILE = "proventure_settings.json"
DB_FILE = "proventure_state.db"


class AppStorage:
    def __init__(self, root_dir: Path):
        self.root_dir = Path(root_dir)
        self.settings_path = self.root_dir / SETTINGS_FILE
        self.db_path = self.root_dir / DB_FILE
        self._init_db()

    def load_settings(self) -> AppSettings:
        if not self.settings_path.exists():
            return AppSettings()
        try:
            data = json.loads(self.settings_path.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError):
            return AppSettings()

        settings = AppSettings()
        for key, value in data.items():
            if hasattr(settings, key):
                setattr(settings, key, value)
        return settings

    def save_settings(self, settings: AppSettings) -> None:
        self.settings_path.write_text(json.dumps(settings.__dict__, indent=2), encoding="utf-8")

    def record_run_start(self, run_label: str, total: int, settings: AppSettings) -> int:
        with self._connect() as conn:
            cursor = conn.execute(
                """
                INSERT INTO runs (
                    started_at, finished_at, run_label, total, success, skipped, failed,
                    missing, retries, bytes_downloaded, avg_speed_mbps, notes, settings_json
                ) VALUES (?, ?, ?, ?, 0, 0, 0, 0, 0, 0, 0, ?, ?)
                """,
                (self._now(), "", run_label, total, "running", json.dumps(settings.__dict__)),
            )
            return int(cursor.lastrowid)

    def record_run_finish(self, run_id: int, record: HistoryRecord, settings: AppSettings) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                UPDATE runs
                SET finished_at = ?, run_label = ?, total = ?, success = ?, skipped = ?, failed = ?,
                    missing = ?, retries = ?, bytes_downloaded = ?, avg_speed_mbps = ?, notes = ?, settings_json = ?
                WHERE id = ?
                """,
                (
                    record.finished_at,
                    record.run_label,
                    record.total,
                    record.success,
                    record.skipped,
                    record.failed,
                    record.missing,
                    record.retries,
                    record.bytes_downloaded,
                    record.avg_speed_mbps,
                    record.notes,
                    json.dumps(settings.__dict__),
                    run_id,
                ),
            )

    def record_item(self, run_id: int, video_id: int, result: str, status: str, size_mb: str, retries: int) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO run_items (run_id, video_id, result, status, size_mb, retries, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (run_id, video_id, result, status, size_mb, retries, self._now()),
            )

    def load_recent_runs(self, limit: int = 20) -> list[sqlite3.Row]:
        with self._connect() as conn:
            cursor = conn.execute(
                """
                SELECT started_at, finished_at, run_label, total, success, skipped, failed, missing,
                       retries, bytes_downloaded, avg_speed_mbps, notes
                FROM runs
                ORDER BY id DESC
                LIMIT ?
                """,
                (limit,),
            )
            return cursor.fetchall()

    def load_known_status_cache(self) -> dict[int, str]:
        with self._connect() as conn:
            cursor = conn.execute(
                """
                SELECT video_id, result
                FROM run_items
                WHERE id IN (SELECT MAX(id) FROM run_items GROUP BY video_id)
                """
            )
            return {int(row["video_id"]): row["result"] for row in cursor.fetchall()}

    def _init_db(self) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS runs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    started_at TEXT NOT NULL,
                    finished_at TEXT NOT NULL,
                    run_label TEXT NOT NULL,
                    total INTEGER NOT NULL,
                    success INTEGER NOT NULL,
                    skipped INTEGER NOT NULL,
                    failed INTEGER NOT NULL,
                    missing INTEGER NOT NULL,
                    retries INTEGER NOT NULL,
                    bytes_downloaded INTEGER NOT NULL,
                    avg_speed_mbps REAL NOT NULL,
                    notes TEXT NOT NULL,
                    settings_json TEXT NOT NULL
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS run_items (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    run_id INTEGER NOT NULL,
                    video_id INTEGER NOT NULL,
                    result TEXT NOT NULL,
                    status TEXT NOT NULL,
                    size_mb TEXT NOT NULL,
                    retries INTEGER NOT NULL,
                    updated_at TEXT NOT NULL,
                    FOREIGN KEY (run_id) REFERENCES runs(id)
                )
                """
            )

    @contextmanager
    def _connect(self):
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        try:
            yield conn
            conn.commit()
        finally:
            conn.close()

    def _now(self) -> str:
        return datetime.now().isoformat(timespec="seconds")
