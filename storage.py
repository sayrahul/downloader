import json
import sqlite3
from contextlib import contextmanager
from dataclasses import asdict
from datetime import datetime
from pathlib import Path

from models import ActiveRunState, AppSettings, HistoryRecord


SETTINGS_FILE = "proventure_settings.json"
DB_FILE = "proventure_state.db"
ACTIVE_RUN_FILE = "proventure_active_run.json"


class AppStorage:
    def __init__(self, root_dir: Path):
        self.root_dir = Path(root_dir)
        self.settings_path = self.root_dir / SETTINGS_FILE
        self.db_path = self.root_dir / DB_FILE
        self.active_run_path = self.root_dir / ACTIVE_RUN_FILE
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
        self.settings_path.write_text(json.dumps(asdict(settings), indent=2), encoding="utf-8")

    def save_active_run(self, run_id: int, ids: list[int], settings: AppSettings) -> None:
        payload = ActiveRunState(run_id=run_id, ids=ids, started_at=self._now(), settings=asdict(settings))
        self.active_run_path.write_text(json.dumps(asdict(payload), indent=2), encoding="utf-8")

    def update_active_run_ids(self, ids: list[int]) -> None:
        active_run = self.load_active_run()
        if not active_run:
            return
        active_run.ids = ids
        self.active_run_path.write_text(json.dumps(asdict(active_run), indent=2), encoding="utf-8")

    def load_active_run(self) -> ActiveRunState | None:
        if not self.active_run_path.exists():
            return None
        try:
            payload = json.loads(self.active_run_path.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError):
            return None
        return ActiveRunState(
            run_id=int(payload.get("run_id", 0)),
            ids=[int(item) for item in payload.get("ids", [])],
            started_at=str(payload.get("started_at", "")),
            settings=dict(payload.get("settings", {})),
        )

    def clear_active_run(self) -> None:
        try:
            self.active_run_path.unlink(missing_ok=True)
        except OSError:
            pass

    def record_run_start(self, run_label: str, total: int, settings: AppSettings) -> int:
        with self._connect() as conn:
            cursor = conn.execute(
                """
                INSERT INTO runs (
                    started_at, finished_at, run_label, total, success, skipped, failed,
                    missing, retries, bytes_downloaded, avg_speed_mbps, notes, settings_json
                ) VALUES (?, ?, ?, ?, 0, 0, 0, 0, 0, 0, 0, ?, ?)
                """,
                (self._now(), "", run_label, total, "running", json.dumps(asdict(settings))),
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
                    json.dumps(asdict(settings)),
                    run_id,
                ),
            )

    def record_items(self, rows: list[tuple[int, int, str, str, str, int, int, str, str]]) -> None:
        if not rows:
            return
        with self._connect() as conn:
            conn.executemany(
                """
                INSERT INTO run_items (
                    run_id, video_id, result, status, size_mb, retries, http_status, content_type, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                rows,
            )

    def load_recent_runs(self, limit: int = 20, search: str = "") -> list[sqlite3.Row]:
        where_clause = ""
        params: list[object] = []
        if search.strip():
            where_clause = "WHERE run_label LIKE ? OR notes LIKE ? OR settings_json LIKE ?"
            pattern = f"%{search.strip()}%"
            params.extend([pattern, pattern, pattern])
        params.append(limit)
        with self._connect() as conn:
            cursor = conn.execute(
                f"""
                SELECT started_at, finished_at, run_label, total, success, skipped, failed, missing,
                       retries, bytes_downloaded, avg_speed_mbps, notes
                FROM runs
                {where_clause}
                ORDER BY id DESC
                LIMIT ?
                """,
                tuple(params),
            )
            return cursor.fetchall()

    def load_run_items(self, run_label: str, limit: int = 200) -> list[sqlite3.Row]:
        with self._connect() as conn:
            cursor = conn.execute(
                """
                SELECT run_items.video_id, run_items.result, run_items.status, run_items.size_mb,
                       run_items.retries, run_items.http_status, run_items.content_type, run_items.updated_at
                FROM run_items
                JOIN runs ON runs.id = run_items.run_id
                WHERE runs.run_label = ?
                ORDER BY run_items.id DESC
                LIMIT ?
                """,
                (run_label, limit),
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
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA synchronous=NORMAL")
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
                    http_status INTEGER NOT NULL DEFAULT 0,
                    content_type TEXT NOT NULL DEFAULT '',
                    updated_at TEXT NOT NULL,
                    FOREIGN KEY (run_id) REFERENCES runs(id)
                )
                """
            )
            self._ensure_column(conn, "run_items", "http_status", "INTEGER NOT NULL DEFAULT 0")
            self._ensure_column(conn, "run_items", "content_type", "TEXT NOT NULL DEFAULT ''")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_runs_label ON runs(run_label)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_items_run_id ON run_items(run_id)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_items_video_id ON run_items(video_id)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_items_result ON run_items(result)")

    def _ensure_column(self, conn: sqlite3.Connection, table_name: str, column_name: str, column_sql: str) -> None:
        columns = {row["name"] for row in conn.execute(f"PRAGMA table_info({table_name})")}
        if column_name not in columns:
            conn.execute(f"ALTER TABLE {table_name} ADD COLUMN {column_name} {column_sql}")

    @contextmanager
    def _connect(self):
        conn = sqlite3.connect(self.db_path, timeout=30, check_same_thread=False)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA busy_timeout=5000")
        try:
            yield conn
            conn.commit()
        finally:
            conn.close()

    def _now(self) -> str:
        return datetime.now().isoformat(timespec="seconds")
