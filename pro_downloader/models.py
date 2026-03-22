from dataclasses import dataclass, field
from typing import Optional


@dataclass
class AppSettings:
    base_url: str = "https://ser3.masahub.cc/myfiless/id/{}.mp4"
    output_dir: str = "downloaded_videos"
    start_id: int = 68000
    end_id: int = 68010
    step: int = 1
    range_mode: str = "sequence"
    max_size_mb: float = 200.0
    workers: int = 12
    threads: int = 6
    chunk_kb: int = 1024
    silent_mode: bool = False
    cleanup_before_run: bool = True
    timeout_seconds: int = 20


@dataclass
class DownloadJob:
    ids: list[int]
    base_url: str
    save_dir: str
    max_bytes: int
    workers: int
    threads: int
    chunk_size: int
    timeout_seconds: int
    silent_mode: bool = False
    clean_before_run: bool = True
    run_label: str = ""


@dataclass
class FileProbe:
    exists: bool
    size: int
    supports_ranges: bool
    status: int


@dataclass
class RuntimeStats:
    total: int = 0
    processed: int = 0
    success: int = 0
    skipped: int = 0
    failed: int = 0
    missing: int = 0
    retries: int = 0
    bytes_downloaded: int = 0
    active_files: int = 0
    started_at: float = 0.0


@dataclass
class HistoryRecord:
    started_at: str
    finished_at: str
    run_label: str
    total: int
    success: int
    skipped: int
    failed: int
    missing: int
    retries: int
    bytes_downloaded: int
    avg_speed_mbps: float
    notes: str = ""


@dataclass
class ProxyEndpoint:
    raw: str
    successes: int = 0
    failures: int = 0
    cooldown_until: float = 0.0

    @property
    def score(self) -> int:
        return self.successes - (self.failures * 2)


@dataclass
class DownloadResult:
    video_id: int
    result: str
    status: str
    size_mb: str = "-"
    retries: int = 0
    bytes_written: int = 0
    notes: Optional[str] = None


@dataclass
class PartialFileState:
    path: str
    start: int
    end: int
    existing_bytes: int = 0
    completed: bool = False


@dataclass
class RunContext:
    run_id: Optional[int] = None
    status_cache: dict[int, str] = field(default_factory=dict)
