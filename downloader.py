import argparse
import concurrent.futures
import os
import random
import threading
import time
from pathlib import Path

import requests
import urllib3


urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

BASE_URL = "https://ser3.masahub.cc/myfiless/id/{}.mp4"
SAVE_DIR = "downloaded_videos"
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36",
]

print_lock = threading.Lock()


def parse_args():
    parser = argparse.ArgumentParser(description="Fast bulk downloader for sequential asset IDs.")
    parser.add_argument("--start", type=int, required=True, help="First numeric ID to download.")
    parser.add_argument("--end", type=int, required=True, help="Last numeric ID to download.")
    parser.add_argument("--workers", type=int, default=8, help="Parallel downloads.")
    parser.add_argument("--max-size-mb", type=float, default=200, help="Skip files larger than this limit.")
    parser.add_argument("--output", default=SAVE_DIR, help="Output directory.")
    parser.add_argument("--timeout", type=int, default=20, help="Request timeout in seconds.")
    parser.add_argument("--step", type=int, default=1, help="ID step size.")
    return parser.parse_args()


def probe(session, url, timeout):
    try:
        response = session.head(url, timeout=timeout, verify=False, allow_redirects=True)
        if response.status_code == 404:
            return response.status_code, 0
        if response.ok:
            return response.status_code, int(response.headers.get("Content-Length", "0") or 0)
    except requests.RequestException:
        pass

    try:
        response = session.get(url, timeout=timeout, verify=False, stream=True)
        status_code = response.status_code
        size = int(response.headers.get("Content-Length", "0") or 0)
        response.close()
        return status_code, size
    except requests.RequestException:
        return 0, 0


def download_video(video_id, base_url, save_dir, max_size_bytes, timeout):
    save_dir.mkdir(parents=True, exist_ok=True)
    url = base_url.format(video_id)
    file_path = save_dir / f"video_{video_id}.mp4"

    session = requests.Session()
    session.headers.update({"User-Agent": random.choice(USER_AGENTS)})

    try:
        status_code, size = probe(session, url, timeout)
        if status_code == 404:
            return video_id, "missing", "not found"
        if size and size > max_size_bytes:
            return video_id, "skipped", f"too large ({size / (1024 * 1024):.1f} MB)"
        if file_path.exists() and (size == 0 or file_path.stat().st_size == size):
            return video_id, "skipped", "already downloaded"

        with session.get(url, timeout=timeout, verify=False, stream=True) as response:
            if not response.ok:
                return video_id, "failed", f"http {response.status_code}"
            with open(file_path, "wb") as file_obj:
                for chunk in response.iter_content(chunk_size=1024 * 512):
                    if chunk:
                        file_obj.write(chunk)
        return video_id, "success", "downloaded"
    except requests.RequestException as exc:
        return video_id, "failed", str(exc)
    finally:
        session.close()


def log_result(video_id, state, detail):
    with print_lock:
        print(f"[{state.upper():7}] ID {video_id}: {detail}")


def main():
    args = parse_args()
    if args.start > args.end:
        raise SystemExit("--start must be less than or equal to --end")
    if args.workers < 1 or args.step < 1:
        raise SystemExit("--workers and --step must be positive")

    save_dir = Path(args.output)
    ids = list(range(args.start, args.end + 1, args.step))
    max_size_bytes = int(args.max_size_mb * 1024 * 1024)
    started_at = time.time()

    print(f"Queueing {len(ids)} downloads with {args.workers} workers into {save_dir}")

    counts = {"success": 0, "skipped": 0, "missing": 0, "failed": 0}
    with concurrent.futures.ThreadPoolExecutor(max_workers=args.workers) as executor:
        futures = [
            executor.submit(download_video, video_id, BASE_URL, save_dir, max_size_bytes, args.timeout)
            for video_id in ids
        ]
        for future in concurrent.futures.as_completed(futures):
            video_id, state, detail = future.result()
            counts[state] += 1
            log_result(video_id, state, detail)

    elapsed = max(time.time() - started_at, 0.1)
    print(
        "Finished in "
        f"{elapsed:.1f}s | success={counts['success']} skipped={counts['skipped']} "
        f"missing={counts['missing']} failed={counts['failed']}"
    )


if __name__ == "__main__":
    main()
