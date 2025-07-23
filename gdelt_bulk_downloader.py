"""gdelt_bulk_downloader.py –v0.7(2025‑07‑23)
================================================
A highly parallel downloader+extractor for **GDELT GKG** zipped CSV files.

Key features
------------
* **Three workflows**
  * `--skip-extract`→ download only, save ZIPs to `.../YEAR/<raw>/`.
  * `--extract-only`→ extract existing ZIPs in parallel into `.../YEAR/`.
  * *Default*→ download each ZIP and immediately extract it.
* **Multithreaded downloads**`-w / --workers` (default CPU×4).
* **Parallel extraction** – `--extract-mode {process,thread}` (default *process*) and
  `--extract-workers N` (default CPU×2).
* **Adjustable connection pool**–`--pool-size N` (default=`workers`) to avoid
  *urllib3* “Connection pool is full” warnings and improve keep‑alive reuse.
* Optional **shuffled queue** (`--shuffle`), **year range filter**, dual progress
  bars (`--bytes-progress`), automatic 404 skip, failure log.

Quick example
-------------
```bash
# Step1 –download only (ZIPs → rawdata/)
python gdelt_bulk_downloader.py masterfilelist.txt -o data -w 64 \
       --shuffle --skip-extract

# Step2 –parallel extraction with 80 processes on a 40‑core host
python gdelt_bulk_downloader.py --extract-only -o data \
       --extract-mode process --extract-workers 80
```"""

from __future__ import annotations

import argparse
import concurrent.futures as cf
import logging
import os
import random
import sys
import time
from pathlib import Path
from typing import List, Tuple
import zipfile

import requests
from requests.adapters import HTTPAdapter
from tqdm import tqdm

# ----------------------------- CLI & globals -----------------------------


def parse_args() -> argparse.Namespace:
    """Parse command‑line arguments."""
    p = argparse.ArgumentParser(
        description="Bulk download and extract GDELT GKG .zip files listed in masterfilelist.txt",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    p.add_argument("masterfiles", nargs="*", default=["masterfilelist.txt"],
                   help="Path(s) to masterfilelist.txt")
    p.add_argument("-o", "--output", default="data", help="Root output folder")
    p.add_argument("-w", "--workers", type=int, default=os.cpu_count() * 4,
                   help="Thread workers for downloading")
    p.add_argument("--pool-size", type=int, help="urllib3 connection pool size (per host)")
    p.add_argument("--shuffle", action="store_true", help="Randomise download order")
    p.add_argument("--year-from", type=int, help="Earliest year to download/extract (inclusive)")
    p.add_argument("--year-to", type=int, help="Last year to download/extract (inclusive)")
    p.add_argument("--skip-extract", action="store_true", help="Download ZIPs but do not extract")
    p.add_argument("--extract-only", action="store_true", help="Extract existing ZIPs only; skip downloading")
    p.add_argument("--raw-subdir", default="rawdata", help="Subfolder under each year to store ZIPs")
    p.add_argument("--extract-workers", type=int, default=os.cpu_count() * 2,
                   help="Worker count for extraction phase")
    p.add_argument("--extract-mode", choices=["thread", "process"], default="process",
                   help="Parallel mode for extraction workers")
    p.add_argument("--bytes-progress", action="store_true", help="Show a second progress bar for bytes/s during download")
    p.add_argument("-r", "--retries", type=int, default=3, help="Retry attempts for each download")
    return p.parse_args()


args = parse_args()

if args.skip_extract and args.extract_only:
    sys.exit("--skip-extract and --extract-only are mutually exclusive.")

RAW_SUBDIR = args.raw_subdir
OUTPUT_ROOT = Path(args.output)
OUTPUT_ROOT.mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="[%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("gdelt")

# ----------------------------- Helpers -----------------------------

Task = Tuple[int, str, int]  # (size_bytes, url, year)


def parse_masterfiles() -> Tuple[List[Task], int]:
    """Return a list of tasks and total bytes to download."""
    tasks: List[Task] = []
    total_bytes = 0
    for mfile in args.masterfiles:
        with open(mfile, "r", encoding="utf-8") as fh:
            for line in fh:
                parts = line.strip().split()
                if len(parts) != 3:
                    continue
                size, _hash, url = parts
                if not url.endswith(".gkg.csv.zip"):
                    continue
                try:
                    size_int = int(size)
                except ValueError:
                    continue
                # The first 14 chars of filename are the timestamp; first 4 are the year.
                try:
                    ts = Path(url).name.split(".")[0]  # e.g. 20150218230000
                    year = int(ts[:4])
                except Exception:
                    continue
                if args.year_from and year < args.year_from:
                    continue
                if args.year_to and year > args.year_to:
                    continue
                tasks.append((size_int, url, year))
                total_bytes += size_int
    if args.shuffle:
        random.shuffle(tasks)
    return tasks, total_bytes


# ----------------------------- Download phase -----------------------------


def make_session() -> requests.Session:
    """Create a `requests` Session with a custom connection pool size."""
    s = requests.Session()
    adapter = HTTPAdapter(pool_connections=args.pool_size or args.workers,
                          pool_maxsize=args.pool_size or args.workers)
    s.mount("http://", adapter)
    s.mount("https://", adapter)
    return s


def download_task(task: Task, session: requests.Session,
                  pbar_files: tqdm, pbar_bytes: tqdm | None):
    """Download a single ZIP and optionally extract it."""
    size, url, year = task
    fname = Path(url).name  # 20150218230000.gkg.csv.zip
    year_dir = OUTPUT_ROOT / str(year)
    raw_dir = year_dir / RAW_SUBDIR
    raw_dir.mkdir(parents=True, exist_ok=True)
    zip_path = raw_dir / fname
    csv_name = fname[:-4]  # strip .zip
    csv_path = year_dir / csv_name

    # Skip if already extracted
    if csv_path.exists():
        pbar_files.update(1)
        if pbar_bytes:
            pbar_bytes.update(size)
        return

    # Download if ZIP missing
    if not zip_path.exists():
        for attempt in range(1, args.retries + 1):
            try:
                with session.get(url, stream=True, timeout=30) as resp:
                    if resp.status_code == 404:
                        logger.warning("404 Not Found – skipped: %s", url)
                        return
                    resp.raise_for_status()
                    with open(zip_path, "wb") as out:
                        for chunk in resp.iter_content(chunk_size=1 << 20):
                            if chunk:
                                out.write(chunk)
                                if pbar_bytes:
                                    pbar_bytes.update(len(chunk))
                break  # success
            except Exception as e:
                if attempt == args.retries:
                    logger.error("FAILED (%s): %s", e.__class__.__name__, url)
                    (OUTPUT_ROOT / "failures.log").open("a").write(url + "\n")
                    return
                time.sleep(2 ** attempt)
    else:
        if pbar_bytes:
            pbar_bytes.update(size)

    # Extract unless the user skipped it
    if not args.skip_extract:
        try:
            with zipfile.ZipFile(zip_path) as zf:
                zf.extractall(year_dir)
        except Exception as e:
            logger.error("Extract error (%s): %s", e.__class__.__name__, zip_path)
            (OUTPUT_ROOT / "failures.log").open("a").write(str(zip_path) + "\n")
    pbar_files.update(1)


# ----------------------------- Extraction‑only phase -----------------------------


def extract_one(zip_path: Path, year_dir: Path):
    """Extract a single ZIP file."""
    try:
        with zipfile.ZipFile(zip_path) as zf:
            zf.extractall(year_dir)
    except Exception as e:
        logger.error("Extract error (%s): %s", e.__class__.__name__, zip_path)
        (OUTPUT_ROOT / "failures.log").open("a").write(str(zip_path) + "\n")


# ----------------------------- Main -----------------------------


def main():
    if args.extract_only:
        # ---------------- Extraction phase ----------------
        zip_paths: List[Path] = list(OUTPUT_ROOT.rglob(f"{RAW_SUBDIR}/*.zip"))
        if args.year_from or args.year_to:
            zip_paths = [p for p in zip_paths if (args.year_from or 0) <= int(p.parts[-3]) <= (args.year_to or 9999)]
        logger.info("Starting extraction of %d ZIP files – %d %s workers", len(zip_paths), args.extract_workers, args.extract_mode)
        pbar = tqdm(total=len(zip_paths), unit="zip")
        executor_cls = cf.ProcessPoolExecutor if args.extract_mode == "process" else cf.ThreadPoolExecutor
        with executor_cls(max_workers=args.extract_workers) as ex:
            futures = [ex.submit(extract_one, zp, zp.parents[2]) for zp in zip_paths]
            for _ in cf.as_completed(futures):
                pbar.update(1)
        pbar.close()
        logger.info("Extraction finished.")
        return

    # ---------------- Download (and optional extraction) phase --------------
    tasks, total_bytes = parse_masterfiles()
    logger.info("Starting download of %d files – %d workers", len(tasks), args.workers)

    pbar_files = tqdm(total=len(tasks), unit="file", desc="Files", position=0)
    pbar_bytes = None
    if args.bytes_progress:
        pbar_bytes = tqdm(total=total_bytes, unit="B", unit_scale=True, unit_divisor=1024, desc="Bytes", position=1)

    session_factory = make_session

    def thread_fn(task_: Task, sess_: requests.Session):
        download_task(task_, sess_, pbar_files, pbar_bytes)

    with cf.ThreadPoolExecutor(max_workers=args.workers) as executor:
        sessions = [session_factory() for _ in range(args.workers)]
        for sess in sessions:
            executor.submit(lambda: None)  # warm‑up noop
        for t_index, t in enumerate(tasks):
            sess = sessions[t_index % len(sessions)]
            executor.submit(thread_fn, t, sess)

    pbar_files.close()
    if pbar_bytes:
        pbar_bytes.close()
    logger.info("Download phase finished.")


if __name__ == "__main__":
    main()


