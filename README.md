
# GDELT GKG Bulk Downloader & Extractor

**Version 0.7 – 2025‑07‑23**

A high‑throughput Python utility that downloads every [GDELT Project](https://www.gdeltproject.org/) **Global Knowledge Graph (GKG)** zipped CSV file listed in *masterfilelist.txt*, then extracts them into year‑based folders.

---

## Key Features

* **Three workflows**  
  * **Download → Extract** *(default)* – stream each ZIP and immediately extract it.  
  * **Download‑only** – keep ZIPs under `YEAR/rawdata/`; extract later with a separate command.  
  * **Extract‑only** – parallel‑unzip pre‑downloaded ZIPs without any network traffic.

* **Parallelism everywhere**  
  * Multithreaded **download** (`--workers`, default CPU × 4).  
  * Multi‑**process** or multi‑thread **extraction** (`--extract-mode {process,thread}`, default process; `--extract-workers`, default CPU × 2).

* **Smart I/O layout**  
  * One folder per year (`data/2015/`, `data/2016/` …).  
  * ZIPs stored in `rawdata/` (name configurable with `--raw-subdir`).  
  * Skips files already present; safe to re‑run at any time.

* **Resilient & informative**  
  * Automatic retries with exponential back‑off.  
  * 404 files are skipped instantly.  
  * `failures.log` lists any unrecoverable files.  
  * Dual progress bars: files/s **and** optional MB/s (`--bytes-progress`).  
  * Adjustable urllib3 connection pool (`--pool-size`) to remove “Connection pool is full” warnings.

---

## Quick Start

```bash
git clone https://github.com/<you>/gdelt-bulk-downloader.git
cd gdelt-bulk-downloader
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt               # requests, tqdm

# 1) Fast download, keep ZIPs
python gdelt_bulk_downloader.py masterfilelist.txt -o data \
       -w 64 --shuffle --skip-extract --bytes-progress

# 2) Multi‑process extract (CPU×2 workers on a 12‑core host)
python gdelt_bulk_downloader.py --extract-only -o data \
       --extract-mode process --extract-workers 24
```

---

## Command‑line Options (abridged)

| Option | Default | Description |
|--------|---------|-------------|
| `-w, --workers` | CPU×4 | Download thread count |
| `--pool-size` | = workers | urllib3 keep‑alive pool per host |
| `--skip-extract` | ‑ | Download ZIPs only |
| `--extract-only` | ‑ | Extract ZIPs only |
| `--extract-mode {process,thread}` | process | Parallelism model for extraction |
| `--extract-workers` | CPU×2 | Workers for extraction |
| `--raw-subdir` | rawdata | Subfolder for ZIPs inside each year |
| `--year-from / --year-to` | ‑ | Inclusive year filter |
| `--shuffle` | false | Shuffle download queue |
| `--bytes-progress` | false | Show MB/s progress bar |
| `-r, --retries` | 3 | Download retry attempts |

Run `python gdelt_bulk_downloader.py -h` to see the full list.

---

## Hardware Tips

* **CPU cores** – Download mostly I/O‑bound; extraction benefits from ≥ core count workers.  
* **Disk** – A single HDD can write ~150 MB/s. If network is faster, keep extraction workers modest or use an SSD scratch.  
* **Network** – GDELT CDN often caps bandwidth per IP; use multiple machines or proxies for higher aggregate throughput.

---

## License

MIT License © 2025 <Yi Wang>
