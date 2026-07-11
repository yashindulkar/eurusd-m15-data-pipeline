#!/usr/bin/env python3
"""Download raw EUR/USD data with source fallback.

Preferred source order:
1. Dukascopy hourly tick .bi5 files.
2. TrueFX monthly historical tick zip files.
3. HistData yearly M1 zip files.

The downloader is intentionally conservative. It keeps raw source files
untouched, writes a manifest, and is safe to resume.
"""

from __future__ import annotations

import argparse
import concurrent.futures as futures
import html
import json
import os
import re
from dataclasses import asdict, dataclass, field
from datetime import UTC, date, datetime, time, timedelta
from pathlib import Path
from typing import Iterable

import requests


SYMBOL = "EURUSD"
DISPLAY_SYMBOL = "EUR/USD"
DUKASCOPY_BASE_URL = "https://datafeed.dukascopy.com/datafeed"
TRUEFX_BASE_URLS = ("https://www.truefx.com/dev/data", "https://truefx.com/dev/data")
HISTDATA_URL_CANDIDATES = (
    "https://www.histdata.com/download-free-forex-data/?/ascii/1-minute-bar-quotes/{symbol}/{year}",
    "https://www.histdata.com/download-free-forex-historical-data/?/ascii/1-minute-bar-quotes/{symbol}/{year}",
)
HISTDATA_POST_URL = "https://www.histdata.com/get.php"


@dataclass
class DownloadStats:
    source: str
    requested_start: str
    requested_end: str
    status: str = "not_started"
    attempted_files: int = 0
    downloaded_files: int = 0
    existing_files: int = 0
    missing_or_closed_files: int = 0
    failed_files: int = 0
    raw_files: list[str] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)


def parse_args() -> argparse.Namespace:
    yesterday_utc = date.today() - timedelta(days=1)
    parser = argparse.ArgumentParser(description="Download raw EUR/USD data.")
    parser.add_argument("--start", default="2010-01-01", help="Start date YYYY-MM-DD, inclusive.")
    parser.add_argument(
        "--end",
        default=yesterday_utc.isoformat(),
        help="End date YYYY-MM-DD, inclusive. Defaults to latest complete UTC day.",
    )
    parser.add_argument("--raw-dir", default="data/raw", help="Raw data directory.")
    parser.add_argument("--workers", type=int, default=8, help="Parallel download workers.")
    parser.add_argument("--timeout", type=int, default=60, help="Per-read HTTP timeout seconds.")
    parser.add_argument("--retries", type=int, default=2, help="Retries per raw file.")
    parser.add_argument("--force", action="store_true", help="Redownload existing raw files.")
    parser.add_argument(
        "--source-priority",
        default="dukascopy,truefx,histdata",
        help="Comma-separated source priority list.",
    )
    parser.add_argument(
        "--max-hours",
        type=int,
        default=None,
        help="Limit Dukascopy hourly files for smoke/chunked runs.",
    )
    parser.add_argument(
        "--max-months",
        type=int,
        default=None,
        help="Limit TrueFX monthly files for smoke/chunked runs.",
    )
    parser.add_argument(
        "--max-years",
        type=int,
        default=None,
        help="Limit HistData yearly files for smoke/chunked runs.",
    )
    return parser.parse_args()


def date_range(start: date, end: date) -> Iterable[date]:
    current = start
    while current <= end:
        yield current
        current += timedelta(days=1)


def hour_range(start: date, end: date) -> Iterable[datetime]:
    current = datetime.combine(start, time.min, tzinfo=UTC)
    stop = datetime.combine(end + timedelta(days=1), time.min, tzinfo=UTC)
    while current < stop:
        yield current
        current += timedelta(hours=1)


def likely_fx_open_utc(ts: datetime) -> bool:
    """Broad prefilter to avoid obvious weekend HTTP misses.

    Validation uses a stricter New-York-close market calendar. This prefilter is
    only for download efficiency and intentionally keeps Sunday evening broad.
    """
    if ts.weekday() == 5:
        return False
    if ts.weekday() == 6 and ts.hour < 20:
        return False
    if ts.weekday() == 4 and ts.hour >= 23:
        return False
    return True


def dukascopy_url(ts: datetime) -> str:
    month_zero_based = ts.month - 1
    return (
        f"{DUKASCOPY_BASE_URL}/{SYMBOL}/"
        f"{ts.year}/{month_zero_based:02d}/{ts.day:02d}/{ts.hour:02d}h_ticks.bi5"
    )


def dukascopy_path(raw_dir: Path, ts: datetime) -> Path:
    return (
        raw_dir
        / "dukascopy"
        / SYMBOL
        / f"{ts.year:04d}"
        / f"{ts.month:02d}"
        / f"{ts.day:02d}"
        / f"{ts.hour:02d}h_ticks.bi5"
    )


def http_get_to_file(url: str, path: Path, timeout: int, force: bool, retries: int = 2) -> tuple[str, str | None]:
    if path.exists() and path.stat().st_size > 0 and not force:
        return "existing", None

    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_suffix(path.suffix + ".part")
    headers = {
        "User-Agent": "codex-quant-data-pipeline/1.0",
        "Accept": "*/*",
    }
    last_error: str | None = None
    for _ in range(max(1, retries + 1)):
        bytes_written = 0
        try:
            with requests.get(url, headers=headers, timeout=(15, timeout), stream=True) as response:
                if response.status_code in {403, 401}:
                    return "blocked", f"{response.status_code} {url}"
                if response.status_code == 404:
                    return "missing", None
                if response.status_code != 200:
                    return "failed", f"{response.status_code} {url}"
                with tmp_path.open("wb") as out:
                    for chunk in response.iter_content(chunk_size=1024 * 256):
                        if chunk:
                            out.write(chunk)
                            bytes_written += len(chunk)
            if bytes_written <= 0:
                return "missing", None
            os.replace(tmp_path, path)
            return "downloaded", None
        except requests.RequestException as exc:
            last_error = f"{type(exc).__name__}: {url}: {exc}"
        finally:
            if tmp_path.exists():
                tmp_path.unlink(missing_ok=True)
    return "failed", last_error


def probe_dukascopy(hours: list[datetime], raw_dir: Path, timeout: int, force: bool) -> tuple[bool, list[str]]:
    errors: list[str] = []
    for ts in hours[:96]:
        status, error = http_get_to_file(dukascopy_url(ts), dukascopy_path(raw_dir, ts), timeout, force)
        if status in {"downloaded", "existing"}:
            return True, errors
        if error:
            errors.append(error)
        if status == "blocked":
            return False, errors
    return False, errors


def download_dukascopy(args: argparse.Namespace, start: date, end: date, raw_dir: Path) -> DownloadStats:
    stats = DownloadStats("dukascopy", start.isoformat(), end.isoformat(), status="running")
    hours = [ts for ts in hour_range(start, end) if likely_fx_open_utc(ts)]
    if args.max_hours is not None:
        hours = hours[: args.max_hours]
    if not hours:
        stats.status = "failed"
        stats.errors.append("No candidate Dukascopy hours in requested range.")
        return stats

    ok, probe_errors = probe_dukascopy(hours, raw_dir, args.timeout, args.force)
    stats.errors.extend(probe_errors[:20])
    if not ok:
        stats.status = "failed"
        stats.errors.append("Dukascopy probe did not return any downloadable raw file.")
        return stats

    def one(ts: datetime) -> tuple[datetime, str, str | None, Path]:
        path = dukascopy_path(raw_dir, ts)
        status, error = http_get_to_file(dukascopy_url(ts), path, args.timeout, args.force, args.retries)
        return ts, status, error, path

    with futures.ThreadPoolExecutor(max_workers=args.workers) as pool:
        for _, status, error, path in pool.map(one, hours):
            stats.attempted_files += 1
            if status == "downloaded":
                stats.downloaded_files += 1
                stats.raw_files.append(str(path))
            elif status == "existing":
                stats.existing_files += 1
                stats.raw_files.append(str(path))
            elif status == "missing":
                stats.missing_or_closed_files += 1
            else:
                stats.failed_files += 1
                if error and len(stats.errors) < 100:
                    stats.errors.append(error)

    if stats.downloaded_files + stats.existing_files > 0:
        stats.status = "success"
    else:
        stats.status = "failed"
        stats.errors.append("No Dukascopy raw files were saved.")
    return stats


def month_range(start: date, end: date) -> Iterable[tuple[int, int]]:
    current = date(start.year, start.month, 1)
    stop = date(end.year, end.month, 1)
    while current <= stop:
        yield current.year, current.month
        if current.month == 12:
            current = date(current.year + 1, 1, 1)
        else:
            current = date(current.year, current.month + 1, 1)


def truefx_url_candidates(year: int, month: int) -> list[str]:
    month_name = date(year, month, 1).strftime("%B")
    candidates: list[str] = []
    for base in TRUEFX_BASE_URLS:
        for folder in (f"{month_name.upper()}-{year}", f"{month_name}-{year}"):
            candidates.append(f"{base}/{year}/{folder}/{SYMBOL}-{year}-{month:02d}.zip")
    return candidates


def download_truefx(args: argparse.Namespace, start: date, end: date, raw_dir: Path) -> DownloadStats:
    stats = DownloadStats("truefx", start.isoformat(), end.isoformat(), status="running")
    months = list(month_range(start, end))
    if args.max_months is not None:
        months = months[: args.max_months]
    if not months:
        stats.status = "failed"
        stats.errors.append("No candidate TrueFX months in requested range.")
        return stats

    for year, month in months:
        dest = raw_dir / "truefx" / SYMBOL / f"{year:04d}" / f"{SYMBOL}-{year}-{month:02d}.zip"
        stats.attempted_files += 1
        if dest.exists() and dest.stat().st_size > 0 and not args.force:
            stats.existing_files += 1
            stats.raw_files.append(str(dest))
            continue

        saved = False
        errors: list[str] = []
        for url in truefx_url_candidates(year, month):
            status, error = http_get_to_file(url, dest, args.timeout, args.force, args.retries)
            if status == "downloaded":
                stats.downloaded_files += 1
                stats.raw_files.append(str(dest))
                saved = True
                break
            if status == "existing":
                stats.existing_files += 1
                stats.raw_files.append(str(dest))
                saved = True
                break
            if error:
                errors.append(error)
            if status == "blocked":
                break
        if not saved:
            stats.failed_files += 1
            if errors and len(stats.errors) < 100:
                stats.errors.append("; ".join(errors[:2]))

    if stats.downloaded_files + stats.existing_files > 0:
        stats.status = "success"
    else:
        stats.status = "failed"
        stats.errors.append("No TrueFX raw files were saved. TrueFX may require login.")
    return stats


def download_histdata(args: argparse.Namespace, start: date, end: date, raw_dir: Path) -> DownloadStats:
    stats = DownloadStats("histdata", start.isoformat(), end.isoformat(), status="running")
    years = list(range(start.year, end.year + 1))
    if args.max_years is not None:
        years = years[: args.max_years]
    session = requests.Session()
    session.headers.update(
        {
            "User-Agent": "codex-quant-data-pipeline/1.0",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        }
    )
    for year in years:
        dest = raw_dir / "histdata" / SYMBOL / f"HISTDATA_COM_ASCII_{SYMBOL}_M1_{year}.zip"
        stats.attempted_files += 1
        if dest.exists() and dest.stat().st_size > 0 and not args.force:
            stats.existing_files += 1
            stats.raw_files.append(str(dest))
            continue

        saved = False
        for url in histdata_page_urls(year):
            status, error = download_histdata_page(session, url, dest, args)
            if status == "downloaded":
                stats.downloaded_files += 1
                stats.raw_files.append(str(dest))
                saved = True
                break
            if error and len(stats.errors) < 100:
                stats.errors.append(error)
        if not saved:
            monthly_saved = False
            month_start = start.month if year == start.year else 1
            month_end = end.month if year == end.year else 12
            for month in range(month_start, month_end + 1):
                month_dest = raw_dir / "histdata" / SYMBOL / f"HISTDATA_COM_ASCII_{SYMBOL}_M1_{year}{month:02d}.zip"
                stats.attempted_files += 1
                if month_dest.exists() and month_dest.stat().st_size > 0 and not args.force:
                    stats.existing_files += 1
                    stats.raw_files.append(str(month_dest))
                    monthly_saved = True
                    continue
                for url in histdata_page_urls(year, month):
                    status, error = download_histdata_page(session, url, month_dest, args)
                    if status == "downloaded":
                        stats.downloaded_files += 1
                        stats.raw_files.append(str(month_dest))
                        monthly_saved = True
                        break
                    if error and len(stats.errors) < 100:
                        stats.errors.append(error)
                # Stop trying URL variants for this month after success.
                if month_dest.exists() and month_dest.stat().st_size > 0:
                    continue
            if not monthly_saved:
                stats.failed_files += 1

    if stats.downloaded_files + stats.existing_files > 0:
        stats.status = "success"
    else:
        stats.status = "failed"
        stats.errors.append("No HistData raw zip files were saved.")
    return stats


def parse_histdata_form(page_html: str) -> dict[str, str]:
    fields: dict[str, str] = {}
    input_pattern = re.compile(r"<input\b[^>]*>", re.IGNORECASE)
    name_pattern = re.compile(r"\bname=['\"]([^'\"]+)['\"]", re.IGNORECASE)
    value_pattern = re.compile(r"\bvalue=['\"]([^'\"]*)['\"]", re.IGNORECASE)
    for input_tag in input_pattern.findall(page_html):
        name_match = name_pattern.search(input_tag)
        value_match = value_pattern.search(input_tag)
        if name_match and value_match:
            fields[html.unescape(name_match.group(1))] = html.unescape(value_match.group(1))
    required = {"tk", "date", "datemonth", "platform", "timeframe", "fxpair"}
    if not required.issubset(fields):
        return {}
    if not fields.get("tk"):
        return {}
    return {key: fields[key] for key in ["tk", "date", "datemonth", "platform", "timeframe", "fxpair"]}


def histdata_page_urls(year: int, month: int | None = None) -> list[str]:
    suffix = f"{year}/{month}" if month is not None else f"{year}"
    return [template.format(symbol=SYMBOL, year=suffix) for template in HISTDATA_URL_CANDIDATES]


def download_histdata_page(
    session: requests.Session,
    page_url: str,
    dest: Path,
    args: argparse.Namespace,
) -> tuple[str, str | None]:
    try:
        page = session.get(page_url, timeout=(15, args.timeout))
        if page.status_code != 200:
            return "failed", f"HistData page status {page.status_code}: {page_url}"
        fields = parse_histdata_form(page.text)
        if not fields:
            return "failed", f"HistData form token not found: {page_url}"
        response = session.post(
            HISTDATA_POST_URL,
            data=fields,
            headers={"Referer": page_url, "Accept": "application/zip,*/*"},
            timeout=(15, args.timeout),
            stream=True,
        )
        if response.status_code != 200:
            return "failed", f"HistData zip status {response.status_code}: {page_url}"
        dest.parent.mkdir(parents=True, exist_ok=True)
        tmp_path = dest.with_suffix(dest.suffix + ".part")
        bytes_written = 0
        with tmp_path.open("wb") as out:
            for chunk in response.iter_content(chunk_size=1024 * 256):
                if chunk:
                    out.write(chunk)
                    bytes_written += len(chunk)
        is_zip = False
        if bytes_written:
            with tmp_path.open("rb") as check:
                is_zip = check.read(2) == b"PK"
        if bytes_written and is_zip:
            os.replace(tmp_path, dest)
            return "downloaded", None
        tmp_path.unlink(missing_ok=True)
        return "failed", f"HistData response was not a zip: {page_url}"
    except requests.RequestException as exc:
        return "failed", f"{type(exc).__name__}: {page_url}: {exc}"


def write_manifest(raw_dir: Path, selected: DownloadStats | None, attempts: list[DownloadStats]) -> None:
    manifest = {
        "symbol": DISPLAY_SYMBOL,
        "generated_at_utc": datetime.now(UTC).isoformat(),
        "selected_source": selected.source if selected else None,
        "selected_status": selected.status if selected else "failed",
        "attempts": [asdict(item) for item in attempts],
    }
    path = raw_dir / "EURUSD_download_manifest.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(manifest, indent=2), encoding="utf-8")


def main() -> int:
    args = parse_args()
    start = date.fromisoformat(args.start)
    end = date.fromisoformat(args.end)
    if start > end:
        raise ValueError("--start must be <= --end")

    raw_dir = Path(args.raw_dir)
    raw_dir.mkdir(parents=True, exist_ok=True)
    source_priority = [item.strip().lower() for item in args.source_priority.split(",") if item.strip()]

    attempts: list[DownloadStats] = []
    selected: DownloadStats | None = None
    for source in source_priority:
        if source == "dukascopy":
            stats = download_dukascopy(args, start, end, raw_dir)
        elif source == "truefx":
            stats = download_truefx(args, start, end, raw_dir)
        elif source == "histdata":
            stats = download_histdata(args, start, end, raw_dir)
        else:
            stats = DownloadStats(source, start.isoformat(), end.isoformat(), status="failed")
            stats.errors.append(f"Unknown source: {source}")
        attempts.append(stats)
        if stats.status == "success":
            selected = stats
            break

    write_manifest(raw_dir, selected, attempts)
    if selected:
        print(f"Selected source: {selected.source}")
        print(f"Raw files available: {selected.downloaded_files + selected.existing_files}")
        print(f"Manifest: {raw_dir / 'EURUSD_download_manifest.json'}")
        return 0

    print("No data source succeeded. Manifest written with failure details.")
    print(f"Manifest: {raw_dir / 'EURUSD_download_manifest.json'}")
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
