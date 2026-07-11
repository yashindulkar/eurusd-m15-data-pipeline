#!/usr/bin/env python3
"""Validate EUR/USD M15 dataset and write quality reports."""

from __future__ import annotations

import argparse
import json
from datetime import UTC, datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd


NY = ZoneInfo("America/New_York")
LONDON = ZoneInfo("Europe/London")
BERLIN = ZoneInfo("Europe/Berlin")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Validate EUR/USD M15 candles.")
    parser.add_argument("--processed-file", default="data/processed/EURUSD_M15_UTC.csv")
    parser.add_argument("--metadata-file", default="data/processed/EURUSD_M15_build_metadata.json")
    parser.add_argument("--reports-dir", default="data/reports")
    return parser.parse_args()


def load_metadata(path: Path) -> dict:
    if not path.exists():
        return {}
    return json.loads(path.read_text(encoding="utf-8"))


def is_fx_market_open_utc(ts: pd.Timestamp) -> bool:
    """FX week rule: Sunday 17:00 New York through Friday 17:00 New York."""
    local = ts.to_pydatetime().astimezone(NY)
    weekday = local.weekday()
    local_time = local.time()
    if weekday == 5:
        return False
    if weekday == 6:
        return local_time >= datetime.strptime("17:00", "%H:%M").time()
    if weekday == 4:
        return local_time < datetime.strptime("17:00", "%H:%M").time()
    return True


def session_flags(ts: pd.Timestamp) -> dict[str, bool]:
    london = ts.to_pydatetime().astimezone(LONDON)
    berlin = ts.to_pydatetime().astimezone(BERLIN)
    new_york = ts.to_pydatetime().astimezone(NY)
    london_open = london.weekday() < 5 and 8 <= london.hour < 17
    berlin_open = berlin.weekday() < 5 and 8 <= berlin.hour < 17
    ny_open = new_york.weekday() < 5 and 8 <= new_york.hour < 17
    return {
        "london_session": london_open,
        "berlin_session": berlin_open,
        "new_york_session": ny_open,
        "london_new_york_overlap": london_open and ny_open,
    }


def detect_missing(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame(columns=["timestamp_utc", "reason"])
    first = df["timestamp_utc"].min().floor("15min")
    last = df["timestamp_utc"].max().floor("15min")
    expected = pd.date_range(first, last, freq="15min", tz="UTC")
    expected_open = [ts for ts in expected if is_fx_market_open_utc(ts)]
    present = set(df["timestamp_utc"])
    missing = [ts for ts in expected_open if ts not in present]
    return pd.DataFrame(
        {
            "timestamp_utc": [ts.strftime("%Y-%m-%dT%H:%M:%SZ") for ts in missing],
            "reason": "missing_during_expected_fx_market_hours",
        }
    )


def detect_spikes(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty or len(df) < 5:
        return pd.DataFrame(columns=["timestamp_utc", "close", "prev_close", "log_return", "range_pct", "reason"])

    work = df.copy()
    work["prev_close"] = work["close"].shift(1)
    work["log_return"] = np.log(work["close"] / work["prev_close"])
    work["range_pct"] = (work["high"] - work["low"]) / work["close"]

    abs_ret = work["log_return"].replace([np.inf, -np.inf], np.nan).abs().dropna()
    range_pct = work["range_pct"].replace([np.inf, -np.inf], np.nan).abs().dropna()
    ret_med = float(abs_ret.median()) if len(abs_ret) else 0.0
    ret_mad = float((abs_ret - ret_med).abs().median()) if len(abs_ret) else 0.0
    range_med = float(range_pct.median()) if len(range_pct) else 0.0
    range_mad = float((range_pct - range_med).abs().median()) if len(range_pct) else 0.0

    ret_threshold = max(0.005, ret_med + 20.0 * 1.4826 * ret_mad)
    range_threshold = max(0.005, range_med + 20.0 * 1.4826 * range_mad)

    conditions = []
    reasons = []
    ret_flag = work["log_return"].abs() > ret_threshold
    range_flag = work["range_pct"].abs() > range_threshold
    price_flag = (work[["open", "high", "low", "close"]] <= 0).any(axis=1)
    conditions.append(ret_flag)
    reasons.append("large_close_to_close_log_return")
    conditions.append(range_flag)
    reasons.append("large_intracandle_range")
    conditions.append(price_flag)
    reasons.append("zero_or_negative_price")

    flag = pd.Series(False, index=work.index)
    reason_col = pd.Series("", index=work.index, dtype="object")
    for condition, reason in zip(conditions, reasons):
        flag = flag | condition.fillna(False)
        reason_col = reason_col.mask(condition.fillna(False), reason_col + "|" + reason)

    out = work.loc[flag, ["timestamp_utc", "close", "prev_close", "log_return", "range_pct"]].copy()
    if out.empty:
        return pd.DataFrame(columns=["timestamp_utc", "close", "prev_close", "log_return", "range_pct", "reason"])
    out["reason"] = reason_col.loc[out.index].str.strip("|")
    out["timestamp_utc"] = out["timestamp_utc"].dt.strftime("%Y-%m-%dT%H:%M:%SZ")
    return out


def ohlc_invalid_mask(df: pd.DataFrame) -> pd.Series:
    return ~(
        (df["high"] >= df["open"])
        & (df["high"] >= df["close"])
        & (df["low"] <= df["open"])
        & (df["low"] <= df["close"])
        & (df["high"] >= df["low"])
    )


def write_report(
    reports_dir: Path,
    processed_file: Path,
    metadata: dict,
    df: pd.DataFrame,
    missing: pd.DataFrame,
    spikes: pd.DataFrame,
    duplicate_timestamps: int,
    invalid_ohlc: int,
    nonpositive_prices: int,
    sorted_ascending: bool,
) -> None:
    selected_source = metadata.get("selected_source")
    final_rows = len(df)
    first_ts = df["timestamp_utc"].min().strftime("%Y-%m-%dT%H:%M:%SZ") if final_rows else None
    last_ts = df["timestamp_utc"].max().strftime("%Y-%m-%dT%H:%M:%SZ") if final_rows else None
    csv_path = processed_file.resolve()
    parquet_path = processed_file.with_suffix(".parquet").resolve()
    suitability = (
        final_rows > 0
        and duplicate_timestamps == 0
        and invalid_ohlc == 0
        and nonpositive_prices == 0
        and len(missing) == 0
        and sorted_ascending
    )

    warnings: list[str] = []
    warnings.extend(metadata.get("warnings", []))
    if len(spikes):
        warnings.append("Suspected abnormal spikes were detected and left untouched for review.")
    if len(missing):
        warnings.append("Missing non-weekend candles were detected; no forward-fill was applied.")
    if metadata.get("spread_available") is False:
        warnings.append("Spread information is unavailable for the selected source.")
    elif metadata.get("spread_available") is True:
        warnings.append("Bid/ask-derived spread columns are preserved in the processed dataset.")

    report = [
        "# EUR/USD M15 UTC Data Quality Report",
        "",
        "## Dataset and Grain",
        "",
        "- Symbol: EUR/USD",
        "- Grain: 15-minute candles, UTC timestamps",
        f"- Selected data source: {selected_source or 'none'}",
        f"- Source detail: {metadata.get('source_detail') or 'n/a'}",
        f"- Requested date range: {metadata.get('requested_start') or 'n/a'} to {metadata.get('requested_end') or 'n/a'}",
        f"- Actual first timestamp: {first_ts or 'n/a'}",
        f"- Actual last timestamp: {last_ts or 'n/a'}",
        f"- Raw rows parsed: {metadata.get('raw_rows', 0)}",
        f"- Final M15 candles: {final_rows}",
        "",
        "## Checks Performed",
        "",
        "- UTC timestamp parsing and ascending sort check",
        "- Duplicate M15 timestamp check",
        "- Missing candle check during expected FX market hours",
        "- Weekend closure separated using Sunday 17:00 New York to Friday 17:00 New York",
        "- OHLC internal consistency checks",
        "- Zero or negative price checks",
        "- Robust close-to-close and intrabar range spike detection",
        "- London, Berlin, and New York session timezone assumptions checked with IANA zones",
        "",
        "## Results",
        "",
        f"- Duplicate raw rows removed during build: {metadata.get('raw_duplicate_rows_removed', 0)}",
        f"- Duplicate M15 timestamps removed during build: {metadata.get('final_duplicate_timestamps_removed', 0)}",
        f"- Duplicate M15 timestamps remaining: {duplicate_timestamps}",
        f"- Missing non-weekend candles: {len(missing)}",
        f"- Suspected abnormal spikes: {len(spikes)}",
        f"- Invalid OHLC rows: {invalid_ohlc}",
        f"- Zero or negative price rows: {nonpositive_prices}",
        f"- Sorted ascending by timestamp: {sorted_ascending}",
        "",
        "## Output Files",
        "",
        f"- CSV: {csv_path}",
        f"- Parquet: {parquet_path}",
        f"- Missing candles: {(reports_dir / 'EURUSD_M15_missing_candles.csv').resolve()}",
        f"- Spikes: {(reports_dir / 'EURUSD_M15_spikes.csv').resolve()}",
        "",
        "## Assumptions",
        "",
        "- Raw prices were not smoothed and missing candles were not forward-filled.",
        "- Dukascopy and TrueFX tick outputs use mid=(bid+ask)/2 for the primary OHLC columns.",
        "- HistData fallback, if used, is M1 OHLC aggregated to M15; raw timestamps are interpreted as America/New_York local session time and converted to UTC.",
        "- For HistData, volume_or_tick_count is the count of contributing M1 bars per M15 candle, not true tick volume.",
        "- Normal FX weekend closure is defined by New York 17:00 Friday close and New York 17:00 Sunday open.",
        "- Session conversion uses Europe/London, Europe/Berlin, and America/New_York IANA time zones.",
        "",
        "## Suitability for Strategy Backtesting",
        "",
        (
            "Suitable for backtesting with normal caution: core validation passed."
            if suitability
            else "Not clean enough to treat as production-grade without reviewing the warnings/results above."
        ),
        "",
        "## Warnings",
        "",
    ]
    if warnings:
        report.extend(f"- {item}" for item in warnings)
    else:
        report.append("- None.")
    report.append("")

    (reports_dir / "EURUSD_M15_quality_report.md").write_text("\n".join(report), encoding="utf-8")


def main() -> int:
    args = parse_args()
    processed_file = Path(args.processed_file)
    metadata_file = Path(args.metadata_file)
    reports_dir = Path(args.reports_dir)
    reports_dir.mkdir(parents=True, exist_ok=True)

    metadata = load_metadata(metadata_file)
    missing_path = reports_dir / "EURUSD_M15_missing_candles.csv"
    spikes_path = reports_dir / "EURUSD_M15_spikes.csv"

    if not processed_file.exists():
        empty_missing = pd.DataFrame(columns=["timestamp_utc", "reason"])
        empty_spikes = pd.DataFrame(columns=["timestamp_utc", "close", "prev_close", "log_return", "range_pct", "reason"])
        empty_missing.to_csv(missing_path, index=False)
        empty_spikes.to_csv(spikes_path, index=False)
        write_report(
            reports_dir,
            processed_file,
            metadata,
            pd.DataFrame(columns=["timestamp_utc", "open", "high", "low", "close"]),
            empty_missing,
            empty_spikes,
            duplicate_timestamps=0,
            invalid_ohlc=0,
            nonpositive_prices=0,
            sorted_ascending=False,
        )
        print("Processed file not found. Failure report written.")
        return 2

    df = pd.read_csv(processed_file)
    df["timestamp_utc"] = pd.to_datetime(df["timestamp_utc"], utc=True, errors="coerce")
    for col in ["open", "high", "low", "close", "volume_or_tick_count"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    df = df.dropna(subset=["timestamp_utc", "open", "high", "low", "close"])

    sorted_ascending = bool(df["timestamp_utc"].is_monotonic_increasing)
    duplicate_timestamps = int(df["timestamp_utc"].duplicated().sum())
    invalid_ohlc = int(ohlc_invalid_mask(df).sum())
    nonpositive_prices = int((df[["open", "high", "low", "close"]] <= 0).any(axis=1).sum())
    missing = detect_missing(df)
    spikes = detect_spikes(df)

    missing.to_csv(missing_path, index=False)
    spikes.to_csv(spikes_path, index=False)
    write_report(
        reports_dir,
        processed_file,
        metadata,
        df,
        missing,
        spikes,
        duplicate_timestamps,
        invalid_ohlc,
        nonpositive_prices,
        sorted_ascending,
    )

    print(f"Rows: {len(df)}")
    print(f"Missing non-weekend candles: {len(missing)}")
    print(f"Suspected spikes: {len(spikes)}")
    print(f"Invalid OHLC rows: {invalid_ohlc}")
    print(f"Report: {reports_dir / 'EURUSD_M15_quality_report.md'}")
    if duplicate_timestamps or invalid_ohlc or nonpositive_prices or len(missing) or not sorted_ascending:
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
