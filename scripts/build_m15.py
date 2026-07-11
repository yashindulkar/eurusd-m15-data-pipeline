#!/usr/bin/env python3
"""Build EUR/USD 15-minute UTC candles from raw downloaded data."""

from __future__ import annotations

import argparse
import json
import lzma
import struct
import zipfile
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime, timedelta
from pathlib import Path

import numpy as np
import pandas as pd


SYMBOL = "EURUSD"
PRICE_SCALE = 100000.0
DUKASCOPY_RECORD = struct.Struct(">IIIff")


@dataclass
class BuildMetadata:
    selected_source: str | None = None
    source_detail: str | None = None
    requested_start: str | None = None
    requested_end: str | None = None
    raw_rows: int = 0
    raw_duplicate_rows_removed: int = 0
    final_m15_candles: int = 0
    final_duplicate_timestamps_removed: int = 0
    first_timestamp: str | None = None
    last_timestamp: str | None = None
    spread_available: bool = False
    warnings: list[str] = field(default_factory=list)
    generated_at_utc: str = field(default_factory=lambda: datetime.now(UTC).isoformat())


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build EUR/USD M15 candles.")
    parser.add_argument("--raw-dir", default="data/raw")
    parser.add_argument("--processed-dir", default="data/processed")
    parser.add_argument("--source", default=None, choices=[None, "dukascopy", "truefx", "histdata"])
    return parser.parse_args()


def load_manifest(raw_dir: Path) -> dict:
    path = raw_dir / "EURUSD_download_manifest.json"
    if not path.exists():
        return {}
    return json.loads(path.read_text(encoding="utf-8"))


def selected_source(raw_dir: Path, explicit_source: str | None) -> str | None:
    if explicit_source:
        return explicit_source
    manifest = load_manifest(raw_dir)
    if manifest.get("selected_source"):
        return manifest["selected_source"]
    if list((raw_dir / "dukascopy" / SYMBOL).glob("**/*h_ticks.bi5")):
        return "dukascopy"
    if list((raw_dir / "truefx" / SYMBOL).glob("**/*.zip")):
        return "truefx"
    if list((raw_dir / "histdata" / SYMBOL).glob("**/*.zip")):
        return "histdata"
    return None


def dukascopy_file_start(path: Path) -> datetime:
    # data/raw/dukascopy/EURUSD/YYYY/MM/DD/HHh_ticks.bi5
    day = int(path.parent.name)
    month = int(path.parent.parent.name)
    year = int(path.parent.parent.parent.name)
    hour = int(path.name[:2])
    return datetime(year, month, day, hour, tzinfo=UTC)


def decode_dukascopy_file(path: Path) -> pd.DataFrame:
    raw = path.read_bytes()
    if not raw:
        return pd.DataFrame()
    try:
        payload = lzma.decompress(raw)
    except lzma.LZMAError:
        return pd.DataFrame()

    usable_size = len(payload) - (len(payload) % DUKASCOPY_RECORD.size)
    if usable_size <= 0:
        return pd.DataFrame()

    base = dukascopy_file_start(path)
    rows = []
    for offset in range(0, usable_size, DUKASCOPY_RECORD.size):
        ms, ask_raw, bid_raw, ask_volume, bid_volume = DUKASCOPY_RECORD.unpack_from(payload, offset)
        timestamp = base + timedelta(milliseconds=int(ms))
        bid = bid_raw / PRICE_SCALE
        ask = ask_raw / PRICE_SCALE
        if bid <= 0 or ask <= 0:
            continue
        rows.append((timestamp, bid, ask, float(bid_volume), float(ask_volume)))

    if not rows:
        return pd.DataFrame()
    df = pd.DataFrame(rows, columns=["timestamp_utc", "bid", "ask", "bid_volume", "ask_volume"])
    df["mid"] = (df["bid"] + df["ask"]) / 2.0
    df["spread"] = df["ask"] - df["bid"]
    return df


def aggregate_ticks(df: pd.DataFrame, source_label: str) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame()
    df = df.sort_values("timestamp_utc")
    df = df.drop_duplicates(subset=["timestamp_utc", "bid", "ask"], keep="first")
    df["timestamp_utc"] = pd.to_datetime(df["timestamp_utc"], utc=True)
    df["bucket"] = df["timestamp_utc"].dt.floor("15min")

    grouped = df.groupby("bucket", sort=True)
    out = pd.DataFrame(
        {
            "timestamp_utc": grouped["timestamp_utc"].first().index,
            "open": grouped["mid"].first().values,
            "high": grouped["mid"].max().values,
            "low": grouped["mid"].min().values,
            "close": grouped["mid"].last().values,
            "volume_or_tick_count": grouped.size().astype("int64").values,
            "source": source_label,
            "bid_open": grouped["bid"].first().values,
            "bid_high": grouped["bid"].max().values,
            "bid_low": grouped["bid"].min().values,
            "bid_close": grouped["bid"].last().values,
            "ask_open": grouped["ask"].first().values,
            "ask_high": grouped["ask"].max().values,
            "ask_low": grouped["ask"].min().values,
            "ask_close": grouped["ask"].last().values,
            "spread_open": grouped["spread"].first().values,
            "spread_high": grouped["spread"].max().values,
            "spread_low": grouped["spread"].min().values,
            "spread_close": grouped["spread"].last().values,
            "spread_mean": grouped["spread"].mean().values,
        }
    )
    out["timestamp_utc"] = pd.to_datetime(out["timestamp_utc"], utc=True)
    return out


def build_from_dukascopy(raw_dir: Path, meta: BuildMetadata) -> pd.DataFrame:
    files = sorted((raw_dir / "dukascopy" / SYMBOL).glob("**/*h_ticks.bi5"))
    if not files:
        meta.warnings.append("No Dukascopy .bi5 files found.")
        return pd.DataFrame()

    chunks: list[pd.DataFrame] = []
    for path in files:
        ticks = decode_dukascopy_file(path)
        if ticks.empty:
            meta.warnings.append(f"Could not decode or empty file: {path}")
            continue
        before = len(ticks)
        ticks = ticks.drop_duplicates(subset=["timestamp_utc", "bid", "ask"], keep="first")
        meta.raw_rows += len(ticks)
        meta.raw_duplicate_rows_removed += before - len(ticks)
        agg = aggregate_ticks(ticks, "dukascopy_tick_mid_bid_ask")
        if not agg.empty:
            chunks.append(agg)

    meta.spread_available = True
    if not chunks:
        return pd.DataFrame()
    return pd.concat(chunks, ignore_index=True)


def parse_truefx_csv_from_zip(path: Path) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    with zipfile.ZipFile(path) as zf:
        for name in zf.namelist():
            if not name.lower().endswith(".csv"):
                continue
            with zf.open(name) as file_obj:
                df = pd.read_csv(
                    file_obj,
                    header=None,
                    names=["pair", "timestamp_raw", "bid", "ask"],
                    usecols=[0, 1, 2, 3],
                )
                frames.append(df)
    if not frames:
        return pd.DataFrame()
    df = pd.concat(frames, ignore_index=True)
    df = df[df["pair"].astype(str).str.replace("/", "", regex=False).str.upper() == SYMBOL]
    df["timestamp_utc"] = pd.to_datetime(df["timestamp_raw"], format="%Y%m%d %H:%M:%S.%f", utc=True, errors="coerce")
    df = df.dropna(subset=["timestamp_utc", "bid", "ask"])
    df["bid"] = pd.to_numeric(df["bid"], errors="coerce")
    df["ask"] = pd.to_numeric(df["ask"], errors="coerce")
    df = df.dropna(subset=["bid", "ask"])
    df["mid"] = (df["bid"] + df["ask"]) / 2.0
    df["spread"] = df["ask"] - df["bid"]
    return df[["timestamp_utc", "bid", "ask", "mid", "spread"]]


def build_from_truefx(raw_dir: Path, meta: BuildMetadata) -> pd.DataFrame:
    files = sorted((raw_dir / "truefx" / SYMBOL).glob("**/*.zip"))
    chunks: list[pd.DataFrame] = []
    for path in files:
        ticks = parse_truefx_csv_from_zip(path)
        if ticks.empty:
            meta.warnings.append(f"No TrueFX CSV rows parsed from: {path}")
            continue
        before = len(ticks)
        ticks = ticks.drop_duplicates(subset=["timestamp_utc", "bid", "ask"], keep="first")
        meta.raw_rows += len(ticks)
        meta.raw_duplicate_rows_removed += before - len(ticks)
        agg = aggregate_ticks(ticks, "truefx_tick_mid_bid_ask")
        if not agg.empty:
            chunks.append(agg)
    meta.spread_available = True
    if not chunks:
        return pd.DataFrame()
    return pd.concat(chunks, ignore_index=True)


def parse_histdata_zip(path: Path) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    with zipfile.ZipFile(path) as zf:
        for name in zf.namelist():
            if not name.lower().endswith(".csv"):
                continue
            with zf.open(name) as file_obj:
                df = pd.read_csv(
                    file_obj,
                    sep=";",
                    header=None,
                    names=["timestamp_raw", "open", "high", "low", "close", "volume"],
                    engine="python",
                )
                frames.append(df)
    if not frames:
        return pd.DataFrame()
    df = pd.concat(frames, ignore_index=True)
    # Typical HistData timestamps look like 20100103 170000 and align to the
    # New York 17:00 FX session boundary. Treat them as New York local time and
    # convert to UTC; do not treat the raw stamps as already-UTC.
    naive_ts = pd.to_datetime(df["timestamp_raw"].astype(str), format="%Y%m%d %H%M%S", errors="coerce")
    df["timestamp_utc"] = naive_ts.dt.tz_localize(
        "America/New_York",
        nonexistent="shift_forward",
        ambiguous="infer",
    ).dt.tz_convert("UTC")
    for col in ["open", "high", "low", "close", "volume"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    return df.dropna(subset=["timestamp_utc", "open", "high", "low", "close"])


def build_from_histdata(raw_dir: Path, meta: BuildMetadata) -> pd.DataFrame:
    files = sorted((raw_dir / "histdata" / SYMBOL).glob("**/*.zip"))
    frames: list[pd.DataFrame] = []
    for path in files:
        m1 = parse_histdata_zip(path)
        if m1.empty:
            meta.warnings.append(f"No HistData M1 rows parsed from: {path}")
            continue
        before = len(m1)
        m1 = m1.drop_duplicates(subset=["timestamp_utc"], keep="first")
        meta.raw_rows += len(m1)
        meta.raw_duplicate_rows_removed += before - len(m1)
        m1["timestamp_utc"] = pd.to_datetime(m1["timestamp_utc"], utc=True)
        m1["bucket"] = m1["timestamp_utc"].dt.floor("15min")
        grouped = m1.sort_values("timestamp_utc").groupby("bucket", sort=True)
        out = pd.DataFrame(
            {
                "timestamp_utc": grouped["timestamp_utc"].first().index,
                "open": grouped["open"].first().values,
                "high": grouped["high"].max().values,
                "low": grouped["low"].min().values,
                "close": grouped["close"].last().values,
                "volume_or_tick_count": grouped.size().astype("int64").values,
                "source": "histdata_m1_aggregated",
            }
        )
        frames.append(out)
    meta.spread_available = False
    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True)


def finalize_candles(candles: pd.DataFrame, meta: BuildMetadata) -> pd.DataFrame:
    if candles.empty:
        return candles
    candles["timestamp_utc"] = pd.to_datetime(candles["timestamp_utc"], utc=True)
    candles = candles.sort_values("timestamp_utc")
    before = len(candles)
    candles = candles.drop_duplicates(subset=["timestamp_utc"], keep="first")
    meta.final_duplicate_timestamps_removed = before - len(candles)
    candles = candles.sort_values("timestamp_utc").reset_index(drop=True)

    required_cols = ["timestamp_utc", "open", "high", "low", "close", "volume_or_tick_count", "source"]
    extra_cols = [col for col in candles.columns if col not in required_cols]
    candles = candles[required_cols + extra_cols]
    candles["timestamp_utc"] = candles["timestamp_utc"].dt.strftime("%Y-%m-%dT%H:%M:%SZ")
    meta.final_m15_candles = len(candles)
    if len(candles):
        meta.first_timestamp = str(candles["timestamp_utc"].iloc[0])
        meta.last_timestamp = str(candles["timestamp_utc"].iloc[-1])
    return candles


def main() -> int:
    args = parse_args()
    raw_dir = Path(args.raw_dir)
    processed_dir = Path(args.processed_dir)
    processed_dir.mkdir(parents=True, exist_ok=True)
    manifest = load_manifest(raw_dir)

    source = selected_source(raw_dir, args.source)
    meta = BuildMetadata(selected_source=source)
    meta.requested_start = None
    meta.requested_end = None
    for attempt in manifest.get("attempts", []):
        if attempt.get("source") == source:
            meta.requested_start = attempt.get("requested_start")
            meta.requested_end = attempt.get("requested_end")
            break

    if source == "dukascopy":
        meta.source_detail = "Dukascopy hourly tick .bi5; final OHLC uses mid=(bid+ask)/2."
        candles = build_from_dukascopy(raw_dir, meta)
    elif source == "truefx":
        meta.source_detail = "TrueFX historical tick CSV; final OHLC uses mid=(bid+ask)/2."
        candles = build_from_truefx(raw_dir, meta)
    elif source == "histdata":
        meta.source_detail = (
            "HistData M1 OHLC aggregated to M15; raw timestamps interpreted as "
            "America/New_York local session time and converted to UTC; volume_or_tick_count "
            "is contributing M1 bar count, not real tick volume; spread unavailable."
        )
        candles = build_from_histdata(raw_dir, meta)
    else:
        meta.warnings.append("No usable raw source found.")
        candles = pd.DataFrame()

    candles = finalize_candles(candles, meta)
    metadata_path = processed_dir / "EURUSD_M15_build_metadata.json"
    metadata_path.write_text(json.dumps(asdict(meta), indent=2), encoding="utf-8")

    if candles.empty:
        print("No candles built. Metadata written.")
        print(f"Metadata: {metadata_path}")
        return 2

    csv_path = processed_dir / "EURUSD_M15_UTC.csv"
    parquet_path = processed_dir / "EURUSD_M15_UTC.parquet"
    candles.to_csv(csv_path, index=False)
    candles.to_parquet(parquet_path, index=False)
    print(f"Built candles: {len(candles)}")
    print(f"CSV: {csv_path}")
    print(f"Parquet: {parquet_path}")
    print(f"Metadata: {metadata_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
