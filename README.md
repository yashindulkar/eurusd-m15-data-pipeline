# EUR/USD M15 Historical Data Pipeline

Reproducible Python pipeline for downloading, building, and validating EUR/USD 15-minute historical data for backtesting research.

This repository was built with a conservative quant-data posture: raw source files are preserved locally, processed candles are generated deterministically, and quality problems are reported rather than hidden.

## Current Dataset Snapshot

- Symbol: EUR/USD
- Output timeframe: 15-minute candles
- Timezone: UTC
- Selected full-range source: HistData M1 fallback
- Actual range: 2010-01-03T22:00:00Z to 2026-06-26T20:45:00Z
- Final candles: 406,945
- Raw rows parsed: 6,043,197
- Strict validation status: failed because missing candles and spike candidates require review

The full quality report is in `data/reports/EURUSD_M15_quality_report.md`.

## Data Quality Position

There is no single perfect universal spot-FX tape. The pipeline tries sources in this order:

1. Dukascopy tick data
2. TrueFX tick data
3. HistData M1 data

In this environment, Dukascopy tick download worked for a small probe but was too slow/throttled for the full 2010-2026 pull. TrueFX direct monthly archives were not available without login. The final full-range dataset therefore uses HistData M1 data aggregated to M15.

Important limitations:

- No bid/ask spread is available in the HistData fallback output.
- `volume_or_tick_count` is the count of contributing M1 bars per M15 candle, not true traded volume.
- Missing candles are not forward-filled.
- Spike candidates are not removed automatically.
- HistData raw timestamps are interpreted as America/New_York local session time and converted to UTC.

## Repository Layout

```text
scripts/
  download_eurusd.py      # Download raw data with source fallback
  build_m15.py            # Build M15 UTC candles from raw data
  validate_data.py        # Validate processed candles and write reports

data/
  processed/
    README.md
    EURUSD_M15_build_metadata.json
  reports/
    EURUSD_M15_quality_report.md
  raw/
    .gitkeep              # Raw vendor downloads are intentionally not tracked
```

## Reproduce The Pipeline

Install dependencies:

```bash
python3 -m pip install -r requirements.txt
```

Run the full source-priority pipeline:

```bash
python3 scripts/download_eurusd.py --source-priority dukascopy,truefx,histdata --start 2010-01-01 --end 2026-07-08 --timeout 120
python3 scripts/build_m15.py
python3 scripts/validate_data.py
```

Or run the exact fallback source used for the current snapshot:

```bash
python3 scripts/download_eurusd.py --source-priority histdata --start 2010-01-01 --end 2026-07-08 --timeout 120
python3 scripts/build_m15.py
python3 scripts/validate_data.py
```

`validate_data.py` returns a non-zero exit code when strict quality checks find missing expected candles, invalid OHLC, nonpositive prices, duplicate timestamps, or spike candidates. That is intentional.

## Backtesting Guidance

Use this dataset only with explicit gap and spike handling. It is suitable for exploratory strategy research after reviewing the reports, but it is not clean enough to call production/institutional-grade without additional source reconciliation.

Recommended next steps before capital-risk use:

- Compare critical periods against a broker-grade or institutional tick source.
- Decide whether holiday and early-close gaps should be excluded from your strategy calendar.
- Review all rows in `data/reports/EURUSD_M15_spikes.csv`.
- Add spread/slippage assumptions externally if using the HistData fallback.

## Generated Artifacts

The full generated CSV and Parquet outputs are intentionally treated as build artifacts:

- `data/processed/EURUSD_M15_UTC.csv`
- `data/processed/EURUSD_M15_UTC.parquet`
- `data/reports/EURUSD_M15_missing_candles.csv`
- `data/reports/EURUSD_M15_spikes.csv`

They are generated locally by the pipeline and were packaged in the local deliverable archive for this run. Rebuild them with the commands above when cloning the repository.

## Data Redistribution

Raw downloaded vendor files are not tracked. Large generated data files are also not tracked in git. Verify third-party data terms before redistributing any processed market-data files publicly.
