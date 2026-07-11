# EUR/USD M15 UTC Data Quality Report

## Dataset and Grain

- Symbol: EUR/USD
- Grain: 15-minute candles, UTC timestamps
- Selected data source: histdata
- Source detail: HistData M1 OHLC aggregated to M15; raw timestamps interpreted as America/New_York local session time and converted to UTC; volume_or_tick_count is contributing M1 bar count, not real tick volume; spread unavailable.
- Requested date range: 2010-01-01 to 2026-07-08
- Actual first timestamp: 2010-01-03T22:00:00Z
- Actual last timestamp: 2026-06-26T20:45:00Z
- Raw rows parsed: 6043197
- Final M15 candles: 406945

## Checks Performed

- UTC timestamp parsing and ascending sort check
- Duplicate M15 timestamp check
- Missing candle check during expected FX market hours
- Weekend closure separated using Sunday 17:00 New York to Friday 17:00 New York
- OHLC internal consistency checks
- Zero or negative price checks
- Robust close-to-close and intrabar range spike detection
- London, Berlin, and New York session timezone assumptions checked with IANA zones

## Results

- Duplicate raw rows removed during build: 360
- Duplicate M15 timestamps removed during build: 0
- Duplicate M15 timestamps remaining: 0
- Missing non-weekend candles: 5974
- Suspected abnormal spikes: 225
- Invalid OHLC rows: 0
- Zero or negative price rows: 0
- Sorted ascending by timestamp: True

## Output Files

- CSV: /Users/yashindulkar/Documents/Codex/2026-07-09/let-s-set-up-a-scheduled-2/data/processed/EURUSD_M15_UTC.csv
- Parquet: /Users/yashindulkar/Documents/Codex/2026-07-09/let-s-set-up-a-scheduled-2/data/processed/EURUSD_M15_UTC.parquet
- Missing candles: /Users/yashindulkar/Documents/Codex/2026-07-09/let-s-set-up-a-scheduled-2/data/reports/EURUSD_M15_missing_candles.csv
- Spikes: /Users/yashindulkar/Documents/Codex/2026-07-09/let-s-set-up-a-scheduled-2/data/reports/EURUSD_M15_spikes.csv

## Assumptions

- Raw prices were not smoothed and missing candles were not forward-filled.
- Dukascopy and TrueFX tick outputs use mid=(bid+ask)/2 for the primary OHLC columns.
- HistData fallback, if used, is M1 OHLC aggregated to M15; raw timestamps are interpreted as America/New_York local session time and converted to UTC.
- For HistData, volume_or_tick_count is the count of contributing M1 bars per M15 candle, not true tick volume.
- Normal FX weekend closure is defined by New York 17:00 Friday close and New York 17:00 Sunday open.
- Session conversion uses Europe/London, Europe/Berlin, and America/New_York IANA time zones.

## Suitability for Strategy Backtesting

Not clean enough to treat as production-grade without reviewing the warnings/results above.

## Warnings

- Suspected abnormal spikes were detected and left untouched for review.
- Missing non-weekend candles were detected; no forward-fill was applied.
- Spread information is unavailable for the selected source.
