# Open Financial Market Datasets (`devffex/dataset`)

This repository serves as the official, globally distributed open market dataset repository powered by the **Savisor Market Node**. It hosts normalized, institutional-grade historical tick and bar datasets (OHLCV + Volume) across standard timeframe partitions (`M1` to `MN1`).

All datasets are compressed with **Apache Parquet (Snappy)** for high-throughput quantitative backtesting and **Compact JSON** for sub-millisecond web chart hydration. Datasets are distributed globally through the **jsDelivr Multi-CDN** (Cloudflare + Fastly edge networks) with **zero egress costs**.

---

## 1. Directory Structure

Datasets are organized directly at the root of the repository by asset symbol and timeframe:

```text
devffex/dataset/
├── symbols.json                   # Global catalog of all tracked symbols and metadata
├── <SYMBOL>/                      # Asset root partition (e.g., EURUSD/, USDJPY/, BTCUSD/)
│   ├── manifest.json              # Timeframe index, row counts, and date boundaries
│   ├── M1/
│   │   ├── history.parquet        # Complete historical dataset (Snappy Parquet)
│   │   └── recent.json            # Latest 500 forming/closed candles array
│   ├── M5/
│   │   ├── history.parquet
│   │   └── recent.json
│   ├── M15/
│   ├── M30/
│   ├── H1/
│   ├── H4/
│   ├── D1/
│   ├── W1/
│   └── MN1/
└── README.md
```

---

## 2. Global CDN Endpoints

Datasets can be accessed directly over HTTP without cloning the repository:

| Asset Type | Global Multi-CDN URL (Recommended) | Raw GitHub URL (Fallback) |
|---|---|---|
| **Parquet History** | `https://cdn.jsdelivr.net/gh/devffex/dataset@main/<SYMBOL>/<TIMEFRAME>/history.parquet` | `https://raw.githubusercontent.com/devffex/dataset/main/<SYMBOL>/<TIMEFRAME>/history.parquet` |
| **Recent JSON (500 Bars)** | `https://cdn.jsdelivr.net/gh/devffex/dataset@main/<SYMBOL>/<TIMEFRAME>/recent.json` | `https://raw.githubusercontent.com/devffex/dataset/main/<SYMBOL>/<TIMEFRAME>/recent.json` |
| **Asset Manifest** | `https://cdn.jsdelivr.net/gh/devffex/dataset@main/<SYMBOL>/manifest.json` | `https://raw.githubusercontent.com/devffex/dataset/main/<SYMBOL>/manifest.json` |
| **Catalog Index** | `https://cdn.jsdelivr.net/gh/devffex/dataset@main/symbols.json` | `https://raw.githubusercontent.com/devffex/dataset/main/symbols.json` |

*Example for EURUSD 1-Hour Parquet*:  
👉 `https://cdn.jsdelivr.net/gh/devffex/dataset@main/EURUSD/H1/history.parquet`

---

## 3. Data Format & Schema Specifications

### A. `history.parquet` (Columnar Apache Parquet)
Compressed using the **Snappy** codec with sorted Unix timestamps and dictionary encoding.

| Column | Type | Description |
|---|---|---|
| `time` | `int64` | UTC Unix epoch timestamp in seconds. |
| `open` | `float64` | Bar opening price. |
| `high` | `float64` | Bar maximum price. |
| `low` | `float64` | Bar minimum price. |
| `close` | `float64` | Bar closing price. |
| `tick_volume` | `int64` | Number of trade ticks received during the period. |

### B. `recent.json` (Compact Array-of-Arrays)
Optimized for ultra-fast JSON parsing and low network payload ($<20\text{ KB}$ for 500 bars):

```json
[
  [1787778000, 1.16534, 1.16534, 1.16527, 1.16529, 6],
  [1787781600, 1.16529, 1.16540, 1.16525, 1.16538, 14]
]
```
*Tuple Schema*: `[timestamp_seconds, open, high, low, close, tick_volume]`

---

## 4. Client Integration Recipes

### Recipe 1: Python / Polars (Blazing Fast Streaming Ingest)

Read Parquet files directly from the CDN into a Polars DataFrame without saving to disk:

```python
import polars as pl

# Stream EURUSD H1 history directly from jsDelivr Multi-CDN
url = "https://cdn.jsdelivr.net/gh/devffex/dataset@main/EURUSD/H1/history.parquet"
df = pl.read_parquet(url)

# Convert Unix timestamp to UTC datetime
df = df.with_columns(
    pl.from_epoch(pl.col("time"), time_unit="s").alias("datetime")
)

print(f"Loaded {len(df):,} bars:")
print(df.tail(5))
```

---

### Recipe 2: DuckDB (Serverless SQL Queries over HTTP)

Query and filter massive historical datasets without downloading the file:

```python
import duckdb

# Query H1 Parquet partition directly using DuckDB httpfs
query = """
    SELECT 
        to_timestamp(time) AS timestamp,
        open, high, low, close, tick_volume,
        (close - open) / open * 100 AS return_pct
    FROM 'https://cdn.jsdelivr.net/gh/devffex/dataset@main/EURUSD/H1/history.parquet'
    WHERE time >= 1700000000
    ORDER BY time ASC
"""

df = duckdb.query(query).df()
print(df.head(10))
```

---

### Recipe 3: Python / Pandas & Indicator Calculation

```python
import pandas as pd

# Load EURUSD daily data
url = "https://cdn.jsdelivr.net/gh/devffex/dataset@main/EURUSD/D1/history.parquet"
df = pd.read_parquet(url)
df['datetime'] = pd.to_datetime(df['time'], unit='s', utc=True)
df.set_index('datetime', inplace=True)

# Compute 20-period and 50-period Simple Moving Averages
df['SMA_20'] = df['close'].rolling(window=20).mean()
df['SMA_50'] = df['close'].rolling(window=50).mean()

print(df[['close', 'SMA_20', 'SMA_50']].tail(10))
```

---

### Recipe 4: React & TypeScript (TradingView Lightweight Charts)

Hydrate interactive candlestick charts instantly from `recent.json`:

```typescript
import { createChart, ISeriesApi } from "lightweight-charts";

export class MarketChartHydrator {
  private series: ISeriesApi<"Candlestick">;

  constructor(container: HTMLElement) {
    const chart = createChart(container, { width: 800, height: 400 });
    this.series = chart.addCandlestickSeries();
  }

  async loadRecentBars(symbol: string = "EURUSD", timeframe: string = "H1") {
    const cdnUrl = `https://cdn.jsdelivr.net/gh/devffex/dataset@main/${symbol}/${timeframe}/recent.json`;
    const response = await fetch(cdnUrl);
    const rawData: [number, number, number, number, number, number][] = await response.json();

    const chartData = rawData.map(([time, open, high, low, close]) => ({
      time: time as any,
      open,
      high,
      low,
      close,
    }));

    this.series.setData(chartData);
  }
}
```

---

### Recipe 5: CLI / Shell Download & Sync

Download or mirror specific datasets locally using `curl` or `wget`:

```bash
# Download EURUSD H1 Parquet file
curl -fLo EURUSD_H1.parquet \
  "https://cdn.jsdelivr.net/gh/devffex/dataset@main/EURUSD/H1/history.parquet"

# Download the latest 500 candles JSON for M15
curl -fLo EURUSD_M15_recent.json \
  "https://cdn.jsdelivr.net/gh/devffex/dataset@main/EURUSD/M15/recent.json"

# Download asset manifest metadata
curl -fLo EURUSD_manifest.json \
  "https://cdn.jsdelivr.net/gh/devffex/dataset@main/EURUSD/manifest.json"
```

---

### Recipe 6: Python Batch Downloader for Multiple Symbols

```python
import os
import httpx
from pathlib import Path

SYMBOLS = ["EURUSD", "USDJPY", "GBPUSD"]
TIMEFRAMES = ["M1", "M5", "H1", "D1"]
CDN_BASE = "https://cdn.jsdelivr.net/gh/devffex/dataset@main"
OUTPUT_DIR = Path("./downloaded_market_data")

def download_dataset():
    with httpx.Client() as client:
        for symbol in SYMBOLS:
            for tf in TIMEFRAMES:
                dest_file = OUTPUT_DIR / symbol / tf / "history.parquet"
                dest_file.parent.mkdir(parents=True, exist_ok=True)
                
                url = f"{CDN_BASE}/{symbol}/{tf}/history.parquet"
                print(f"Downloading {symbol} {tf} from {url}...")
                
                res = client.get(url)
                if res.status_code == 200:
                    dest_file.write_bytes(res.content)
                    print(f" Saved: {dest_file} ({len(res.content):,} bytes)")
                else:
                    print(f"⚠️ Skipped {symbol} {tf} (Status: {res.status_code})")

if __name__ == "__main__":
    download_dataset()
```

---

## 5. Automated Data Ingestion & Live Updates

Datasets in this repository are managed autonomously by the **Savisor Market Node**:
1. **Real-time Candle 0 Aggregation**: Ticks from broker trade servers are aggregated into in-flight bars.
2. **Rollover Trigger**: When a period closes (e.g. at `:00` seconds for `M1` or `:00` minutes for `H1`), the node finalizes the candle, merges it with historical Parquet partitions, and pushes an atomic commit to this repository.
3. **Instant Edge Cache Invalidation**: After every commit, the node automatically issues a purge request to the jsDelivr Purge API (`https://purge.jsdelivr.net/gh/devffex/dataset@main/...`), guaranteeing global cache freshness across all CDN edge nodes within seconds.

---

## 6. License & Disclaimer

* **Disclaimer**: All financial datasets provided in this repository are intended for informational, research, machine learning, and algorithmic trading backtesting purposes.
* **License**: MIT License. Free for both open-source and commercial use.
