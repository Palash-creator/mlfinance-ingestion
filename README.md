# mlfinance-ingestion

Production-grade Python ingestion pipeline for quant finance time series.

## Features
- Pulls macro series from FRED and market proxies from Yahoo Finance.
- Standardizes to UTC-normalized `date` index and canonical column names.
- Runs completeness, data-quality, freshness, and schema checks.
- Writes raw + standardized datasets as Snappy Parquet.
- Appends run metadata to `data_catalog.json`.
- Prints a clear final run summary with success/failure and issues.

## Install
```bash
pip install pandas pyarrow yfinance fredapi pytest streamlit
```

## Configure
All configuration and credentials are in `risk_lab/config.py`.

Primary approach:
- Set `fred_api_key` inside `AppConfig` in `risk_lab/config.py`.

Recommended secure fallback (already supported):
```bash
export FRED_API_KEY="your_real_fred_key"
```

## Run
```bash
python -m risk_lab.run_ingest --start 2005-01-01 --end 2026-02-14
```

## Streamlit Frontend
```bash
streamlit run streamlit_app.py
```

## Test
```bash
python -m pytest risk_lab/tests/test_validation.py
```
