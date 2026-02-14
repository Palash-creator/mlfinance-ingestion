from __future__ import annotations

import argparse
import sys
import uuid
from dataclasses import asdict
from datetime import datetime
from typing import Any

import pandas as pd

from risk_lab.config import CONFIG
from risk_lab.ingestion.fred_loader import load_fred_series
from risk_lab.ingestion.yfinance_loader import load_yfinance
from risk_lab.storage.parquet_store import (
    append_catalog_entry,
    iso_now,
    serialize_write_results,
    write_partitioned_by_year,
    write_standardized,
)
from risk_lab.utils.dates import normalize_to_utc_date_index, parse_date
from risk_lab.utils.logging import setup_logger
from risk_lab.validation.checks import ValidationReport, validate_dataset


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run ingestion pipeline for market + macro data.")
    parser.add_argument("--start", default=CONFIG.default_start, help="YYYY-MM-DD")
    parser.add_argument("--end", default=CONFIG.default_end, help="YYYY-MM-DD")
    return parser


def _print_summary(
    success: bool,
    report: ValidationReport,
    write_paths: dict[str, Any],
) -> None:
    status = "✅ SUCCESS" if success else "❌ FAILURE"
    print(f"\n{status}")
    print("Datasets written:")
    for key, val in write_paths.items():
        print(f"  - {key}: {val}")

    print(f"Warnings: {len(report.warnings)}")
    print(f"Errors: {len(report.errors)}")

    missing = report.metrics.get("missing_pct", {})
    print("Top 10 columns by missing %:")
    for col, pct in sorted(missing.items(), key=lambda x: x[1], reverse=True)[:10]:
        print(f"  - {col}: {pct:.2f}%")

    stale = report.metrics.get("stale_series", [])
    print("Stale series/tickers:")
    if stale:
        for col in stale:
            print(f"  - {col}")
    else:
        print("  - none")

    if report.errors:
        print("Errors detail:")
        for err in report.errors:
            print(f"  - {err}")
    if report.warnings:
        print("Warnings detail:")
        for warn in report.warnings:
            print(f"  - {warn}")


def main() -> int:
    args = _build_parser().parse_args()
    run_id = str(uuid.uuid4())
    logger = setup_logger(run_id=run_id, log_dir=CONFIG.log_dir)

    try:
        start = parse_date(args.start)
        end = parse_date(args.end)
    except ValueError as exc:
        print(f"❌ FAILURE\nInvalid date format: {exc}")
        return 2

    if start > end:
        print("❌ FAILURE\n--start must be <= --end")
        return 2

    logger.info("Starting ingestion run_id=%s start=%s end=%s", run_id, args.start, args.end)

    if not CONFIG.fred_api_key:
        print("❌ FAILURE\nFRED_API_KEY is required in config.py or environment.")
        return 2

    try:
        fred_res = load_fred_series(CONFIG.fred_series, start=start, end=end, api_key=CONFIG.fred_api_key)
        yfin_res = load_yfinance(CONFIG.yfinance_tickers, start=start, end=end)

        if fred_res.standardized.empty:
            raise RuntimeError("FRED returned no data.")
        if yfin_res.standardized.empty:
            raise RuntimeError("Yahoo Finance returned no data.")

        merged = pd.concat([fred_res.standardized, yfin_res.standardized], axis=1).sort_index()
        merged.index = normalize_to_utc_date_index(merged.index)
        merged.index.name = "date"

        expected_fred = [f"fred_{s}" for s in CONFIG.fred_series]
        required_market = [f"yfin_{t}__adj_close" for t in CONFIG.yfinance_tickers]
        optional_market = [f"yfin_{t}__volume" for t in CONFIG.yfinance_tickers]
        required_columns = expected_fred + required_market
        all_expected_columns = required_columns + optional_market

        report = validate_dataset(
            data=merged,
            required_columns=required_columns,
            optional_columns=optional_market,
            market_columns=required_market,
            fred_columns=expected_fred,
        )

        raw_fred_df = pd.DataFrame({k: v for k, v in fred_res.raw.items()})
        raw_fred_df.index = normalize_to_utc_date_index(raw_fred_df.index)
        raw_fred_df.index.name = "date"
        raw_yf_df = yfin_res.raw.copy()
        if not raw_yf_df.empty:
            raw_yf_df.index = normalize_to_utc_date_index(raw_yf_df.index)
            raw_yf_df.index.name = "date"

        fred_raw_paths = write_partitioned_by_year(raw_fred_df, CONFIG.output_root, "fred", run_id)
        yfin_raw_paths = write_partitioned_by_year(raw_yf_df, CONFIG.output_root, "yfinance", run_id)
        standardized_path = write_standardized(merged, CONFIG.output_root, run_id)

        catalog_entry = {
            "run_id": run_id,
            "run_timestamp": iso_now(),
            "date_range": {"start": args.start, "end": args.end},
            "sources_pulled": ["fred", "yfinance"],
            "datasets": {
                "raw_fred": serialize_write_results(fred_raw_paths),
                "raw_yfinance": serialize_write_results(yfin_raw_paths),
                "standardized": asdict(standardized_path),
            },
            "per_series": {
                col: {
                    "row_count": int(merged[col].notna().sum()) if col in merged.columns else 0,
                    "missing_pct": report.metrics.get("missing_pct", {}).get(col),
                    "latest_date": report.metrics.get("latest_date", {}).get(col),
                }
                for col in all_expected_columns
            },
            "validation": {
                "warnings": report.warnings,
                "errors": report.errors,
                "metrics": report.metrics,
            },
        }
        append_catalog_entry(CONFIG.catalog_path, catalog_entry)

        write_paths = {
            "raw_fred": [d.path for d in fred_raw_paths],
            "raw_yfinance": [d.path for d in yfin_raw_paths],
            "standardized": standardized_path.path,
            "catalog": str(CONFIG.catalog_path),
            "log": str(CONFIG.log_dir / f"{run_id}.log"),
        }
        success = report.is_success()
        _print_summary(success, report, write_paths)
        return 0 if success else 1

    except Exception as exc:  # fail-safe at CLI boundary
        logger.exception("Ingestion failed: %s", exc)
        fail_report = ValidationReport(errors=[str(exc)], warnings=[], metrics={"missing_pct": {}, "stale_series": []})
        _print_summary(False, fail_report, {"log": str(CONFIG.log_dir / f"{run_id}.log")})
        return 1


if __name__ == "__main__":
    sys.exit(main())
