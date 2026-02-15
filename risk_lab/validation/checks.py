from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from collections.abc import Callable
from typing import Any

import numpy as np
import pandas as pd


@dataclass
class ValidationReport:
    errors: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)
    metrics: dict[str, Any] = field(default_factory=dict)

    def is_success(self) -> bool:
        return len(self.errors) == 0


def _largest_consecutive_missing_business_days(series: pd.Series) -> int:
    if series.empty:
        return 0
    miss = series.isna()
    max_gap = 0
    current = 0
    for is_missing in miss.tolist():
        if is_missing:
            current += 1
            max_gap = max(max_gap, current)
        else:
            current = 0
    return int(max_gap)


def _zscore_flags(values: pd.Series, kind: str) -> pd.Series:
    if values.empty:
        return pd.Series(dtype=bool)
    if kind == "pct_change":
        changes = values.pct_change()
    else:
        changes = values.diff()
    std = changes.std(skipna=True)
    if std is None or np.isclose(std, 0, equal_nan=True):
        return pd.Series(False, index=values.index)
    z = (changes - changes.mean(skipna=True)) / std
    return z.abs() > 8


def validate_dataset(
    data: pd.DataFrame,
    required_columns: list[str],
    optional_columns: list[str],
    market_columns: list[str],
    fred_columns: list[str],
) -> ValidationReport:
    report = ValidationReport()
    metrics: dict[str, Any] = {
        "row_count": int(len(data)),
        "column_count": int(data.shape[1]),
        "missing_pct": {},
        "largest_missing_business_gap": {},
        "latest_date": {},
        "outlier_counts": {},
        "stale_series": [],
    }

    if data.index.name != "date":
        report.errors.append("Schema error: index name must be 'date'.")

    if not isinstance(data.index, pd.DatetimeIndex):
        report.errors.append("Schema error: index must be DatetimeIndex.")
    else:
        if data.index.duplicated().any():
            report.errors.append("Duplicate dates found in standardized dataset.")
        if not data.index.is_monotonic_increasing:
            report.errors.append("Dates are not monotonic increasing.")

    missing_columns = sorted(set(required_columns) - set(data.columns.tolist()))
    if missing_columns:
        report.errors.append(f"Schema error: missing expected columns: {missing_columns}")

    missing_optional = sorted(set(optional_columns) - set(data.columns.tolist()))
    for col in missing_optional:
        report.warnings.append(f"Optional column unavailable: {col}")

    for col in required_columns:
        if col not in data.columns:
            continue
        series = data[col]
        if series.empty or series.dropna().empty:
            report.errors.append(f"Required series/ticker {col} has empty data.")

        num = pd.to_numeric(series, errors="coerce")
        if num.isna().sum() != series.isna().sum():
            report.errors.append(f"Mixed/non-numeric values found in column {col}.")

        if pd.api.types.is_object_dtype(series.dtype):
            report.warnings.append(f"Column {col} has object dtype; expected numeric.")

        missing_pct = float(series.isna().mean() * 100)
        metrics["missing_pct"][col] = round(missing_pct, 4)

        b_idx = pd.date_range(data.index.min(), data.index.max(), freq="B", tz="UTC")
        reindexed = series.reindex(b_idx)
        metrics["largest_missing_business_gap"][col] = _largest_consecutive_missing_business_days(reindexed)

        non_na_index = series.dropna().index
        metrics["latest_date"][col] = non_na_index.max().date().isoformat() if len(non_na_index) else None

    impossible_rules: list[tuple[str, Callable[[pd.Series], pd.Series]]] = [
        ("fred_DGS10", lambda s: s < 0),
        ("fred_DGS2", lambda s: s < 0),
        ("fred_CPIAUCSL", lambda s: s < 0),
        ("fred_UNRATE", lambda s: s < 0),
        ("fred_BAA10YM", lambda s: s < 0),
    ]

    for col in [c for c in data.columns if c.endswith("__adj_close")]:
        impossible_rules.append((col, lambda s: s < 0))
    for col in [c for c in data.columns if c.endswith("__volume")]:
        impossible_rules.append((col, lambda s: s < 0))

    for col, rule in impossible_rules:
        if col in data.columns:
            invalid_count = int(rule(pd.to_numeric(data[col], errors="coerce")).fillna(False).sum())
            if invalid_count > 0:
                report.warnings.append(f"Impossible values detected in {col}: {invalid_count} rows.")

    for col in market_columns:
        if col not in data.columns:
            continue
        flags = _zscore_flags(pd.to_numeric(data[col], errors="coerce"), kind="pct_change")
        count = int(flags.fillna(False).sum())
        metrics["outlier_counts"][col] = count
        if count > 0:
            report.warnings.append(f"Outlier flag on market series {col}: {count} rows with |z| > 8.")

    for col in fred_columns:
        if col not in data.columns:
            continue
        flags = _zscore_flags(pd.to_numeric(data[col], errors="coerce"), kind="abs_change")
        count = int(flags.fillna(False).sum())
        metrics["outlier_counts"][col] = count
        if count > 0:
            report.warnings.append(f"Outlier flag on FRED series {col}: {count} rows with |z| > 8.")

    now_date = datetime.now(timezone.utc).date()
    for col, latest in metrics["latest_date"].items():
        if latest is None:
            continue
        latest_date = datetime.strptime(latest, "%Y-%m-%d").date()
        age = (now_date - latest_date).days
        is_macro = col.startswith("fred_")
        threshold = 45 if is_macro else 7
        if age > threshold:
            msg = f"Stale series {col}: latest {latest} ({age} days old, threshold {threshold})."
            report.warnings.append(msg)
            metrics["stale_series"].append(col)

    metrics["warnings_count"] = len(report.warnings)
    metrics["errors_count"] = len(report.errors)
    report.metrics = metrics
    return report
