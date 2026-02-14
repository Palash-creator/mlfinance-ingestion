from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd


@dataclass(frozen=True)
class DatasetWriteResult:
    path: str
    rows: int


def _write_parquet(df: pd.DataFrame, path: Path) -> DatasetWriteResult:
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(path, engine="pyarrow", compression="snappy", index=True)
    return DatasetWriteResult(path=str(path), rows=int(len(df)))


def write_partitioned_by_year(df: pd.DataFrame, base_dir: Path, source: str, run_id: str) -> list[DatasetWriteResult]:
    if df.empty:
        return []
    results: list[DatasetWriteResult] = []
    years = sorted(df.index.year.unique().tolist())
    for year in years:
        ydf = df[df.index.year == year]
        path = base_dir / "raw" / f"source={source}" / f"year={year}" / f"run_id={run_id}.parquet"
        results.append(_write_parquet(ydf, path))
    return results


def write_standardized(df: pd.DataFrame, base_dir: Path, run_id: str) -> DatasetWriteResult:
    path = base_dir / "standardized" / f"run_id={run_id}.parquet"
    return _write_parquet(df, path)


def append_catalog_entry(catalog_path: Path, entry: dict[str, Any]) -> None:
    catalog_path.parent.mkdir(parents=True, exist_ok=True)
    if catalog_path.exists():
        existing = json.loads(catalog_path.read_text())
        if not isinstance(existing, list):
            existing = [existing]
    else:
        existing = []
    existing.append(entry)
    catalog_path.write_text(json.dumps(existing, indent=2, sort_keys=True, default=str))


def serialize_write_results(results: list[DatasetWriteResult]) -> list[dict[str, Any]]:
    return [asdict(item) for item in results]


def iso_now() -> str:
    return datetime.utcnow().replace(microsecond=0).isoformat() + "Z"
