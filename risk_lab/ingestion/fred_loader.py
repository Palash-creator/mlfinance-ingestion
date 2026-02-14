from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

import pandas as pd
from fredapi import Fred


@dataclass(frozen=True)
class FredLoadResult:
    raw: dict[str, pd.Series]
    standardized: pd.DataFrame


def load_fred_series(
    series_ids: list[str],
    start: datetime,
    end: datetime,
    api_key: str,
) -> FredLoadResult:
    fred = Fred(api_key=api_key)
    raw: dict[str, pd.Series] = {}
    std_parts: list[pd.DataFrame] = []

    for series_id in series_ids:
        series = fred.get_series(series_id, observation_start=start, observation_end=end)
        series.name = series_id
        raw[series_id] = series

        frame = series.to_frame(name=f"fred_{series_id}")
        frame.index = pd.to_datetime(frame.index, utc=True).normalize()
        frame = frame[~frame.index.isna()]
        frame.index.name = "date"
        std_parts.append(frame)

    standardized = pd.concat(std_parts, axis=1).sort_index() if std_parts else pd.DataFrame()
    if not standardized.empty:
        standardized = standardized.loc[(standardized.index >= pd.Timestamp(start, tz="UTC")) & (standardized.index <= pd.Timestamp(end, tz="UTC"))]
    return FredLoadResult(raw=raw, standardized=standardized)
