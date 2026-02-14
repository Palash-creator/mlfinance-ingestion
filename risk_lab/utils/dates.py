from __future__ import annotations

from datetime import datetime

import pandas as pd


def normalize_to_utc_date_index(index_like: pd.Index | pd.Series) -> pd.DatetimeIndex:
    """Normalize index-like values to UTC and strip time component."""
    dt = pd.to_datetime(index_like, utc=True, errors="coerce")
    return pd.DatetimeIndex(dt.tz_convert("UTC").normalize())


def parse_date(date_value: str) -> datetime:
    return datetime.strptime(date_value, "%Y-%m-%d")
