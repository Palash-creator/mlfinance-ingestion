from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

import pandas as pd
import yfinance as yf


@dataclass(frozen=True)
class YFinanceLoadResult:
    raw: pd.DataFrame
    standardized: pd.DataFrame


def load_yfinance(
    tickers: list[str],
    start: datetime,
    end: datetime,
) -> YFinanceLoadResult:
    raw = yf.download(
        tickers=tickers,
        start=start.strftime("%Y-%m-%d"),
        end=end.strftime("%Y-%m-%d"),
        auto_adjust=False,
        group_by="column",
        progress=False,
        threads=True,
    )

    if raw.empty:
        return YFinanceLoadResult(raw=raw, standardized=pd.DataFrame())

    raw.index = pd.to_datetime(raw.index, utc=True).normalize()
    raw.index.name = "date"

    standardized = pd.DataFrame(index=raw.index)

    is_multi = isinstance(raw.columns, pd.MultiIndex)
    for ticker in tickers:
        adj_series: pd.Series | None = None
        vol_series: pd.Series | None = None

        if is_multi:
            if "Adj Close" in raw.columns.get_level_values(0) and ticker in raw["Adj Close"].columns:
                adj_series = raw["Adj Close"][ticker]
            if "Volume" in raw.columns.get_level_values(0) and ticker in raw["Volume"].columns:
                vol_series = raw["Volume"][ticker]
        else:
            if "Adj Close" in raw.columns and len(tickers) == 1:
                adj_series = raw["Adj Close"]
            if "Volume" in raw.columns and len(tickers) == 1:
                vol_series = raw["Volume"]

        if adj_series is not None:
            standardized[f"yfin_{ticker}__adj_close"] = adj_series
        if vol_series is not None:
            standardized[f"yfin_{ticker}__volume"] = vol_series

    standardized = standardized.sort_index()
    return YFinanceLoadResult(raw=raw, standardized=standardized)
