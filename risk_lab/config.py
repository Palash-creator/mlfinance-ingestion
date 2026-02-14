from __future__ import annotations

import os
from dataclasses import dataclass, field
from datetime import date
from pathlib import Path
from typing import Final


DEFAULT_START_DATE: Final[str] = "2005-01-01"
FRED_SERIES: Final[list[str]] = ["DGS10", "DGS2", "CPIAUCSL", "UNRATE", "BAA10YM"]
YFINANCE_TICKERS: Final[list[str]] = ["SPY", "TLT", "HYG", "GLD", "UUP"]


@dataclass(frozen=True)
class AppConfig:
    fred_api_key: str = field(default_factory=lambda: os.getenv("FRED_API_KEY", ""))
    default_start: str = DEFAULT_START_DATE
    default_end: str = field(default_factory=lambda: date.today().isoformat())
    fred_series: list[str] = field(default_factory=lambda: list(FRED_SERIES))
    yfinance_tickers: list[str] = field(default_factory=lambda: list(YFINANCE_TICKERS))
    output_root: Path = Path("data")
    log_dir: Path = Path("logs")
    catalog_path: Path = Path("data_catalog.json")
    partition_by_year: bool = True


CONFIG = AppConfig()
