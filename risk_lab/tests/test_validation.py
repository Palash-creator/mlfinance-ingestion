from __future__ import annotations

import pandas as pd

from risk_lab.validation.checks import validate_dataset


def _base_df() -> pd.DataFrame:
    idx = pd.date_range("2024-01-01", periods=5, freq="B", tz="UTC")
    df = pd.DataFrame(
        {
            "fred_DGS10": [4.0, 4.1, 4.2, 4.3, 4.4],
            "fred_DGS2": [4.5, 4.4, 4.3, 4.2, 4.1],
            "fred_CPIAUCSL": [300, 301, 302, 303, 304],
            "fred_UNRATE": [3.8, 3.8, 3.9, 3.9, 4.0],
            "fred_BAA10YM": [2.0, 2.0, 2.1, 2.2, 2.2],
            "yfin_SPY__adj_close": [400, 401, 402, 403, 404],
            "yfin_TLT__adj_close": [90, 89, 88, 87, 86],
            "yfin_HYG__adj_close": [75, 75, 76, 76, 77],
            "yfin_GLD__adj_close": [180, 181, 181, 182, 183],
            "yfin_UUP__adj_close": [29, 29.1, 29.2, 29.1, 29.0],
            "yfin_SPY__volume": [1, 2, 3, 4, 5],
            "yfin_TLT__volume": [1, 2, 3, 4, 5],
            "yfin_HYG__volume": [1, 2, 3, 4, 5],
            "yfin_GLD__volume": [1, 2, 3, 4, 5],
            "yfin_UUP__volume": [1, 2, 3, 4, 5],
        },
        index=idx,
    )
    df.index.name = "date"
    return df


def test_validation_success_for_clean_data() -> None:
    df = _base_df()
    expected = df.columns.tolist()
    report = validate_dataset(
        data=df,
        required_columns=expected,
        optional_columns=[],
        market_columns=[c for c in expected if c.endswith("__adj_close")],
        fred_columns=[c for c in expected if c.startswith("fred_")],
    )
    assert report.is_success()
    assert report.errors == []


def test_validation_fails_on_missing_required_column() -> None:
    df = _base_df().drop(columns=["fred_DGS10"])
    expected = _base_df().columns.tolist()
    report = validate_dataset(
        data=df,
        required_columns=expected,
        optional_columns=[],
        market_columns=[c for c in expected if c.endswith("__adj_close")],
        fred_columns=[c for c in expected if c.startswith("fred_")],
    )
    assert not report.is_success()
    assert any("missing expected columns" in e for e in report.errors)


def test_validation_warns_for_negative_values() -> None:
    df = _base_df()
    df.loc[df.index[0], "yfin_SPY__adj_close"] = -1
    expected = df.columns.tolist()
    report = validate_dataset(
        data=df,
        required_columns=expected,
        optional_columns=[],
        market_columns=[c for c in expected if c.endswith("__adj_close")],
        fred_columns=[c for c in expected if c.startswith("fred_")],
    )
    assert report.is_success()
    assert any("Impossible values detected" in w for w in report.warnings)


def test_validation_optional_columns_warn_not_fail() -> None:
    df = _base_df().drop(columns=["yfin_SPY__volume"])
    expected = [c for c in _base_df().columns if not c.endswith("__volume")]
    report = validate_dataset(
        data=df,
        required_columns=expected,
        optional_columns=["yfin_SPY__volume"],
        market_columns=[c for c in expected if c.endswith("__adj_close")],
        fred_columns=[c for c in expected if c.startswith("fred_")],
    )
    assert report.is_success()
    assert any("Optional column unavailable" in w for w in report.warnings)
