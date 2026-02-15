from __future__ import annotations

from datetime import date

import pandas as pd
import streamlit as st

from risk_lab.config import CONFIG
from risk_lab.run_ingest import run_ingestion


def render_app() -> None:
    st.set_page_config(page_title="Risk Lab Ingestion", page_icon="📈", layout="wide")

    st.markdown(
        """
        <style>
        .main-title {font-size:2.2rem;font-weight:700;margin-bottom:0.2rem;}
        .subtle {color:#6b7280;font-size:0.95rem;}
        </style>
        """,
        unsafe_allow_html=True,
    )

    st.markdown('<div class="main-title">📈 Risk Lab Data Ingestion Dashboard</div>', unsafe_allow_html=True)
    st.markdown(
        '<div class="subtle">Run FRED + Yahoo Finance ingestion, validate quality, and inspect run outputs.</div>',
        unsafe_allow_html=True,
    )

    with st.sidebar:
        st.header("Run Configuration")
        start = st.date_input("Start date", value=pd.to_datetime(CONFIG.default_start).date())
        end = st.date_input("End date", value=date.today())
        st.caption("FRED series: " + ", ".join(CONFIG.fred_series))
        st.caption("Yahoo tickers: " + ", ".join(CONFIG.yfinance_tickers))
        run_clicked = st.button("🚀 Run ingestion", type="primary", use_container_width=True)

    if not run_clicked:
        st.info("Choose a date range and click **Run ingestion**.")
        return

    with st.spinner("Running ingestion pipeline..."):
        result = run_ingestion(start.isoformat(), end.isoformat())

    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("Status", "✅ Success" if result.success else "❌ Failure")
    with col2:
        st.metric("Warnings", len(result.report.warnings))
    with col3:
        st.metric("Errors", len(result.report.errors))

    st.markdown("### Run Metadata")
    st.write({"run_id": result.run_id, "exit_code": result.exit_code, "paths": result.write_paths})

    if result.report.errors:
        st.error("\n".join(result.report.errors))
    if result.report.warnings:
        st.warning("\n".join(result.report.warnings[:20]))

    missing = result.report.metrics.get("missing_pct", {})
    if missing:
        st.markdown("### Top Missing Columns")
        missing_df = pd.DataFrame(
            [{"column": k, "missing_pct": v} for k, v in missing.items()]
        ).sort_values("missing_pct", ascending=False)
        st.dataframe(missing_df.head(10), use_container_width=True)

    latest_dates = result.report.metrics.get("latest_date", {})
    if latest_dates:
        st.markdown("### Freshness (Latest Date by Series)")
        freshness_df = pd.DataFrame(
            [{"column": k, "latest_date": v} for k, v in latest_dates.items()]
        ).sort_values("column")
        st.dataframe(freshness_df, use_container_width=True)

    stale = result.report.metrics.get("stale_series", [])
    st.markdown("### Stale Series")
    if stale:
        st.write(stale)
    else:
        st.success("No stale series detected.")
