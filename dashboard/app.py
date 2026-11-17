"""Streamlit dashboard over the GOLD layer. Refresh the page after a pipeline run."""
import json
import os
from pathlib import Path

import pandas as pd
import streamlit as st

ROOT = Path(os.environ.get("PIPELINE_DATA_DIR", Path(__file__).resolve().parent.parent / "data"))
GOLD = ROOT / "gold"

st.set_page_config(page_title="Weather pipeline", layout="wide")
st.title("Hourly temperature pipeline")

manifest_path = ROOT / "manifest.json"
if not manifest_path.exists() or not (GOLD / "city_latest.parquet").exists():
    st.info("No data yet. Run the pipeline from the Prefect UI (Deployments -> Run).")
    st.stop()

m = json.loads(manifest_path.read_text())
if m.get("status") == "success":
    st.success(f"Last run OK at {m['finished_at']} UTC | {m.get('mode')} | {m.get('silver_rows'):,} silver rows")
else:
    st.error(f"Last run FAILED at {m['finished_at']} UTC: {m.get('error')}. Dashboard still shows the last good data.")

latest = pd.read_parquet(GOLD / "city_latest.parquet")
cols = st.columns(len(latest))
for col, (_, r) in zip(cols, latest.iterrows()):
    col.metric(r["city"], f"{r['temperature_c']:.1f} °C", f"{r['humidity_pct']:.0f}% humidity", delta_color="off")

hourly = pd.read_parquet(GOLD / "hourly_temperature.parquet")
st.subheader("Hourly temperature (past 2 days + today)")
st.line_chart(hourly.pivot(index="time", columns="city", values="temperature_c"))

daily = pd.read_parquet(GOLD / "daily_summary.parquet")
left, right = st.columns(2)
with left:
    st.subheader("Daily summary")
    st.dataframe(daily, hide_index=True, use_container_width=True)
with right:
    st.subheader("Quality checks (latest run)")
    if m.get("checks"):
        st.dataframe(pd.DataFrame(m["checks"]), hide_index=True, use_container_width=True)

runs_path = ROOT / "runs.jsonl"
if runs_path.exists():
    st.subheader("Run history")
    runs = pd.DataFrame([json.loads(l) for l in runs_path.read_text().splitlines() if l.strip()])
    show = [c for c in ["finished_at", "status", "mode", "silver_rows", "error"] if c in runs.columns]
    st.dataframe(runs[show].tail(10).iloc[::-1], hide_index=True, use_container_width=True)
