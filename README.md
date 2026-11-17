# Hourly weather pipeline (guest lecture demo)

A small but complete data engineering project: **ingest -> bronze -> silver -> quality gate -> gold -> dashboard**, orchestrated by Prefect.
Data: hourly temperature, humidity and wind for 6 cities from the free Open-Meteo API (no key needed).

```
Open-Meteo API
     |  1 ingest (requests, retries)
     v
BRONZE  raw JSONL, append-only, partitioned by date      data/bronze/dt=YYYY-MM-DD/
     |  2 transform (pandas): flatten, types, dedupe (latest wins)
     v
QUALITY GATE  range / null / duplicate / completeness checks
     |  pass -> continue        fail -> run stops, bronze file quarantined
     v
SILVER  typed, deduplicated Parquet (zstd), 1 file per city   data/silver/
     |  5 SQL aggregation (DuckDB)
     v
GOLD    daily_summary, hourly_temperature, city_latest   data/gold/ (.parquet + .csv)
     |
     v
Dashboard (Streamlit, or Power BI reading data/gold)     + manifest.json / runs.jsonl
```

## Run it

### Option A: Docker Compose (full demo)
```bash
docker compose up --build
```
- Prefect UI: http://localhost:4200  -> Deployments -> `weather-pipeline/hourly-weather` -> **Run**
- Dashboard: http://localhost:8501 (refresh after a run)
- No internet at the venue? Edit `docker-compose.yml` and add `--offline` to the pipeline command.

### Option B: plain Python (no Docker)
```bash
python -m venv .venv && source .venv/bin/activate     # Windows: .venv\Scripts\activate
pip install -r requirements.txt
prefect server start                                  # terminal 1 (UI on :4200)
export PREFECT_API_URL=http://127.0.0.1:4200/api      # terminal 2 (Windows: set ...)
python -m pipeline.flow                               # serves hourly schedule + Run button
streamlit run dashboard/app.py                        # terminal 3
```

### Quick checks
```bash
python -m pipeline.flow --once --offline       # one run, synthetic data, no server needed
python -m pipeline.flow --once --inject-bad    # corrupts a value: the quality gate stops the run
pytest tests -q
```

## Power BI (Windows)
Get Data -> Folder/Parquet -> point at `data/gold/`. After a pipeline run, click **Refresh**.
CSV copies of each gold table are also written.

## Demo script (about 5 minutes)
1. Show the empty dashboard ("no data yet").
2. Prefect UI -> Run. Narrate the task graph as it turns green: ingest, transform, quality gate, publish silver, gold.
3. Refresh the dashboard: cities, charts, "Last run OK", quality checks table.
4. Failure beat: Run again with the `inject_bad` parameter ticked (Custom run). The gate task goes red, the run fails, the dashboard banner says FAILED but still shows the last good data. Open `data/quarantine/`.
5. Recap on the architecture slide.

## What to say (concept -> tool)
| Concept | Here | Spark/Hadoop world |
|---|---|---|
| Object storage / data lake | local folders (swap for S3/MinIO) | HDFS |
| Distributed compute | pandas + DuckDB (scale up with Dask) | MapReduce -> Spark |
| Columnar format + compression | Parquet + zstd | Parquet / ORC |
| Orchestration | Prefect | Airflow / Oozie |
| Medallion layers | bronze / silver / gold | same |
| Quality gate + quarantine | quality.py | Great Expectations / dbt tests |
| Atomic publish | write `.partial` then rename | commit protocols |

## Notes
- `--offline` creates synthetic data with the same JSON shape as Open-Meteo, so the demo never depends on Wi-Fi.
- Silver dedupe keeps the latest ingestion per (city, hour), so forecast revisions overwrite older values.
- Not run end-to-end where this was written (duckdb, pyarrow, prefect and streamlit were not installable there). The ingest, silver transform and quality gate are unit-tested; the gold SQL, Prefect flow and dashboard need a first run on your machine.
