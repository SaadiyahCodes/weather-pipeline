"""Prefect flow: ingest -> silver -> quality gate -> publish -> gold -> manifest.

  python -m pipeline.flow                       # serve: hourly schedule + Run button in the UI
  python -m pipeline.flow --once --offline      # run once without Prefect server or internet
  python -m pipeline.flow --once --inject-bad   # demo: corrupt one value, watch the gate stop the run
"""
import argparse
import uuid
from datetime import datetime, timezone

from prefect import flow, get_run_logger, task

from . import config
from .gold import build_gold
from .ingest import run_ingest
from .quality import DataQualityError, assert_quality, quarantine
from .runlog import record_run
from .silver import build_silver, load_bronze, publish_silver


@task(name="1 ingest -> bronze", retries=2, retry_delay_seconds=10)
def ingest_task(offline: bool, inject_bad: bool):
    path, n = run_ingest(offline=offline, inject_bad=inject_bad)
    get_run_logger().info(f"bronze file {path.name}: {n} cities")
    return path


@task(name="2 transform -> silver (in memory)")
def transform_task():
    df = build_silver(load_bronze())
    get_run_logger().info(f"{len(df)} clean city-hours across {df['city'].nunique()} cities")
    return df


@task(name="3 quality gate")
def quality_task(df, bronze_file):
    log = get_run_logger()
    try:
        checks = assert_quality(df)
    except DataQualityError as err:
        moved = quarantine(bronze_file)
        log.error(f"{err} | bronze file quarantined at {moved}")
        raise
    log.info(f"{sum(c.passed for c in checks)}/{len(checks)} checks passed")
    return [c.as_dict() for c in checks]


@task(name="4 publish silver (parquet)")
def publish_task(df):
    return publish_silver(df)


@task(name="5 build gold (duckdb sql)")
def gold_task():
    return build_gold()


@flow(name="weather-pipeline", log_prints=True)
def weather_pipeline(offline: bool = False, inject_bad: bool = False):
    info = {
        "run_id": uuid.uuid4().hex[:8],
        "started_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "mode": "offline (synthetic)" if offline else "live (Open-Meteo)",
        "inject_bad": inject_bad,
    }
    try:
        bronze_file = ingest_task(offline, inject_bad)
        silver_df = transform_task()
        checks = quality_task(silver_df, bronze_file)
        rows = publish_task(silver_df)
        gold_counts = gold_task()
        info.update(status="success", checks=checks, silver_rows=rows, gold_tables=gold_counts, error=None)
    except Exception as err:
        info.update(status="failed", error=str(err), checks=getattr(err, "checks", None) and [c.as_dict() for c in err.checks])
        raise
    finally:
        info["finished_at"] = datetime.now(timezone.utc).isoformat(timespec="seconds")
        record_run(info)
    return info


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--once", action="store_true", help="run one time and exit")
    ap.add_argument("--offline", action="store_true", help="use synthetic data (no internet)")
    ap.add_argument("--inject-bad", action="store_true", help="corrupt one value to trigger the quality gate")
    args = ap.parse_args()
    if args.once:
        weather_pipeline(offline=args.offline, inject_bad=args.inject_bad)
    else:
        weather_pipeline.serve(
            name="hourly-weather",
            cron="0 * * * *",
            parameters={"offline": args.offline, "inject_bad": False},
        )


if __name__ == "__main__":
    main()
