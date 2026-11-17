"""GOLD: business-ready tables built with DuckDB SQL over silver Parquet.

Each table is written as Parquet (and CSV for BI tools like Power BI).
"""
import os
from datetime import datetime, timezone
from pathlib import Path

import duckdb

from . import config

TABLES = {
    # one row per city per day
    "daily_summary": """
        SELECT city,
               CAST(time AS DATE)                 AS date,
               ROUND(MIN(temperature_c), 1)       AS min_temp_c,
               ROUND(MAX(temperature_c), 1)       AS max_temp_c,
               ROUND(AVG(temperature_c), 1)       AS avg_temp_c,
               ROUND(AVG(humidity_pct), 0)        AS avg_humidity_pct,
               COUNT(*)                           AS hours
        FROM silver GROUP BY city, CAST(time AS DATE) ORDER BY city, date
    """,
    # every hour, for the line chart
    "hourly_temperature": """
        SELECT city, time, temperature_c, humidity_pct, wind_kmh
        FROM silver ORDER BY city, time
    """,
    # most recent observation at or before now, per city
    "city_latest": """
        SELECT city, time, temperature_c, humidity_pct, wind_kmh
        FROM silver WHERE time <= TIMESTAMP '{cutoff}'
        QUALIFY row_number() OVER (PARTITION BY city ORDER BY time DESC) = 1
        ORDER BY city
    """,
}


def build_gold(silver_dir: Path = config.SILVER, gold_dir: Path = config.GOLD) -> dict:
    """Build all gold tables. Returns {table: row_count}."""
    gold_dir = Path(gold_dir)
    gold_dir.mkdir(parents=True, exist_ok=True)
    cutoff = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    con = duckdb.connect()
    con.execute(
        f"CREATE VIEW silver AS SELECT * FROM read_parquet('{Path(silver_dir).as_posix()}/*/data.parquet')"
    )
    counts = {}
    for name, sql in TABLES.items():
        query = sql.format(cutoff=cutoff)
        for fmt, opts in (("parquet", "FORMAT PARQUET, COMPRESSION ZSTD"), ("csv", "FORMAT CSV, HEADER")):
            final = gold_dir / f"{name}.{fmt}"
            tmp = gold_dir / f"{name}.{fmt}.partial"
            con.execute(f"COPY ({query}) TO '{tmp.as_posix()}' ({opts})")
            os.replace(tmp, final)
        counts[name] = con.execute(f"SELECT COUNT(*) FROM ({query})").fetchone()[0]
    con.close()
    return counts
