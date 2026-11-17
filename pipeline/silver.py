"""SILVER: parse bronze, enforce types, remove duplicates, store as compressed Parquet."""
import json
import os
from pathlib import Path

import pandas as pd

from . import config


def load_bronze(bronze_dir: Path = config.BRONZE) -> pd.DataFrame:
    """Read every bronze file and flatten the hourly arrays into one row per city-hour."""
    frames = []
    for path in sorted(Path(bronze_dir).glob("dt=*/*.jsonl")):
        with open(path, encoding="utf-8") as f:
            for line in f:
                rec = json.loads(line)
                df = pd.DataFrame(rec["payload"]["hourly"])
                df["city"] = rec["city"]
                df["ingested_at"] = rec["ingested_at"]
                frames.append(df)
    if not frames:
        return pd.DataFrame(columns=["time", "city", "ingested_at"])
    return pd.concat(frames, ignore_index=True)


def build_silver(raw: pd.DataFrame) -> pd.DataFrame:
    """Rename, type, deduplicate (latest ingestion wins) and sort."""
    df = raw.rename(
        columns={
            "temperature_2m": "temperature_c",
            "relative_humidity_2m": "humidity_pct",
            "wind_speed_10m": "wind_kmh",
        }
    ).copy()
    df["time"] = pd.to_datetime(df["time"])
    df["ingested_at"] = pd.to_datetime(df["ingested_at"]).dt.tz_localize(None)
    for col, dtype in config.SILVER_DTYPES.items():
        if col in df.columns:
            df[col] = df[col].astype(dtype)
    df = df.sort_values("ingested_at").drop_duplicates(["city", "time"], keep="last")
    return df.sort_values(["city", "time"]).reset_index(drop=True)


def publish_silver(df: pd.DataFrame, silver_dir: Path = config.SILVER) -> int:
    """One Parquet file per city, written atomically. Returns row count."""
    for city, part in df.groupby("city"):
        folder = Path(silver_dir) / f"city={config.slug(city)}"
        folder.mkdir(parents=True, exist_ok=True)
        final = folder / "data.parquet"
        tmp = folder / "data.parquet.partial"
        part.to_parquet(tmp, compression="zstd", index=False)
        os.replace(tmp, final)
    return len(df)
