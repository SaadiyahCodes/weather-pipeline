"""Data quality gate. Runs BEFORE silver is published, so bad data never reaches gold."""
import shutil
from dataclasses import asdict, dataclass
from pathlib import Path

import pandas as pd

from . import config

REQUIRED = ["time", "city", "temperature_c", "humidity_pct", "wind_kmh"]


@dataclass
class Check:
    name: str
    passed: bool
    detail: str

    def as_dict(self) -> dict:
        return asdict(self)


class DataQualityError(Exception):
    def __init__(self, checks: list):
        self.checks = checks
        failed = "; ".join(f"{c.name}: {c.detail}" for c in checks if not c.passed)
        super().__init__(f"Data quality gate failed -> {failed}")


def run_checks(
    df: pd.DataFrame,
    expected_cities=None,
    temp_range=(-60.0, 60.0),
    max_null_frac=0.05,
) -> list:
    expected_cities = list(expected_cities if expected_cities is not None else config.CITIES)
    checks = []

    missing = [c for c in REQUIRED if c not in df.columns]
    checks.append(Check("required_columns", not missing, f"missing {missing}" if missing else "all present"))
    if missing:
        return checks

    checks.append(Check("not_empty", len(df) > 0, f"{len(df)} rows"))

    dups = int(df.duplicated(["city", "time"]).sum())
    checks.append(Check("no_duplicate_city_hours", dups == 0, f"{dups} duplicates"))

    t = df["temperature_c"]
    bad_t = int(((t < temp_range[0]) | (t > temp_range[1])).sum())
    checks.append(Check("temperature_in_range", bad_t == 0, f"{bad_t} values outside {temp_range}"))

    h = df["humidity_pct"]
    bad_h = int(((h < 0) | (h > 100)).sum())
    checks.append(Check("humidity_0_to_100", bad_h == 0, f"{bad_h} values outside 0-100"))

    bad_w = int((df["wind_kmh"] < 0).sum())
    checks.append(Check("wind_non_negative", bad_w == 0, f"{bad_w} negative values"))

    null_frac = float(df[["temperature_c", "humidity_pct", "wind_kmh"]].isna().mean().max()) if len(df) else 1.0
    checks.append(Check("null_rate_ok", null_frac <= max_null_frac, f"max null rate {null_frac:.1%}"))

    absent = sorted(set(expected_cities) - set(df["city"].unique()))
    checks.append(Check("all_cities_present", not absent, f"missing {absent}" if absent else "all cities present"))
    return checks


def assert_quality(df: pd.DataFrame, **kwargs) -> list:
    checks = run_checks(df, **kwargs)
    if not all(c.passed for c in checks):
        raise DataQualityError(checks)
    return checks


def quarantine(bronze_file: Path) -> Path:
    """Move a failing bronze file aside so the next run does not re-read it."""
    config.QUARANTINE.mkdir(parents=True, exist_ok=True)
    dest = config.QUARANTINE / Path(bronze_file).name
    shutil.move(str(bronze_file), dest)
    return dest
