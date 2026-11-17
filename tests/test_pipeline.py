"""Tests that need only pandas/numpy/requests (no duckdb, pyarrow or prefect)."""
import importlib

import pandas as pd
import pytest


@pytest.fixture()
def cfg(tmp_path, monkeypatch):
    monkeypatch.setenv("PIPELINE_DATA_DIR", str(tmp_path))
    import pipeline.config as config
    importlib.reload(config)
    import pipeline.ingest, pipeline.silver, pipeline.quality
    for m in (pipeline.ingest, pipeline.silver, pipeline.quality):
        importlib.reload(m)
    return config


def _silver(cfg, offline=True, inject_bad=False):
    from pipeline.ingest import run_ingest
    from pipeline.silver import build_silver, load_bronze
    path, _ = run_ingest(offline=offline, inject_bad=inject_bad)
    return path, build_silver(load_bronze(cfg.BRONZE))


def test_clean_run_passes_gate(cfg):
    from pipeline.quality import assert_quality
    _, df = _silver(cfg)
    checks = assert_quality(df)
    assert all(c.passed for c in checks)
    assert df["city"].nunique() == len(cfg.CITIES)
    assert str(df["temperature_c"].dtype) == "float32"
    assert len(df) == len(cfg.CITIES) * 72


def test_bad_value_stopped_and_quarantined(cfg):
    from pipeline.quality import DataQualityError, assert_quality, quarantine
    path, df = _silver(cfg, inject_bad=True)
    with pytest.raises(DataQualityError) as e:
        assert_quality(df)
    assert any(c.name == "temperature_in_range" and not c.passed for c in e.value.checks)
    moved = quarantine(path)
    assert moved.exists() and not path.exists()


def test_rerun_deduplicates_latest_wins(cfg):
    import time
    from pipeline.silver import build_silver, load_bronze
    _silver(cfg)
    time.sleep(1.1)  # different run timestamp -> second bronze file
    _silver(cfg)
    df = build_silver(load_bronze(cfg.BRONZE))
    assert len(df) == len(cfg.CITIES) * 72
    assert not df.duplicated(["city", "time"]).any()


def test_missing_city_and_nulls_detected(cfg):
    from pipeline.quality import run_checks
    _, df = _silver(cfg)
    df = df[df["city"] != "Dubai"]
    names = {c.name for c in run_checks(df) if not c.passed}
    assert "all_cities_present" in names
    df2 = df.copy()
    df2.loc[df2.index[:200], "temperature_c"] = float("nan")
    assert "null_rate_ok" in {c.name for c in run_checks(df2) if not c.passed}
