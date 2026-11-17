"""Atomic file writes and the run manifest."""
import json
import os
from pathlib import Path

from . import config


def atomic_write_text(path: Path, text: str) -> None:
    """Write to a temp name, then rename: a file with its final name is always complete."""
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + ".partial")
    tmp.write_text(text, encoding="utf-8")
    os.replace(tmp, path)


def record_run(info: dict) -> None:
    """Save the latest run as manifest.json and append it to runs.jsonl."""
    atomic_write_text(config.MANIFEST, json.dumps(info, indent=2, default=str))
    config.RUNLOG.parent.mkdir(parents=True, exist_ok=True)
    with open(config.RUNLOG, "a", encoding="utf-8") as f:
        f.write(json.dumps(info, default=str) + "\n")
