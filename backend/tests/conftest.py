from __future__ import annotations

import os
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "backend"))
os.environ.setdefault("DATA_DIR", str(ROOT / "backend" / "data_test"))
os.environ.setdefault("QUARANTINE_RETRY_COOLDOWN_SECONDS", "60")
os.environ.setdefault("FIXTURES_DIR", str(ROOT / "fixtures"))


@pytest.fixture(autouse=True)
def fresh_db():
    """每个用例从夹具重建库，避免用例之间互相吃流水/隔离项。"""
    from app import seed
    from app.db import DB_PATH

    for suffix in ("", "-wal", "-shm"):
        path = Path(str(DB_PATH) + suffix)
        if path.exists():
            path.unlink()
    seed.init_db()
    yield
