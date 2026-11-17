from __future__ import annotations

import os
import sys
from datetime import timedelta
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "backend"))
os.environ.setdefault("DATA_DIR", str(ROOT / "backend" / "data_test"))
os.environ.setdefault("FIXTURES_DIR", str(ROOT / "fixtures"))
os.environ["QUARANTINE_RETRY_COOLDOWN_SECONDS"] = "60"
os.environ["QUARANTINE_RETRY_BACKOFF_FACTOR"] = "2"
os.environ["QUARANTINE_RETRY_COOLDOWN_MAX_SECONDS"] = "3600"
os.environ["QUARANTINE_RETRY_MAX"] = "3"

from fastapi.testclient import TestClient  # noqa: E402

from app.db import connect  # noqa: E402
from app.main import app  # noqa: E402
from app.modules import quarantine_retry as qr  # noqa: E402


_C: TestClient | None = None


@pytest.fixture()
def client():
    global _C
    with TestClient(app) as c:
        _C = c
        yield c


def _settle() -> None:
    r = _C.post("/api/settle-runs")
    assert r.status_code == 200, r.text


def _items(**params) -> list[dict]:
    r = _C.get("/api/quarantine", params=params)
    assert r.status_code == 200, r.text
    return r.json()


def _item_by_key(key: str) -> dict:
    for row in _items():
        if row["entry_key"] == key:
            return row
    raise AssertionError(f"未找到隔离项 {key}")


def _balance(account_id: int) -> int:
    with connect() as conn:
        return conn.execute(
            "SELECT balance_cents FROM accounts WHERE id = ?", (account_id,)
        ).fetchone()["balance_cents"]


def _rewind_cooldown(item_id: int, seconds_ago: int = 1) -> None:
    """测试辅助：把 next_at 拨到过去，模拟冷却结束。"""
    with connect() as conn:
        conn.execute(
            "UPDATE quarantine_retry_state SET next_at = ? WHERE item_id = ?",
            (qr.fmt_time(qr.utcnow() - timedelta(seconds=seconds_ago)), item_id),
        )
        conn.commit()


def _make_quarantined_reversal(key: str, account_id: int, amount: int, target: str) -> int:
    """造一条已隔离冲正 + 隔离项 + 重试进度，返回隔离项 id。"""
    now = qr.fmt_time(qr.utcnow())
    with connect() as conn:
        conn.execute(
            """
            INSERT INTO ledger_entries(
              entry_key, account_id, kind, amount_cents, biz_ref,
              target_entry_key, booked_at, status, note
            ) VALUES (?, ?, '冲正', ?, 'ORD-T', ?, ?, '已隔离', '测试隔离')
            """,
            (key, account_id, amount, target, now),
        )
        cur = conn.execute(
            """
            INSERT INTO quarantine_items(
              entry_key, account_id, reason, amount_cents, settle_run_id, created_at
            ) VALUES (?, ?, '冲正无原单', ?, 1, ?)
            """,
            (key, account_id, amount, now),
        )
        item_id = cur.lastrowid
        qr.register_item(conn, item_id, now)
        conn.commit()
    return item_id


def _add_late_forward(key: str, account_id: int, amount: int) -> None:
    with connect() as conn:
        conn.execute(
            """
            INSERT INTO ledger_entries(
              entry_key, account_id, kind, amount_cents, biz_ref,
              target_entry_key, booked_at, status, note
            ) VALUES (?, ?, '正向', ?, 'ORD-T', NULL, ?, '待归并', '迟到原单')
            """,
            (key, account_id, amount, qr.fmt_time(qr.utcnow())),
        )
        conn.commit()


def test_settle_registers_retry_state(client):
    _settle()
    items = _items()
    assert items, "应产生隔离项"
    for row in items:
        assert row["retries"] == 0
        assert row["max_retries"] == 3
        assert row["next_at"] is not None
        assert row["state"] in (qr.COOLING, qr.READY)
    victim = _item_by_key("REV-20261116-004")
    assert victim["state"] == qr.COOLING
    assert victim["can_retry"] is False
    assert "冷却" in victim["block_reason"]


def test_manual_retry_rejected_while_cooling(client):
    _settle()
    item = _item_by_key("REV-20261116-004")
    r = _C.post(f"/api/quarantine/{item['id']}/retry")
    assert r.status_code == 409, r.text
    assert "冷却" in r.json()["detail"]
    # 拒绝不得推进次数，也不得改余额
    after = _item_by_key("REV-20261116-004")
    assert after["retries"] == 0
    assert after["state"] == qr.COOLING


def test_retry_failure_increments_and_sets_next_at_without_balance_change(client):
    acct = 1
    item_id = _make_quarantined_reversal("REV-RETRY-FAIL", acct, 700, "FWD-ABSENT")
    item = next(x for x in _items() if x["id"] == item_id)
    before_balance = _balance(acct)
    assert item["can_retry"] is False
    assert "冷却" in item["block_reason"]

    _rewind_cooldown(item_id)
    assert next(x for x in _items() if x["id"] == item_id)["can_retry"] is True

    r = _C.post(f"/api/quarantine/{item_id}/retry")
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["outcome"] == "失败"
    assert body["retries"] == 1
    assert body["next_at"]
    assert body["balance_delta_cents"] == 0

    after = next(x for x in _items() if x["id"] == item_id)
    assert after["retries"] == 1
    assert after["state"] == qr.COOLING
    assert after["next_at"] == body["next_at"]
    assert after["cooldown_seconds_remaining"] > 0
    assert _balance(acct) == before_balance, "失败路径不得改余额"

    attempts = _C.get(f"/api/quarantine/{item_id}/attempts").json()
    assert attempts[0]["outcome"] == "失败"
    assert attempts[0]["attempt_no"] == 1
    assert attempts[0]["retries_after"] == 1

    # 计数已落库（新连接读得到），不是调度内存
    with connect() as conn:
        row = conn.execute(
            "SELECT retries, state, next_at FROM quarantine_retry_state WHERE item_id = ?",
            (item_id,),
        ).fetchone()
    assert row["retries"] == 1 and row["state"] == qr.COOLING


def test_retry_success_leaves_quarantine_and_moves_balance(client):
    acct = 1
    item_id = _make_quarantined_reversal("REV-RETRY-OK", acct, 3000, "FWD-LATE-OK")
    before_balance = _balance(acct)
    before_ledger = _C.get("/api/quarantine/retry-report").json()["ledger_net_cents"]
    _rewind_cooldown(item_id)

    _add_late_forward("FWD-LATE-OK", acct, 5000)

    r = _C.post(f"/api/quarantine/{item_id}/retry")
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["outcome"] == "成功"
    assert body["state"] == qr.RELEASED
    assert body["retries"] == 1
    assert body["balance_delta_cents"] == 2000  # 原单 5000 − 冲正 3000
    assert _balance(acct) == before_balance + 2000, "成功路径必须改余额"

    # 离开隔离：默认列表不再出现，显式查询可见
    assert all(x["id"] != item_id for x in _items())
    released = _items(include_released="true")
    assert any(x["id"] == item_id and x["state"] == qr.RELEASED for x in released)

    # 已解除项禁止再次重试，余额不再变化
    again = _C.post(f"/api/quarantine/{item_id}/retry")
    assert again.status_code == 409
    assert "已解除" in again.json()["detail"]
    assert _balance(acct) == before_balance + 2000

    # 隔离态、余额、流水同源：互证一致
    report = _C.get("/api/quarantine/retry-report").json()
    assert report["retry_success_count"] >= 1
    assert report["consistent"] == 1
    assert report["ledger_net_cents"] == before_ledger + 2000

    # 互证报告的重试口径与隔离列表/余额同源
    dash = _C.get("/api/dashboard").json()
    assert dash["余额与流水一致"] == 1
    assert dash["重试统计"]["retry_success_count"] == report["retry_success_count"]

    # 成功尝试也落明细
    attempts = _C.get(f"/api/quarantine/{item_id}/attempts").json()
    assert attempts[0]["outcome"] == "成功"
    assert attempts[0]["balance_delta_cents"] == 2000
    assert attempts[0]["retries_after"] == 1

    # 重跑归并不得把重试补记的余额抹掉：余额仍等于该账户已归并净额
    rerun = _C.post("/api/settle-runs")
    assert rerun.status_code == 200, rerun.text
    with connect() as conn:
        expected = conn.execute(
            """
            SELECT COALESCE(SUM(CASE WHEN kind = '正向' THEN amount_cents
                                     ELSE -amount_cents END), 0) AS s
            FROM ledger_entries
            WHERE account_id = ? AND status = '已归并'
            """,
            (acct,),
        ).fetchone()["s"]
        retry_rows = conn.execute(
            "SELECT status FROM ledger_entries WHERE entry_key IN ('REV-RETRY-OK','FWD-LATE-OK')"
        ).fetchall()
    assert _balance(acct) == expected
    assert len(retry_rows) == 2
    assert all(r["status"] == "已归并" for r in retry_rows)
    report_after = _C.get("/api/quarantine/retry-report").json()
    assert report_after["consistent"] == 1
    assert report_after["released_count"] == 1


def test_dead_letter_after_max_retries_blocks_auto_and_manual(client):
    item_id = _make_quarantined_reversal("REV-RETRY-DEAD", 2, 1200, "FWD-NEVER")
    acct = 2
    before_balance = _balance(acct)
    max_retries = next(x for x in _items() if x["id"] == item_id)["max_retries"]

    for n in range(1, max_retries + 1):
        _rewind_cooldown(item_id)
        r = _C.post(f"/api/quarantine/{item_id}/retry")
        assert r.status_code == 200, r.text
        body = r.json()
        assert body["outcome"] == "失败"
        assert body["retries"] == n
        assert body["balance_delta_cents"] == 0

    row = next(x for x in _items() if x["id"] == item_id)
    assert row["state"] == qr.DEAD
    assert row["dead_letter"] is True
    assert row["retries"] == max_retries
    assert row["can_retry"] is False
    assert "死信" in row["block_reason"]
    assert _balance(acct) == before_balance

    # 单独可查
    dead = _items(state="死信")
    assert any(x["id"] == item_id for x in dead)

    # 手动重试被拒
    r = _C.post(f"/api/quarantine/{item_id}/retry")
    assert r.status_code == 409
    assert "死信" in r.json()["detail"]

    # 自动扫描不得再碰死信（即使把 next_at 拨到过去）
    _rewind_cooldown(item_id)
    auto = _C.post("/api/quarantine/retry-due").json()
    assert all(x.get("item_id") != item_id for x in auto["results"])
    assert next(x for x in _items() if x["id"] == item_id)["retries"] == max_retries

    report = _C.get("/api/quarantine/retry-report").json()
    assert report["dead_letter_count"] >= 1
    assert report["dead_letter_exposure_cents"] >= 1200

    dash = _C.get("/api/dashboard").json()
    assert dash["死信"] >= 1
    assert dash["重试统计"]["dead_letter_count"] == report["dead_letter_count"]


def test_auto_retry_only_due_items_and_persists_progress(client):
    _settle()
    _make_quarantined_reversal("REV-AUTO-A", 1, 400, "FWD-AUTO-MISSING-A")
    _make_quarantined_reversal("REV-AUTO-B", 2, 500, "FWD-AUTO-MISSING-B")
    with connect() as conn:
        rows = conn.execute(
            "SELECT item_id FROM quarantine_retry_state WHERE state IN ('冷却中','可重试')"
        ).fetchall()
    for row in rows:
        _rewind_cooldown(row["item_id"])

    before = {
        x["id"]: x["retries"] for x in _items(include_released="true")
    }
    auto = _C.post("/api/quarantine/retry-due").json()

    after = {
        x["id"]: x["retries"] for x in _items(include_released="true")
    }
    for res in auto["results"]:
        if res.get("rejected"):
            continue
        # 每次真正尝试都落库 +1（成功与失败一致）
        assert after[res["item_id"]] == before[res["item_id"]] + 1

    # 报告从库里读，和隔离列表口径一致
    report = _C.get("/api/quarantine/retry-report").json()
    open_items = _items()
    assert report["open_count"] == len(open_items)
    assert report["cooldown_count"] == sum(1 for x in open_items if x["state"] == qr.COOLING)
    assert report["ready_count"] == sum(1 for x in open_items if x["state"] == qr.READY)
    assert report["dead_letter_count"] == sum(1 for x in open_items if x["state"] == qr.DEAD)
    released = _items(include_released="true")
    assert report["retry_success_count"] == sum(1 for x in released if x["state"] == qr.RELEASED)


def test_retry_state_persisted_not_in_memory(client):
    _settle()
    with connect() as conn:
        rows = conn.execute(
            "SELECT item_id, retries, next_at, max_retries, state FROM quarantine_retry_state"
        ).fetchall()
    assert rows, "重试进度必须落库"
    for row in rows:
        assert row["max_retries"] >= 1
        assert row["retries"] <= row["max_retries"]
        assert row["state"] in (qr.COOLING, qr.READY, qr.DEAD, qr.RELEASED)
