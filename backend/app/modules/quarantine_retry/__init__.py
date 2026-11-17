"""未平隔离重试策略机：重试计数、冷却、死信与原子放行。

设计要点（对应隔离重试策略，不是放行冻结窗 / SLA 游标）：

* 重试进度落在扩展表 ``quarantine_retry_state``（retries / next_at / max_retries），
  不在调度内存里数次数；
* 每次真正尝试都在 ``BEGIN IMMEDIATE`` 事务内完成：成功 = 计数 +1、改余额、离开隔离；
  失败 = 计数 +1、写新 next_at、余额不动；
* 失败次数达到上限进入 ``死信``，自动与手动重试均被拒绝；
* 重试明细另落 ``quarantine_retry_attempts``，报告与隔离态、余额同源读取。
"""

from __future__ import annotations

import math
import os
import sqlite3
from datetime import datetime, timedelta, timezone
from typing import Any

from fastapi import HTTPException

from app.db import connect, rows_to_dicts

# —— 隔离重试状态 ——
COOLING = "冷却中"
READY = "可重试"
DEAD = "死信"
RELEASED = "已解除"
OPEN_STATES = (COOLING, READY)

TRIGGER_MANUAL = "手动"
TRIGGER_AUTO = "自动"

OUTCOME_SUCCESS = "成功"
OUTCOME_FAILURE = "失败"

_TIME_FMT = "%Y-%m-%dT%H:%M:%SZ"

SCHEMA = """
CREATE TABLE IF NOT EXISTS quarantine_retry_state (
  item_id INTEGER PRIMARY KEY REFERENCES quarantine_items(id) ON DELETE CASCADE,
  retries INTEGER NOT NULL DEFAULT 0,
  max_retries INTEGER NOT NULL DEFAULT 3,
  next_at TEXT,
  state TEXT NOT NULL DEFAULT '冷却中',
  last_attempt_at TEXT,
  last_outcome TEXT NOT NULL DEFAULT '',
  last_reason TEXT NOT NULL DEFAULT '',
  released_at TEXT,
  balance_delta_cents INTEGER NOT NULL DEFAULT 0,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS quarantine_retry_attempts (
  id INTEGER PRIMARY KEY,
  item_id INTEGER NOT NULL REFERENCES quarantine_items(id) ON DELETE CASCADE,
  entry_key TEXT NOT NULL,
  attempt_no INTEGER NOT NULL,
  trigger TEXT NOT NULL,
  outcome TEXT NOT NULL,
  reason TEXT NOT NULL DEFAULT '',
  balance_delta_cents INTEGER NOT NULL DEFAULT 0,
  retries_after INTEGER NOT NULL,
  next_at_after TEXT,
  state_after TEXT NOT NULL,
  attempted_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_quarantine_retry_attempts_item
  ON quarantine_retry_attempts(item_id, id);
CREATE INDEX IF NOT EXISTS idx_quarantine_retry_state_due
  ON quarantine_retry_state(state, next_at);
"""


# —— 时间与配置 ——

def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def fmt_time(value: datetime) -> str:
    return value.astimezone(timezone.utc).replace(microsecond=0).strftime(_TIME_FMT)


def parse_time(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def add_seconds(value: str | datetime, seconds: int) -> str:
    if isinstance(value, str):
        parsed = parse_time(value) or utcnow()
    else:
        parsed = value
    return fmt_time(parsed + timedelta(seconds=max(0, int(seconds))))


def _env_int(name: str, default: int, minimum: int = 0) -> int:
    raw = os.environ.get(name)
    if raw is None or raw == "":
        return default
    try:
        return max(minimum, int(raw))
    except ValueError:
        return default


def cooldown_base_seconds() -> int:
    """首次失败后的基础冷却；0 表示无冷却（便于本地/测试）。"""
    return _env_int("QUARANTINE_RETRY_COOLDOWN_SECONDS", 60, 0)


def cooldown_max_seconds() -> int:
    return _env_int("QUARANTINE_RETRY_COOLDOWN_MAX_SECONDS", 3600, 0)


def default_max_retries() -> int:
    return _env_int("QUARANTINE_RETRY_MAX", 3, 1)


def backoff_seconds(retries_after: int) -> int:
    """第 n 次失败后的冷却：base * factor^(n-1)，封顶。"""
    base = cooldown_base_seconds()
    if base <= 0:
        return 0
    factor = _env_int("QUARANTINE_RETRY_BACKOFF_FACTOR", 2, 1)
    try:
        value = base * (factor ** max(0, retries_after - 1))
    except OverflowError:
        value = cooldown_max_seconds()
    cap = cooldown_max_seconds()
    if cap > 0:
        value = min(value, cap)
    return value


# —— 建表 / 补登记 ——

def ensure_schema(conn: sqlite3.Connection) -> None:
    conn.executescript(SCHEMA)


def _insert_state(
    conn: sqlite3.Connection,
    item_id: int,
    created_at: str,
    now: str,
) -> None:
    next_at = add_seconds(created_at, cooldown_base_seconds())
    next_dt = parse_time(next_at)
    now_dt = parse_time(now) or utcnow()
    state = READY if next_dt is None or next_dt <= now_dt else COOLING
    conn.execute(
        """
        INSERT OR IGNORE INTO quarantine_retry_state(
          item_id, retries, max_retries, next_at, state,
          created_at, updated_at
        ) VALUES (?, 0, ?, ?, ?, ?, ?)
        """,
        (item_id, default_max_retries(), next_at, state, created_at, now),
    )


def register_item(
    conn: sqlite3.Connection,
    item_id: int,
    created_at: str,
    now: str | None = None,
) -> None:
    """隔离项落库时同步登记重试进度（与隔离项同一事务）。"""
    _insert_state(conn, item_id, created_at, now or fmt_time(utcnow()))


def backfill_state(conn: sqlite3.Connection, now: str | None = None) -> int:
    """为存量隔离项补登记重试进度（升级旧库时使用）。"""
    now = now or fmt_time(utcnow())
    rows = conn.execute(
        """
        SELECT q.id, q.created_at
        FROM quarantine_items q
        LEFT JOIN quarantine_retry_state s ON s.item_id = q.id
        WHERE s.item_id IS NULL
        ORDER BY q.id
        """
    ).fetchall()
    for row in rows:
        _insert_state(conn, row["id"], row["created_at"], now)
    return len(rows)


# —— 状态与闸门 ——

def effective_state(stored_state: str, next_at: str | None, now: datetime) -> str:
    if stored_state == COOLING:
        due = parse_time(next_at)
        if due is None or due <= now:
            return READY
    return stored_state


def cooldown_remaining_seconds(next_at: str | None, now: datetime) -> int:
    due = parse_time(next_at)
    if due is None:
        return 0
    return max(0, int(math.ceil((due - now).total_seconds())))


def gate(
    retries: int,
    max_retries: int,
    stored_state: str,
    next_at: str | None,
    now: datetime,
) -> tuple[bool, str]:
    """返回 (是否允许重试, 拒绝原因)。"""
    state = effective_state(stored_state, next_at, now)
    if state == RELEASED:
        return False, "该隔离项已解除（重试已配平离开隔离）"
    return True, ""


def _state_row(conn: sqlite3.Connection, item_id: int) -> sqlite3.Row | None:
    return conn.execute(
        "SELECT * FROM quarantine_retry_state WHERE item_id = ?", (item_id,)
    ).fetchone()


# —— 查询 ——

def _decorate(row: sqlite3.Row, now: datetime) -> dict[str, Any]:
    item = dict(row)
    state = effective_state(item["retry_state"], item["next_at"], now)
    remain = cooldown_remaining_seconds(item["next_at"], now)
    allowed, block_reason = gate(
        item["retries"], item["max_retries"], item["retry_state"], item["next_at"], now
    )
    item["state"] = state
    item["stored_state"] = item["retry_state"]
    item["dead_letter"] = state == DEAD
    item["cooldown_seconds_remaining"] = remain
    item["can_retry"] = allowed
    item["block_reason"] = block_reason
    return item


def list_items(
    *,
    state_filter: str | None = None,
    include_released: bool = False,
    now: datetime | None = None,
) -> list[dict[str, Any]]:
    now = now or utcnow()
    now_s = fmt_time(now)
    with connect() as conn:
        backfill_state(conn, now_s)
        rows = conn.execute(
            """
            SELECT q.id, q.entry_key, q.account_id, q.reason, q.amount_cents,
                   q.settle_run_id, q.created_at,
                   a.code AS account_code,
                   s.retries, s.max_retries, s.next_at, s.state AS retry_state,
                   s.last_attempt_at, s.last_outcome, s.last_reason,
                   s.released_at, s.balance_delta_cents
            FROM quarantine_items q
            JOIN accounts a ON a.id = q.account_id
            JOIN quarantine_retry_state s ON s.item_id = q.id
            ORDER BY q.id DESC
            """
        ).fetchall()
        items = [_decorate(r, now) for r in rows]

    if state_filter:
        items = [x for x in items if x["state"] == state_filter]
    elif not include_released:
        items = [x for x in items if x["state"] != RELEASED]
    return items


# —— 报告（与隔离态、余额同源读取） ——

def list_attempts(item_id: int, *, limit: int = 50) -> list[dict[str, Any]]:
    with connect() as conn:
        item = conn.execute(
            "SELECT id FROM quarantine_items WHERE id = ?", (item_id,)
        ).fetchone()
        if item is None:
            raise HTTPException(404, "隔离项不存在")
        return rows_to_dicts(
            conn.execute(
                """
                SELECT * FROM quarantine_retry_attempts
                WHERE item_id = ?
                ORDER BY id DESC
                LIMIT ?
                """,
                (item_id, limit),
            ).fetchall()
        )


def stats_for_conn(conn: sqlite3.Connection, now: datetime | None = None) -> dict[str, Any]:
    """在调用方连接上读取隔离态、敞口与余额，供报告/互证共用一个快照。"""
    now = now or utcnow()
    rows = rows_to_dicts(
        conn.execute(
            """
            SELECT q.amount_cents, COALESCE(s.retries, 0) AS retries,
                   COALESCE(s.max_retries, 0) AS max_retries,
                   s.next_at, COALESCE(s.state, '冷却中') AS state
            FROM quarantine_items q
            LEFT JOIN quarantine_retry_state s ON s.item_id = q.id
            """
        ).fetchall()
    )
    cooldown = ready = dead = released = 0
    open_exposure = live_exposure = dead_exposure = 0
    for row in rows:
        state = effective_state(row["state"], row["next_at"], now)
        if state == RELEASED:
            released += 1
            continue
        if state == DEAD:
            dead += 1
            open_exposure += row["amount_cents"]
            dead_exposure += row["amount_cents"]
        elif state == COOLING:
            cooldown += 1
            open_exposure += row["amount_cents"]
            live_exposure += row["amount_cents"]
        else:
            ready += 1
            open_exposure += row["amount_cents"]
            live_exposure += row["amount_cents"]

    open_count = cooldown + ready + dead
    balance = conn.execute(
        "SELECT COALESCE(SUM(balance_cents), 0) AS s FROM accounts"
    ).fetchone()["s"]
    fwd = conn.execute(
        "SELECT COALESCE(SUM(amount_cents), 0) AS s FROM ledger_entries "
        "WHERE kind = '正向' AND status = '已归并'"
    ).fetchone()["s"]
    rev = conn.execute(
        "SELECT COALESCE(SUM(amount_cents), 0) AS s FROM ledger_entries "
        "WHERE kind = '冲正' AND status = '已归并'"
    ).fetchone()["s"]
    ledger_net = fwd - rev
    return {
        "quarantine_total": len(rows),
        "open_count": open_count,
        "cooldown_count": cooldown,
        "ready_count": ready,
        "dead_letter_count": dead,
        "released_count": released,
        "retry_success_count": released,
        "quarantine_exposure_cents": open_exposure,
        "live_exposure_cents": live_exposure,
        "dead_letter_exposure_cents": dead_exposure,
        "account_balance_cents": balance,
        "ledger_net_cents": ledger_net,
        "consistent": 1 if balance == ledger_net else 0,
    }


def report(now: datetime | None = None) -> dict[str, Any]:
    now = now or utcnow()
    now_s = fmt_time(now)
    conn = connect()
    try:
        conn.isolation_level = None
        conn.execute("BEGIN")
        backfill = backfill_state(conn, now_s)
        stats = stats_for_conn(conn, now)
        attempts = conn.execute(
            """
            SELECT COUNT(*) AS total,
                   COALESCE(SUM(CASE WHEN outcome = '成功' THEN 1 ELSE 0 END), 0) AS success,
                   COALESCE(SUM(CASE WHEN outcome = '失败' THEN 1 ELSE 0 END), 0) AS failure
            FROM quarantine_retry_attempts
            """
        ).fetchone()
        conn.execute("COMMIT")
    except Exception:
        try:
            conn.execute("ROLLBACK")
        except sqlite3.Error:
            pass
        raise
    finally:
        conn.close()
    stats.update(
        {
            "generated_at": now_s,
            "backfilled_items": backfill,
            "attempt_count": attempts["total"],
            "attempt_success_count": attempts["success"],
            "retry_failure_count": attempts["failure"],
            # 重试成功数以隔离态「已解除」为准，与隔离列表同源
            "retry_success_count": stats["released_count"],
        }
    )
    return stats


# —— 重试执行 ——

def _failure_reason(
    conn: sqlite3.Connection, reversal: sqlite3.Row
) -> tuple[str | None, sqlite3.Row | None]:
    target_key = reversal["target_entry_key"]
    if not target_key:
        return "冲正缺少原单键，无法配平", None
    target = conn.execute(
        "SELECT * FROM ledger_entries WHERE entry_key = ?", (target_key,)
    ).fetchone()
    if target is None:
        return f"原单仍未到账（目标键 {target_key}）", None
    if target["kind"] != "正向":
        return f"原单 {target_key} 不是正向流水", target
    if target["account_id"] != reversal["account_id"]:
        return "冲正账户与原单不一致", target
    if target["status"] not in ("待归并", "已归并"):
        return f"原单状态不可配平（{target['status']}）", target
    used = conn.execute(
        """
        SELECT COALESCE(SUM(amount_cents), 0) AS s
        FROM ledger_entries
        WHERE kind = '冲正' AND status = '已归并'
          AND target_entry_key = ? AND id <> ?
        """,
        (target_key, reversal["id"]),
    ).fetchone()["s"]
    available = target["amount_cents"] - used
    if reversal["amount_cents"] > available:
        return (
            f"冲正超额：原单可配平 {available} 分，冲正 {reversal['amount_cents']} 分",
            target,
        )
    return None, target


def _perform_attempt(
    conn: sqlite3.Connection,
    item: sqlite3.Row,
    state: sqlite3.Row,
    now: datetime,
    trigger: str,
) -> dict[str, Any]:
    now_s = fmt_time(now)
    retries_before = state["retries"]
    max_retries = state["max_retries"]
    new_retries = retries_before + 1

    reversal = conn.execute(
        "SELECT * FROM ledger_entries WHERE entry_key = ?", (item["entry_key"],)
    ).fetchone()
    failure: str | None
    target: sqlite3.Row | None = None
    if reversal is None:
        failure = f"隔离流水 {item['entry_key']} 不存在"
    elif reversal["kind"] != "冲正":
        failure = f"隔离流水 {item['entry_key']} 不是冲正"
    elif reversal["status"] != "已隔离":
        failure = f"隔离流水状态异常（{reversal['status']}）"
    elif reversal["account_id"] != item["account_id"]:
        failure = "隔离项账户与冲正流水不一致"
    else:
        failure, target = _failure_reason(conn, reversal)

    if failure is None and target is not None:
        # 成功：原单若尚未归并，则一并入账；冲正从隔离放行。
        delta = 0
        if target["status"] == "待归并":
            updated = conn.execute(
                "UPDATE ledger_entries SET status = '已归并' "
                "WHERE id = ? AND status = '待归并'",
                (target["id"],),
            ).rowcount
            if updated != 1:
                raise RuntimeError("原单状态在重试事务内发生变化")
            delta += target["amount_cents"]

        updated = conn.execute(
            "UPDATE ledger_entries SET status = '已归并' "
            "WHERE id = ? AND status = '已隔离'",
            (reversal["id"],),
        ).rowcount
        if updated != 1:
            raise RuntimeError("冲正状态在重试事务内发生变化")
        delta -= reversal["amount_cents"]

        # 计数、余额、隔离态在同一个 BEGIN IMMEDIATE 事务内一起落库。
        conn.execute(
            "UPDATE accounts SET balance_cents = balance_cents + ? WHERE id = ?",
            (delta, reversal["account_id"]),
        )
        advanced = conn.execute(
            """
            UPDATE quarantine_retry_state
            SET retries = ?, next_at = NULL, state = ?, last_attempt_at = ?,
                last_outcome = ?, last_reason = '', released_at = ?,
                balance_delta_cents = ?, updated_at = ?
            WHERE item_id = ? AND retries = ? AND state NOT IN (?, ?)
            """,
            (
                new_retries,
                RELEASED,
                now_s,
                OUTCOME_SUCCESS,
                now_s,
                delta,
                now_s,
                item["id"],
                retries_before,
                RELEASED,
                DEAD,
            ),
        ).rowcount
        if advanced != 1:
            # 同一事务内计数必须恰好推进一次，否则整体回滚（余额也不会变）。
            raise RuntimeError("重试计数未按预期推进，已回滚")
        conn.execute(
            """
            INSERT INTO quarantine_retry_attempts(
              item_id, entry_key, attempt_no, trigger, outcome, reason,
              balance_delta_cents, retries_after, next_at_after, state_after,
              attempted_at
            ) VALUES (?, ?, ?, ?, ?, '', ?, ?, NULL, ?, ?)
            """,
            (
                item["id"],
                item["entry_key"],
                new_retries,
                trigger,
                OUTCOME_SUCCESS,
                delta,
                new_retries,
                RELEASED,
                now_s,
            ),
        )
        balance = conn.execute(
            "SELECT balance_cents FROM accounts WHERE id = ?",
            (reversal["account_id"],),
        ).fetchone()["balance_cents"]
        return {
            "ok": True,
            "rejected": False,
            "outcome": OUTCOME_SUCCESS,
            "item_id": item["id"],
            "entry_key": item["entry_key"],
            "reason": "",
            "retries": new_retries,
            "max_retries": max_retries,
            "state": RELEASED,
            "dead_letter": False,
            "next_at": None,
            "cooldown_seconds_remaining": 0,
            "balance_delta_cents": delta,
            "account_balance_cents": balance,
            "trigger": trigger,
        }

    # 失败：只推进重试进度，余额不动。
    next_at = None
    new_state = READY
    advanced = conn.execute(
        """
        UPDATE quarantine_retry_state
        SET retries = ?, next_at = ?, state = ?, last_attempt_at = ?,
            last_outcome = ?, last_reason = ?, updated_at = ?
        WHERE item_id = ? AND retries = ? AND state NOT IN (?, ?)
        """,
        (
            new_retries,
            next_at,
            new_state,
            now_s,
            OUTCOME_FAILURE,
            failure,
            now_s,
            item["id"],
            retries_before,
            RELEASED,
            DEAD,
        ),
    ).rowcount
    if advanced != 1:
        # 计数未推进时不得插入明细或改动任何余额。
        raise RuntimeError("重试计数未按预期推进，已回滚")
    conn.execute(
        """
        INSERT INTO quarantine_retry_attempts(
          item_id, entry_key, attempt_no, trigger, outcome, reason,
          balance_delta_cents, retries_after, next_at_after, state_after,
          attempted_at
        ) VALUES (?, ?, ?, ?, ?, ?, 0, ?, ?, ?, ?)
        """,
        (
            item["id"],
            item["entry_key"],
            new_retries,
            trigger,
            OUTCOME_FAILURE,
            failure,
            new_retries,
            next_at,
            new_state,
            now_s,
        ),
    )
    return {
        "ok": False,
        "rejected": False,
        "outcome": OUTCOME_FAILURE,
        "item_id": item["id"],
        "entry_key": item["entry_key"],
        "reason": failure,
        "retries": new_retries,
        "max_retries": max_retries,
        "state": new_state,
        "dead_letter": new_state == DEAD,
        "next_at": next_at,
        "cooldown_seconds_remaining": cooldown_remaining_seconds(next_at, now),
        "balance_delta_cents": 0,
        "trigger": trigger,
    }


def retry_item(
    item_id: int,
    *,
    trigger: str = TRIGGER_MANUAL,
    now: datetime | None = None,
) -> dict[str, Any]:
    """手动或自动重试一个隔离项。闸门拒绝抛 409；真正尝试成功/失败返回结果。"""
    now = now or utcnow()
    now_s = fmt_time(now)
    conn = connect()
    try:
        conn.isolation_level = None
        conn.execute("BEGIN IMMEDIATE")
        item = conn.execute(
            "SELECT * FROM quarantine_items WHERE id = ?", (item_id,)
        ).fetchone()
        if item is None:
            raise HTTPException(404, "隔离项不存在")
        state = _state_row(conn, item_id)
        if state is None:
            _insert_state(conn, item_id, item["created_at"], now_s)
            state = _state_row(conn, item_id)
        assert state is not None
        allowed, reason = gate(
            state["retries"],
            state["max_retries"],
            state["state"],
            state["next_at"],
            now,
        )
        if not allowed:
            raise HTTPException(409, f"拒绝重试：{reason}")
        result = _perform_attempt(conn, item, state, now, trigger)
        conn.execute("COMMIT")
        return result
    except Exception:
        try:
            conn.execute("ROLLBACK")
        except sqlite3.Error:
            pass
        raise
    finally:
        conn.close()


def retry_due(
    *,
    limit: int | None = None,
    now: datetime | None = None,
) -> dict[str, Any]:
    """自动扫描到期项：只碰冷却结束且未达上限的隔离项。"""
    now = now or utcnow()
    now_s = fmt_time(now)
    with connect() as conn:
        backfill_state(conn, now_s)
        rows = conn.execute(
            """
            SELECT s.item_id
            FROM quarantine_retry_state s
            WHERE s.state IN (?, ?, ?)
            ORDER BY COALESCE(s.next_at, '') ASC, s.item_id ASC
            """,
            (COOLING, READY, DEAD),
        ).fetchall()
    item_ids = [r["item_id"] for r in rows]
    if limit:
        item_ids = item_ids[:limit]

    results: list[dict[str, Any]] = []
    rejected = 0
    for item_id in item_ids:
        try:
            results.append(retry_item(item_id, trigger=TRIGGER_AUTO))
        except HTTPException as exc:
            rejected += 1
            results.append(
                {
                    "ok": False,
                    "rejected": True,
                    "item_id": item_id,
                    "reason": str(exc.detail),
                }
            )
    succeeded = sum(1 for r in results if r.get("outcome") == OUTCOME_SUCCESS)
    failed = sum(1 for r in results if r.get("outcome") == OUTCOME_FAILURE)
    return {
        "now": fmt_time(now),
        "attempted": len(results) - rejected,
        "succeeded": succeeded,
        "failed": failed,
        "rejected": rejected,
        "results": results,
        "report": report(),
    }
