"""未平隔离重试策略机：重试计数、冷却、死信与原子放行。

设计要点（对应隔离重试策略，不是放行冻结窗 / SLA 游标）：

* 重试进度只落在扩展表 ``quarantine_retry_state``（retries / max_retries / next_at /
  cooldown_interval_seconds / last_attempt_at），隔离行本身不另存一份计数，也不在调度内存里数次数；
* 重试入口先过闸门：未到 ``next_at``（冷却中）或已达死信上限直接 409 拒绝，
  拒绝路径不改余额、不推进次数；
* 每次真正尝试都在 ``BEGIN IMMEDIATE`` 事务内完成：成功 = 计数 +1、改余额、离开隔离；
  失败 = 计数 +1、按 ``base * factor^(n-1)`` 写新 next_at、余额不动；达到上限转 ``死信``；
* 每次闸门评估（登记、尝试、拒绝）都落一条冷却证明 ``quarantine_cooldown_proofs``
  （隔离键、间隔、是否放行、是否死信），与扩展表同一事务写入，可按隔离 id 只读查询；
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
  cooldown_interval_seconds INTEGER NOT NULL DEFAULT 0,
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
CREATE TABLE IF NOT EXISTS quarantine_cooldown_proofs (
  id INTEGER PRIMARY KEY,
  item_id INTEGER NOT NULL REFERENCES quarantine_items(id) ON DELETE CASCADE,
  entry_key TEXT NOT NULL,
  retries INTEGER NOT NULL,
  max_retries INTEGER NOT NULL,
  cooldown_interval_seconds INTEGER NOT NULL DEFAULT 0,
  next_at TEXT,
  remaining_seconds INTEGER NOT NULL DEFAULT 0,
  cooldown_active INTEGER NOT NULL DEFAULT 0,
  allowed INTEGER NOT NULL DEFAULT 0,
  dead_letter INTEGER NOT NULL DEFAULT 0,
  state TEXT NOT NULL,
  source TEXT NOT NULL,
  evaluated_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_quarantine_retry_attempts_item
  ON quarantine_retry_attempts(item_id, id);
CREATE INDEX IF NOT EXISTS idx_quarantine_retry_state_due
  ON quarantine_retry_state(state, next_at);
CREATE INDEX IF NOT EXISTS idx_quarantine_cooldown_proofs_item
  ON quarantine_cooldown_proofs(item_id, id);
"""


def _migrate(conn: sqlite3.Connection) -> None:
    """补偿旧库缺列：扩展表加冷却间隔列（幂等）。"""
    cols = {
        r["name"]
        for r in conn.execute("PRAGMA table_info(quarantine_retry_state)").fetchall()
    }
    if "cooldown_interval_seconds" not in cols:
        conn.execute(
            "ALTER TABLE quarantine_retry_state "
            "ADD COLUMN cooldown_interval_seconds INTEGER NOT NULL DEFAULT 0"
        )


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
    _migrate(conn)


def _insert_state(
    conn: sqlite3.Connection,
    item_id: int,
    created_at: str,
    now: str,
) -> None:
    interval = cooldown_base_seconds()
    next_at = add_seconds(created_at, interval)
    next_dt = parse_time(next_at)
    now_dt = parse_time(now) or utcnow()
    state = READY if next_dt is None or next_dt <= now_dt else COOLING
    inserted = conn.execute(
        """
        INSERT OR IGNORE INTO quarantine_retry_state(
          item_id, retries, max_retries, next_at, cooldown_interval_seconds,
          state, created_at, updated_at
        ) VALUES (?, 0, ?, ?, ?, ?, ?, ?)
        """,
        (item_id, default_max_retries(), next_at, interval, state, created_at, now),
    ).rowcount
    if not inserted:
        return
    entry = conn.execute(
        "SELECT entry_key FROM quarantine_items WHERE id = ?", (item_id,)
    ).fetchone()
    allowed, _ = gate(0, default_max_retries(), state, next_at, now_dt)
    _record_proof(
        conn,
        item_id=item_id,
        entry_key=entry["entry_key"] if entry else "",
        retries=0,
        max_retries=default_max_retries(),
        cooldown_interval_seconds=interval,
        next_at=next_at,
        state=state,
        allowed=allowed,
        now=now_dt,
        source="隔离登记",
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
    _normalize_dead_letter(conn, now)
    return len(rows)


def _normalize_dead_letter(conn: sqlite3.Connection, now: str) -> int:
    """次数已达上限却未标死信的存量行，收敛为死信（停配平、可查）。"""
    rows = conn.execute(
        """
        SELECT s.item_id, s.retries, s.max_retries, s.next_at, s.cooldown_interval_seconds,
               q.entry_key
        FROM quarantine_retry_state s
        JOIN quarantine_items q ON q.id = s.item_id
        WHERE s.state IN (?, ?) AND s.retries >= s.max_retries
        """,
        (COOLING, READY),
    ).fetchall()
    changed = 0
    for row in rows:
        updated = conn.execute(
            """
            UPDATE quarantine_retry_state
            SET state = ?, next_at = NULL, updated_at = ?
            WHERE item_id = ? AND state IN (?, ?) AND retries >= max_retries
            """,
            (DEAD, now, row["item_id"], COOLING, READY),
        ).rowcount
        if not updated:
            continue
        changed += 1
        _record_proof(
            conn,
            item_id=row["item_id"],
            entry_key=row["entry_key"],
            retries=row["retries"],
            max_retries=row["max_retries"],
            cooldown_interval_seconds=row["cooldown_interval_seconds"],
            next_at=None,
            state=DEAD,
            allowed=False,
            now=parse_time(now) or utcnow(),
            source="死信收敛",
        )
    return changed


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
    """再配入口闸门：返回 (是否放行, 拒绝原因)。

    放行条件同时满足：未解除、未达次数上限、冷却已到期。
    未到点或已死信一律拒绝，不推进次数、不改余额。
    """
    if stored_state == RELEASED:
        return False, "该隔离项已解除（重试已配平离开隔离）"
    if stored_state == DEAD or retries >= max_retries:
        return False, (
            f"该隔离项已进入死信（{retries}/{max_retries} 次），"
            "按策略不再重试"
        )
    state = effective_state(stored_state, next_at, now)
    if state == COOLING:
        remain = cooldown_remaining_seconds(next_at, now)
        if remain > 0:
            return False, f"冷却未满，剩余 {remain} 秒（下次可重试 {next_at}）"
    return True, ""


def _state_row(conn: sqlite3.Connection, item_id: int) -> sqlite3.Row | None:
    return conn.execute(
        "SELECT * FROM quarantine_retry_state WHERE item_id = ?", (item_id,)
    ).fetchone()


def _record_proof(
    conn: sqlite3.Connection,
    *,
    item_id: int,
    entry_key: str,
    retries: int,
    max_retries: int,
    cooldown_interval_seconds: int,
    next_at: str | None,
    state: str,
    allowed: bool,
    now: datetime,
    source: str,
) -> None:
    """落一条冷却证明：与扩展表同一事务写，禁止只靠内存计数。"""
    remain = cooldown_remaining_seconds(next_at, now)
    conn.execute(
        """
        INSERT INTO quarantine_cooldown_proofs(
          item_id, entry_key, retries, max_retries,
          cooldown_interval_seconds, next_at, remaining_seconds,
          cooldown_active, allowed, dead_letter, state, source, evaluated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            item_id,
            entry_key,
            retries,
            max_retries,
            cooldown_interval_seconds,
            next_at,
            remain,
            1 if remain > 0 and state == COOLING else 0,
            1 if allowed else 0,
            1 if state == DEAD or retries >= max_retries else 0,
            state,
            source,
            fmt_time(now),
        ),
    )


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
    # 冷却证明取自最新一条已落库证明（与扩展表同一事务写入），字段不做实时重算；
    # 实时放行结论另在 can_retry / block_reason 上给，避免把新结论挂到旧证明 id。
    proof_id = item.pop("proof_id", None)
    item["cooldown_proof"] = {
        "proof_id": proof_id,
        "cooldown_interval_seconds": item.pop(
            "proof_interval", item.get("cooldown_interval_seconds", 0)
        ),
        "remaining_seconds": item.pop("proof_remaining", remain),
        "allowed": item.pop("proof_allowed", 1 if allowed else 0),
        "dead_letter": item.pop("proof_dead", 1 if item["dead_letter"] else 0),
        "source": item.pop("proof_source", ""),
        "evaluated_at": item.pop("proof_at", None),
    }
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
                   s.cooldown_interval_seconds,
                   s.last_attempt_at, s.last_outcome, s.last_reason,
                   s.released_at, s.balance_delta_cents,
                   p.id AS proof_id, p.cooldown_interval_seconds AS proof_interval,
                   p.remaining_seconds AS proof_remaining,
                   p.allowed AS proof_allowed, p.dead_letter AS proof_dead,
                   p.source AS proof_source, p.evaluated_at AS proof_at
            FROM quarantine_items q
            JOIN accounts a ON a.id = q.account_id
            JOIN quarantine_retry_state s ON s.item_id = q.id
            LEFT JOIN quarantine_cooldown_proofs p ON p.id = (
              SELECT MAX(p2.id) FROM quarantine_cooldown_proofs p2
              WHERE p2.item_id = q.id
            )
            ORDER BY q.id DESC
            """
        ).fetchall()
        items = [_decorate(r, now) for r in rows]

    if state_filter:
        items = [x for x in items if x["state"] == state_filter]
    elif not include_released:
        items = [x for x in items if x["state"] != RELEASED]
    return items


def list_proofs(item_id: int, *, limit: int = 50) -> list[dict[str, Any]]:
    """按隔离 id 只读查询冷却证明（隔离键、间隔、是否放行、是否死信）。"""
    with connect() as conn:
        item = conn.execute(
            "SELECT id FROM quarantine_items WHERE id = ?", (item_id,)
        ).fetchone()
        if item is None:
            raise HTTPException(404, "隔离项不存在")
        return rows_to_dicts(
            conn.execute(
                """
                SELECT * FROM quarantine_cooldown_proofs
                WHERE item_id = ?
                ORDER BY id DESC
                LIMIT ?
                """,
                (item_id, limit),
            ).fetchall()
        )


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
    interval = backoff_seconds(new_retries)

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
            SET retries = ?, next_at = NULL, cooldown_interval_seconds = 0,
                state = ?, last_attempt_at = ?,
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
        # 成功：冷却间隔清零，次状态转已解除；同一事务写明细与证明。
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
        _record_proof(
            conn,
            item_id=item["id"],
            entry_key=item["entry_key"],
            retries=new_retries,
            max_retries=max_retries,
            cooldown_interval_seconds=0,
            next_at=None,
            state=RELEASED,
            allowed=True,
            now=now,
            source=f"{trigger}尝试·{OUTCOME_SUCCESS}",
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

    # 失败：按 cool-down 公式写新 next_at；次数到上限则转死信（不再进配平）。余额不动。
    dead = new_retries >= max_retries
    new_state = DEAD if dead else COOLING
    next_at = None if dead else add_seconds(now, interval)
    advanced = conn.execute(
        """
        UPDATE quarantine_retry_state
        SET retries = ?, next_at = ?, cooldown_interval_seconds = ?, state = ?,
            last_attempt_at = ?, last_outcome = ?, last_reason = ?, updated_at = ?
        WHERE item_id = ? AND retries = ? AND state NOT IN (?, ?)
        """,
        (
            new_retries,
            next_at,
            interval,
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
    _record_proof(
        conn,
        item_id=item["id"],
        entry_key=item["entry_key"],
        retries=new_retries,
        max_retries=max_retries,
        cooldown_interval_seconds=interval,
        next_at=next_at,
        state=new_state,
        allowed=True,
        now=now,
        source=f"{trigger}尝试·{OUTCOME_FAILURE}",
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
        "dead_letter": dead,
        "next_at": next_at,
        "cooldown_interval_seconds": interval,
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
            # 拒绝也留证：不改余额、不推进次数；证明与扩展表同一事务提交。
            _record_proof(
                conn,
                item_id=item["id"],
                entry_key=item["entry_key"],
                retries=state["retries"],
                max_retries=state["max_retries"],
                cooldown_interval_seconds=state["cooldown_interval_seconds"],
                next_at=state["next_at"],
                state=effective_state(state["state"], state["next_at"], now),
                allowed=False,
                now=now,
                source=f"{trigger}入口·拒绝",
            )
            conn.execute("COMMIT")
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
    """自动扫描到期项：只碰冷却结束且未达上限的隔离项；死信不进配平。"""
    now = now or utcnow()
    now_s = fmt_time(now)
    with connect() as conn:
        backfill_state(conn, now_s)
        rows = conn.execute(
            """
            SELECT s.item_id
            FROM quarantine_retry_state s
            WHERE s.state IN (?, ?)
              AND s.retries < s.max_retries
              AND (s.next_at IS NULL OR s.next_at <= ?)
            ORDER BY COALESCE(s.next_at, '') ASC, s.item_id ASC
            """,
            (COOLING, READY, now_s),
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
