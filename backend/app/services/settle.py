from __future__ import annotations

import json
from datetime import datetime, timezone

from fastapi import HTTPException

from app.db import connect, rows_to_dicts
from app.modules import quarantine_retry


def _now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _quarantine(
    conn,
    *,
    entry_key: str,
    account_id: int,
    reason: str,
    amount_cents: int,
    settle_run_id: int,
    created_at: str,
) -> int:
    """隔离项与重试进度同一事务落库：重试次数/冷却从登记起就可查。"""
    cur = conn.execute(
        """
        INSERT INTO quarantine_items(
          entry_key, account_id, reason, amount_cents, settle_run_id, created_at
        ) VALUES (?, ?, ?, ?, ?, ?)
        """,
        (entry_key, account_id, reason, amount_cents, settle_run_id, created_at),
    )
    item_id = cur.lastrowid
    # 隔离行与重试进度（计数/冷却/证明）同一事务登记，从落库起就可查。
    quarantine_retry.register_item(conn, item_id, created_at)
    return item_id


def dashboard() -> dict:
    with connect() as conn:
        accounts = rows_to_dicts(conn.execute("SELECT * FROM accounts ORDER BY id").fetchall())
        fwd = conn.execute(
            "SELECT COUNT(*) AS c FROM ledger_entries WHERE kind = '正向'"
        ).fetchone()["c"]
        rev = conn.execute(
            "SELECT COUNT(*) AS c FROM ledger_entries WHERE kind = '冲正'"
        ).fetchone()["c"]
        pending = conn.execute(
            "SELECT COUNT(*) AS c FROM ledger_entries WHERE status = '待归并'"
        ).fetchone()["c"]
        quarantine_retry.backfill_state(conn)
        retry_stats = quarantine_retry.stats_for_conn(conn)
        bal = retry_stats["account_balance_cents"]
        last = conn.execute("SELECT * FROM settle_runs ORDER BY id DESC LIMIT 1").fetchone()
        return {
            "账户数": len(accounts),
            "正向笔数": fwd,
            "冲正笔数": rev,
            "待归并": pending,
            "未平隔离": retry_stats["open_count"],
            "冷却中": retry_stats["cooldown_count"],
            "可重试": retry_stats["ready_count"],
            "死信": retry_stats["dead_letter_count"],
            "重试成功": retry_stats["retry_success_count"],
            "隔离敞口分": retry_stats["quarantine_exposure_cents"],
            "死信敞口分": retry_stats["dead_letter_exposure_cents"],
            "余额合计分": bal,
            "余额与流水一致": retry_stats["consistent"],
            "重试统计": retry_stats,
            "最近归并": dict(last) if last else None,
            "账户": accounts,
        }


def list_accounts() -> list[dict]:
    with connect() as conn:
        return rows_to_dicts(conn.execute("SELECT * FROM accounts ORDER BY id").fetchall())


def list_entries(kind: str | None = None) -> list[dict]:
    with connect() as conn:
        if kind:
            rows = conn.execute(
                """
                SELECT e.*, a.code AS account_code, a.name AS account_name
                FROM ledger_entries e
                JOIN accounts a ON a.id = e.account_id
                WHERE e.kind = ?
                ORDER BY e.id
                """,
                (kind,),
            ).fetchall()
        else:
            rows = conn.execute(
                """
                SELECT e.*, a.code AS account_code, a.name AS account_name
                FROM ledger_entries e
                JOIN accounts a ON a.id = e.account_id
                ORDER BY e.id
                """
            ).fetchall()
        return rows_to_dicts(rows)


def list_quarantine() -> list[dict]:
    return quarantine_retry.list_items()


def list_settle_runs() -> list[dict]:
    with connect() as conn:
        return rows_to_dicts(
            conn.execute("SELECT * FROM settle_runs ORDER BY id DESC").fetchall()
        )


def list_verify_reports() -> list[dict]:
    with connect() as conn:
        return rows_to_dicts(
            conn.execute("SELECT * FROM verify_reports ORDER BY id DESC").fetchall()
        )


def run_settle() -> dict:
    """底座冒烟：按账户汇总正向−已匹配冲正；无原单/超额冲正进隔离；余额写账户。"""
    with connect() as conn:
        now = _now()
        cur = conn.execute(
            """
            INSERT INTO settle_runs(status, started_at)
            VALUES ('进行中', ?)
            """,
            (now,),
        )
        run_id = cur.lastrowid

        forwards = rows_to_dicts(
            conn.execute(
                "SELECT * FROM ledger_entries WHERE kind = '正向' AND status = '待归并' ORDER BY id"
            ).fetchall()
        )
        reversals = rows_to_dicts(
            conn.execute(
                "SELECT * FROM ledger_entries WHERE kind = '冲正' AND status = '待归并' ORDER BY id"
            ).fetchall()
        )
        fwd_by_key = {f["entry_key"]: f for f in forwards}

        # 每个正向已占用冲正额度
        used: dict[str, int] = {f["entry_key"]: 0 for f in forwards}
        settled = 0
        quarantined = 0
        account_net: dict[int, int] = {}

        for f in forwards:
            account_net[f["account_id"]] = account_net.get(f["account_id"], 0) + f["amount_cents"]

        for r in reversals:
            target = r.get("target_entry_key")
            if not target or target not in fwd_by_key:
                _quarantine(
                    conn,
                    entry_key=r["entry_key"],
                    account_id=r["account_id"],
                    reason="冲正无原单",
                    amount_cents=r["amount_cents"],
                    settle_run_id=run_id,
                    created_at=now,
                )
                conn.execute(
                    "UPDATE ledger_entries SET status = '已隔离', settle_run_id = ? WHERE id = ?",
                    (run_id, r["id"]),
                )
                quarantined += 1
                continue

            fwd = fwd_by_key[target]
            if fwd["account_id"] != r["account_id"]:
                _quarantine(
                    conn,
                    entry_key=r["entry_key"],
                    account_id=r["account_id"],
                    reason="冲正账户与原单不一致",
                    amount_cents=r["amount_cents"],
                    settle_run_id=run_id,
                    created_at=now,
                )
                conn.execute(
                    "UPDATE ledger_entries SET status = '已隔离', settle_run_id = ? WHERE id = ?",
                    (run_id, r["id"]),
                )
                quarantined += 1
                continue

            remain = fwd["amount_cents"] - used[target]
            if r["amount_cents"] > remain:
                _quarantine(
                    conn,
                    entry_key=r["entry_key"],
                    account_id=r["account_id"],
                    reason="冲正超额",
                    amount_cents=r["amount_cents"],
                    settle_run_id=run_id,
                    created_at=now,
                )
                conn.execute(
                    "UPDATE ledger_entries SET status = '已隔离', settle_run_id = ? WHERE id = ?",
                    (run_id, r["id"]),
                )
                quarantined += 1
                continue

            used[target] += r["amount_cents"]
            account_net[r["account_id"]] = account_net.get(r["account_id"], 0) - r["amount_cents"]
            conn.execute(
                "UPDATE ledger_entries SET status = '已归并', settle_run_id = ? WHERE id = ?",
                (run_id, r["id"]),
            )
            settled += 1

        for f in forwards:
            conn.execute(
                "UPDATE ledger_entries SET status = '已归并', settle_run_id = ? WHERE id = ?",
                (run_id, f["id"]),
            )
            settled += 1

        # 余额按「全部已归并流水」重算，而不是只按本轮待归并：
        # 隔离重试成功会补记已归并流水，重跑归并不能把它抹掉。
        account_ids = [
            r["id"] for r in conn.execute("SELECT id FROM accounts ORDER BY id").fetchall()
        ]
        for acc_id in account_ids:
            merged_fwd = conn.execute(
                """
                SELECT COALESCE(SUM(amount_cents), 0) AS s FROM ledger_entries
                WHERE account_id = ? AND kind = '正向' AND status = '已归并'
                """,
                (acc_id,),
            ).fetchone()["s"]
            merged_rev = conn.execute(
                """
                SELECT COALESCE(SUM(amount_cents), 0) AS s FROM ledger_entries
                WHERE account_id = ? AND kind = '冲正' AND status = '已归并'
                """,
                (acc_id,),
            ).fetchone()["s"]
            balance = merged_fwd - merged_rev
            if acc_id in account_net:
                conn.execute(
                    "UPDATE accounts SET balance_cents = ?, last_settle_run_id = ? WHERE id = ?",
                    (balance, run_id, acc_id),
                )
            else:
                conn.execute(
                    "UPDATE accounts SET balance_cents = ? WHERE id = ?",
                    (balance, acc_id),
                )

        net_total = conn.execute(
            "SELECT COALESCE(SUM(balance_cents), 0) AS s FROM accounts"
        ).fetchone()["s"]
        finished = _now()
        conn.execute(
            """
            UPDATE settle_runs
            SET status = '完成',
                forward_count = ?,
                reversal_count = ?,
                settled_count = ?,
                quarantine_count = ?,
                net_cents = ?,
                finished_at = ?
            WHERE id = ?
            """,
            (
                len(forwards),
                len(reversals),
                settled,
                quarantined,
                net_total,
                finished,
                run_id,
            ),
        )
        conn.commit()
        return get_settle_run(run_id)


def get_settle_run(run_id: int) -> dict:
    with connect() as conn:
        row = conn.execute("SELECT * FROM settle_runs WHERE id = ?", (run_id,)).fetchone()
        if not row:
            raise HTTPException(404, "归并运行不存在")
        return dict(row)


def run_verify(settle_run_id: int | None = None) -> dict:
    with connect() as conn:
        if settle_run_id is None:
            last = conn.execute("SELECT id FROM settle_runs ORDER BY id DESC LIMIT 1").fetchone()
            if not last:
                raise HTTPException(400, "尚无归并运行，无法互证")
            settle_run_id = last["id"]
        run = conn.execute("SELECT * FROM settle_runs WHERE id = ?", (settle_run_id,)).fetchone()
        if not run:
            raise HTTPException(404, "归并运行不存在")

        # 已归并正向 − 已归并冲正
        fwd_sum = conn.execute(
            """
            SELECT COALESCE(SUM(amount_cents), 0) AS s FROM ledger_entries
            WHERE kind = '正向' AND status = '已归并' AND settle_run_id = ?
            """,
            (settle_run_id,),
        ).fetchone()["s"]
        rev_sum = conn.execute(
            """
            SELECT COALESCE(SUM(amount_cents), 0) AS s FROM ledger_entries
            WHERE kind = '冲正' AND status = '已归并' AND settle_run_id = ?
            """,
            (settle_run_id,),
        ).fetchone()["s"]
        ledger_net = fwd_sum - rev_sum
        balance_sum = conn.execute(
            "SELECT COALESCE(SUM(balance_cents), 0) AS s FROM accounts WHERE last_settle_run_id = ?",
            (settle_run_id,),
        ).fetchone()["s"]
        # 账户余额合计可能含历史；底座约定：互证看「本轮触及账户余额合计」vs 本轮流水净额
        # 简化：用全部账户余额 vs 全部已归并净额（多轮后仍应一致若每轮覆盖写）
        all_bal = conn.execute("SELECT COALESCE(SUM(balance_cents), 0) AS s FROM accounts").fetchone()["s"]
        q_count = conn.execute(
            "SELECT COUNT(*) AS c FROM quarantine_items WHERE settle_run_id = ?",
            (settle_run_id,),
        ).fetchone()["c"]

        # 全量已归并净额
        all_fwd = conn.execute(
            "SELECT COALESCE(SUM(amount_cents), 0) AS s FROM ledger_entries WHERE kind = '正向' AND status = '已归并'"
        ).fetchone()["s"]
        all_rev = conn.execute(
            "SELECT COALESCE(SUM(amount_cents), 0) AS s FROM ledger_entries WHERE kind = '冲正' AND status = '已归并'"
        ).fetchone()["s"]
        all_ledger_net = all_fwd - all_rev

        consistent = 1 if all_ledger_net == all_bal else 0
        retry_stats = quarantine_retry.stats_for_conn(conn)
        detail = {
            "本轮正向分": fwd_sum,
            "本轮冲正分": rev_sum,
            "本轮流水净额分": ledger_net,
            "本轮触及余额合计分": balance_sum,
            "全量流水净额分": all_ledger_net,
            "全量账户余额分": all_bal,
            "本轮隔离数": q_count,
            # 重试策略与隔离态、余额同源（同一连接同一快照读出）
            "隔离冷却中": retry_stats["cooldown_count"],
            "隔离可重试": retry_stats["ready_count"],
            "隔离死信": retry_stats["dead_letter_count"],
            "重试成功数": retry_stats["retry_success_count"],
            "重试后余额与流水一致": retry_stats["consistent"],
        }
        now = _now()
        cur = conn.execute(
            """
            INSERT INTO verify_reports(
              settle_run_id, ledger_net_cents, balance_sum_cents,
              quarantine_count, consistent, detail_json, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                settle_run_id,
                all_ledger_net,
                all_bal,
                q_count,
                consistent,
                json.dumps(detail, ensure_ascii=False),
                now,
            ),
        )
        conn.commit()
        rid = cur.lastrowid
        row = conn.execute("SELECT * FROM verify_reports WHERE id = ?", (rid,)).fetchone()
        out = dict(row)
        out["detail"] = detail
        return out
