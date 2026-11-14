"""Sales-amount correction batches (销售补正).

A batch carries up to ``MAX_CORRECTION_ROWS`` rows of
``sale_id, revision, amount`` and obeys these rules:

* Only existing sales rows are modified; rows never corrected stay at
  revision 0.
* A correction is applied only when its revision is strictly higher than
  the stored one.  Re-submitting the same revision with the same amount is
  an idempotent success (safe retry).  An older revision, the same revision
  with a different amount, or a missing sales row rejects the WHOLE batch:
  every write of the batch is rolled back, nothing is left half-applied.
* All rows are evaluated inside one transaction that takes row locks
  (``SELECT ... FOR UPDATE``), so a concurrent writer can never push a
  stored revision backwards and can never observe a half-written batch.
* Correction is mutually exclusive with a replay round and with another
  correction batch through the site-wide gate on ``RunManager``; a
  conflicting upload is rejected immediately with a clear message.
* Everything runs on the shared two-connection pool.  Pooled sessions are
  read-only for the replay workload, so the correction transaction lifts
  the read-only default for itself only, via ``SET TRANSACTION READ
  WRITE``; replay queries keep running as read-only templates.
"""

from __future__ import annotations

from decimal import Decimal
from typing import Optional

from csvutil import MAX_CORRECTION_ROWS

# Conflict reason codes surfaced to the UI.
REASON_NOT_FOUND = "not_found"
REASON_STALE = "stale_revision"
REASON_SAME_REV_DIFF_AMOUNT = "same_revision_different_amount"

# How long a correction batch may wait for a pooled connection.  The mutual
# exclusion gate guarantees no replay round holds connections while a batch
# runs, so this is only a defensive backstop.
GETCONN_TIMEOUT_SECONDS = 10.0


class CorrectionConflict(RuntimeError):
    """A replay round or another correction batch is currently in progress."""


class _BatchRejected(Exception):
    """Internal control flow: conflicts were found, roll the batch back."""

    def __init__(self, result: dict):
        super().__init__("correction batch rejected")
        self.result = result


class CorrectionManager:
    """Applies validated correction batches against the shared pool."""

    def __init__(self, pool, run_manager):
        self._pool = pool
        self._run_manager = run_manager

    # ------------------------------------------------------------------ API

    def apply(self, rows: list[dict]) -> dict:
        """Apply one validated batch; return a JSON-serializable result.

        Raises :class:`CorrectionConflict` when a replay round or another
        batch is in progress.  A rejected batch returns a result dict with
        ``status == "rejected"`` and every conflicting row listed.
        """
        if not self._run_manager.try_begin_correction():
            raise CorrectionConflict(
                "回放进行中或另一批补正正在处理，本次补正被拒绝；"
                "请等待其结束后重试。"
            )
        try:
            return self._apply_exclusive(rows)
        finally:
            self._run_manager.end_correction()

    # ------------------------------------------------------------- internals

    def _apply_exclusive(self, rows: list[dict]) -> dict:
        conn = self._pool.getconn(timeout=GETCONN_TIMEOUT_SECONDS)
        try:
            return self._apply_on_connection(conn, rows)
        finally:
            self._return_conn_safely(conn)

    def _apply_on_connection(self, conn, rows: list[dict]) -> dict:
        applied: list[dict] = []
        conflicts: list[dict] = []
        try:
            # Defensive: make sure the pooled connection starts idle so the
            # transaction block below is the outermost one (SET TRANSACTION
            # is only valid at the top level of a transaction).
            conn.rollback()
            with conn.transaction():
                with conn.cursor() as cur:
                    # Pooled sessions default to read-only (replay safety);
                    # lift it for THIS transaction only.  The session default
                    # is untouched, so replay queries stay read-only after.
                    cur.execute("SET TRANSACTION READ WRITE")
                    for row in rows:
                        cur.execute(
                            "SELECT revision, amount FROM sales "
                            "WHERE sale_id = %s FOR UPDATE",
                            (row["sale_id"],),
                        )
                        rec = cur.fetchone()
                        if rec is None:
                            conflicts.append(self._conflict(
                                row, REASON_NOT_FOUND,
                                "销售行不存在，只修改已有销售行",
                                None, None,
                            ))
                            continue
                        cur_rev = int(rec[0])
                        cur_amount = Decimal(str(rec[1]))
                        new_rev = row["revision"]
                        new_amount = row["amount"]
                        if new_rev < cur_rev:
                            conflicts.append(self._conflict(
                                row, REASON_STALE,
                                f"已有更高修订 {cur_rev}，禁止旧版回退",
                                cur_rev, cur_amount,
                            ))
                        elif new_rev == cur_rev:
                            if new_amount == cur_amount:
                                # Same revision + same amount: idempotent.
                                applied.append(self._outcome(
                                    row, "idempotent", cur_rev, cur_amount))
                            else:
                                conflicts.append(self._conflict(
                                    row, REASON_SAME_REV_DIFF_AMOUNT,
                                    f"修订 {cur_rev} 已存在且金额不同"
                                    f"（当前 {cur_amount}），同版异值被拒绝",
                                    cur_rev, cur_amount,
                                ))
                        else:
                            cur.execute(
                                "UPDATE sales SET amount = %s, revision = %s "
                                "WHERE sale_id = %s",
                                (new_amount, new_rev, row["sale_id"]),
                            )
                            applied.append(self._outcome(
                                row, "updated", cur_rev, cur_amount))
                if conflicts:
                    # Raising inside the transaction block rolls back every
                    # UPDATE above: a batch is all-or-nothing.
                    raise _BatchRejected(self._rejected_result(rows,
                                                               conflicts))
        except _BatchRejected as rejected:
            return rejected.result
        return self._applied_result(rows, applied)

    # ------------------------------------------------------------- results

    @staticmethod
    def _outcome(row: dict, result: str, prev_rev: int,
                 prev_amount: Decimal) -> dict:
        return {
            "line": row["line_no"],
            "sale_id": row["sale_id"],
            "revision": row["revision"],
            "amount": str(row["amount"]),
            "result": result,
            "previous_revision": prev_rev,
            "previous_amount": str(prev_amount),
        }

    @staticmethod
    def _conflict(row: dict, reason: str, message: str,
                  cur_rev: Optional[int],
                  cur_amount: Optional[Decimal]) -> dict:
        return {
            "line": row["line_no"],
            "sale_id": row["sale_id"],
            "revision": row["revision"],
            "amount": str(row["amount"]),
            "reason": reason,
            "message": message,
            "current_revision": cur_rev,
            "current_amount": str(cur_amount) if cur_amount is not None
                                else None,
        }

    @staticmethod
    def _applied_result(rows: list[dict], applied: list[dict]) -> dict:
        updated = sum(1 for r in applied if r["result"] == "updated")
        idempotent = sum(1 for r in applied if r["result"] == "idempotent")
        return {
            "status": "applied",
            "message": f"补正完成：更新 {updated} 行，幂等跳过 {idempotent} 行。",
            "summary": {
                "total": len(rows),
                "updated": updated,
                "idempotent": idempotent,
                "conflicts": 0,
            },
            "rows": applied,
        }

    @staticmethod
    def _rejected_result(rows: list[dict], conflicts: list[dict]) -> dict:
        return {
            "status": "rejected",
            "message": f"批次存在 {len(conflicts)} 行冲突，已整批拒绝，"
                       f"未修改任何数据。",
            "summary": {
                "total": len(rows),
                "updated": 0,
                "idempotent": 0,
                "conflicts": len(conflicts),
            },
            "conflicts": conflicts,
        }

    # ------------------------------------------------------------- cleanup

    def _return_conn_safely(self, conn) -> None:
        """Mirror the engine's cleanup: no open transaction may leak back."""
        try:
            try:
                conn.rollback()
            except Exception:
                self._pool.putconn(conn, discard=True)
                return
            self._pool.putconn(conn)
        except Exception:
            try:
                self._pool.putconn(conn, discard=True)
            except Exception:
                pass


__all__ = [
    "MAX_CORRECTION_ROWS",
    "CorrectionConflict",
    "CorrectionManager",
    "REASON_NOT_FOUND",
    "REASON_STALE",
    "REASON_SAME_REV_DIFF_AMOUNT",
]
