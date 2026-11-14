"""Correction-batch semantics tests with a fake pool / fake connections.

Run with:  python3 -I tests/test_correction_fake.py
No third-party packages are needed: minimal psycopg / psycopg_pool stubs
are injected before importing the real engine module, and an in-memory
sales table models transactions (snapshot rollback) and row lookups.
"""

from __future__ import annotations

import sys
import threading
import time
import types
from decimal import Decimal
from pathlib import Path

# ---- inject minimal psycopg / psycopg_pool stubs (for engine import) -----

psycopg = types.ModuleType("psycopg")
errors = types.ModuleType("psycopg.errors")


class _PgError(Exception):
    sqlstate = None


class QueryCanceled(_PgError):
    sqlstate = "57014"


errors.QueryCanceled = QueryCanceled
psycopg.errors = errors
psycopg.Error = _PgError
psycopg.connect = lambda *a, **k: None

pool_mod = types.ModuleType("psycopg_pool")


class PoolTimeout(Exception):
    pass


pool_mod.PoolTimeout = PoolTimeout
sys.modules["psycopg"] = psycopg
sys.modules["psycopg.errors"] = errors
sys.modules["psycopg_pool"] = pool_mod

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "app"))

import engine  # noqa: E402
from correction import (  # noqa: E402
    CorrectionConflict,
    CorrectionManager,
    REASON_NOT_FOUND,
    REASON_SAME_REV_DIFF_AMOUNT,
    REASON_STALE,
)
from csvutil import CSVValidationError, parse_correction_csv  # noqa: E402


# ---- in-memory sales table with transaction semantics --------------------


class FakeSalesTable:
    """Models the sales table plus BEGIN/COMMIT/ROLLBACK snapshot behavior."""

    def __init__(self, rows: dict[int, tuple[int, str]]):
        self.data = {sid: (rev, Decimal(amount))
                     for sid, (rev, amount) in rows.items()}
        self.commits = 0
        self.rollbacks = 0
        self.committed_updates = []  # (sale_id, revision, amount)
        self._snapshot = None
        self._txn_updates = None
        # Test hook: block inside the transaction until this event is set.
        self.block_in_txn: threading.Event | None = None

    def begin(self):
        self._snapshot = dict(self.data)
        self._txn_updates = []

    def commit(self):
        self.commits += 1
        self.committed_updates.extend(self._txn_updates or [])
        self._snapshot = None
        self._txn_updates = None

    def rollback(self):
        self.rollbacks += 1
        if self._snapshot is not None:
            self.data = self._snapshot
        self._snapshot = None
        self._txn_updates = None


class FakeCursor:
    def __init__(self, conn):
        self.conn = conn
        self._result = None

    def execute(self, sql, params=None):
        table = self.conn.table
        if sql.startswith("SET TRANSACTION"):
            return
        if sql.startswith("SELECT revision, amount FROM sales"):
            if table.block_in_txn is not None:
                table.block_in_txn.wait(timeout=10)
                table.block_in_txn = None
            self._result = table.data.get(params[0])
            return
        if sql.startswith("UPDATE sales"):
            amount, revision, sale_id = params
            table.data[sale_id] = (revision, Decimal(str(amount)))
            table._txn_updates.append((sale_id, revision, Decimal(str(amount))))
            return
        raise AssertionError(f"unexpected SQL: {sql}")

    def fetchone(self):
        return self._result

    def close(self):
        pass

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False


class FakeTransaction:
    def __init__(self, conn):
        self.conn = conn

    def __enter__(self):
        self.conn.table.begin()
        return self.conn

    def __exit__(self, exc_type, exc, tb):
        if exc_type is None:
            self.conn.table.commit()
        else:
            self.conn.table.rollback()
        return False


class FakeConn:
    def __init__(self, table):
        self.table = table

    def cursor(self):
        return FakeCursor(self)

    def transaction(self):
        return FakeTransaction(self)

    def rollback(self):
        pass  # no open transaction outside the with-block in the fake


class FakePool:
    def __init__(self, table):
        self.conn = FakeConn(table)
        self.gets = 0
        self.puts = 0

    def getconn(self, timeout=None):
        self.gets += 1
        return self.conn

    def putconn(self, conn, discard=False):
        self.puts += 1


def make_rows(spec):
    """spec: list of (line_no, sale_id, revision, amount-str)."""
    return [
        {"line_no": ln, "sale_id": sid, "revision": rev,
         "amount": Decimal(amount)}
        for ln, sid, rev, amount in spec
    ]


def fresh_manager(rows=None):
    table = FakeSalesTable(rows or {})
    pool = FakePool(table)
    run_mgr = engine.RunManager(pool, {})
    mgr = CorrectionManager(pool, run_mgr)
    return table, pool, run_mgr, mgr


# ---- CSV validation tests -------------------------------------------------


def test_csv_valid():
    rows = parse_correction_csv(
        b"sale_id,revision,amount\n200,1,199.90\n201,2,88\n")
    assert rows[0] == {"line_no": 2, "sale_id": 200, "revision": 1,
                       "amount": Decimal("199.90")}
    assert rows[1]["amount"] == Decimal("88")
    print("OK test_csv_valid")


def test_csv_header_aliases_and_order():
    rows = parse_correction_csv(
        b"amount,sale,rev\n10.5,7,3\n")
    assert rows[0]["sale_id"] == 7 and rows[0]["revision"] == 3
    assert rows[0]["amount"] == Decimal("10.5")
    print("OK test_csv_header_aliases_and_order")


def test_csv_rejects_bad_rows():
    cases = [
        (b"sale_id,revision\n1,1\n", "missing column"),
        (b"sale_id,revision,amount\n1,1\n", "amount missing in row"),
        (b"sale_id,revision,amount\n1,,10.00\n", "revision empty"),
        (b"sale_id,revision,amount\n1,1,\n", "amount empty"),
        (b"sale_id,revision,amount\n0,1,10.00\n", "sale_id zero"),
        (b"sale_id,revision,amount\n-1,1,10.00\n", "sale_id negative"),
        (b"sale_id,revision,amount\n1,0,10.00\n", "revision zero"),
        (b"sale_id,revision,amount\n1,-2,10.00\n", "revision negative"),
        (b"sale_id,revision,amount\n1,1.5,10.00\n", "revision non-integer"),
        (b"sale_id,revision,amount\n1,1,-0.01\n", "amount negative"),
        (b"sale_id,revision,amount\n1,1,10.999\n", "amount 3 decimals"),
        (b"sale_id,revision,amount\n1,1,abc\n", "amount not a number"),
        (b"sale_id,revision,amount\n1,1,1e3\n", "amount scientific"),
        (b"sale_id,revision,amount\n1,1,10000000000.00\n", "amount overflow"),
        (b"sale_id,revision,amount\n1,1,10.00\n1,2,20.00\n", "duplicate id"),
        (b"", "empty file"),
        (b"sale_id,revision,amount\n", "no data rows"),
    ]
    for raw, label in cases:
        try:
            parse_correction_csv(raw)
        except CSVValidationError:
            continue
        raise AssertionError(f"case not rejected: {label}")
    print("OK test_csv_rejects_bad_rows")


def test_csv_row_limit_20():
    good = b"sale_id,revision,amount\n" + b"".join(
        f"{i},1,1.00\n".encode() for i in range(1, 21))
    assert len(parse_correction_csv(good)) == 20
    too_many = b"sale_id,revision,amount\n" + b"".join(
        f"{i},1,1.00\n".encode() for i in range(1, 22))
    try:
        parse_correction_csv(too_many)
        raise AssertionError("21 rows must be rejected")
    except CSVValidationError:
        pass
    print("OK test_csv_row_limit_20")


# ---- batch application semantics ------------------------------------------


def test_newer_revision_applies_and_commits():
    table, pool, _, mgr = fresh_manager({200: (0, "50.00")})
    result = mgr.apply(make_rows([(2, 200, 1, "199.90")]))
    assert result["status"] == "applied", result
    assert result["summary"]["updated"] == 1
    assert table.data[200] == (1, Decimal("199.90"))
    assert table.commits == 1 and table.rollbacks == 0
    assert pool.gets == 1 and pool.puts == 1, "connection returned to pool"
    print("OK test_newer_revision_applies_and_commits")


def test_idempotent_same_revision_same_amount():
    table, _, _, mgr = fresh_manager({200: (1, "199.90")})
    result = mgr.apply(make_rows([(2, 200, 1, "199.90")]))
    assert result["status"] == "applied", result
    assert result["summary"]["idempotent"] == 1
    assert result["summary"]["updated"] == 0
    assert table.data[200] == (1, Decimal("199.90")), "row untouched"
    assert table.commits == 1
    print("OK test_idempotent_same_revision_same_amount")


def test_stale_revision_rejects_and_rolls_back():
    table, _, _, mgr = fresh_manager({200: (2, "150.00"), 201: (0, "10.00")})
    rows = make_rows([(2, 201, 1, "99.00"),   # would apply...
                      (3, 200, 1, "100.00")])  # ...but this one is stale
    result = mgr.apply(rows)
    assert result["status"] == "rejected", result
    assert result["conflicts"][0]["reason"] == REASON_STALE
    assert result["conflicts"][0]["current_revision"] == 2
    # Whole batch rolled back: the valid first row must NOT be applied.
    assert table.data[201] == (0, Decimal("10.00")), table.data
    assert table.data[200] == (2, Decimal("150.00"))
    assert table.rollbacks == 1 and table.commits == 0
    assert table.committed_updates == []
    print("OK test_stale_revision_rejects_and_rolls_back")


def test_same_revision_different_amount_rejects():
    table, _, _, mgr = fresh_manager({200: (1, "199.90")})
    result = mgr.apply(make_rows([(2, 200, 1, "200.00")]))
    assert result["status"] == "rejected"
    assert result["conflicts"][0]["reason"] == REASON_SAME_REV_DIFF_AMOUNT
    assert table.data[200] == (1, Decimal("199.90"))
    assert table.commits == 0
    print("OK test_same_revision_different_amount_rejects")


def test_missing_sale_row_rejects():
    table, _, _, mgr = fresh_manager({200: (0, "50.00")})
    result = mgr.apply(make_rows([(2, 999999, 1, "10.00")]))
    assert result["status"] == "rejected"
    assert result["conflicts"][0]["reason"] == REASON_NOT_FOUND
    assert result["conflicts"][0]["current_revision"] is None
    assert table.commits == 0
    print("OK test_missing_sale_row_rejects")


def test_all_conflicts_reported_in_one_pass():
    _, _, _, mgr = fresh_manager({200: (2, "150.00")})
    rows = make_rows([(2, 200, 1, "1.00"),      # stale
                      (3, 123456, 1, "2.00")])  # missing
    result = mgr.apply(rows)
    assert result["status"] == "rejected"
    reasons = {c["reason"] for c in result["conflicts"]}
    assert reasons == {REASON_STALE, REASON_NOT_FOUND}, reasons
    assert len(result["conflicts"]) == 2
    print("OK test_all_conflicts_reported_in_one_pass")


def test_higher_revision_chain_across_batches():
    table, _, _, mgr = fresh_manager({200: (0, "50.00")})
    r1 = mgr.apply(make_rows([(2, 200, 1, "60.00")]))
    r2 = mgr.apply(make_rows([(2, 200, 3, "70.00")]))
    assert r1["status"] == r2["status"] == "applied"
    assert table.data[200] == (3, Decimal("70.00"))
    # Revision 2 is now older than the stored 3: rejected, no regression.
    r3 = mgr.apply(make_rows([(2, 200, 2, "65.00")]))
    assert r3["status"] == "rejected"
    assert table.data[200] == (3, Decimal("70.00"))
    print("OK test_higher_revision_chain_across_batches")


# ---- mutual exclusion ------------------------------------------------------


def test_correction_rejected_while_replay_running():
    table, pool, run_mgr, mgr = fresh_manager({200: (0, "50.00")})
    # Occupy the replay side with a real (fake) running round.
    gate_pool = _EngineFakePool()
    run_mgr2 = engine.RunManager(gate_pool, {"slow": "SELECT pg_sleep"})
    mgr2 = CorrectionManager(pool, run_mgr2)
    run_mgr2.start([{"arrival_ms": 0, "template": "slow", "store": "1"}])
    try:
        try:
            mgr2.apply(make_rows([(2, 200, 1, "1.00")]))
            raise AssertionError("correction must be rejected during replay")
        except CorrectionConflict:
            pass
        assert table.commits == 0 and table.rollbacks == 0
    finally:
        gate_pool.release_all()
        run_mgr2.cancel()
        _wait_terminal(run_mgr2)
    print("OK test_correction_rejected_while_replay_running")


def test_replay_rejected_while_correction_active():
    table, pool, run_mgr, mgr = fresh_manager({200: (0, "50.00")})
    # Hold the correction slot directly (a batch is mid-flight).
    assert run_mgr.try_begin_correction()
    try:
        try:
            run_mgr.start([{"arrival_ms": 0, "template": "fast",
                            "store": "1"}])
            raise AssertionError("replay must be rejected during correction")
        except engine.RunConflict:
            pass
    finally:
        run_mgr.end_correction()
    print("OK test_replay_rejected_while_correction_active")


def test_concurrent_corrections_are_serialized_by_gate():
    table, pool, _, mgr = fresh_manager({200: (0, "50.00")})
    table.block_in_txn = threading.Event()  # first batch blocks inside txn
    outcomes = {}

    def first():
        outcomes["first"] = mgr.apply(make_rows([(2, 200, 1, "60.00")]))

    t = threading.Thread(target=first)
    t.start()
    time.sleep(0.2)  # let the first batch enter its transaction
    try:
        mgr.apply(make_rows([(2, 200, 2, "70.00")]))
        raise AssertionError("second concurrent batch must be rejected")
    except CorrectionConflict:
        pass
    finally:
        table.block_in_txn.set()
    t.join(timeout=10)
    assert outcomes["first"]["status"] == "applied"
    # The rejected second batch never regressed the stored revision.
    assert table.data[200] == (1, Decimal("60.00"))
    print("OK test_concurrent_corrections_are_serialized_by_gate")


# ---- minimal engine fakes for the replay-running test ----------------------


class _EngineFakeConn:
    def __init__(self, pool):
        self.pool = pool
        self.info = types.SimpleNamespace(backend_pid=id(self) % 100000)
        self.replay_dsn = "postgresql://stub/stub"

    def cursor(self):
        return self

    def execute(self, sql, params=None):
        # Block until the round is canceled (models a slow query).
        self.pool.release_event.wait(timeout=10)
        raise QueryCanceled("canceling statement due to user request")

    def fetchall(self):
        return []

    def close(self):
        pass

    def rollback(self):
        pass

    def cancel(self):
        pass


class _EngineFakePool:
    def __init__(self):
        self.release_event = threading.Event()
        self._conns = [_EngineFakeConn(self), _EngineFakeConn(self)]
        self._idx = 0
        self._lock = threading.Lock()

    def getconn(self, timeout=None):
        with self._lock:
            conn = self._conns[self._idx % len(self._conns)]
            self._idx += 1
        return conn

    def putconn(self, conn, discard=False):
        pass

    def release_all(self):
        self.release_event.set()


def _wait_terminal(mgr, timeout=10.0):
    deadline = time.time() + timeout
    while time.time() < deadline:
        if mgr.snapshot()["status"] in ("completed", "canceled"):
            return
        time.sleep(0.02)
    raise AssertionError("run did not finish in time")


if __name__ == "__main__":
    test_csv_valid()
    test_csv_header_aliases_and_order()
    test_csv_rejects_bad_rows()
    test_csv_row_limit_20()
    test_newer_revision_applies_and_commits()
    test_idempotent_same_revision_same_amount()
    test_stale_revision_rejects_and_rolls_back()
    test_same_revision_different_amount_rejects()
    test_missing_sale_row_rejects()
    test_all_conflicts_reported_in_one_pass()
    test_higher_revision_chain_across_batches()
    test_correction_rejected_while_replay_running()
    test_replay_rejected_while_correction_active()
    test_concurrent_corrections_are_serialized_by_gate()
    print("\nALL CORRECTION TESTS PASSED")
