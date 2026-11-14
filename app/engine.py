"""Load-replay engine.

Single-process scheduler that replays read-only queries according to the
relative arrival times found in an uploaded CSV.

Guarantees implemented here:

* Exactly one run is active at any time ("one round, site-wide").
* At most ``MAX_CONNS`` database connections exist, all re-used from one pool.
* Every request has a 3s budget measured from its *planned* arrival, queue
  wait included.  A request whose deadline expires while queued is never
  executed (it is marked ``timeout`` and never touches a connection).
* A request already running past its deadline is stopped by cancelling the
  in-flight PostgreSQL query (``pg_cancel_backend`` under the hood), and the
  connection is only returned to the pool after the SQL has actually stopped
  and the transaction has been rolled back / cleaned up.
* Cancelling the whole round prevents any not-yet-started request from
  running; in-flight requests are stopped.  Each cancellation carries an
  ownership token so a late cancel of a previous request can never stop the
  request that later re-uses the same connection.
* "completed" / "canceled" is only reported after every worker and every
  SQL statement has finished and every connection has been returned.
"""

from __future__ import annotations

import threading
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from typing import Optional

import psycopg
import psycopg_pool

# --- Tunable policy constants ---------------------------------------------

MAX_CONNS = 2
# Hard cap on requests in one round (mirrors csvutil.MAX_ROWS).
MAX_REQUESTS = 50
REQUEST_DEADLINE_SECONDS = 3.0
# Short blocking chunks while waiting for a connection so we stay responsive
# to the round-cancel event and to the per-request deadline.
ACQUIRE_CHUNK_SECONDS = 0.05
# Hard server-side backstop.  The 3s budget is enforced in application code;
# this only protects against a query whose cancellation somehow failed.
STATEMENT_TIMEOUT_MS = 15_000


# --- Per-request state -----------------------------------------------------


@dataclass
class RequestState:
    id: int
    arrival_ms: int
    template: str
    store: str
    status: str = "queued"  # queued|running|success|timeout|error|canceled
    queue_ms: Optional[float] = None
    exec_ms: Optional[float] = None
    total_ms: Optional[float] = None
    rows: Optional[int] = None
    error: Optional[str] = None
    planned_at: float = 0.0
    started_at: Optional[float] = None
    finished_at: Optional[float] = None
    sqlstate: Optional[str] = None

    def as_dict(self) -> dict:
        def rnd(v):
            return round(v, 1) if v is not None else None

        return {
            "id": self.id,
            "arrival_ms": self.arrival_ms,
            "template": self.template,
            "store": self.store,
            "status": self.status,
            "queue_ms": rnd(self.queue_ms),
            "exec_ms": rnd(self.exec_ms),
            "total_ms": rnd(self.total_ms),
            "rows": self.rows,
            "error": self.error,
            "sqlstate": self.sqlstate,
        }


class RunConflict(RuntimeError):
    """Raised when a new round is requested while one is already running."""


@dataclass
class InFlight:
    """Ownership record for a request currently holding a connection."""

    req: RequestState
    conn: object
    timeout_token: threading.Event = field(default_factory=threading.Event)
    cancel_token: threading.Event = field(default_factory=threading.Event)
    done_event: threading.Event = field(default_factory=threading.Event)
    cancel_issued: bool = False
    cancel_lock: threading.Lock = field(default_factory=threading.Lock)


# --- The engine ------------------------------------------------------------


class RunManager:
    def __init__(self, pool, templates: dict[str, str]):
        self._pool = pool
        self._templates = templates  # name -> SQL text
        self._lock = threading.RLock()
        self._run: Optional[Run] = None

    # ---- public API used by the HTTP layer ----

    def start(self, rows: list[dict]) -> dict:
        with self._lock:
            if self._run is not None and self._run.status == "running":
                raise RunConflict("A replay round is already running.")
            run = Run(self._pool, self._templates, rows)
            self._run = run
        run.launch()
        return run.snapshot()

    def cancel(self) -> dict:
        with self._lock:
            run = self._run
            if run is None:
                return {"status": "idle", "requests": [],
                        "stats": RunManager._empty_stats()}
        return run.cancel()

    def snapshot(self) -> dict:
        with self._lock:
            if self._run is None:
                return {"status": "idle", "requests": [],
                        "stats": RunManager._empty_stats()}
            return self._run.snapshot()

    @staticmethod
    def _empty_stats() -> dict:
        return {"total": 0, "success": 0, "timeout": 0,
                "error": 0, "canceled": 0, "in_flight": 0}


class Run:
    """One replay round.  All mutable state is guarded by ``self._lock``."""

    def __init__(self, pool, templates: dict[str, str], rows: list[dict]):
        self._pool = pool
        self._templates = templates
        self._lock = threading.RLock()
        self._cancel_event = threading.Event()
        self.status = "running"  # running | completed | canceled
        self.started_wall = time.time()
        self.finished_wall: Optional[float] = None

        # Requests ordered by planned arrival.
        self._requests: list[RequestState] = [
            RequestState(
                id=i + 1,
                arrival_ms=int(r["arrival_ms"]),
                template=r["template"],
                store=str(r["store"]),
            )
            for i, r in enumerate(rows)
        ]

        # One worker thread per possible request.  Only MAX_CONNS can hold a
        # connection at once; every other worker blocks on the pool semaphore,
        # which is exactly where queueing time is measured and the 3s deadline
        # / cancel are honored.  Sizing this >= MAX_REQUESTS guarantees no
        # request stalls in the executor's own internal (unmeasured) queue.
        self._executor = ThreadPoolExecutor(
            max_workers=max(MAX_CONNS, MAX_REQUESTS),
            thread_name_prefix="replay",
        )
        # In-flight (running) requests keyed by connection id, each with its
        # own cancellation token so late cancels cannot hit the wrong request.
        self._inflight: dict[int, InFlight] = {}
        self._inflight_done = threading.Condition(self._lock)
        self._pending_futures = set()
        self._scheduler: Optional[threading.Thread] = None
        self._finalizer: Optional[threading.Thread] = None

    # ------------------------------------------------------------------ launch

    def launch(self) -> None:
        self._scheduler = threading.Thread(
            target=self._schedule, name="scheduler", daemon=True)
        self._scheduler.start()
        # The finalizer waits on the scheduler + all worker tasks, then marks
        # the round finished.  HTTP /cancel therefore never reports
        # completion before the real cleanup is done.
        self._finalizer = threading.Thread(
            target=self._finalize, name="finalizer", daemon=True)
        self._finalizer.start()

    # ------------------------------------------------------------- scheduling

    def _schedule(self) -> None:
        """Release each request at its planned arrival time."""
        origin = time.monotonic()
        with self._lock:
            for req in self._requests:
                req.planned_at = origin + req.arrival_ms / 1000.0

        for req in self._requests:
            # Sleep in small chunks so cancel() is picked up promptly.
            while True:
                if self._cancel_event.is_set():
                    with self._lock:
                        self._mark_unscheduled_canceled()
                    return
                remaining = req.planned_at - time.monotonic()
                if remaining <= 0:
                    break
                if self._cancel_event.wait(min(remaining, 0.05)):
                    with self._lock:
                        self._mark_unscheduled_canceled()
                    return

            with self._lock:
                if self._cancel_event.is_set():
                    self._mark_unscheduled_canceled()
                    return
                # Only requests still queued are dispatched; a concurrent
                # cancel sweep flips them to "canceled".
                if req.status != "queued":
                    continue
                fut = self._executor.submit(self._worker, req)
                self._pending_futures.add(fut)
                fut.add_done_callback(self._pending_futures.discard)

    def _mark_unscheduled_canceled(self) -> None:
        # Caller holds the lock.
        for req in self._requests:
            if req.status == "queued":
                req.status = "canceled"
                req.error = "round canceled before dispatch"

    # ------------------------------------------------------------- per request

    def _worker(self, req: RequestState) -> None:
        deadline = req.planned_at + REQUEST_DEADLINE_SECONDS
        conn = None
        timed_out = False
        canceled = False

        # ---- 1. acquire a connection (queueing happens here) ----
        acquired = time.monotonic()
        try:
            while True:
                if self._cancel_event.is_set():
                    self._finish_without_conn(req, "canceled",
                                              "round canceled while queued")
                    return
                if time.monotonic() >= deadline:
                    # Expired in the queue: never take/run a connection.
                    self._finish_without_conn(req, "timeout",
                                              "3s deadline expired while queued")
                    return
                try:
                    conn = self._pool.getconn(timeout=ACQUIRE_CHUNK_SECONDS)
                    break
                except psycopg_pool.PoolTimeout:
                    continue
        except Exception as exc:  # pool failure itself
            self._finish_without_conn(req, "error", f"connection error: {exc}")
            return

        conn_id = id(conn)
        req.queue_ms = (time.monotonic() - acquired) * 1000.0

        # ---- 2. re-check after acquiring: an expired/canceled request
        #         must never execute its SQL ----
        if self._cancel_event.is_set():
            self._return_conn_safely(conn)
            self._finish_after_queue(req, "canceled",
                                     "round canceled before execution")
            return
        if time.monotonic() >= deadline:
            self._return_conn_safely(conn)
            self._finish_after_queue(req, "timeout",
                                     "3s deadline expired while queued")
            return

        sql = self._templates.get(req.template)
        if sql is None:
            self._return_conn_safely(conn)
            self._finish_after_queue(req, "error",
                                     f"unknown template: {req.template}")
            return

        # ---- 3. register in-flight ownership, then run the SQL ----
        inflight = InFlight(req=req, conn=conn)
        with self._lock:
            # A cancel() that landed in the tiny window above is caught here.
            if self._cancel_event.is_set():
                self._return_conn_safely(conn)
                self._finish_after_queue(req, "canceled",
                                         "round canceled before execution")
                return
            req.status = "running"
            req.started_at = time.monotonic()
            self._inflight[conn_id] = inflight

        watchdog = threading.Thread(
            target=self._watchdog, args=(inflight, deadline),
            name=f"watchdog-{req.id}", daemon=True)
        watchdog.start()

        try:
            cur = conn.cursor()
            cur.execute(sql, {"store": req.store})
            rows = cur.fetchall()
            req.rows = len(rows)
            cur.close()
        except psycopg.errors.QueryCanceled as exc:
            # The query was stopped.  Decide why from our own flags rather
            # than than the message text.
            req.sqlstate = getattr(exc, "sqlstate", None)
            if inflight.cancel_token.is_set():
                canceled = True
                req.error = "query stopped: round canceled"
            else:
                timed_out = True
                req.error = "query stopped: 3s deadline exceeded"
        except psycopg.Error as exc:
            req.error = f"{type(exc).__name__}: {exc}"
            req.sqlstate = getattr(exc, "sqlstate", None)
        except Exception as exc:  # pragma: no cover - defensive
            req.error = f"{type(exc).__name__}: {exc}"
        finally:
            # Tell the watchdog the statement is no longer running, then wait
            # for it to settle so the connection is only touched by one thread
            # at a time.  A fast query releases its connection immediately.
            inflight.done_event.set()
            watchdog.join(timeout=5.0)
            with self._lock:
                self._inflight.pop(conn_id, None)
                self._inflight_done.notify_all()

            # ---- 4. cleanup THEN return.  The SQL has stopped by here:
            #         execute() raised/returned and the watchdog joined. ----
            self._return_conn_safely(conn)

        req.finished_at = time.monotonic()
        req.exec_ms = (req.finished_at - req.started_at) * 1000.0
        req.total_ms = (req.finished_at - req.planned_at) * 1000.0

        with self._lock:
            if canceled:
                req.status = "canceled"
            elif timed_out:
                req.status = "timeout"
            elif req.error is not None:
                req.status = "error"
            else:
                req.status = "success"

    def _watchdog(self, inflight: InFlight, deadline: float) -> None:
        """Stop the SQL exactly once, only for its owning request."""
        while True:
            # The statement finished on its own: nothing to cancel.
            if inflight.done_event.is_set():
                return
            # Round cancellation: the cancel sweep issues conn.cancel()
            # itself, so the watchdog just stands down.
            if inflight.cancel_token.is_set():
                return
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                if not inflight.done_event.is_set():
                    # Deadline reached while the query is still running.
                    inflight.timeout_token.set()
                    self._cancel_conn(inflight)
                return
            # Sleep in short chunks so completion/cancel are noticed promptly.
            inflight.done_event.wait(timeout=min(remaining, 0.05))

    def _cancel_conn(self, inflight: InFlight) -> None:
        # Open a short-lived sibling connection solely to issue
        # pg_cancel_backend() against the busy backend's PID.  The server
        # asks that backend to stop its current statement without waiting on
        # it, so this call never blocks on the running query and never touches
        # the busy connection object concurrently.
        target_conn = inflight.conn
        try:
            with inflight.cancel_lock:
                if inflight.cancel_issued:
                    return
                inflight.cancel_issued = True
            backend_pid = target_conn.info.backend_pid
            dsn = getattr(target_conn, "replay_dsn", None)
            if not dsn:
                raise RuntimeError("connection is missing replay_dsn")
            with psycopg.connect(dsn, connect_timeout=3,
                                 autocommit=True) as cancel_conn:
                with cancel_conn.cursor() as cur:
                    cur.execute(
                        "SELECT pg_cancel_backend(%s)", (backend_pid,))
        except Exception:
            # Fall back to the in-process cancel handle if the sibling
            # connection failed; statement_timeout remains the final backstop.
            try:
                target_conn.cancel()
            except Exception:
                pass

    # ------------------------------------------------------------- cancellation

    def cancel(self) -> dict:
        self._cancel_event.set()
        with self._lock:
            # Nothing queued may start; everything in flight must stop.
            for req in self._requests:
                if req.status == "queued":
                    req.status = "canceled"
                    req.error = "round canceled while queued"
            for inflight in list(self._inflight.values()):
                inflight.cancel_token.set()
                self._cancel_conn(inflight)
        return self.snapshot()

    # ------------------------------------------------------------- bookkeeping

    def _finish_without_conn(self, req: RequestState, status: str,
                             message: str) -> None:
        now = time.monotonic()
        with self._lock:
            req.status = status
            req.error = message
            req.finished_at = now
            if req.planned_at:
                req.total_ms = (now - req.planned_at) * 1000.0
                # It never reached a connection, so the whole wait was queue.
                req.queue_ms = req.total_ms
            req.exec_ms = 0.0

    def _finish_after_queue(self, req: RequestState, status: str,
                            message: str) -> None:
        now = time.monotonic()
        with self._lock:
            req.status = status
            req.error = message
            req.finished_at = now
            req.exec_ms = 0.0
            req.total_ms = (now - req.planned_at) * 1000.0

    def _return_conn_safely(self, conn) -> None:
        """Roll back any open transaction before handing the connection back.

        For a canceled query the server has already aborted the transaction;
        the rollback clears the aborted state so the next request starts
        clean.  A broken connection is discarded for the pool to recycle.
        """
        try:
            try:
                conn.rollback()
            except psycopg.Error:
                self._pool.putconn(conn, discard=True)
                return
            self._pool.putconn(conn)
        except Exception:
            try:
                self._pool.putconn(conn, discard=True)
            except Exception:
                pass

    def _finalize(self) -> None:
        # Scheduler has released every request...
        self._scheduler.join()
        # ...then every submitted request has fully finished (SQL stopped,
        # transaction cleaned, connection returned).
        self._executor.shutdown(wait=True)
        with self._lock:
            self.finished_wall = time.time()
            self.status = "canceled" if self._cancel_event.is_set() else "completed"

    # ---------------------------------------------------------------- snapshot

    def snapshot(self) -> dict:
        with self._lock:
            stats = {
                "total": len(self._requests),
                "success": sum(r.status == "success" for r in self._requests),
                "timeout": sum(r.status == "timeout" for r in self._requests),
                "error": sum(r.status == "error" for r in self._requests),
                "canceled": sum(r.status == "canceled" for r in self._requests),
                "in_flight": len(self._inflight),
            }
            return {
                "status": self.status,
                "cancel_requested": self._cancel_event.is_set(),
                "started_wall": self.started_wall,
                "finished_wall": self.finished_wall,
                "stats": stats,
                "config": {
                    "max_connections": MAX_CONNS,
                    "deadline_seconds": REQUEST_DEADLINE_SECONDS,
                    "statement_timeout_ms": STATEMENT_TIMEOUT_MS,
                },
                "requests": [r.as_dict() for r in self._requests],
            }
