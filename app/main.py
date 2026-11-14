"""FastAPI application: replay rounds + sales-correction batches.

Endpoints
---------
GET  /                 single-page UI
GET  /api/state        current round snapshot (polled by the UI)
POST /api/run          upload CSV and start the single site-wide round
POST /api/cancel       cancel the running round
POST /api/correction   upload a sales-correction CSV and apply the batch
GET  /api/health       liveness + pool/template readiness
GET  /samples/replay.csv      bundled example replay log
GET  /samples/correction.csv  bundled example correction batch
"""

from __future__ import annotations

import os
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI, File, HTTPException, UploadFile
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles

from correction import (MAX_CORRECTION_ROWS, CorrectionConflict,
                        CorrectionManager)
from csvutil import MAX_ROWS, CSVValidationError, parse_correction_csv, \
    parse_replay_csv
from db import create_pool, load_templates
from engine import RunConflict, RunManager

BASE_DIR = Path(__file__).resolve().parent
STATIC_DIR = BASE_DIR / "static"
SAMPLE_CSV = BASE_DIR.parent / "samples" / "replay.csv"
SAMPLE_CORRECTION_CSV = BASE_DIR.parent / "samples" / "correction.csv"


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup: open the two-connection pool and load the SQL templates.
    pool = create_pool()
    templates = load_templates(pool)
    app.state.pool = pool
    app.state.manager = RunManager(pool, templates)
    app.state.corrections = CorrectionManager(pool, app.state.manager)
    app.state.templates = templates
    try:
        yield
    finally:
        # Shutdown: drain and close every connection.
        pool.close()


app = FastAPI(title="Nightly Settlement Load Replay", version="1.1.0",
              lifespan=lifespan)


@app.get("/api/health")
def health() -> dict:
    return {
        "status": "ok",
        "max_rows": MAX_ROWS,
        "max_correction_rows": MAX_CORRECTION_ROWS,
        "templates": sorted(app.state.templates.keys()),
    }


@app.get("/api/state")
def get_state() -> dict:
    return app.state.manager.snapshot()


@app.post("/api/run")
async def start_run(file: UploadFile = File(...)) -> JSONResponse:
    raw = await file.read()
    if not raw:
        raise HTTPException(status_code=400, detail="Uploaded file is empty.")
    try:
        rows = parse_replay_csv(raw, set(app.state.templates.keys()))
    except CSVValidationError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    try:
        snapshot = app.state.manager.start(rows)
    except RunConflict as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    return JSONResponse(status_code=201, content=snapshot)


@app.post("/api/cancel")
def cancel_run() -> dict:
    return app.state.manager.cancel()


@app.post("/api/correction")
async def apply_correction(file: UploadFile = File(...)) -> JSONResponse:
    raw = await file.read()
    if not raw:
        raise HTTPException(status_code=400, detail="Uploaded file is empty.")
    try:
        rows = parse_correction_csv(raw)
    except CSVValidationError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    try:
        result = app.state.corrections.apply(rows)
    except CorrectionConflict as exc:
        # Mutually exclusive with a running replay round / another batch.
        return JSONResponse(
            status_code=409,
            content={"status": "busy", "message": str(exc)},
        )
    if result["status"] == "rejected":
        # Whole batch rolled back; the conflict rows are in the payload.
        return JSONResponse(status_code=409, content=result)
    return JSONResponse(status_code=200, content=result)


@app.get("/samples/replay.csv")
def sample_csv() -> FileResponse:
    return FileResponse(
        SAMPLE_CSV, media_type="text/csv", filename="replay.csv"
    )


@app.get("/samples/correction.csv")
def sample_correction_csv() -> FileResponse:
    return FileResponse(
        SAMPLE_CORRECTION_CSV, media_type="text/csv",
        filename="correction.csv"
    )


@app.get("/")
def index() -> FileResponse:
    return FileResponse(STATIC_DIR / "index.html")


# Any other static asset (kept last so the API routes win).
app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=int(os.environ.get("PORT", "8000")),
        workers=1,  # single process: the whole site runs one round at a time
    )
