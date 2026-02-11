from __future__ import annotations

from pathlib import Path

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field

from .config import DEFAULT_TOK_NAME, default_db_path
from .db import DB
from .syncer import SyncRunner


db = DB(default_db_path())
runner = SyncRunner(db)

app = FastAPI(title="Plan ZUT Sync Backend", version="0.1.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.on_event("startup")
def _startup() -> None:
    db.init()


class SyncRequest(BaseModel):
    # domyslnie ustalony z gory
    tok_name: str = Field(default=DEFAULT_TOK_NAME)
    # ISO datetime albo YYYY-MM-DD; jesli puste -> ostatnie 3 miesiace
    start: str | None = None
    end: str | None = None
    max_workers: int = Field(default=10, ge=1, le=32)


@app.get("/api/health")
def health() -> dict:
    return {"ok": True}


@app.post("/api/sync")
def start_sync(req: SyncRequest) -> dict:
    try:
        run_id = runner.start(
            tok_name=req.tok_name,
            start_iso=req.start,
            end_iso=req.end,
            max_workers=req.max_workers,
        )
        return {"run_id": run_id}
    except RuntimeError as e:
        raise HTTPException(status_code=409, detail=str(e)) from e


@app.get("/api/runs/{run_id}")
def get_run(run_id: int) -> dict:
    run = db.get_run(run_id)
    if not run:
        raise HTTPException(status_code=404, detail="run not found")
    return run.__dict__


@app.get("/api/runs/active")
def active_run() -> dict:
    run_id = runner.active_run_id()
    return {"run_id": run_id}


@app.get("/api/groups")
def list_groups(
    tok_name: str = Query(default=DEFAULT_TOK_NAME),
    run_id: int | None = Query(default=None),
) -> dict:
    if run_id is None:
        latest = db.get_latest_successful_run(tok_name)
        if not latest:
            return {"tok_name": tok_name, "run_id": None, "groups": []}
        run_id = latest.id

    groups = db.list_groups_for_run(run_id, tok_name)
    return {"tok_name": tok_name, "run_id": run_id, "groups": groups}


@app.get("/api/rooms")
def list_rooms() -> dict:
    return {"rooms": db.list_rooms()}


# Serve the UI (frontend/) at /, without impacting /api routes.
FRONTEND_DIR = (Path(__file__).resolve().parent.parent / "frontend").resolve()
if FRONTEND_DIR.exists():
    app.mount("/", StaticFiles(directory=str(FRONTEND_DIR), html=True), name="frontend")
