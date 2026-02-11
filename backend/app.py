from __future__ import annotations

import sqlite3
from pathlib import Path

from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field

from .config import DEFAULT_TOK_NAME, default_db_path
from .db import DB
from .student_workflow import (
    discover_groups_for_tok_names,
    local_iso_to_api_iso,
    monday_for_week,
    range_bounds_local,
    resolve_tok_names_for_student,
    week_range_local,
    weeks_ceil_between_local,
)
from .syncer import SyncRunner
from .zut_client import fetch_group_schedule


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


@app.exception_handler(sqlite3.Error)
def _sqlite_error_handler(_: Request, exc: sqlite3.Error) -> JSONResponse:
    # Zamiast "Internal Server Error" bez kontekstu, zwracamy czytelny detail dla frontendu.
    return JSONResponse(status_code=500, content={"detail": f"DB error: {exc}"})


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


class StudentEnsureRequest(BaseModel):
    album_number: str = Field(min_length=1)
    majors_count: int = Field(ge=1, le=6)
    week_start: str | None = None  # YYYY-MM-DD lub ISO; i tak wyrównujemy do poniedziałku
    range_start: str | None = None  # YYYY-MM-DD lub ISO; zakres do pobrania/przetworzenia
    range_end: str | None = None
    force_refresh: bool = False
    max_workers: int = Field(default=10, ge=1, le=32)
    weeks_search_limit: int = Field(default=8, ge=1, le=26)


@app.post("/api/student/ensure")
def student_ensure(req: StudentEnsureRequest) -> dict:
    album = req.album_number.strip()
    majors_count = int(req.majors_count)

    try:
        monday = monday_for_week(req.week_start)
        week_start_local, week_end_local = week_range_local(monday)
        range_start_local, range_end_local = range_bounds_local(req.range_start, req.range_end, monday_fallback=monday)
    except ValueError as e:
        raise HTTPException(status_code=422, detail=str(e)) from e

    # Zawsze upewniamy sie, ze rekord studenta istnieje (oraz aktualizujemy majors_count).
    db.upsert_student(album, majors_count)

    if not req.force_refresh:
        tok_names = db.list_student_tok_names(album)
        if len(tok_names) >= majors_count:
            tok_names = tok_names[:majors_count]
            groups_by_tok = db.list_student_groups(album)
            ok = True
            for t in tok_names:
                if not groups_by_tok.get(t):
                    ok = False
                    break
            if ok:
                return {
                    "album_number": album,
                    "majors_count": majors_count,
                    "week_start": monday.isoformat(),
                    "start": week_start_local,
                    "end": week_end_local,
                    "range_start": range_start_local,
                    "range_end": range_end_local,
                    "tok_names": tok_names,
                    "groups_by_tok": {t: groups_by_tok.get(t, []) for t in tok_names},
                    "cached": True,
                }

    # Force lub brak kompletu w cache: odswiezamy.
    try:
        tok_monday = monday_for_week(req.range_start or req.week_start)
        # Najpierw przeszukujemy caly zaznaczony zakres (w tygodniach).
        # Dodatkowy fallback "wstecz" do roku obsluguje resolve_tok_names_for_student.
        weeks_limit = max(1, weeks_ceil_between_local(range_start_local, range_end_local))
    except ValueError as e:
        raise HTTPException(status_code=422, detail=str(e)) from e
    try:
        tok_res = resolve_tok_names_for_student(
            album_number=album,
            majors_count=majors_count,
            monday=tok_monday,
            weeks_limit=weeks_limit,
            backward_days_limit=366,
        )
    except HTTPException:
        raise
    except Exception as e:  # noqa: BLE001
        # Np. chwilowy problem z plan.zut.edu.pl
        raise HTTPException(status_code=502, detail=str(e)) from e
    if len(tok_res.tok_names) < majors_count:
        raise HTTPException(
            status_code=404,
            detail=(
                "Nie udalo sie znalezc wymaganej liczby tok_name dla studenta "
                "w zadanym zakresie ani przy szukaniu wstecz do ostatniego roku. "
                f"Znalezione={len(tok_res.tok_names)} oczekiwane={majors_count}. "
                "Sprobuj ustawic inny week_start/range_start."
            ),
        )

    tok_names = tok_res.tok_names[:majors_count]
    db.replace_student_tok_names(album, tok_names)
    # Sprzatanie: jesli tok_name sie zmienily, stare grupy nie powinny "wisiec" na tym albumie.
    db.delete_student_groups_not_in_tok_names(album, tok_names)

    # Przy force_refresh czyscimy mapowanie grup zanim je odbudujemy.
    if req.force_refresh:
        db.clear_student_groups(album)

    groups_by_tok_out: dict[str, list[str]] = {}
    to_discover: set[str] = set()

    existing_groups_by_tok = db.list_student_groups(album)
    for t in tok_names:
        # Jesli student ma juz grupy dla tego tok_name i nie wymuszamy - zachowujemy.
        if not req.force_refresh:
            existing = existing_groups_by_tok.get(t)
            if existing:
                groups_by_tok_out[t] = existing
                continue

        canon = db.list_canonical_groups(t)
        if canon and not req.force_refresh:
            db.replace_student_groups(album, t, canon)
            groups_by_tok_out[t] = canon
        else:
            to_discover.add(t)

    discovery_meta: dict = {"performed": False}
    if to_discover:
        discovery_meta["performed"] = True
        # Zakres discovery bierzemy z zakresu pobran, nie tylko z widocznego tygodnia.
        start_api = local_iso_to_api_iso(range_start_local)
        end_api = local_iso_to_api_iso(range_end_local)

        try:
            disc = discover_groups_for_tok_names(
                tok_names=to_discover,
                start_api=start_api,
                end_api=end_api,
                max_workers=req.max_workers,
            )
        except Exception as e:  # noqa: BLE001
            raise HTTPException(status_code=502, detail=str(e)) from e
        discovery_meta.update(
            {
                "rooms_total": disc.rooms_total,
                "rooms_processed": disc.rooms_processed,
                "errors": disc.errors,
                "last_error": disc.last_error,
            }
        )

        for t in sorted(to_discover):
            gs = sorted(disc.groups_by_tok.get(t, set()))
            db.upsert_canonical_groups(t, gs)
            db.replace_student_groups(album, t, gs)
            groups_by_tok_out[t] = gs

    # Dla tok_name, gdzie nie bylo potrzeby discovery, ale tez nie ustawilismy groups_by_tok_out (np. canon + force)
    # pobieramy z DB.
    final_map = db.list_student_groups(album)
    for t in tok_names:
        groups_by_tok_out.setdefault(t, final_map.get(t, []))

    return {
        "album_number": album,
        "majors_count": majors_count,
        "week_start": monday.isoformat(),
        "start": week_start_local,
        "end": week_end_local,
        "range_start": range_start_local,
        "range_end": range_end_local,
        "tok_names": tok_names,
        "groups_by_tok": groups_by_tok_out,
        "cached": False,
        "tok_name_weeks_used": tok_res.weeks_used,
        "group_discovery": discovery_meta,
    }


class StudentWeekRequest(BaseModel):
    album_number: str = Field(min_length=1)
    week_start: str | None = None
    range_start: str | None = None
    range_end: str | None = None
    force_refresh: bool = False  # tylko dla zajec (group schedule)
    max_workers: int = Field(default=10, ge=1, le=32)


@app.post("/api/student/week")
def student_week(req: StudentWeekRequest) -> dict:
    album = req.album_number.strip()
    if not db.student_exists(album):
        raise HTTPException(status_code=404, detail="student not found; call /api/student/ensure first")

    try:
        monday = monday_for_week(req.week_start)
        week_start_local, week_end_local = week_range_local(monday)
        range_start_local, range_end_local = range_bounds_local(req.range_start, req.range_end, monday_fallback=monday)
    except ValueError as e:
        raise HTTPException(status_code=422, detail=str(e)) from e
    range_start_api = local_iso_to_api_iso(range_start_local)
    range_end_api = local_iso_to_api_iso(range_end_local)

    groups = db.list_student_groups_flat(album)
    if not groups:
        raise HTTPException(status_code=404, detail="no groups for student; call /api/student/ensure first")

    # Fetch/refresh lessons per-group (cache w group_fetches).
    to_fetch: list[str] = []
    skipped = 0
    for g in groups:
        st = db.get_group_fetch_status(g, range_start_local, range_end_local)
        if (not req.force_refresh) and st == "success":
            skipped += 1
            continue
        to_fetch.append(g)

    errors = 0
    last_error: str | None = None
    fetched = 0

    def _fetch_one(group_name: str) -> tuple[str, list[dict]]:
        evs = fetch_group_schedule(group_name, start_iso=range_start_api, end_iso=range_end_api)
        return group_name, evs

    if to_fetch:
        from concurrent.futures import ThreadPoolExecutor, as_completed

        with ThreadPoolExecutor(max_workers=max(1, int(req.max_workers))) as ex:
            futures = {ex.submit(_fetch_one, g): g for g in to_fetch}
            for fut in as_completed(futures):
                g = futures[fut]
                try:
                    group_name, evs = fut.result()
                    # Zeby nie trzymac starych zajec gdy plan sie zmieni: kasujemy zakres i zapisujemy aktualny snapshot.
                    db.delete_lessons_for_group_in_range(group_name, range_start_local, range_end_local)
                    db.upsert_lessons(evs)
                    db.upsert_group_fetch(
                        group_name,
                        range_start_local,
                        range_end_local,
                        status="success",
                        last_error=None,
                    )
                    fetched += 1
                except Exception as e:  # noqa: BLE001
                    errors += 1
                    last_error = f"{g}: {e}"
                    db.upsert_group_fetch(g, range_start_local, range_end_local, status="failed", last_error=str(e))

    # Wyswietlamy tylko wybrany tydzien, nawet jesli dane pobieralismy w szerszym zakresie.
    lessons = db.list_lessons_for_groups(groups, week_start_local, week_end_local)
    # Filtry budujemy z calego zakresu, a nie tylko z biezacego tygodnia.
    filter_items = db.list_filter_items_for_groups(groups, range_start_local, range_end_local)
    return {
        "album_number": album,
        "week_start": monday.isoformat(),
        "start": week_start_local,
        "end": week_end_local,
        "range_start": range_start_local,
        "range_end": range_end_local,
        "groups_total": len(groups),
        "groups_skipped": skipped,
        "groups_fetched": fetched,
        "errors": errors,
        "last_error": last_error,
        "lessons": lessons,
        "filter_items": filter_items,
    }


# Serve the UI (frontend/) at /, without impacting /api routes.
FRONTEND_DIR = (Path(__file__).resolve().parent.parent / "frontend").resolve()
if FRONTEND_DIR.exists():
    @app.get("/")
    def _root() -> FileResponse:
        plan = (FRONTEND_DIR / "plan.html").resolve()
        if not plan.exists():
            raise HTTPException(status_code=404, detail="plan.html not found")
        return FileResponse(str(plan))

    app.mount("/", StaticFiles(directory=str(FRONTEND_DIR), html=True), name="frontend")
