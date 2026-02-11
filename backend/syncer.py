from __future__ import annotations

import datetime as dt
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from typing import Optional
from zoneinfo import ZoneInfo

from .config import DEFAULT_TOK_NAME
from .db import DB
from .zut_client import fetch_room_groups, fetch_rooms


WARSAW = ZoneInfo("Europe/Warsaw")


def _last_3_months_range_iso(now: Optional[dt.datetime] = None) -> tuple[str, str]:
    if now is None:
        now = dt.datetime.now(WARSAW)

    # "ostatnie 3 miesiace" interpretujemy jako 3 miesiace kalendarzowe wstecz od dzisiaj (ten sam dzien miesiaca)
    # z obcieciem dnia (np. 31 -> 30/28) jesli miesiac ma mniej dni.
    import calendar

    today = now.date()
    y = today.year
    m = today.month - 3
    while m <= 0:
        m += 12
        y -= 1
    day = min(today.day, calendar.monthrange(y, m)[1])
    start_date = dt.date(y, m, day)

    start_dt = dt.datetime(start_date.year, start_date.month, start_date.day, 0, 0, 0, tzinfo=WARSAW)
    end_dt = dt.datetime(today.year, today.month, today.day, 23, 59, 59, tzinfo=WARSAW)
    return start_dt.isoformat(timespec="seconds"), end_dt.isoformat(timespec="seconds")


def _parse_iso_or_date(value: str, *, is_end: bool) -> str:
    """
    Akceptuje:
    - YYYY-MM-DD
    - ISO datetime z offsetem (np. 2026-02-09T00:00:00+01:00)
    - ISO datetime bez offsetu (wtedy zakladamy Europe/Warsaw)
    """
    value = value.strip()
    if "T" not in value:
        d = dt.date.fromisoformat(value)
        if is_end:
            return dt.datetime(d.year, d.month, d.day, 23, 59, 59, tzinfo=WARSAW).isoformat(timespec="seconds")
        return dt.datetime(d.year, d.month, d.day, 0, 0, 0, tzinfo=WARSAW).isoformat(timespec="seconds")

    dtt = dt.datetime.fromisoformat(value)
    if dtt.tzinfo is None:
        dtt = dtt.replace(tzinfo=WARSAW)
    return dtt.isoformat(timespec="seconds")


@dataclass
class SyncParams:
    tok_name: str = DEFAULT_TOK_NAME
    start_iso: Optional[str] = None
    end_iso: Optional[str] = None
    max_workers: int = 10


class SyncRunner:
    def __init__(self, db: DB):
        self._db = db
        self._lock = threading.Lock()
        self._thread: Optional[threading.Thread] = None
        self._active_run_id: Optional[int] = None

    def active_run_id(self) -> Optional[int]:
        with self._lock:
            if self._thread and self._thread.is_alive():
                return self._active_run_id
            return None

    def start(self, *, tok_name: str, start_iso: Optional[str], end_iso: Optional[str], max_workers: int) -> int:
        if not tok_name:
            tok_name = DEFAULT_TOK_NAME

        if start_iso is None or end_iso is None:
            start_default, end_default = _last_3_months_range_iso()
            start_iso = start_iso or start_default
            end_iso = end_iso or end_default

        # normalizacja formatow
        start_iso = _parse_iso_or_date(start_iso, is_end=False)
        end_iso = _parse_iso_or_date(end_iso, is_end=True)

        with self._lock:
            if self._thread and self._thread.is_alive():
                raise RuntimeError("sync already running")

            run_id = self._db.create_run(tok_name, start_iso, end_iso)
            t = threading.Thread(
                target=self._run,
                args=(run_id, tok_name, start_iso, end_iso, max_workers),
                daemon=True,
                name=f"sync-run-{run_id}",
            )
            self._thread = t
            self._active_run_id = run_id
            t.start()
            return run_id

    def _run(self, run_id: int, tok_name: str, start_iso: str, end_iso: str, max_workers: int) -> None:
        errors = 0
        last_error: Optional[str] = None
        groups_found: set[str] = set()
        rooms_processed = 0
        groups_added_total = 0

        try:
            self._db.mark_run_started(run_id)

            rooms = fetch_rooms()
            self._db.upsert_rooms(rooms)
            self._db.update_run_progress(run_id, rooms_total=len(rooms))

            # Pobieramy w watkach, zapis do DB w tym watku (jeden writer).
            with ThreadPoolExecutor(max_workers=max(1, int(max_workers))) as ex:
                futures = {
                    ex.submit(fetch_room_groups, room, tok_name=tok_name, start_iso=start_iso, end_iso=end_iso): room
                    for room in rooms
                }
                for fut in as_completed(futures):
                    room = futures[fut]
                    rooms_processed += 1
                    try:
                        groups = fut.result()
                    except Exception as e:  # noqa: BLE001
                        errors += 1
                        last_error = f"{room}: {e}"
                        self._db.update_run_progress(run_id, errors=errors, last_error=last_error)
                        groups = set()

                    if groups:
                        groups_found.update(groups)
                        groups_added_total += self._db.add_groups_for_run(run_id, tok_name, groups)

                    if rooms_processed % 25 == 0 or rooms_processed == len(rooms):
                        self._db.update_run_progress(
                            run_id,
                            rooms_processed=rooms_processed,
                            groups_found=len(groups_found),
                            groups_added=groups_added_total,
                            errors=errors,
                            last_error=last_error,
                        )

            self._db.mark_run_finished(run_id, status="success", last_error=last_error)
        except Exception as e:  # noqa: BLE001
            last_error = str(e)
            try:
                self._db.update_run_progress(run_id, errors=errors + 1, last_error=last_error)
                self._db.mark_run_finished(run_id, status="failed", last_error=last_error)
            except Exception:
                pass
        finally:
            with self._lock:
                if self._active_run_id == run_id:
                    self._active_run_id = None

