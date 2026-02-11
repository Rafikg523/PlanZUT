from __future__ import annotations

import datetime as dt
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from typing import Optional
from zoneinfo import ZoneInfo

from .zut_client import fetch_room_groups_multi, fetch_rooms, fetch_student_schedule


WARSAW = ZoneInfo("Europe/Warsaw")


def parse_date_or_iso_to_date(value: str) -> dt.date:
    value = value.strip()
    if not value:
        raise ValueError("empty date")

    if "T" in value:
        dtt = dt.datetime.fromisoformat(value)
        if dtt.tzinfo is None:
            dtt = dtt.replace(tzinfo=WARSAW)
        dtt = dtt.astimezone(WARSAW)
        return dtt.date()

    return dt.date.fromisoformat(value)


def parse_date_or_iso_to_local_iso(value: str, *, default_hms: tuple[int, int, int] = (0, 0, 0)) -> str:
    """
    Zwraca lokalne ISO bez offsetu (Europe/Warsaw jako referencja).
    - jesli value jest data (YYYY-MM-DD): dokleja default_hms
    - jesli value jest ISO datetime (z offsetem lub bez): normalizuje do Europe/Warsaw i usuwa tzinfo
    """
    value = value.strip()
    if not value:
        raise ValueError("empty date")

    if "T" in value:
        dtt = dt.datetime.fromisoformat(value)
        if dtt.tzinfo is None:
            dtt = dtt.replace(tzinfo=WARSAW)
        dtt = dtt.astimezone(WARSAW).replace(tzinfo=None)
        return dtt.isoformat(timespec="seconds")

    d = dt.date.fromisoformat(value)
    hh, mm, ss = default_hms
    return dt.datetime(d.year, d.month, d.day, int(hh), int(mm), int(ss)).isoformat(timespec="seconds")


def range_bounds_local(range_start: Optional[str], range_end: Optional[str], *, monday_fallback: dt.date) -> tuple[str, str]:
    """
    Zakres jako lokalne ISO bez offsetu.
    Jesli range_start/range_end nie podane: fallback do zakresu tygodnia monday_fallback.
    """
    if range_start and range_end:
        start_local = parse_date_or_iso_to_local_iso(range_start, default_hms=(0, 0, 0))
        # range_end traktujemy "user-friendly" jako date wlacznie.
        # - jesli podana jest data (YYYY-MM-DD), to end = nastepny dzien 00:00:00 (czyli [start, end) obejmuje caly dzien end)
        # - jesli podano ISO datetime, to bierzemy wprost (musi byc > start)
        if "T" not in range_end.strip():
            d_end = dt.date.fromisoformat(range_end.strip())
            d_end_next = d_end + dt.timedelta(days=1)
            end_local = dt.datetime(d_end_next.year, d_end_next.month, d_end_next.day, 0, 0, 0).isoformat(
                timespec="seconds"
            )
        else:
            end_local = parse_date_or_iso_to_local_iso(range_end, default_hms=(0, 0, 0))
        if end_local <= start_local:
            raise ValueError("range_end must be after range_start")
        return start_local, end_local

    return week_range_local(monday_fallback)


def weeks_ceil_between_local(start_local: str, end_local: str) -> int:
    """
    Ile 7-dniowych okien trzeba pobrać, aby pokryć [start_local, end_local).
    """
    d0 = dt.datetime.fromisoformat(start_local).date()
    d1 = dt.datetime.fromisoformat(end_local).date()
    days = (d1 - d0).days
    if days <= 0:
        return 1
    return max(1, (days + 6) // 7)


def monday_for_week(value: Optional[str]) -> dt.date:
    """
    Zwraca poniedzialek tygodnia dla wskazanej daty/ISO.
    Jesli value=None: bierze "dzisiaj" w strefie Europe/Warsaw.
    """
    if value:
        d = parse_date_or_iso_to_date(value)
    else:
        d = dt.datetime.now(WARSAW).date()
    return d - dt.timedelta(days=d.weekday())


def week_range_local(monday: dt.date) -> tuple[str, str]:
    """
    Zakres tygodnia jako lokalne ISO bez offsetu (00:00:00 poniedzialek -> 00:00:00 nastepny poniedzialek).
    """
    start_dt = dt.datetime(monday.year, monday.month, monday.day, 0, 0, 0)
    end_dt = start_dt + dt.timedelta(days=7)
    return start_dt.isoformat(timespec="seconds"), end_dt.isoformat(timespec="seconds")


def local_iso_to_api_iso(local_iso: str) -> str:
    """
    Zamienia lokalne ISO bez offsetu na ISO z offsetem (Europe/Warsaw) dla zapytan do plan.zut.edu.pl.
    """
    dtt = dt.datetime.fromisoformat(local_iso)
    if dtt.tzinfo is None:
        dtt = dtt.replace(tzinfo=WARSAW)
    return dtt.astimezone(WARSAW).isoformat(timespec="seconds")


@dataclass(frozen=True)
class TokResolveResult:
    tok_names: list[str]
    weeks_used: int


def resolve_tok_names_for_student(
    *,
    album_number: str,
    majors_count: int,
    monday: dt.date,
    weeks_limit: int,
) -> TokResolveResult:
    """
    Pobiera harmonogram studenta oknami 7-dniowymi, dopoki nie zbierze majors_count unikatowych tok_name.
    """
    majors_count = int(majors_count)
    if majors_count <= 0:
        return TokResolveResult(tok_names=[], weeks_used=0)

    seen: set[str] = set()
    tok_names: list[str] = []

    for i in range(max(1, int(weeks_limit))):
        w_monday = monday + dt.timedelta(days=7 * i)
        start_local, end_local = week_range_local(w_monday)
        start_api = local_iso_to_api_iso(start_local)
        end_api = local_iso_to_api_iso(end_local)

        events = fetch_student_schedule(album_number, start_iso=start_api, end_iso=end_api)
        for ev in events:
            t = ev.get("tok_name")
            if not t:
                continue
            t = str(t)
            if t in seen:
                continue
            seen.add(t)
            tok_names.append(t)
            if len(tok_names) >= majors_count:
                return TokResolveResult(tok_names=tok_names, weeks_used=i + 1)

    return TokResolveResult(tok_names=tok_names, weeks_used=max(1, int(weeks_limit)))


@dataclass(frozen=True)
class GroupDiscoveryResult:
    groups_by_tok: dict[str, set[str]]
    rooms_total: int
    rooms_processed: int
    errors: int
    last_error: Optional[str]


def discover_groups_for_tok_names(
    *,
    tok_names: set[str],
    start_api: str,
    end_api: str,
    max_workers: int,
) -> GroupDiscoveryResult:
    """
    Skanuje wszystkie sale i wyciaga group_name dla wskazanych tok_name w zadanym zakresie.
    """
    tok_names = {str(t).strip() for t in tok_names if str(t).strip()}
    if not tok_names:
        return GroupDiscoveryResult(groups_by_tok={}, rooms_total=0, rooms_processed=0, errors=0, last_error=None)

    rooms = fetch_rooms()
    groups_by_tok: dict[str, set[str]] = {t: set() for t in tok_names}

    errors = 0
    last_error: Optional[str] = None
    rooms_processed = 0

    with ThreadPoolExecutor(max_workers=max(1, int(max_workers))) as ex:
        futures = {
            ex.submit(fetch_room_groups_multi, room, tok_names=tok_names, start_iso=start_api, end_iso=end_api): room
            for room in rooms
        }
        for fut in as_completed(futures):
            room = futures[fut]
            rooms_processed += 1
            try:
                m = fut.result()
            except Exception as e:  # noqa: BLE001
                errors += 1
                last_error = f"{room}: {e}"
                continue

            for t, gs in m.items():
                if t in groups_by_tok and gs:
                    groups_by_tok[t].update(gs)

    # Usuwamy puste sety, zeby response byl mniejszy.
    groups_by_tok = {t: gs for t, gs in groups_by_tok.items() if gs}
    return GroupDiscoveryResult(
        groups_by_tok=groups_by_tok,
        rooms_total=len(rooms),
        rooms_processed=rooms_processed,
        errors=errors,
        last_error=last_error,
    )
