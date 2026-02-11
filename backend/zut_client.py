from __future__ import annotations

import json
import time
import urllib.parse
import urllib.request
from typing import Any

from .config import BASE_URL


class ZutClientError(RuntimeError):
    pass


def _fetch_json(url: str, *, timeout_s: int = 30, retries: int = 3) -> Any:
    last_err: Exception | None = None
    for attempt in range(1, retries + 1):
        try:
            req = urllib.request.Request(
                url,
                headers={
                    "User-Agent": "plan-sync/1.0",
                    "Accept": "application/json,text/plain,*/*",
                },
            )
            with urllib.request.urlopen(req, timeout=timeout_s) as r:
                data = r.read()
            return json.loads(data)
        except Exception as e:  # noqa: BLE001 - pragmatycznie: retry na wszystko
            last_err = e
            if attempt < retries:
                time.sleep(0.4 * attempt)
                continue
            raise ZutClientError(f"fetch_json failed ({url}): {e}") from e
    raise ZutClientError(f"fetch_json failed ({url}): {last_err}")


def fetch_rooms() -> list[str]:
    url = f"{BASE_URL}/schedule.php?kind=room&query="
    j = _fetch_json(url, timeout_s=60, retries=3)
    if not isinstance(j, list):
        raise ZutClientError(f"rooms response is not a list: {type(j)}")
    rooms: list[str] = []
    for item in j:
        if isinstance(item, dict) and "item" in item:
            rooms.append(str(item["item"]))
        else:
            rooms.append(str(item))
    # server sometimes returns duplicates
    return sorted(set(r for r in rooms if r))


def fetch_room_groups(room: str, *, tok_name: str, start_iso: str, end_iso: str) -> set[str]:
    params = {"room": room, "start": start_iso, "end": end_iso}
    q = urllib.parse.urlencode(params, quote_via=urllib.parse.quote)
    url = f"{BASE_URL}/schedule_student.php?{q}"
    j = _fetch_json(url, timeout_s=60, retries=2)
    if not isinstance(j, list):
        raise ZutClientError(f"schedule response is not a list (room={room}): {type(j)}")

    groups: set[str] = set()
    for ev in j:
        if not isinstance(ev, dict):
            continue
        if ev.get("tok_name") != tok_name:
            continue
        g = ev.get("group_name")
        if g:
            groups.add(str(g))
    return groups


def fetch_room_groups_multi(room: str, *, tok_names: set[str], start_iso: str, end_iso: str) -> dict[str, set[str]]:
    """
    Jedno pobranie /schedule_student.php?room=..., ale zwraca grupy dla wielu tok_name naraz.
    """
    tok_names = {str(t).strip() for t in tok_names if str(t).strip()}
    if not tok_names:
        return {}

    params = {"room": room, "start": start_iso, "end": end_iso}
    q = urllib.parse.urlencode(params, quote_via=urllib.parse.quote)
    url = f"{BASE_URL}/schedule_student.php?{q}"
    j = _fetch_json(url, timeout_s=60, retries=2)
    if not isinstance(j, list):
        raise ZutClientError(f"schedule response is not a list (room={room}): {type(j)}")

    out: dict[str, set[str]] = {}
    for ev in j:
        if not isinstance(ev, dict):
            continue
        t = ev.get("tok_name")
        if not t or t not in tok_names:
            continue
        g = ev.get("group_name")
        if not g:
            continue
        out.setdefault(str(t), set()).add(str(g))
    return out


def fetch_student_schedule(number: str, *, start_iso: str, end_iso: str) -> list[dict[str, Any]]:
    params = {"number": str(number).strip(), "start": start_iso, "end": end_iso}
    q = urllib.parse.urlencode(params, quote_via=urllib.parse.quote)
    url = f"{BASE_URL}/schedule_student.php?{q}"
    j = _fetch_json(url, timeout_s=60, retries=2)
    if not isinstance(j, list):
        raise ZutClientError(f"student schedule response is not a list (number={number}): {type(j)}")
    return [ev for ev in j if isinstance(ev, dict)]


def fetch_group_schedule(group: str, *, start_iso: str, end_iso: str) -> list[dict[str, Any]]:
    params = {"group": str(group).strip(), "start": start_iso, "end": end_iso}
    q = urllib.parse.urlencode(params, quote_via=urllib.parse.quote)
    url = f"{BASE_URL}/schedule_student.php?{q}"
    j = _fetch_json(url, timeout_s=60, retries=2)
    if not isinstance(j, list):
        raise ZutClientError(f"group schedule response is not a list (group={group}): {type(j)}")
    return [ev for ev in j if isinstance(ev, dict)]
