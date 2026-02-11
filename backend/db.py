from __future__ import annotations

import datetime as dt
import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Optional


@dataclass(frozen=True)
class SyncRun:
    id: int
    tok_name: str
    start_iso: str
    end_iso: str
    created_at: str
    started_at: Optional[str]
    finished_at: Optional[str]
    status: str
    rooms_total: int
    rooms_processed: int
    groups_found: int
    groups_added: int
    errors: int
    last_error: Optional[str]


class DB:
    def __init__(self, path: Path):
        self.path = path
        self.path.parent.mkdir(parents=True, exist_ok=True)

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.path, timeout=30)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute("PRAGMA synchronous=NORMAL;")
        conn.execute("PRAGMA foreign_keys=ON;")
        return conn

    def init(self) -> None:
        with self._connect() as conn:
            conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS rooms (
                    name TEXT PRIMARY KEY,
                    first_seen_at TEXT NOT NULL,
                    last_seen_at  TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS sync_runs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    tok_name TEXT NOT NULL,
                    start_iso TEXT NOT NULL,
                    end_iso TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    started_at TEXT,
                    finished_at TEXT,
                    status TEXT NOT NULL,
                    rooms_total INTEGER NOT NULL DEFAULT 0,
                    rooms_processed INTEGER NOT NULL DEFAULT 0,
                    groups_found INTEGER NOT NULL DEFAULT 0,
                    groups_added INTEGER NOT NULL DEFAULT 0,
                    errors INTEGER NOT NULL DEFAULT 0,
                    last_error TEXT
                );

                CREATE TABLE IF NOT EXISTS run_groups (
                    run_id INTEGER NOT NULL,
                    tok_name TEXT NOT NULL,
                    group_name TEXT NOT NULL,
                    PRIMARY KEY (run_id, tok_name, group_name),
                    FOREIGN KEY (run_id) REFERENCES sync_runs(id) ON DELETE CASCADE
                );

                -- Globalne (canonical) grupy widziane kiedykolwiek dla tok_name.
                CREATE TABLE IF NOT EXISTS groups (
                    tok_name TEXT NOT NULL,
                    group_name TEXT NOT NULL,
                    first_seen_at TEXT NOT NULL,
                    last_seen_at  TEXT NOT NULL,
                    PRIMARY KEY (tok_name, group_name)
                );

                -- Uzytkownicy po numerze albumu (frontend nie powinien za kazdym razem przeszukiwac ZUT).
                CREATE TABLE IF NOT EXISTS students (
                    album_number TEXT PRIMARY KEY,
                    majors_count INTEGER NOT NULL,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS student_tok_names (
                    album_number TEXT NOT NULL,
                    tok_name TEXT NOT NULL,
                    first_seen_at TEXT NOT NULL,
                    last_seen_at  TEXT NOT NULL,
                    PRIMARY KEY (album_number, tok_name),
                    FOREIGN KEY (album_number) REFERENCES students(album_number) ON DELETE CASCADE
                );

                CREATE TABLE IF NOT EXISTS student_groups (
                    album_number TEXT NOT NULL,
                    tok_name TEXT NOT NULL,
                    group_name TEXT NOT NULL,
                    first_seen_at TEXT NOT NULL,
                    last_seen_at  TEXT NOT NULL,
                    PRIMARY KEY (album_number, tok_name, group_name),
                    FOREIGN KEY (album_number) REFERENCES students(album_number) ON DELETE CASCADE
                );

                -- Cache pobran zajec dla grup w konkretnym zakresie (start/end jako lokalne ISO bez offsetu).
                CREATE TABLE IF NOT EXISTS group_fetches (
                    group_name TEXT NOT NULL,
                    start_iso TEXT NOT NULL,
                    end_iso TEXT NOT NULL,
                    fetched_at TEXT NOT NULL,
                    status TEXT NOT NULL,
                    last_error TEXT,
                    PRIMARY KEY (group_name, start_iso, end_iso)
                );

                -- Zajecia zwracane z ZUT /schedule_student.php?group=...
                -- start/end przechowujemy w formacie jak w API (lokalne ISO bez offsetu), zeby latwo filtrowac po tygodniach.
                CREATE TABLE IF NOT EXISTS lessons (
                    group_name TEXT NOT NULL,
                    start TEXT NOT NULL,
                    end TEXT NOT NULL,
                    title TEXT,
                    description TEXT,
                    worker_title TEXT,
                    worker TEXT,
                    worker_cover TEXT,
                    lesson_form TEXT,
                    lesson_form_short TEXT,
                    tok_name TEXT,
                    room TEXT,
                    lesson_status TEXT,
                    lesson_status_short TEXT,
                    status_item TEXT,
                    subject TEXT,
                    hours TEXT,
                    color TEXT,
                    border_color TEXT,
                    first_seen_at TEXT NOT NULL,
                    last_seen_at  TEXT NOT NULL,
                    PRIMARY KEY (group_name, start, end)
                );

                CREATE INDEX IF NOT EXISTS idx_lessons_start ON lessons(start);
                CREATE INDEX IF NOT EXISTS idx_lessons_group ON lessons(group_name);
                """
            )

            # Minimal migrations for dev/iterative runs: CREATE TABLE IF NOT EXISTS does not evolve schema.
            # We only add non-key columns here; if a key/PK is wrong, user should delete the cache DB.
            def _cols(table: str) -> set[str]:
                rows = conn.execute(f"PRAGMA table_info({table});").fetchall()
                return {str(r["name"]) for r in rows}

            cols = _cols("students")
            if "majors_count" not in cols:
                conn.execute("ALTER TABLE students ADD COLUMN majors_count INTEGER NOT NULL DEFAULT 1;")
            if "created_at" not in cols:
                conn.execute("ALTER TABLE students ADD COLUMN created_at TEXT NOT NULL DEFAULT '';")
            if "updated_at" not in cols:
                conn.execute("ALTER TABLE students ADD COLUMN updated_at TEXT NOT NULL DEFAULT '';")

            cols = _cols("student_tok_names")
            if "first_seen_at" not in cols:
                conn.execute("ALTER TABLE student_tok_names ADD COLUMN first_seen_at TEXT NOT NULL DEFAULT '';")
            if "last_seen_at" not in cols:
                conn.execute("ALTER TABLE student_tok_names ADD COLUMN last_seen_at TEXT NOT NULL DEFAULT '';")

            cols = _cols("student_groups")
            if "first_seen_at" not in cols:
                conn.execute("ALTER TABLE student_groups ADD COLUMN first_seen_at TEXT NOT NULL DEFAULT '';")
            if "last_seen_at" not in cols:
                conn.execute("ALTER TABLE student_groups ADD COLUMN last_seen_at TEXT NOT NULL DEFAULT '';")

    @staticmethod
    def _now_iso() -> str:
        return dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds")

    def upsert_rooms(self, rooms: Iterable[str]) -> None:
        now = self._now_iso()
        rows = [(r, now, now) for r in rooms]
        with self._connect() as conn:
            conn.executemany(
                """
                INSERT INTO rooms(name, first_seen_at, last_seen_at)
                VALUES (?, ?, ?)
                ON CONFLICT(name) DO UPDATE SET last_seen_at=excluded.last_seen_at;
                """,
                rows,
            )

    def create_run(self, tok_name: str, start_iso: str, end_iso: str) -> int:
        now = self._now_iso()
        with self._connect() as conn:
            cur = conn.execute(
                """
                INSERT INTO sync_runs(tok_name, start_iso, end_iso, created_at, status)
                VALUES (?, ?, ?, ?, 'queued');
                """,
                (tok_name, start_iso, end_iso, now),
            )
            return int(cur.lastrowid)

    def mark_run_started(self, run_id: int) -> None:
        now = self._now_iso()
        with self._connect() as conn:
            conn.execute(
                "UPDATE sync_runs SET started_at=?, status='running' WHERE id=?;",
                (now, run_id),
            )

    def mark_run_finished(self, run_id: int, status: str, last_error: Optional[str] = None) -> None:
        now = self._now_iso()
        with self._connect() as conn:
            conn.execute(
                "UPDATE sync_runs SET finished_at=?, status=?, last_error=? WHERE id=?;",
                (now, status, last_error, run_id),
            )

    def update_run_progress(
        self,
        run_id: int,
        *,
        rooms_total: Optional[int] = None,
        rooms_processed: Optional[int] = None,
        groups_found: Optional[int] = None,
        groups_added: Optional[int] = None,
        errors: Optional[int] = None,
        last_error: Optional[str] = None,
    ) -> None:
        sets = []
        params = []
        if rooms_total is not None:
            sets.append("rooms_total=?")
            params.append(int(rooms_total))
        if rooms_processed is not None:
            sets.append("rooms_processed=?")
            params.append(int(rooms_processed))
        if groups_found is not None:
            sets.append("groups_found=?")
            params.append(int(groups_found))
        if groups_added is not None:
            sets.append("groups_added=?")
            params.append(int(groups_added))
        if errors is not None:
            sets.append("errors=?")
            params.append(int(errors))
        if last_error is not None:
            sets.append("last_error=?")
            params.append(str(last_error))

        if not sets:
            return

        params.append(int(run_id))
        sql = f"UPDATE sync_runs SET {', '.join(sets)} WHERE id=?;"
        with self._connect() as conn:
            conn.execute(sql, params)

    def add_groups_for_run(self, run_id: int, tok_name: str, groups: Iterable[str]) -> int:
        """
        Zwraca liczbe nowych wpisow dodanych do tabeli `groups` (canonical).
        """
        now = self._now_iso()
        groups = [g for g in groups if g]
        if not groups:
            return 0

        run_rows = [(run_id, tok_name, g) for g in groups]
        canon_rows = [(tok_name, g, now, now) for g in groups]
        with self._connect() as conn:
            conn.executemany(
                "INSERT OR IGNORE INTO run_groups(run_id, tok_name, group_name) VALUES (?, ?, ?);",
                run_rows,
            )

            # Liczymy ile nowych grup weszlo do tabeli canonical (INSERT OR IGNORE),
            # a osobno aktualizujemy last_seen_at dla wszystkich grup.
            before = conn.total_changes
            conn.executemany(
                """
                INSERT OR IGNORE INTO groups(tok_name, group_name, first_seen_at, last_seen_at)
                VALUES (?, ?, ?, ?)
                """,
                canon_rows,
            )
            added = conn.total_changes - before

            conn.executemany(
                "UPDATE groups SET last_seen_at=? WHERE tok_name=? AND group_name=?;",
                [(now, tok_name, g) for g in groups],
            )
        return int(added)

    def get_run(self, run_id: int) -> Optional[SyncRun]:
        with self._connect() as conn:
            row = conn.execute("SELECT * FROM sync_runs WHERE id=?;", (run_id,)).fetchone()
            if not row:
                return None
            return SyncRun(**dict(row))

    def get_latest_successful_run(self, tok_name: str) -> Optional[SyncRun]:
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT * FROM sync_runs
                WHERE tok_name=? AND status='success'
                ORDER BY finished_at DESC
                LIMIT 1;
                """,
                (tok_name,),
            ).fetchone()
            if not row:
                return None
            return SyncRun(**dict(row))

    def list_groups_for_run(self, run_id: int, tok_name: str) -> list[str]:
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT group_name FROM run_groups
                WHERE run_id=? AND tok_name=?
                ORDER BY group_name ASC;
                """,
                (run_id, tok_name),
            ).fetchall()
            return [r["group_name"] for r in rows]

    def list_rooms(self) -> list[str]:
        with self._connect() as conn:
            rows = conn.execute("SELECT name FROM rooms ORDER BY name ASC;").fetchall()
            return [r["name"] for r in rows]

    # ----------------------------
    # Student workflow (album -> tok_name -> grupy -> zajecia)
    # ----------------------------

    def upsert_student(self, album_number: str, majors_count: int) -> None:
        now = self._now_iso()
        album_number = str(album_number).strip()
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO students(album_number, majors_count, created_at, updated_at)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(album_number) DO UPDATE SET
                    majors_count=excluded.majors_count,
                    updated_at=excluded.updated_at;
                """,
                (album_number, int(majors_count), now, now),
            )

    def student_exists(self, album_number: str) -> bool:
        album_number = str(album_number).strip()
        with self._connect() as conn:
            row = conn.execute("SELECT 1 FROM students WHERE album_number=?;", (album_number,)).fetchone()
            return bool(row)

    def replace_student_tok_names(self, album_number: str, tok_names: Iterable[str]) -> None:
        """
        Nadpisuje tok_name dla studenta (uzywane przy force refresh).
        """
        now = self._now_iso()
        album_number = str(album_number).strip()
        tok_names = [t for t in (str(x).strip() for x in tok_names) if t]
        with self._connect() as conn:
            conn.execute("DELETE FROM student_tok_names WHERE album_number=?;", (album_number,))
            if tok_names:
                conn.executemany(
                    """
                    INSERT INTO student_tok_names(album_number, tok_name, first_seen_at, last_seen_at)
                    VALUES (?, ?, ?, ?);
                    """,
                    [(album_number, t, now, now) for t in tok_names],
                )

    def list_student_tok_names(self, album_number: str) -> list[str]:
        album_number = str(album_number).strip()
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT tok_name FROM student_tok_names
                WHERE album_number=?
                ORDER BY first_seen_at ASC, tok_name ASC;
                """,
                (album_number,),
            ).fetchall()
            return [str(r["tok_name"]) for r in rows]

    def replace_student_groups(self, album_number: str, tok_name: str, groups: Iterable[str]) -> None:
        """
        Nadpisuje mapowanie grup dla studenta w ramach jednego tok_name.
        """
        now = self._now_iso()
        album_number = str(album_number).strip()
        tok_name = str(tok_name).strip()
        groups = [g for g in (str(x).strip() for x in groups) if g]
        with self._connect() as conn:
            conn.execute(
                "DELETE FROM student_groups WHERE album_number=? AND tok_name=?;",
                (album_number, tok_name),
            )
            if groups:
                conn.executemany(
                    """
                    INSERT INTO student_groups(album_number, tok_name, group_name, first_seen_at, last_seen_at)
                    VALUES (?, ?, ?, ?, ?);
                    """,
                    [(album_number, tok_name, g, now, now) for g in groups],
                )

    def clear_student_groups(self, album_number: str) -> None:
        album_number = str(album_number).strip()
        with self._connect() as conn:
            conn.execute("DELETE FROM student_groups WHERE album_number=?;", (album_number,))

    def delete_student_groups_not_in_tok_names(self, album_number: str, tok_names: Iterable[str]) -> int:
        """
        Usuwa mapowania grup dla tok_name, ktore nie sa juz przypisane studentowi.
        Zwraca liczbe usunietych wierszy.
        """
        album_number = str(album_number).strip()
        toks = [t for t in (str(x).strip() for x in tok_names) if t]
        with self._connect() as conn:
            if not toks:
                cur = conn.execute("DELETE FROM student_groups WHERE album_number=?;", (album_number,))
                return int(cur.rowcount or 0)

            qs = ",".join(["?"] * len(toks))
            sql = f"DELETE FROM student_groups WHERE album_number=? AND tok_name NOT IN ({qs});"
            cur = conn.execute(sql, [album_number, *toks])
            return int(cur.rowcount or 0)

    def list_student_groups(self, album_number: str) -> dict[str, list[str]]:
        album_number = str(album_number).strip()
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT tok_name, group_name FROM student_groups
                WHERE album_number=?
                ORDER BY tok_name ASC, group_name ASC;
                """,
                (album_number,),
            ).fetchall()
        out: dict[str, list[str]] = {}
        for r in rows:
            t = str(r["tok_name"])
            g = str(r["group_name"])
            out.setdefault(t, []).append(g)
        return out

    def list_student_groups_flat(self, album_number: str) -> list[str]:
        album_number = str(album_number).strip()
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT group_name FROM student_groups
                WHERE album_number=?
                ORDER BY group_name ASC;
                """,
                (album_number,),
            ).fetchall()
            return [str(r["group_name"]) for r in rows]

    def list_canonical_groups(self, tok_name: str) -> list[str]:
        tok_name = str(tok_name).strip()
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT group_name FROM groups
                WHERE tok_name=?
                ORDER BY group_name ASC;
                """,
                (tok_name,),
            ).fetchall()
            return [str(r["group_name"]) for r in rows]

    def upsert_canonical_groups(self, tok_name: str, groups: Iterable[str]) -> int:
        """
        Zwraca liczbe nowych grup dodanych do tabeli canonical `groups`.
        """
        now = self._now_iso()
        tok_name = str(tok_name).strip()
        groups = [g for g in (str(x).strip() for x in groups) if g]
        if not tok_name or not groups:
            return 0

        rows = [(tok_name, g, now, now) for g in groups]
        with self._connect() as conn:
            before = conn.total_changes
            conn.executemany(
                """
                INSERT OR IGNORE INTO groups(tok_name, group_name, first_seen_at, last_seen_at)
                VALUES (?, ?, ?, ?);
                """,
                rows,
            )
            added = conn.total_changes - before
            conn.executemany(
                "UPDATE groups SET last_seen_at=? WHERE tok_name=? AND group_name=?;",
                [(now, tok_name, g) for g in groups],
            )
        return int(added)

    def get_group_fetch_status(self, group_name: str, start_iso: str, end_iso: str) -> Optional[str]:
        group_name = str(group_name).strip()
        start_iso = str(start_iso).strip()
        end_iso = str(end_iso).strip()
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT status FROM group_fetches
                WHERE group_name=? AND start_iso=? AND end_iso=?;
                """,
                (group_name, start_iso, end_iso),
            ).fetchone()
            return str(row["status"]) if row else None

    def upsert_group_fetch(
        self,
        group_name: str,
        start_iso: str,
        end_iso: str,
        *,
        status: str,
        last_error: Optional[str] = None,
    ) -> None:
        now = self._now_iso()
        group_name = str(group_name).strip()
        start_iso = str(start_iso).strip()
        end_iso = str(end_iso).strip()
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO group_fetches(group_name, start_iso, end_iso, fetched_at, status, last_error)
                VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(group_name, start_iso, end_iso) DO UPDATE SET
                    fetched_at=excluded.fetched_at,
                    status=excluded.status,
                    last_error=excluded.last_error;
                """,
                (group_name, start_iso, end_iso, now, str(status), last_error),
            )

    def upsert_lessons(self, lessons: Iterable[dict]) -> int:
        """
        Upsertuje zajecia (key: group_name + start + end). Zwraca liczbe nowych rekordow.
        """
        now = self._now_iso()
        rows: list[tuple] = []
        for ev in lessons:
            if not isinstance(ev, dict):
                continue
            group_name = ev.get("group_name")
            start = ev.get("start")
            end = ev.get("end")
            if not group_name or not start or not end:
                continue

            rows.append(
                (
                    str(group_name),
                    str(start),
                    str(end),
                    ev.get("title"),
                    ev.get("description"),
                    ev.get("worker_title"),
                    ev.get("worker"),
                    ev.get("worker_cover"),
                    ev.get("lesson_form"),
                    ev.get("lesson_form_short"),
                    ev.get("tok_name"),
                    ev.get("room"),
                    ev.get("lesson_status"),
                    ev.get("lesson_status_short"),
                    ev.get("status_item"),
                    ev.get("subject"),
                    ev.get("hours"),
                    ev.get("color"),
                    ev.get("borderColor") if "borderColor" in ev else ev.get("border_color"),
                    now,
                    now,
                )
            )

        if not rows:
            return 0

        with self._connect() as conn:
            before = conn.total_changes
            conn.executemany(
                """
                INSERT INTO lessons(
                    group_name, start, end,
                    title, description,
                    worker_title, worker, worker_cover,
                    lesson_form, lesson_form_short,
                    tok_name, room,
                    lesson_status, lesson_status_short,
                    status_item, subject, hours,
                    color, border_color,
                    first_seen_at, last_seen_at
                ) VALUES (
                    ?, ?, ?,
                    ?, ?,
                    ?, ?, ?,
                    ?, ?,
                    ?, ?,
                    ?, ?,
                    ?, ?, ?,
                    ?, ?,
                    ?, ?
                )
                ON CONFLICT(group_name, start, end) DO UPDATE SET
                    title=excluded.title,
                    description=excluded.description,
                    worker_title=excluded.worker_title,
                    worker=excluded.worker,
                    worker_cover=excluded.worker_cover,
                    lesson_form=excluded.lesson_form,
                    lesson_form_short=excluded.lesson_form_short,
                    tok_name=excluded.tok_name,
                    room=excluded.room,
                    lesson_status=excluded.lesson_status,
                    lesson_status_short=excluded.lesson_status_short,
                    status_item=excluded.status_item,
                    subject=excluded.subject,
                    hours=excluded.hours,
                    color=excluded.color,
                    border_color=excluded.border_color,
                    last_seen_at=excluded.last_seen_at;
                """,
                rows,
            )
            # conn.total_changes policzy rowniez update; interesuje nas tylko "nowe".
            # SQLite nie podaje tego latwo, wiec szacujemy po zmianie liczby wierszy (SELECT changes() tez miesza update).
            # Pragmatycznie: zwracamy ile "insertowalo lub zupsertowalo" (>= nowe).
            return int(conn.total_changes - before)

    def list_lessons_for_groups(self, groups: Iterable[str], start: str, end: str) -> list[dict]:
        """
        Zwraca zajecia dla podanych grup w zakresie [start, end).
        start/end: lokalne ISO bez offsetu (np. 2026-03-16T00:00:00)
        """
        groups = [g for g in (str(x).strip() for x in groups) if g]
        if not groups:
            return []
        start = str(start).strip()
        end = str(end).strip()

        # SQLite ma limit na liczbe parametrow w IN (...); chunkujemy.
        out: list[dict] = []
        chunk_size = 900
        with self._connect() as conn:
            for i in range(0, len(groups), chunk_size):
                chunk = groups[i : i + chunk_size]
                qs = ",".join(["?"] * len(chunk))
                sql = f"""
                SELECT
                    group_name, start, end,
                    title, description,
                    worker_title, worker, worker_cover,
                    lesson_form, lesson_form_short,
                    tok_name, room,
                    lesson_status, lesson_status_short,
                    status_item, subject, hours,
                    color, border_color
                FROM lessons
                WHERE group_name IN ({qs})
                  AND start >= ?
                  AND start < ?
                ORDER BY start ASC, group_name ASC;
                """
                rows = conn.execute(sql, [*chunk, start, end]).fetchall()
                out.extend([dict(r) for r in rows])
        return out

    def list_filter_items_for_groups(self, groups: Iterable[str], start: str, end: str) -> list[dict]:
        """
        Zwraca unikatowe "pozycje do filtra" (caly zakres), niezaleznie od aktualnie wyswietlanego tygodnia.

        Frontend grupuje po:
        - base: tytul bez ostatniego nawiasu (np. "Sieci komputerowe (L)" -> "Sieci komputerowe")
        - formTitle: title (np. "Sieci komputerowe (L)")
        - option: group_name | worker/worker_title

        start/end: lokalne ISO bez offsetu (np. 2026-03-16T00:00:00), zakres [start, end)
        """
        groups = [g for g in (str(x).strip() for x in groups) if g]
        if not groups:
            return []
        start = str(start).strip()
        end = str(end).strip()

        out: list[dict] = []
        chunk_size = 900
        with self._connect() as conn:
            for i in range(0, len(groups), chunk_size):
                chunk = groups[i : i + chunk_size]
                qs = ",".join(["?"] * len(chunk))
                sql = f"""
                SELECT DISTINCT
                    title,
                    subject,
                    group_name,
                    tok_name,
                    worker,
                    worker_title
                FROM lessons
                WHERE group_name IN ({qs})
                  AND start >= ?
                  AND start < ?
                ORDER BY
                    COALESCE(tok_name, ''),
                    COALESCE(subject, ''),
                    COALESCE(title, ''),
                    group_name ASC,
                    COALESCE(worker, ''),
                    COALESCE(worker_title, '');
                """
                rows = conn.execute(sql, [*chunk, start, end]).fetchall()
                out.extend([dict(r) for r in rows])
        return out

    def delete_lessons_for_group_in_range(self, group_name: str, start: str, end: str) -> int:
        """
        Usuwa zajecia dla jednej grupy w zakresie [start, end). Zwraca liczbe usunietych wierszy.
        """
        group_name = str(group_name).strip()
        start = str(start).strip()
        end = str(end).strip()
        with self._connect() as conn:
            cur = conn.execute(
                """
                DELETE FROM lessons
                WHERE group_name=?
                  AND start >= ?
                  AND start < ?;
                """,
                (group_name, start, end),
            )
            return int(cur.rowcount or 0)
