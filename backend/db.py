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
                """
            )

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
