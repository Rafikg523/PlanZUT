from __future__ import annotations

import os
from pathlib import Path

BASE_URL = "https://plan.zut.edu.pl"

# Ustalony z gory TOK name (mozna nadpisac w API parametrem tok_name).
DEFAULT_TOK_NAME = "I_1A_S_2023_2024_1"


def default_db_path() -> Path:
    env = os.getenv("PLAN_DB_PATH")
    if env:
        return Path(env).expanduser().resolve()
    # repo_root/data/plan.sqlite3
    return (Path(__file__).resolve().parent.parent / "data" / "plan.sqlite3").resolve()

