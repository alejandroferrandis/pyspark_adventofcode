"""Cache-first puzzle input loading: local file -> Postgres -> adventofcode.com.

The network is last on purpose; AoC asks that inputs be fetched once and cached.
"""

from __future__ import annotations

import os
import urllib.error
import urllib.request
from pathlib import Path

BASE = "https://adventofcode.com"

USER_AGENT = os.environ.get(
    "AOC_USER_AGENT",
    "github.com/alejandroferrandis/pyspark_adventofcode by alejandro.ferrandis@marvalanalytics.com",
)

CACHE_ROOT = Path(os.environ.get("AOC_CACHE_DIR", Path(__file__).resolve().parent.parent / "inputs"))


class AoCError(RuntimeError):
    """Input could not be resolved from any source."""


def _cache_path(year: int, day: int) -> Path:
    return CACHE_ROOT / str(year) / f"day{day:02d}.txt"


def _from_file(year: int, day: int) -> str | None:
    path = _cache_path(year, day)
    return path.read_text() if path.exists() else None


def _to_file(year: int, day: int, text: str) -> None:
    path = _cache_path(year, day)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text)


def _pg_dsn() -> str | None:
    """DSN from AOC_PG_DSN or the standard PG* vars, or None if unconfigured."""
    dsn = os.environ.get("AOC_PG_DSN")
    if dsn:
        return dsn
    host = os.environ.get("PGHOST")
    if not host:
        return None
    user = os.environ.get("PGUSER", "postgres")
    password = os.environ.get("PGPASSWORD", "")
    port = os.environ.get("PGPORT", "5432")
    dbname = os.environ.get("PGDATABASE", "adventofcode")
    return f"host={host} port={port} user={user} password={password} dbname={dbname}"


def _from_postgres(year: int, day: int) -> str | None:
    dsn = _pg_dsn()
    if not dsn:
        return None
    try:
        import psycopg2
    except ImportError:
        return None
    try:
        with psycopg2.connect(dsn) as conn, conn.cursor() as cur:
            cur.execute(f"SELECT input_text FROM y{year}.input_data WHERE day = %s", (day,))
            row = cur.fetchone()
            return row[0] if row else None
    except Exception:  # noqa: BLE001 - a cache miss must not break the notebook
        return None


def _from_aoc(year: int, day: int) -> str:
    session = os.environ.get("AOC_SESSION")
    if not session:
        raise AoCError(
            f"{year} day {day} not in local or Postgres cache, and AOC_SESSION is unset"
        )
    req = urllib.request.Request(
        f"{BASE}/{year}/day/{day}/input",
        headers={"Cookie": f"session={session}", "User-Agent": USER_AGENT},
    )
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:  # noqa: S310 - trusted host
            return resp.read().decode()
    except urllib.error.HTTPError as exc:  # 400 = bad cookie, 404 = not live yet
        raise AoCError(f"AoC returned HTTP {exc.code} for {year} day {day}") from exc
    except urllib.error.URLError as exc:
        raise AoCError(f"could not reach adventofcode.com: {exc.reason}") from exc


def get_input(year: int, day: int) -> str:
    text = _from_file(year, day)
    if text is not None:
        return text

    text = _from_postgres(year, day)
    if text is not None:
        _to_file(year, day, text)
        return text

    text = _from_aoc(year, day)
    _to_file(year, day, text)
    return text
