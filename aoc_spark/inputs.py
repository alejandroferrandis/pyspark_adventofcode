"""Cache-first puzzle input loading.

Resolution order, cheapest and most private first:

  1. local file  ``inputs/<year>/day<NN>.txt``  -- gitignored, never committed
  2. Postgres    ``adventofcode.y<year>.input_data`` -- the shared homelab cache
  3. adventofcode.com -- only on a full miss, then written back to (1) and (2)

Step 3 is deliberately last. AoC's automation guidelines ask that inputs are
fetched **once** and cached; hammering the site is how people get blocked.

Nothing here logs the session cookie or the input text.
"""

from __future__ import annotations

import os
import urllib.error
import urllib.request
from pathlib import Path

BASE = "https://adventofcode.com"

# AoC asks automated clients to identify themselves.
USER_AGENT = os.environ.get(
    "AOC_USER_AGENT",
    "github.com/alejandroferrandis/pyspark_adventofcode by alejandro.ferrandis@marvalanalytics.com",
)

# Repo root -> inputs/ lives beside the package, not inside it.
CACHE_ROOT = Path(os.environ.get("AOC_CACHE_DIR", Path(__file__).resolve().parent.parent / "inputs"))


class AoCError(RuntimeError):
    """Raised when input cannot be resolved from any source."""


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
    """Build a DSN from env, or None when Postgres isn't configured.

    Set AOC_PG_DSN (or the standard PG* vars) to enable the shared cache. No
    credentials are stored in this repo.
    """
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
    except Exception:  # noqa: BLE001 - cache miss must never break the notebook
        return None


def _from_aoc(year: int, day: int) -> str:
    session = os.environ.get("AOC_SESSION")
    if not session:
        raise AoCError(
            f"{year} day {day} not in local or Postgres cache, and AOC_SESSION is unset "
            "(expected from the k8s Secret 'aoc-session')"
        )
    req = urllib.request.Request(
        f"{BASE}/{year}/day/{day}/input",
        headers={"Cookie": f"session={session}", "User-Agent": USER_AGENT},
    )
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:  # noqa: S310 - trusted host
            return resp.read().decode()
    except urllib.error.HTTPError as exc:  # 400 = bad/expired cookie, 404 = not live yet
        raise AoCError(f"AoC returned HTTP {exc.code} for {year} day {day}") from exc
    except urllib.error.URLError as exc:
        raise AoCError(f"could not reach adventofcode.com: {exc.reason}") from exc


def get_input(year: int, day: int) -> str:
    """Return the raw puzzle input, consulting caches before the network."""
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
