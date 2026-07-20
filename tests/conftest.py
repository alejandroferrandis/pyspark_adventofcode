"""Shared Spark Connect session for the test suite.

Needs a reachable endpoint: run inside JupyterLab, or start
`scripts/port-forward.sh` first.
"""

from __future__ import annotations

import pytest

from aoc_spark.session import get_spark


@pytest.fixture(scope="session")
def spark():
    session = get_spark("aoc-pyspark-tests")
    yield session
    session.stop()
