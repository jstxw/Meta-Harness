"""Shared fixtures for backend tests."""

from __future__ import annotations

import asyncio
import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from app.meta_harness.persistence import healthcheck  # noqa: E402


@pytest.fixture(scope="session")
def postgres_available() -> bool:
    """Single Postgres healthcheck for the whole session.

    Runs in its own short-lived loop *inside a fixture* rather than at
    module import time — import-time loops misreport Postgres as
    unreachable under some event-loop policies.
    """
    return asyncio.run(healthcheck())


@pytest.fixture
def require_postgres(postgres_available: bool) -> None:
    """Skip the requesting test when Postgres is unreachable."""
    if not postgres_available:
        pytest.skip(
            "Postgres not reachable at configured DSN; bring up via "
            "docker compose -f infra/docker-compose.yml up -d postgres"
        )
