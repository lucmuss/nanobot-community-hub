from __future__ import annotations

import os
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from nanobot_hub.app import create_app


@pytest.fixture
def client(tmp_path: Path) -> TestClient:
    db_path = tmp_path / "hub.sqlite3"
    os.environ.pop("NANOBOT_HUB_DATABASE_URL", None)
    os.environ["NANOBOT_HUB_DB_PATH"] = str(db_path)
    os.environ["NANOBOT_HUB_PUBLIC_URL"] = "https://nanobot-community-hub.kolibri-kollektiv.eu"
    os.environ["NANOBOT_HUB_INSTANCE_NAME"] = "nanobot-community-hub"
    app = create_app()
    return TestClient(app)
