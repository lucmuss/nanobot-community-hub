from __future__ import annotations

from pathlib import Path

from fastapi.testclient import TestClient

from nanobot_hub.app import create_app


def build_client(tmp_path: Path) -> TestClient:
    db_path = tmp_path / "hub.sqlite3"
    import os

    os.environ["NANOBOT_HUB_DB_PATH"] = str(db_path)
    os.environ["NANOBOT_HUB_PUBLIC_URL"] = "https://nanobot-community-hub.kolibri-kollektiv.eu"
    app = create_app()
    return TestClient(app)


def test_health_endpoint(tmp_path: Path) -> None:
    client = build_client(tmp_path)
    response = client.get("/api/v1/health")
    assert response.status_code == 200
    assert response.json()["ok"] is True


def test_marketplace_seeded(tmp_path: Path) -> None:
    client = build_client(tmp_path)
    response = client.get("/api/v1/marketplace")
    assert response.status_code == 200
    payload = response.json()
    assert len(payload["items"]) >= 5
    assert any(item["slug"] == "chrome-devtools-mcp" for item in payload["items"])


def test_repo_resolve(tmp_path: Path) -> None:
    client = build_client(tmp_path)
    response = client.get(
        "/api/v1/marketplace/resolve",
        params={"repo_url": "https://github.com/ChromeDevTools/chrome-devtools-mcp"},
    )
    assert response.status_code == 200
    assert response.json()["match"]["slug"] == "chrome-devtools-mcp"


def test_telemetry_ingest(tmp_path: Path) -> None:
    client = build_client(tmp_path)
    response = client.post(
        "/api/v1/telemetry/events",
        json={
            "mcp_slug": "chrome-devtools-mcp",
            "version": "0.1.0",
            "success": True,
            "latency_ms": 2100,
            "transport": "stdio",
            "timeout_bucket": "90-120",
            "retries": 2,
            "instance_hash": "anon-local-test",
            "nanobot_version": "0.2.0",
        },
    )
    assert response.status_code == 202
    assert response.json()["ok"] is True
