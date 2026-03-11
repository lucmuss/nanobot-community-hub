from __future__ import annotations

from fastapi.testclient import TestClient


def test_health_endpoint(client: TestClient) -> None:
    response = client.get("/api/v1/health")
    assert response.status_code == 200
    assert response.json()["ok"] is True
    assert response.json()["database_backend"] == "sqlite"
    assert response.json()["public_url"] == "https://nanobot-community-hub.kolibri-kollektiv.eu"
    assert response.json()["admin_write_api"] is True


def test_marketplace_seeded(client: TestClient) -> None:
    response = client.get("/api/v1/marketplace")
    assert response.status_code == 200
    payload = response.json()
    assert len(payload["items"]) >= 5
    assert any(item["slug"] == "chrome-devtools-mcp" for item in payload["items"])


def test_repo_resolve(client: TestClient) -> None:
    response = client.get(
        "/api/v1/marketplace/resolve",
        params={"repo_url": "https://github.com/ChromeDevTools/chrome-devtools-mcp"},
    )
    assert response.status_code == 200
    assert response.json()["match"]["slug"] == "chrome-devtools-mcp"


def test_telemetry_ingest(client: TestClient) -> None:
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


def test_api_write_endpoints_require_admin_or_token(client: TestClient) -> None:
    denied = client.post(
        "/api/v1/submissions/mcp",
        json={"repo_url": "https://github.com/example/private-mcp"},
    )
    assert denied.status_code == 403

    allowed = client.post(
        "/api/v1/submissions/mcp",
        headers={"Authorization": "Bearer hub-test-api-token"},
        json={"repo_url": "https://github.com/example/private-mcp"},
    )
    assert allowed.status_code in {200, 201}
