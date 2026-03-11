from __future__ import annotations

from fastapi.testclient import TestClient


def test_health_endpoint(client: TestClient) -> None:
    response = client.get("/api/v1/health")
    assert response.status_code == 200
    assert response.json()["ok"] is True
    assert response.json()["database_backend"] == "sqlite"
    assert response.json()["public_url"] == "https://nanobot-community-hub.kolibri-kollektiv.eu"
    assert response.json()["admin_write_api"] is True
    assert response.json()["runtime_settings"]["recommendation_mode"] == "balanced"
    assert response.json()["runtime_settings"]["default_gui_url"] == ""


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

    missing = client.get(
        "/api/v1/marketplace/resolve",
        params={"repo_url": "https://github.com/example/does-not-exist"},
    )
    assert missing.status_code == 200
    assert missing.json()["match"] is None


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
            "nanobot_version": "0.3.0",
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

    denied_stack = client.post(
        "/api/v1/submissions/stack",
        json={"title": "x"},
    )
    assert denied_stack.status_code == 403

    denied_showcase = client.post(
        "/api/v1/submissions/showcase",
        json={"title": "x"},
    )
    assert denied_showcase.status_code == 403


def test_import_endpoints_return_404_for_unknown_items(client: TestClient) -> None:
    assert client.post("/api/v1/marketplace/does-not-exist/installs").status_code == 404
    assert client.post("/api/v1/stacks/does-not-exist/imports").status_code == 404
    assert client.post("/api/v1/showcase/does-not-exist/imports").status_code == 404


def test_admin_can_update_runtime_settings(client: TestClient) -> None:
    response = client.post(
        "/setup/admin",
        data={
            "username": "hub-admin",
            "email": "hub-admin@example.com",
            "password": "HubAdmin!123",
            "password_confirm": "HubAdmin!123",
        },
        follow_redirects=False,
    )
    assert response.status_code == 303

    save = client.post(
        "/admin/settings",
        data={
            "telemetry_ingest_enabled": "on",
            "recommendation_mode": "conservative",
            "featured_min_trust_score": "8.4",
            "featured_min_signal_count": "5",
            "discover_cache_ttl_seconds": "45",
            "overview_cache_ttl_seconds": "60",
            "default_gui_url": "https://nanobot-gui.kolibri-kollektiv.eu",
        },
        follow_redirects=False,
    )
    assert save.status_code == 303

    health = client.get("/api/v1/health")
    assert health.status_code == 200
    runtime_settings = health.json()["runtime_settings"]
    assert runtime_settings["recommendation_mode"] == "conservative"
    assert runtime_settings["featured_min_trust_score"] == 8.4
    assert runtime_settings["featured_min_signal_count"] == 5
    assert runtime_settings["discover_cache_ttl_seconds"] == 45
    assert runtime_settings["overview_cache_ttl_seconds"] == 60
    assert runtime_settings["default_gui_url"] == "https://nanobot-gui.kolibri-kollektiv.eu"


def test_runtime_settings_can_disable_telemetry_and_api_token_writes(client: TestClient) -> None:
    response = client.post(
        "/setup/admin",
        data={
            "username": "hub-admin",
            "email": "hub-admin@example.com",
            "password": "HubAdmin!123",
            "password_confirm": "HubAdmin!123",
        },
        follow_redirects=False,
    )
    assert response.status_code == 303

    save = client.post(
        "/admin/settings",
        data={
            "recommendation_mode": "balanced",
            "featured_min_trust_score": "7.5",
            "featured_min_signal_count": "3",
            "discover_cache_ttl_seconds": "20",
            "overview_cache_ttl_seconds": "30",
        },
        follow_redirects=False,
    )
    assert save.status_code == 303

    telemetry = client.post(
        "/api/v1/telemetry/events",
        json={
            "mcp_slug": "chrome-devtools-mcp",
            "success": True,
            "instance_hash": "anon-test",
        },
    )
    assert telemetry.status_code == 403

    logout = client.post("/logout", follow_redirects=False)
    assert logout.status_code == 303

    denied_write = client.post(
        "/api/v1/submissions/mcp",
        headers={"Authorization": "Bearer hub-test-api-token"},
        json={"repo_url": "https://github.com/example/blocked-mcp"},
    )
    assert denied_write.status_code == 403
