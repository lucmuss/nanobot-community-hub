from __future__ import annotations

from fastapi.testclient import TestClient


AUTH_HEADERS = {"Authorization": "Bearer hub-test-api-token"}


def test_marketplace_filters_and_sorting(client: TestClient) -> None:
    response = client.get("/api/v1/marketplace", params={"category": "Research", "sort": "installed"})
    assert response.status_code == 200
    payload = response.json()
    items = payload["items"]
    assert items
    assert all(item["category"] == "Research" for item in items)
    assert items[0]["slug"] == "context7"


def test_marketplace_recommendation_and_showcase_api(client: TestClient) -> None:
    recommendation = client.get("/api/v1/marketplace/context7/recommendation")
    assert recommendation.status_code == 200
    assert recommendation.json()["recommended_config"]["transport"] == "remote"

    showcase = client.get("/api/v1/showcase", params={"category": "Coding"})
    assert showcase.status_code == 200
    items = showcase.json()["items"]
    assert len(items) == 1
    assert items[0]["slug"] == "repository-review-pilot"


def test_repo_resolve_normalizes_git_urls(client: TestClient) -> None:
    response = client.get(
        "/api/v1/marketplace/resolve",
        params={"repo_url": "git@github.com:ChromeDevTools/chrome-devtools-mcp.git"},
    )
    assert response.status_code == 200
    match = response.json()["match"]
    assert match["slug"] == "chrome-devtools-mcp"
    assert match["recommended_config"]["timeout"] == 120


def test_telemetry_errors_are_rejected_cleanly(client: TestClient) -> None:
    missing_slug = client.post("/api/v1/telemetry/events", json={"success": True})
    assert missing_slug.status_code == 400
    assert missing_slug.json()["detail"] == "mcp_slug is required."

    unknown_slug = client.post(
        "/api/v1/telemetry/events",
        json={
            "mcp_slug": "unknown-server",
            "success": False,
            "instance_hash": "anon-x",
        },
    )
    assert unknown_slug.status_code == 400
    assert "Unknown MCP slug" in unknown_slug.json()["detail"]


def test_telemetry_aggregation_updates_detail_and_overview(client: TestClient) -> None:
    events = [
        {
            "mcp_slug": "chrome-devtools-mcp",
            "version": "0.1.0",
            "success": True,
            "latency_ms": 1000,
            "transport": "stdio",
            "timeout_bucket": "90-120",
            "retries": 2,
            "instance_hash": "instance-a",
            "nanobot_version": "0.2.0",
        },
        {
            "mcp_slug": "chrome-devtools-mcp",
            "version": "0.1.0",
            "success": False,
            "latency_ms": 2000,
            "transport": "stdio",
            "timeout_bucket": "90-120",
            "retries": 2,
            "instance_hash": "instance-a",
            "nanobot_version": "0.2.0",
        },
        {
            "mcp_slug": "chrome-devtools-mcp",
            "version": "0.1.0",
            "success": True,
            "latency_ms": 3000,
            "transport": "stdio",
            "timeout_bucket": "90-120",
            "retries": 2,
            "instance_hash": "instance-b",
            "nanobot_version": "0.2.0",
        },
    ]
    for event in events:
        response = client.post("/api/v1/telemetry/events", json=event)
        assert response.status_code == 202

    detail = client.get("/api/v1/marketplace/chrome-devtools-mcp")
    assert detail.status_code == 200
    payload = detail.json()
    assert payload["recent_runs"] == 3
    assert payload["recent_errors"] == 1
    assert payload["active_instances"] == 1820
    assert payload["success_rate"] == 0.6667
    assert payload["avg_latency_ms"] == 2000.0
    assert payload["recent_telemetry"]["active_instances"] == 2

    overview = client.get("/api/v1/stats/overview")
    assert overview.status_code == 200
    stats = overview.json()
    assert stats["runs_today"] == 3
    assert stats["telemetry_active"] is True
    assert stats["top_mcps"][0]["slug"] == "context7"


def test_submit_mcp_api_creates_new_registry_entry(client: TestClient) -> None:
    response = client.post(
        "/api/v1/submissions/mcp",
        headers=AUTH_HEADERS,
        json={
            "repo_url": "https://github.com/example/super-browser-mcp",
            "name": "Super Browser MCP",
            "description": "Community submitted browser MCP.",
            "category": "Research",
            "install_method": "npm",
            "tags": ["browser", "automation"],
            "tools": ["open_page", "extract_text"],
            "submitted_by": "gui-admin",
            "source_instance": "nanobot-dev",
            "source_public_url": "https://nanobot-gui.kolibri-kollektiv.eu",
        },
    )
    assert response.status_code == 201
    payload = response.json()
    assert payload["created"] is True
    assert payload["item"]["slug"] == "super-browser-mcp"
    assert payload["item"]["tool_count"] == 2
    assert payload["submission"]["status"] == "published"

    marketplace = client.get("/api/v1/marketplace", params={"q": "super-browser-mcp"})
    assert marketplace.status_code == 200
    items = marketplace.json()["items"]
    assert len(items) == 1
    assert items[0]["repo_url"] == "https://github.com/example/super-browser-mcp"


def test_submit_mcp_api_deduplicates_known_repositories(client: TestClient) -> None:
    response = client.post(
        "/api/v1/submissions/mcp",
        headers=AUTH_HEADERS,
        json={
            "repo_url": "https://github.com/ChromeDevTools/chrome-devtools-mcp",
            "name": "Chrome DevTools MCP",
            "submitted_by": "gui-admin",
        },
    )
    assert response.status_code == 200
    payload = response.json()
    assert payload["created"] is False
    assert payload["duplicate"] is True
    assert payload["item"]["slug"] == "chrome-devtools-mcp"
    assert payload["submission"]["status"] == "duplicate"


def test_submit_mcp_api_rejects_invalid_or_secret_like_payloads(client: TestClient) -> None:
    invalid_repo = client.post(
        "/api/v1/submissions/mcp",
        headers=AUTH_HEADERS,
        json={"repo_url": "https://gitlab.com/example/not-supported"},
    )
    assert invalid_repo.status_code == 400
    assert "Only GitHub repository URLs" in invalid_repo.json()["detail"]

    secret_like = client.post(
        "/api/v1/submissions/mcp",
        headers=AUTH_HEADERS,
        json={
            "repo_url": "https://github.com/example/secret-mcp",
            "description": "contains sk-1234567890abcd token",
        },
    )
    assert secret_like.status_code == 400
    assert "secret-like content" in secret_like.json()["detail"]


def test_submit_stack_and_showcase_apis_create_private_entries(client: TestClient) -> None:
    stack = client.post(
        "/api/v1/submissions/stack",
        headers=AUTH_HEADERS,
        json={
            "title": "Docs Research Stack",
            "description": "Context-heavy docs analysis stack.",
            "use_case": "Research documentation and summarize stable recommendations.",
            "recommended_model": "moonshot/kimi-k2.5",
            "example_prompt": "Research the library docs and summarize important API changes.",
            "items": ["context7", "chrome-devtools-mcp"],
            "is_public": False,
            "created_by": "hub-admin",
        },
    )
    assert stack.status_code == 201
    stack_payload = stack.json()
    assert stack_payload["item"]["slug"] == "docs-research-stack"
    assert stack_payload["item"]["status"] == "draft"
    assert stack_payload["item"]["is_public"] is False

    showcase = client.post(
        "/api/v1/submissions/showcase",
        headers=AUTH_HEADERS,
        json={
            "title": "Docs Research Assistant",
            "description": "Practical docs research setup.",
            "use_case": "Use MCPs to inspect docs and browser flows together.",
            "category": "Research",
            "example_prompt": "Inspect the docs and extract the most relevant endpoints.",
            "stack_slug": stack_payload["item"]["slug"],
            "is_public": False,
            "created_by": "hub-admin",
        },
    )
    assert showcase.status_code == 201
    showcase_payload = showcase.json()
    assert showcase_payload["item"]["slug"] == "docs-research-assistant"
    assert showcase_payload["item"]["status"] == "draft"
    assert showcase_payload["item"]["is_public"] is False


def test_install_and_import_metrics_endpoints_update_counts(client: TestClient) -> None:
    install = client.post("/api/v1/marketplace/context7/installs")
    assert install.status_code == 202
    assert install.json()["result"]["installs"] >= 1

    stack = client.post(
        "/api/v1/submissions/stack",
        headers=AUTH_HEADERS,
        json={
            "title": "Automation Bundle",
            "description": "Automation focused MCP bundle.",
            "use_case": "Use browser and GitHub tools together.",
            "recommended_model": "moonshot/kimi-k2.5",
            "example_prompt": "Review a repo and inspect browser output.",
            "items": ["chrome-devtools-mcp", "github-mcp-server"],
            "is_public": True,
            "created_by": "hub-admin",
        },
    )
    stack_slug = stack.json()["item"]["slug"]
    stack_import = client.post(f"/api/v1/stacks/{stack_slug}/imports")
    assert stack_import.status_code == 202
    assert stack_import.json()["result"]["imports_count"] == 1
