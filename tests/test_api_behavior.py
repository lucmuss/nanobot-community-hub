from __future__ import annotations

from fastapi.testclient import TestClient


AUTH_HEADERS = {"Authorization": "Bearer hub-test-api-token"}


def test_marketplace_filters_and_sorting(client: TestClient) -> None:
    response = client.get(
        "/api/v1/marketplace",
        params={
            "category": "Research",
            "language": "Remote",
            "min_reliability": 90,
            "sort": "installed",
        },
    )
    assert response.status_code == 200
    payload = response.json()
    items = payload["items"]
    assert items
    assert all(item["category"] == "Research" for item in items)
    assert all(item["language"] == "Remote" for item in items)
    assert all(item["reliability"]["percent"] >= 90 for item in items)
    assert items[0]["slug"] == "context7"
    assert any("Makes outbound network requests" in item.get("permission_hints", []) for item in items)
    assert "Remote" in payload["languages"]
    assert "Remote/API" in payload["runtime_options"]
    assert 95 in payload["reliability_options"]


def test_marketplace_runtime_filter_and_local_gui_setting(client: TestClient) -> None:
    create_admin = client.post(
        "/setup/admin",
        data={
            "username": "hub-admin",
            "email": "hub-admin@example.com",
            "password": "HubAdmin!123",
            "password_confirm": "HubAdmin!123",
        },
        follow_redirects=False,
    )
    assert create_admin.status_code == 303

    save = client.post(
        "/admin/settings",
        data={
            "telemetry_ingest_enabled": "on",
            "api_token_writes_enabled": "on",
            "recommendation_mode": "balanced",
            "featured_min_trust_score": "7.5",
            "featured_min_signal_count": "3",
            "discover_cache_ttl_seconds": "20",
            "overview_cache_ttl_seconds": "30",
            "default_gui_url": "https://nanobot-gui.kolibri-kollektiv.eu/",
        },
        follow_redirects=False,
    )
    assert save.status_code == 303

    response = client.get("/api/v1/marketplace", params={"runtime": "Remote/API"})
    assert response.status_code == 200
    payload = response.json()
    assert payload["runtime"] == "Remote/API"
    assert payload["items"]
    assert all(item["runtime_engine"]["label"] == "Remote/API" for item in payload["items"])

    health = client.get("/api/v1/health")
    assert health.status_code == 200
    assert health.json()["runtime_settings"]["default_gui_url"] == "https://nanobot-gui.kolibri-kollektiv.eu"


def test_marketplace_recommendation_and_showcase_api(client: TestClient) -> None:
    recommendation = client.get("/api/v1/marketplace/context7/recommendation")
    assert recommendation.status_code == 200
    assert recommendation.json()["recommended_config"]["transport"] == "remote"

    detail = client.get("/api/v1/marketplace/context7")
    assert detail.status_code == 200
    detail_payload = detail.json()
    assert detail_payload["install_confidence"]["score"] >= 9.0
    assert detail_payload["trust_score"]["score"] >= 8.0
    assert "runs_30d" in detail_payload["install_confidence"]
    assert "config_consensus" in detail_payload["trust_score"]
    assert "repair_rate" in detail_payload["trust_score"]
    assert detail_payload["mcp_type"]["label"] == "Remote MCP"
    assert detail_payload["usage_trend"]["runs_7d"] >= 0
    assert "Makes outbound network requests" in detail_payload["permission_hints"]

    showcase = client.get("/api/v1/showcase", params={"category": "Coding"})
    assert showcase.status_code == 200
    items = showcase.json()["items"]
    assert len(items) == 1
    assert items[0]["slug"] == "repository-review-pilot"
    assert "best_for" in items[0]

    showcase_detail = client.get("/api/v1/showcase/repository-review-pilot")
    assert showcase_detail.status_code == 200
    assert showcase_detail.json()["stack"]["slug"] == "github-developer-stack"


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
            "nanobot_version": "0.3.0",
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
            "nanobot_version": "0.3.0",
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
            "nanobot_version": "0.3.0",
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
    assert stats["average_success_rate"] > 0
    assert stats["average_latency_ms"] > 0
    assert stats["top_categories"]
    assert stats["network_health"]["score"] > 0
    assert stats["fastest_growing_mcps"]
    assert stats["most_installed_mcps"]
    assert stats["common_combinations"]


def test_marketplace_fix_suggestions_endpoint_uses_recommendations_and_runtime_hints(client: TestClient) -> None:
    response = client.get(
        "/api/v1/marketplace/chrome-devtools-mcp/fixes",
        params={
            "error_code": "timeout",
            "current_transport": "sse",
            "current_timeout": 30,
            "missing_runtimes": "node,npx",
        },
    )
    assert response.status_code == 200
    payload = response.json()
    fixes = payload["fixes"]
    assert any(item["id"] == "apply-recommended-config" for item in fixes)
    assert any(item["id"] == "repair-install-node" for item in fixes)


def test_marketplace_detail_includes_known_fix_and_error_cluster_data(client: TestClient) -> None:
    telemetry = [
        {
            "mcp_slug": "chrome-devtools-mcp",
            "version": "0.1.0",
            "success": False,
            "error_code": "timeout",
            "latency_ms": 31000,
            "transport": "sse",
            "timeout_bucket": "0-30",
            "retries": 1,
            "instance_hash": "cluster-a",
            "nanobot_version": "0.3.0",
        },
        {
            "mcp_slug": "chrome-devtools-mcp",
            "version": "0.1.0",
            "success": False,
            "error_code": "timeout",
            "latency_ms": 30000,
            "transport": "sse",
            "timeout_bucket": "0-30",
            "retries": 1,
            "instance_hash": "cluster-b",
            "nanobot_version": "0.3.0",
        },
    ]
    for event in telemetry:
        response = client.post("/api/v1/telemetry/events", json=event)
        assert response.status_code == 202

    detail = client.get("/api/v1/marketplace/chrome-devtools-mcp")
    assert detail.status_code == 200
    payload = detail.json()
    assert payload["error_clusters"]
    assert payload["error_clusters"][0]["error_code"] == "timeout"
    assert payload["known_fixes"]
    assert "Increase timeout" in payload["known_fixes"][0]["title"]
    assert payload["common_combinations"]


def test_recommendation_endpoint_can_use_telemetry_derived_profile(client: TestClient) -> None:
    create = client.post(
        "/api/v1/submissions/mcp",
        headers=AUTH_HEADERS,
        json={
            "repo_url": "https://github.com/example/dynamic-profile-mcp",
            "name": "Dynamic Profile MCP",
            "description": "Telemetry-derived recommendation test.",
            "category": "Research",
            "install_method": "remote",
            "language": "Remote",
            "submitted_by": "gui-admin",
        },
    )
    assert create.status_code == 201

    for idx in range(5):
        response = client.post(
            "/api/v1/telemetry/events",
            json={
                "mcp_slug": "dynamic-profile-mcp",
                "version": "0.1.0",
                "success": True,
                "latency_ms": 1400,
                "transport": "stdio",
                "timeout_bucket": "90-120",
                "retries": 2,
                "instance_hash": f"dyn-{idx}",
                "nanobot_version": "0.3.0",
            },
        )
        assert response.status_code == 202

    for idx in range(2):
        response = client.post(
            "/api/v1/telemetry/events",
            json={
                "mcp_slug": "dynamic-profile-mcp",
                "version": "0.1.0",
                "success": False,
                "latency_ms": 30000,
                "transport": "sse",
                "timeout_bucket": "0-30",
                "retries": 0,
                "instance_hash": f"dyn-error-{idx}",
                "nanobot_version": "0.3.0",
            },
        )
        assert response.status_code == 202

    recommendation = client.get("/api/v1/marketplace/dynamic-profile-mcp/recommendation")
    assert recommendation.status_code == 200
    payload = recommendation.json()["recommended_config"]
    assert payload["transport"] == "stdio"
    assert payload["timeout"] == 120
    assert payload["retries"] == 2
    assert payload["source"] == "telemetry"


def test_confidence_penalizes_single_instance_local_bias(client: TestClient) -> None:
    for slug, name in (
        ("single-instance-mcp", "Single Instance MCP"),
        ("community-backed-mcp", "Community Backed MCP"),
    ):
        create = client.post(
            "/api/v1/submissions/mcp",
            headers=AUTH_HEADERS,
            json={
                "repo_url": f"https://github.com/example/{slug}",
                "name": name,
                "description": "Confidence scoring regression test.",
                "category": "Research",
                "install_method": "remote",
                "language": "Remote",
                "submitted_by": "gui-admin",
            },
        )
        assert create.status_code == 201

    for idx in range(120):
        response = client.post(
            "/api/v1/telemetry/events",
            json={
                "mcp_slug": "single-instance-mcp",
                "version": "0.1.0",
                "success": True,
                "latency_ms": 1200,
                "transport": "remote",
                "timeout_bucket": "60-90",
                "retries": 1,
                "instance_hash": "single-box",
                "nanobot_version": "0.3.0",
            },
        )
        assert response.status_code == 202

    for idx in range(120):
        response = client.post(
            "/api/v1/telemetry/events",
            json={
                "mcp_slug": "community-backed-mcp",
                "version": "0.1.0",
                "success": True,
                "latency_ms": 1300,
                "transport": "remote",
                "timeout_bucket": "60-90",
                "retries": 1,
                "instance_hash": f"community-{idx % 40}",
                "nanobot_version": "0.3.0",
            },
        )
        assert response.status_code == 202

    single_detail = client.get("/api/v1/marketplace/single-instance-mcp")
    assert single_detail.status_code == 200
    community_detail = client.get("/api/v1/marketplace/community-backed-mcp")
    assert community_detail.status_code == 200

    single_payload = single_detail.json()
    community_payload = community_detail.json()
    assert single_payload["install_confidence"]["instances_30d"] == 1
    assert community_payload["install_confidence"]["instances_30d"] == 40
    assert community_payload["install_confidence"]["score"] > single_payload["install_confidence"]["score"]
    assert single_payload["install_confidence"]["score"] < 5.0


def test_trust_breakdown_tracks_consensus_and_repairs(client: TestClient) -> None:
    create = client.post(
        "/api/v1/submissions/mcp",
        headers=AUTH_HEADERS,
        json={
            "repo_url": "https://github.com/example/repair-aware-mcp",
            "name": "Repair Aware MCP",
            "description": "Trust breakdown regression test.",
            "category": "Coding",
            "install_method": "remote",
            "language": "Remote",
            "submitted_by": "gui-admin",
            "recommended_config": {
                "transport": "remote",
                "timeout": 90,
                "retries": 2,
                "confidence_score": 0.8,
                "based_on_instances": 20,
            },
        },
    )
    assert create.status_code == 201

    telemetry = [
        {
            "mcp_slug": "repair-aware-mcp",
            "version": "0.1.0",
            "success": False,
            "error_code": "timeout",
            "latency_ms": 30000,
            "transport": "sse",
            "timeout_bucket": "0-30",
            "retries": 0,
            "instance_hash": "repair-1",
            "nanobot_version": "0.3.0",
        },
        {
            "mcp_slug": "repair-aware-mcp",
            "version": "0.1.0",
            "success": True,
            "latency_ms": 1800,
            "transport": "remote",
            "timeout_bucket": "60-90",
            "retries": 2,
            "instance_hash": "repair-1",
            "nanobot_version": "0.3.0",
        },
        {
            "mcp_slug": "repair-aware-mcp",
            "version": "0.1.0",
            "success": True,
            "latency_ms": 1700,
            "transport": "remote",
            "timeout_bucket": "60-90",
            "retries": 2,
            "instance_hash": "repair-2",
            "nanobot_version": "0.3.0",
        },
    ]
    for event in telemetry:
        response = client.post("/api/v1/telemetry/events", json=event)
        assert response.status_code == 202

    detail = client.get("/api/v1/marketplace/repair-aware-mcp")
    assert detail.status_code == 200
    payload = detail.json()
    assert payload["community_signals"]["repair_opportunities"] == 1
    assert payload["community_signals"]["repaired_instances"] == 1
    assert payload["trust_score"]["repair_rate"] == 1.0
    assert payload["trust_score"]["config_consensus"] > 0


def test_reliable_sort_prefers_trust_over_raw_success_rate(client: TestClient) -> None:
    fixtures = [
        ("trust-fragile-mcp", "Trust Fragile MCP"),
        ("trust-backed-mcp", "Trust Backed MCP"),
    ]
    for slug, name in fixtures:
        create = client.post(
            "/api/v1/submissions/mcp",
            headers=AUTH_HEADERS,
            json={
                "repo_url": f"https://github.com/example/{slug}",
                "name": name,
                "description": "Ranking regression test entry.",
                "category": "Automation",
                "install_method": "remote",
                "language": "Remote",
                "submitted_by": "gui-admin",
            },
        )
        assert create.status_code == 201

    for _idx in range(120):
        response = client.post(
            "/api/v1/telemetry/events",
            json={
                "mcp_slug": "trust-fragile-mcp",
                "version": "0.1.0",
                "success": True,
                "latency_ms": 1200,
                "transport": "remote",
                "timeout_bucket": "60-90",
                "retries": 1,
                "instance_hash": "fragile-single-host",
                "nanobot_version": "0.3.0",
            },
        )
        assert response.status_code == 202

    for idx in range(120):
        response = client.post(
            "/api/v1/telemetry/events",
            json={
                "mcp_slug": "trust-backed-mcp",
                "version": "0.1.0",
                "success": idx % 10 != 0,
                "latency_ms": 1500,
                "transport": "remote",
                "timeout_bucket": "60-90",
                "retries": 1,
                "instance_hash": f"trust-backed-{idx % 30}",
                "nanobot_version": "0.3.0",
            },
        )
        assert response.status_code == 202

    response = client.get(
        "/api/v1/marketplace",
        params={"q": "Trust", "sort": "reliable"},
    )
    assert response.status_code == 200
    items = response.json()["items"]
    slugs = [item["slug"] for item in items[:2]]
    assert slugs[0] == "trust-backed-mcp"
    assert slugs[1] == "trust-fragile-mcp"


def test_common_combinations_include_telemetry_co_usage(client: TestClient) -> None:
    for slug, name in (("telemetry-alpha-mcp", "Telemetry Alpha MCP"), ("telemetry-beta-mcp", "Telemetry Beta MCP")):
        create = client.post(
            "/api/v1/submissions/mcp",
            headers=AUTH_HEADERS,
            json={
                "repo_url": f"https://github.com/example/{slug}",
                "name": name,
                "description": "Compatibility regression test entry.",
                "category": "Research",
                "install_method": "remote",
                "language": "Remote",
                "submitted_by": "gui-admin",
            },
        )
        assert create.status_code == 201

    for event in (
        {
            "mcp_slug": "telemetry-alpha-mcp",
            "version": "0.1.0",
            "success": True,
            "latency_ms": 1000,
            "transport": "remote",
            "timeout_bucket": "60-90",
            "retries": 1,
            "instance_hash": "combo-instance-1",
            "nanobot_version": "0.3.0",
        },
        {
            "mcp_slug": "telemetry-beta-mcp",
            "version": "0.1.0",
            "success": True,
            "latency_ms": 1200,
            "transport": "remote",
            "timeout_bucket": "60-90",
            "retries": 1,
            "instance_hash": "combo-instance-1",
            "nanobot_version": "0.3.0",
        },
    ):
        response = client.post("/api/v1/telemetry/events", json=event)
        assert response.status_code == 202

    detail = client.get("/api/v1/marketplace/telemetry-alpha-mcp")
    assert detail.status_code == 200
    combos = detail.json()["common_combinations"]
    pair = next((item for item in combos if item["slug"] == "telemetry-beta-mcp"), None)
    assert pair is not None
    assert pair["telemetry_instances"] == 1
    assert pair["strength_score"] > 0


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


def test_stack_and_showcase_submission_validate_references(client: TestClient) -> None:
    bad_stack = client.post(
        "/api/v1/submissions/stack",
        headers=AUTH_HEADERS,
        json={
            "title": "Broken Stack",
            "description": "Invalid item reference.",
            "use_case": "Test invalid MCP references.",
            "recommended_model": "moonshot/kimi-k2.5",
            "items": ["does-not-exist"],
        },
    )
    assert bad_stack.status_code == 400
    assert "Unknown MCP reference" in bad_stack.json()["detail"]

    bad_showcase = client.post(
        "/api/v1/submissions/showcase",
        headers=AUTH_HEADERS,
        json={
            "title": "Broken Showcase",
            "description": "Missing stack reference.",
            "use_case": "Test invalid stack references.",
            "example_prompt": "Run something",
            "category": "Coding",
            "stack_slug": "does-not-exist",
        },
    )
    assert bad_showcase.status_code == 400
    assert "referenced stack does not exist" in bad_showcase.json()["detail"]


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
