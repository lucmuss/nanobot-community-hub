from __future__ import annotations

from fastapi.testclient import TestClient


def test_root_redirects_to_discover(client: TestClient) -> None:
    response = client.get("/", follow_redirects=False)
    assert response.status_code == 302
    assert response.headers["location"] == "/discover"


def test_discover_page_renders_seeded_content(client: TestClient) -> None:
    response = client.get("/discover")
    assert response.status_code == 200
    body = response.text
    assert "Discover MCP" in body
    assert "Chrome DevTools MCP" in body
    assert "Context7" in body
    assert "Submit MCP" in body
    assert 'data-testid="hub-submit-form"' in body


def test_discover_partial_respects_filters(client: TestClient) -> None:
    response = client.get(
        "/partials/discover-results",
        params={"q": "github", "category": "Coding", "sort": "reliable"},
    )
    assert response.status_code == 200
    body = response.text
    assert "GitHub MCP Server" in body
    assert "Context7" not in body


def test_mcp_detail_page_renders_tools_and_issues(client: TestClient) -> None:
    response = client.get("/mcp/chrome-devtools-mcp")
    assert response.status_code == 200
    body = response.text
    assert "Chrome DevTools MCP" in body
    assert "Known Issues" in body
    assert "open_page" in body


def test_stacks_pages_render_seeded_data(client: TestClient) -> None:
    list_response = client.get("/stacks")
    assert list_response.status_code == 200
    assert "GitHub Developer Stack" in list_response.text

    detail_response = client.get("/stacks/github-developer-stack")
    assert detail_response.status_code == 200
    assert "moonshot/kimi-k2.5" in detail_response.text
    assert "github-mcp-server" in detail_response.text


def test_showcase_and_stats_pages_render_seeded_data(client: TestClient) -> None:
    showcase_response = client.get("/showcase", params={"category": "Research"})
    assert showcase_response.status_code == 200
    assert "AI Research Assistant" in showcase_response.text
    assert "Repository Review Pilot" not in showcase_response.text

    stats_response = client.get("/community-stats")
    assert stats_response.status_code == 200
    assert "Community Stats" in stats_response.text
    assert "Tracked MCP servers" in stats_response.text


def test_missing_detail_routes_return_404(client: TestClient) -> None:
    assert client.get("/mcp/does-not-exist").status_code == 404
    assert client.get("/stacks/does-not-exist").status_code == 404
    assert client.get("/api/v1/marketplace/does-not-exist").status_code == 404
    assert client.get("/api/v1/stacks/does-not-exist").status_code == 404
