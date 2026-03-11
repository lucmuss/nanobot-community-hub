from __future__ import annotations

from fastapi.testclient import TestClient


def _bootstrap_admin(client: TestClient) -> None:
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


def test_root_redirects_to_setup_admin_when_no_admin(client: TestClient) -> None:
    response = client.get("/", follow_redirects=False)
    assert response.status_code == 302
    assert response.headers["location"] == "/setup/admin"


def test_setup_and_login_flow_renders_admin_page(client: TestClient) -> None:
    setup = client.get("/setup/admin")
    assert setup.status_code == 200
    assert "Create the first hub admin" in setup.text

    _bootstrap_admin(client)

    admin_page = client.get("/admin")
    assert admin_page.status_code == 200
    assert "Controlled write flows and simple moderation" in admin_page.text

    logout = client.post("/logout", follow_redirects=False)
    assert logout.status_code == 303
    assert logout.headers["location"] == "/login"

    login = client.post(
        "/login",
        data={"identifier": "hub-admin", "password": "HubAdmin!123"},
        follow_redirects=False,
    )
    assert login.status_code == 303
    assert login.headers["location"] == "/admin"


def test_discover_page_renders_seeded_content(client: TestClient) -> None:
    response = client.get("/discover")
    assert response.status_code == 200
    body = response.text
    assert "Discover MCP" in body
    assert "Chrome DevTools MCP" in body
    assert "Context7" in body
    assert "Controlled submissions and moderation" in body
    assert "Create First Admin" in body


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


def test_admin_page_requires_login(client: TestClient) -> None:
    _bootstrap_admin(client)
    client.post("/logout")
    response = client.get("/admin", follow_redirects=False)
    assert response.status_code == 303
    assert response.headers["location"] == "/login"


def test_admin_can_submit_and_moderate_stack_and_showcase(client: TestClient) -> None:
    _bootstrap_admin(client)

    stack = client.post(
        "/admin/submit/stack",
        data={
            "title": "Browser Coding Stack",
            "description": "Use browser and repository tools together.",
            "use_case": "Review code and inspect browser behavior.",
            "recommended_model": "moonshot/kimi-k2.5",
            "example_prompt": "Review this repo and verify the UI flow in a browser.",
            "items": "chrome-devtools-mcp, github-mcp-server",
        },
        follow_redirects=False,
    )
    assert stack.status_code == 303

    showcase = client.post(
        "/admin/submit/showcase",
        data={
            "title": "Browser Code Review",
            "description": "Showcase for browser-assisted code review.",
            "use_case": "Audit UI and repository changes together.",
            "category": "Coding",
            "example_prompt": "Review the repository and inspect the UI in the browser.",
            "stack_slug": "browser-coding-stack",
        },
        follow_redirects=False,
    )
    assert showcase.status_code == 303

    publish_stack = client.post(
        "/admin/moderate/stack/browser-coding-stack",
        data={"action": "publish"},
        follow_redirects=False,
    )
    assert publish_stack.status_code == 303

    publish_showcase = client.post(
        "/admin/moderate/showcase/browser-code-review",
        data={"action": "publish"},
        follow_redirects=False,
    )
    assert publish_showcase.status_code == 303

    public_stack = client.get("/stacks/browser-coding-stack")
    assert public_stack.status_code == 200
    assert "Browser Coding Stack" in public_stack.text

    public_showcase = client.get("/showcase", params={"q": "Browser Code Review"})
    assert public_showcase.status_code == 200
    assert "Browser Code Review" in public_showcase.text


def test_missing_detail_routes_return_404(client: TestClient) -> None:
    assert client.get("/mcp/does-not-exist").status_code == 404
    assert client.get("/stacks/does-not-exist").status_code == 404
    assert client.get("/api/v1/marketplace/does-not-exist").status_code == 404
    assert client.get("/api/v1/stacks/does-not-exist").status_code == 404
