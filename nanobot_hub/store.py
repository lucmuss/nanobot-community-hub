"""SQLite-backed store for the community hub."""

from __future__ import annotations

import hashlib
import json
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _normalize_repo_url(value: str) -> str:
    raw = str(value or "").strip()
    if not raw:
        return ""
    normalized = raw.removesuffix(".git")
    normalized = normalized.replace("git@github.com:", "https://github.com/")
    if normalized.startswith("github.com/"):
        normalized = f"https://{normalized}"
    return normalized.rstrip("/")


SEED_MCPS: list[dict[str, Any]] = [
    {
        "slug": "chrome-devtools-mcp",
        "name": "Chrome DevTools MCP",
        "repo_url": "https://github.com/ChromeDevTools/chrome-devtools-mcp",
        "description": "Browser debugging and DevTools automation tools for web agents.",
        "category": "Coding",
        "language": "Node.js",
        "tags": ["browser", "debugging", "devtools", "automation"],
        "install_method": "npm",
        "verified": 1,
        "status": "active",
        "active_instances": 1820,
        "installs": 12640,
        "success_rate": 0.96,
        "avg_latency_ms": 2100,
        "tools": ["open_page", "evaluate_script", "take_snapshot", "list_network_requests"],
        "known_issues": [
            "Requires a Chromium-capable host runtime.",
            "Headed mode can fail on servers without browser libraries.",
        ],
        "recommended_config": {
            "transport": "stdio",
            "timeout": 120,
            "retries": 2,
            "confidence_score": 0.94,
            "based_on_instances": 820,
        },
    },
    {
        "slug": "context7",
        "name": "Context7",
        "repo_url": "https://github.com/upstash/context7",
        "description": "High-signal documentation retrieval and context search for MCP-aware agents.",
        "category": "Research",
        "language": "Remote",
        "tags": ["docs", "search", "retrieval", "research"],
        "install_method": "remote",
        "verified": 1,
        "status": "active",
        "active_instances": 2410,
        "installs": 15110,
        "success_rate": 0.97,
        "avg_latency_ms": 1650,
        "tools": ["search_context", "fetch_doc", "rank_context"],
        "known_issues": [
            "Remote endpoint health depends on external availability.",
        ],
        "recommended_config": {
            "transport": "remote",
            "timeout": 90,
            "retries": 2,
            "confidence_score": 0.96,
            "based_on_instances": 1320,
        },
    },
    {
        "slug": "playwright-mcp",
        "name": "Playwright MCP",
        "repo_url": "https://github.com/microsoft/playwright-mcp",
        "description": "Reliable browser automation through the Playwright MCP package.",
        "category": "Automation",
        "language": "Node.js",
        "tags": ["browser", "playwright", "testing", "automation"],
        "install_method": "workspace_package",
        "verified": 1,
        "status": "active",
        "active_instances": 1690,
        "installs": 9440,
        "success_rate": 0.95,
        "avg_latency_ms": 2380,
        "tools": ["navigate", "click", "fill", "screenshot"],
        "known_issues": [
            "Monorepo resolution must target the published package.",
            "Servers need browser dependencies installed.",
        ],
        "recommended_config": {
            "transport": "stdio",
            "timeout": 150,
            "retries": 2,
            "confidence_score": 0.92,
            "based_on_instances": 640,
        },
    },
    {
        "slug": "firecrawl-mcp-server",
        "name": "Firecrawl MCP Server",
        "repo_url": "https://github.com/firecrawl/firecrawl-mcp-server",
        "description": "Remote crawling and extraction workflows backed by Firecrawl.",
        "category": "Research",
        "language": "Node.js",
        "tags": ["crawl", "extract", "web", "research"],
        "install_method": "npm",
        "verified": 1,
        "status": "needs_configuration",
        "active_instances": 910,
        "installs": 6220,
        "success_rate": 0.91,
        "avg_latency_ms": 2840,
        "tools": ["crawl", "extract", "map_site"],
        "known_issues": [
            "Requires FIRECRAWL_API_KEY before activation.",
        ],
        "recommended_config": {
            "transport": "stdio",
            "timeout": 180,
            "retries": 2,
            "confidence_score": 0.9,
            "based_on_instances": 410,
        },
    },
    {
        "slug": "github-mcp-server",
        "name": "GitHub MCP Server",
        "repo_url": "https://github.com/github/github-mcp-server",
        "description": "GitHub workflows, repositories, issues, and pull requests through MCP.",
        "category": "Coding",
        "language": "Remote",
        "tags": ["github", "repos", "issues", "pull-requests"],
        "install_method": "remote",
        "verified": 1,
        "status": "needs_configuration",
        "active_instances": 2030,
        "installs": 11880,
        "success_rate": 0.89,
        "avg_latency_ms": 2010,
        "tools": ["list_repositories", "get_issue", "create_pull_request"],
        "known_issues": [
            "Requires GitHub authentication for most runtime calls.",
            "401 Unauthorized is the most common first-run failure.",
        ],
        "recommended_config": {
            "transport": "remote",
            "timeout": 120,
            "retries": 2,
            "confidence_score": 0.88,
            "based_on_instances": 980,
        },
    },
    {
        "slug": "dalle-mcp",
        "name": "DALL-E MCP",
        "repo_url": "https://github.com/Garoth/dalle-mcp",
        "description": "Image generation MCP wrapper with OpenAI-backed generation commands.",
        "category": "Automation",
        "language": "Node.js",
        "tags": ["image", "generation", "openai", "media"],
        "install_method": "npm",
        "verified": 0,
        "status": "needs_configuration",
        "active_instances": 340,
        "installs": 1890,
        "success_rate": 0.83,
        "avg_latency_ms": 3530,
        "tools": ["generate_image", "list_models"],
        "known_issues": [
            "Requires OPENAI_API_KEY before the first successful probe.",
        ],
        "recommended_config": {
            "transport": "stdio",
            "timeout": 180,
            "retries": 1,
            "confidence_score": 0.74,
            "based_on_instances": 120,
        },
    },
]


SEED_STACKS: list[dict[str, Any]] = [
    {
        "slug": "github-developer-stack",
        "title": "GitHub Developer Stack",
        "description": "Repository analysis, code browsing, and patch planning for developer workflows.",
        "use_case": "Analyze repositories, inspect issues, and review pull requests.",
        "recommended_model": "moonshot/kimi-k2.5",
        "example_prompt": "Analyze this repository and suggest the three most important improvements.",
        "rating": 4.9,
        "imports_count": 320,
        "items": ["github-mcp-server", "context7", "playwright-mcp"],
    },
    {
        "slug": "research-assistant-stack",
        "title": "Research Assistant Stack",
        "description": "Web extraction and documentation lookup for fast research loops.",
        "use_case": "Collect background information and summarize sources quickly.",
        "recommended_model": "openrouter/google/gemini-2.5-flash-preview",
        "example_prompt": "Research the latest browser automation approaches and summarize what changed.",
        "rating": 4.8,
        "imports_count": 214,
        "items": ["context7", "firecrawl-mcp-server", "chrome-devtools-mcp"],
    },
]


SEED_SHOWCASE: list[dict[str, Any]] = [
    {
        "slug": "ai-research-assistant",
        "title": "AI Research Assistant",
        "description": "A setup for web-backed research with retrieval, crawling, and browser inspection.",
        "category": "Research",
        "example_prompt": "Research the latest diffusion model papers and summarize practical takeaways.",
        "stack_slug": "research-assistant-stack",
        "imports_count": 780,
        "upvotes_count": 128,
    },
    {
        "slug": "repository-review-pilot",
        "title": "Repository Review Pilot",
        "description": "A coding-focused setup for repository inspection and review preparation.",
        "category": "Coding",
        "example_prompt": "Inspect this GitHub repository and identify the highest-risk regressions.",
        "stack_slug": "github-developer-stack",
        "imports_count": 463,
        "upvotes_count": 91,
    },
]


@dataclass(slots=True)
class HubStore:
    db_path: Path

    def connect(self) -> sqlite3.Connection:
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        return conn

    def init(self) -> None:
        with self.connect() as conn:
            conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS mcp_servers (
                    slug TEXT PRIMARY KEY,
                    name TEXT NOT NULL,
                    repo_url TEXT NOT NULL,
                    description TEXT NOT NULL,
                    category TEXT NOT NULL,
                    language TEXT NOT NULL,
                    tags_json TEXT NOT NULL,
                    install_method TEXT NOT NULL,
                    verified INTEGER NOT NULL DEFAULT 0,
                    status TEXT NOT NULL,
                    active_instances INTEGER NOT NULL DEFAULT 0,
                    installs INTEGER NOT NULL DEFAULT 0,
                    success_rate REAL NOT NULL DEFAULT 0.0,
                    avg_latency_ms REAL NOT NULL DEFAULT 0.0,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS mcp_tools (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    mcp_slug TEXT NOT NULL,
                    tool_name TEXT NOT NULL,
                    UNIQUE(mcp_slug, tool_name)
                );

                CREATE TABLE IF NOT EXISTS mcp_known_issues (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    mcp_slug TEXT NOT NULL,
                    issue_text TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS mcp_recommended_configs (
                    mcp_slug TEXT PRIMARY KEY,
                    transport TEXT NOT NULL,
                    timeout INTEGER NOT NULL,
                    retries INTEGER NOT NULL,
                    confidence_score REAL NOT NULL,
                    based_on_instances INTEGER NOT NULL,
                    updated_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS telemetry_events (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    mcp_slug TEXT NOT NULL,
                    version TEXT NOT NULL DEFAULT '',
                    success INTEGER NOT NULL,
                    error_code TEXT NOT NULL DEFAULT '',
                    latency_ms INTEGER NOT NULL DEFAULT 0,
                    transport TEXT NOT NULL DEFAULT '',
                    timeout_bucket TEXT NOT NULL DEFAULT '',
                    retries INTEGER NOT NULL DEFAULT 0,
                    instance_hash TEXT NOT NULL,
                    nanobot_version TEXT NOT NULL DEFAULT '',
                    created_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS mcp_stacks (
                    slug TEXT PRIMARY KEY,
                    title TEXT NOT NULL,
                    description TEXT NOT NULL,
                    use_case TEXT NOT NULL,
                    recommended_model TEXT NOT NULL,
                    example_prompt TEXT NOT NULL,
                    rating REAL NOT NULL DEFAULT 0,
                    imports_count INTEGER NOT NULL DEFAULT 0,
                    created_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS mcp_stack_items (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    stack_slug TEXT NOT NULL,
                    mcp_slug TEXT NOT NULL,
                    required INTEGER NOT NULL DEFAULT 1,
                    sort_order INTEGER NOT NULL DEFAULT 0
                );

                CREATE TABLE IF NOT EXISTS showcase_entries (
                    slug TEXT PRIMARY KEY,
                    title TEXT NOT NULL,
                    description TEXT NOT NULL,
                    category TEXT NOT NULL,
                    example_prompt TEXT NOT NULL,
                    stack_slug TEXT NOT NULL,
                    imports_count INTEGER NOT NULL DEFAULT 0,
                    upvotes_count INTEGER NOT NULL DEFAULT 0,
                    created_at TEXT NOT NULL
                );
                """
            )
            count = conn.execute("SELECT COUNT(*) FROM mcp_servers").fetchone()[0]
            if count == 0:
                self._seed(conn)

    def _seed(self, conn: sqlite3.Connection) -> None:
        now = _utc_now()
        for entry in SEED_MCPS:
            conn.execute(
                """
                INSERT INTO mcp_servers (
                    slug, name, repo_url, description, category, language, tags_json,
                    install_method, verified, status, active_instances, installs,
                    success_rate, avg_latency_ms, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    entry["slug"],
                    entry["name"],
                    entry["repo_url"],
                    entry["description"],
                    entry["category"],
                    entry["language"],
                    json.dumps(entry["tags"]),
                    entry["install_method"],
                    entry["verified"],
                    entry["status"],
                    entry["active_instances"],
                    entry["installs"],
                    entry["success_rate"],
                    entry["avg_latency_ms"],
                    now,
                    now,
                ),
            )
            for tool_name in entry["tools"]:
                conn.execute(
                    "INSERT INTO mcp_tools (mcp_slug, tool_name) VALUES (?, ?)",
                    (entry["slug"], tool_name),
                )
            for issue in entry["known_issues"]:
                conn.execute(
                    "INSERT INTO mcp_known_issues (mcp_slug, issue_text) VALUES (?, ?)",
                    (entry["slug"], issue),
                )
            recommendation = entry["recommended_config"]
            conn.execute(
                """
                INSERT INTO mcp_recommended_configs (
                    mcp_slug, transport, timeout, retries, confidence_score,
                    based_on_instances, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    entry["slug"],
                    recommendation["transport"],
                    recommendation["timeout"],
                    recommendation["retries"],
                    recommendation["confidence_score"],
                    recommendation["based_on_instances"],
                    now,
                ),
            )

        for stack in SEED_STACKS:
            conn.execute(
                """
                INSERT INTO mcp_stacks (
                    slug, title, description, use_case, recommended_model,
                    example_prompt, rating, imports_count, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    stack["slug"],
                    stack["title"],
                    stack["description"],
                    stack["use_case"],
                    stack["recommended_model"],
                    stack["example_prompt"],
                    stack["rating"],
                    stack["imports_count"],
                    now,
                ),
            )
            for index, item_slug in enumerate(stack["items"]):
                conn.execute(
                    """
                    INSERT INTO mcp_stack_items (stack_slug, mcp_slug, required, sort_order)
                    VALUES (?, ?, 1, ?)
                    """,
                    (stack["slug"], item_slug, index),
                )

        for showcase in SEED_SHOWCASE:
            conn.execute(
                """
                INSERT INTO showcase_entries (
                    slug, title, description, category, example_prompt,
                    stack_slug, imports_count, upvotes_count, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    showcase["slug"],
                    showcase["title"],
                    showcase["description"],
                    showcase["category"],
                    showcase["example_prompt"],
                    showcase["stack_slug"],
                    showcase["imports_count"],
                    showcase["upvotes_count"],
                    now,
                ),
            )

    def list_mcps(self, *, search: str = "", category: str = "", sort: str = "trending") -> list[dict[str, Any]]:
        telemetry = self._telemetry_stats_by_slug()
        with self.connect() as conn:
            rows = conn.execute(
                """
                SELECT *
                FROM mcp_servers
                WHERE (? = '' OR lower(name) LIKE '%' || lower(?) || '%' OR lower(description) LIKE '%' || lower(?) || '%' OR lower(repo_url) LIKE '%' || lower(?) || '%')
                  AND (? = '' OR category = ?)
                """,
                (search, search, search, search, category, category),
            ).fetchall()
            items = [self._build_mcp_summary(conn, row, telemetry.get(row["slug"], {})) for row in rows]

        sort_key = str(sort or "trending").lower()
        if sort_key == "new":
            items.sort(key=lambda item: item["created_at"], reverse=True)
        elif sort_key == "reliable":
            items.sort(key=lambda item: (item["success_rate"], item["active_instances"]), reverse=True)
        elif sort_key == "installed":
            items.sort(key=lambda item: item["installs"], reverse=True)
        else:
            items.sort(
                key=lambda item: (item["active_instances"], item["success_rate"], item["installs"]),
                reverse=True,
            )
        return items

    def get_mcp(self, slug: str) -> dict[str, Any] | None:
        telemetry = self._telemetry_stats_by_slug().get(slug, {})
        with self.connect() as conn:
            row = conn.execute("SELECT * FROM mcp_servers WHERE slug = ?", (slug,)).fetchone()
            if row is None:
                return None
            item = self._build_mcp_summary(conn, row, telemetry, detailed=True)
            recommendation = conn.execute(
                "SELECT * FROM mcp_recommended_configs WHERE mcp_slug = ?",
                (slug,),
            ).fetchone()
            item["recommended_config"] = dict(recommendation) if recommendation else None
            item["known_issues"] = [
                issue["issue_text"]
                for issue in conn.execute(
                    "SELECT issue_text FROM mcp_known_issues WHERE mcp_slug = ? ORDER BY id ASC",
                    (slug,),
                ).fetchall()
            ]
            return item

    def resolve_repo(self, repo_url: str) -> dict[str, Any] | None:
        normalized = _normalize_repo_url(repo_url)
        if not normalized:
            return None
        with self.connect() as conn:
            row = conn.execute(
                "SELECT slug, name, repo_url FROM mcp_servers WHERE repo_url = ?",
                (normalized,),
            ).fetchone()
            if row is None:
                return None
            recommendation = conn.execute(
                "SELECT transport, timeout, retries, confidence_score, based_on_instances FROM mcp_recommended_configs WHERE mcp_slug = ?",
                (row["slug"],),
            ).fetchone()
            return {
                "slug": row["slug"],
                "name": row["name"],
                "repo_url": row["repo_url"],
                "recommended_config": dict(recommendation) if recommendation else None,
            }

    def list_stacks(self, *, search: str = "") -> list[dict[str, Any]]:
        with self.connect() as conn:
            rows = conn.execute(
                """
                SELECT *
                FROM mcp_stacks
                WHERE (? = '' OR lower(title) LIKE '%' || lower(?) || '%' OR lower(description) LIKE '%' || lower(?) || '%')
                ORDER BY rating DESC, imports_count DESC
                """,
                (search, search, search),
            ).fetchall()
            return [self._build_stack_summary(conn, row) for row in rows]

    def get_stack(self, slug: str) -> dict[str, Any] | None:
        with self.connect() as conn:
            row = conn.execute("SELECT * FROM mcp_stacks WHERE slug = ?", (slug,)).fetchone()
            if row is None:
                return None
            return self._build_stack_summary(conn, row, detailed=True)

    def list_showcase(self, *, search: str = "", category: str = "") -> list[dict[str, Any]]:
        with self.connect() as conn:
            rows = conn.execute(
                """
                SELECT *
                FROM showcase_entries
                WHERE (? = '' OR lower(title) LIKE '%' || lower(?) || '%' OR lower(description) LIKE '%' || lower(?) || '%')
                  AND (? = '' OR category = ?)
                ORDER BY imports_count DESC, upvotes_count DESC
                """,
                (search, search, search, category, category),
            ).fetchall()
            items: list[dict[str, Any]] = []
            for row in rows:
                stack_row = conn.execute(
                    "SELECT slug, title FROM mcp_stacks WHERE slug = ?",
                    (row["stack_slug"],),
                ).fetchone()
                items.append(
                    {
                        **dict(row),
                        "stack": dict(stack_row) if stack_row else None,
                    }
                )
            return items

    def get_overview_stats(self) -> dict[str, Any]:
        telemetry = self._telemetry_stats_by_slug()
        marketplace = self.list_mcps()
        today_start = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
        with self.connect() as conn:
            total_events_today = conn.execute(
                "SELECT COUNT(*) FROM telemetry_events WHERE created_at >= ?",
                (today_start.isoformat(timespec="seconds"),),
            ).fetchone()[0]
        active_instances = sum(item["active_instances"] for item in marketplace)
        return {
            "registry_count": len(marketplace),
            "verified_count": sum(1 for item in marketplace if item["verified"]),
            "active_instances": active_instances,
            "runs_today": int(total_events_today),
            "top_mcps": marketplace[:5],
            "telemetry_active": bool(telemetry),
        }

    def record_telemetry_event(self, payload: dict[str, Any]) -> dict[str, Any]:
        slug = str(payload.get("mcp_slug", "")).strip()
        if not slug:
            raise ValueError("mcp_slug is required.")
        instance_raw = str(payload.get("instance_hash", "")).strip() or "anonymous"
        instance_hash = hashlib.sha256(instance_raw.encode("utf-8")).hexdigest()[:16]
        created_at = str(payload.get("created_at", "")).strip() or _utc_now()
        with self.connect() as conn:
            exists = conn.execute("SELECT 1 FROM mcp_servers WHERE slug = ?", (slug,)).fetchone()
            if exists is None:
                raise ValueError(f"Unknown MCP slug: {slug}")
            conn.execute(
                """
                INSERT INTO telemetry_events (
                    mcp_slug, version, success, error_code, latency_ms,
                    transport, timeout_bucket, retries, instance_hash,
                    nanobot_version, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    slug,
                    str(payload.get("version", "")),
                    1 if bool(payload.get("success")) else 0,
                    str(payload.get("error_code", "")),
                    int(payload.get("latency_ms", 0) or 0),
                    str(payload.get("transport", "")),
                    str(payload.get("timeout_bucket", "")),
                    int(payload.get("retries", 0) or 0),
                    instance_hash,
                    str(payload.get("nanobot_version", "")),
                    created_at,
                ),
            )
        return {
            "mcp_slug": slug,
            "instance_hash": instance_hash,
            "created_at": created_at,
        }

    def categories(self) -> list[str]:
        return sorted({entry["category"] for entry in SEED_MCPS})

    def showcase_categories(self) -> list[str]:
        return sorted({entry["category"] for entry in SEED_SHOWCASE})

    def _build_mcp_summary(
        self,
        conn: sqlite3.Connection,
        row: sqlite3.Row,
        telemetry: dict[str, Any],
        *,
        detailed: bool = False,
    ) -> dict[str, Any]:
        tools = [
            tool["tool_name"]
            for tool in conn.execute(
                "SELECT tool_name FROM mcp_tools WHERE mcp_slug = ? ORDER BY tool_name ASC",
                (row["slug"],),
            ).fetchall()
        ]
        effective_active = max(int(row["active_instances"]), int(telemetry.get("active_instances", 0) or 0))
        effective_success = float(telemetry.get("success_rate")) if telemetry.get("run_count", 0) >= 3 else float(row["success_rate"])
        effective_latency = float(telemetry.get("avg_latency_ms")) if telemetry.get("run_count", 0) >= 3 else float(row["avg_latency_ms"])
        summary = {
            **dict(row),
            "repo_url": _normalize_repo_url(row["repo_url"]),
            "tags": json.loads(row["tags_json"]),
            "tools": tools,
            "tool_count": len(tools),
            "active_instances": effective_active,
            "success_rate": round(effective_success, 4),
            "avg_latency_ms": round(effective_latency, 1),
            "recent_runs": int(telemetry.get("run_count", 0) or 0),
            "recent_errors": int(telemetry.get("error_count", 0) or 0),
            "verified": bool(row["verified"]),
        }
        if detailed:
            summary["recent_telemetry"] = telemetry
        return summary

    def _build_stack_summary(
        self,
        conn: sqlite3.Connection,
        row: sqlite3.Row,
        *,
        detailed: bool = False,
    ) -> dict[str, Any]:
        item_rows = conn.execute(
            """
            SELECT m.slug, m.name, m.repo_url
            FROM mcp_stack_items i
            JOIN mcp_servers m ON m.slug = i.mcp_slug
            WHERE i.stack_slug = ?
            ORDER BY i.sort_order ASC, i.id ASC
            """,
            (row["slug"],),
        ).fetchall()
        summary = {
            **dict(row),
            "items": [dict(item) for item in item_rows],
        }
        if detailed:
            summary["mcp_count"] = len(item_rows)
        return summary

    def _telemetry_stats_by_slug(self) -> dict[str, dict[str, Any]]:
        window_start = (datetime.now(timezone.utc) - timedelta(days=7)).isoformat(timespec="seconds")
        with self.connect() as conn:
            rows = conn.execute(
                """
                SELECT
                    mcp_slug,
                    COUNT(*) AS run_count,
                    SUM(CASE WHEN success = 1 THEN 1 ELSE 0 END) AS success_count,
                    SUM(CASE WHEN success = 0 THEN 1 ELSE 0 END) AS error_count,
                    AVG(NULLIF(latency_ms, 0)) AS avg_latency_ms,
                    COUNT(DISTINCT instance_hash) AS active_instances
                FROM telemetry_events
                WHERE created_at >= ?
                GROUP BY mcp_slug
                """,
                (window_start,),
            ).fetchall()
        stats: dict[str, dict[str, Any]] = {}
        for row in rows:
            run_count = int(row["run_count"] or 0)
            success_count = int(row["success_count"] or 0)
            stats[row["mcp_slug"]] = {
                "run_count": run_count,
                "success_count": success_count,
                "error_count": int(row["error_count"] or 0),
                "success_rate": (success_count / run_count) if run_count else 0.0,
                "avg_latency_ms": float(row["avg_latency_ms"] or 0.0),
                "active_instances": int(row["active_instances"] or 0),
            }
        return stats
