"""Database-backed store for the community hub."""

from __future__ import annotations

import hashlib
import json
import math
import re
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from itertools import combinations
from pathlib import Path
from time import monotonic
from typing import Any

from sqlalchemy import (
    Boolean,
    Column,
    Float,
    Index,
    Integer,
    MetaData,
    String,
    Table,
    Text,
    UniqueConstraint,
    and_,
    case,
    create_engine,
    delete,
    func,
    inspect,
    or_,
    select,
    update,
)
from sqlalchemy.engine import Engine


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


metadata = MetaData()

mcp_servers = Table(
    "mcp_servers",
    metadata,
    Column("slug", String(255), primary_key=True),
    Column("name", String(255), nullable=False),
    Column("repo_url", Text, nullable=False),
    Column("description", Text, nullable=False),
    Column("category", String(120), nullable=False),
    Column("language", String(120), nullable=False),
    Column("tags_json", Text, nullable=False),
    Column("install_method", String(120), nullable=False),
    Column("verified", Boolean, nullable=False, default=False),
    Column("status", String(120), nullable=False),
    Column("active_instances", Integer, nullable=False, default=0),
    Column("installs", Integer, nullable=False, default=0),
    Column("success_rate", Float, nullable=False, default=0.0),
    Column("avg_latency_ms", Float, nullable=False, default=0.0),
    Column("created_at", String(40), nullable=False),
    Column("updated_at", String(40), nullable=False),
)

mcp_tools = Table(
    "mcp_tools",
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("mcp_slug", String(255), nullable=False),
    Column("tool_name", String(255), nullable=False),
    UniqueConstraint("mcp_slug", "tool_name", name="uq_mcp_tools_slug_name"),
)

mcp_known_issues = Table(
    "mcp_known_issues",
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("mcp_slug", String(255), nullable=False),
    Column("issue_text", Text, nullable=False),
)

mcp_recommended_configs = Table(
    "mcp_recommended_configs",
    metadata,
    Column("mcp_slug", String(255), primary_key=True),
    Column("transport", String(120), nullable=False),
    Column("timeout", Integer, nullable=False),
    Column("retries", Integer, nullable=False),
    Column("confidence_score", Float, nullable=False),
    Column("based_on_instances", Integer, nullable=False),
    Column("updated_at", String(40), nullable=False),
)

telemetry_events = Table(
    "telemetry_events",
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("mcp_slug", String(255), nullable=False),
    Column("version", String(120), nullable=False, default=""),
    Column("success", Boolean, nullable=False),
    Column("error_code", String(255), nullable=False, default=""),
    Column("latency_ms", Integer, nullable=False, default=0),
    Column("transport", String(120), nullable=False, default=""),
    Column("timeout_bucket", String(120), nullable=False, default=""),
    Column("retries", Integer, nullable=False, default=0),
    Column("instance_hash", String(255), nullable=False),
    Column("nanobot_version", String(120), nullable=False, default=""),
    Column("created_at", String(40), nullable=False),
)

hub_admin_users = Table(
    "hub_admin_users",
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("username", String(255), nullable=False, unique=True),
    Column("email", String(255), nullable=False, unique=True),
    Column("password_hash", Text, nullable=False),
    Column("created_at", String(40), nullable=False),
)

hub_runtime_settings = Table(
    "hub_runtime_settings",
    metadata,
    Column("key", String(255), primary_key=True),
    Column("value_json", Text, nullable=False),
    Column("updated_at", String(40), nullable=False),
)

mcp_stacks = Table(
    "mcp_stacks",
    metadata,
    Column("slug", String(255), primary_key=True),
    Column("title", String(255), nullable=False),
    Column("description", Text, nullable=False),
    Column("use_case", Text, nullable=False),
    Column("recommended_model", String(255), nullable=False),
    Column("example_prompt", Text, nullable=False),
    Column("rating", Float, nullable=False, default=0.0),
    Column("imports_count", Integer, nullable=False, default=0),
    Column("status", String(120), nullable=False, default="published"),
    Column("is_public", Boolean, nullable=False, default=True),
    Column("created_by", String(255), nullable=False, default=""),
    Column("created_at", String(40), nullable=False),
    Column("updated_at", String(40), nullable=False, default=""),
)

mcp_stack_items = Table(
    "mcp_stack_items",
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("stack_slug", String(255), nullable=False),
    Column("mcp_slug", String(255), nullable=False),
    Column("required", Boolean, nullable=False, default=True),
    Column("sort_order", Integer, nullable=False, default=0),
)

showcase_entries = Table(
    "showcase_entries",
    metadata,
    Column("slug", String(255), primary_key=True),
    Column("title", String(255), nullable=False),
    Column("description", Text, nullable=False),
    Column("category", String(120), nullable=False),
    Column("use_case", Text, nullable=False, default=""),
    Column("example_prompt", Text, nullable=False),
    Column("stack_slug", String(255), nullable=False),
    Column("imports_count", Integer, nullable=False, default=0),
    Column("upvotes_count", Integer, nullable=False, default=0),
    Column("status", String(120), nullable=False, default="published"),
    Column("is_public", Boolean, nullable=False, default=True),
    Column("created_by", String(255), nullable=False, default=""),
    Column("created_at", String(40), nullable=False),
    Column("updated_at", String(40), nullable=False, default=""),
)

mcp_submissions = Table(
    "mcp_submissions",
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("proposed_slug", String(255), nullable=False, default=""),
    Column("published_slug", String(255), nullable=False, default=""),
    Column("repo_url", Text, nullable=False),
    Column("name", String(255), nullable=False),
    Column("submitted_by", String(255), nullable=False, default=""),
    Column("source_instance", String(255), nullable=False, default=""),
    Column("source_public_url", Text, nullable=False, default=""),
    Column("status", String(120), nullable=False),
    Column("details_json", Text, nullable=False, default="{}"),
    Column("submitted_at", String(40), nullable=False),
)

Index("ix_mcp_servers_status", mcp_servers.c.status)
Index("ix_mcp_servers_category", mcp_servers.c.category)
Index("ix_mcp_servers_language", mcp_servers.c.language)
Index("ix_mcp_servers_install_method", mcp_servers.c.install_method)
Index("ix_mcp_servers_repo_url", mcp_servers.c.repo_url)
Index("ix_mcp_tools_mcp_slug", mcp_tools.c.mcp_slug)
Index("ix_mcp_known_issues_mcp_slug", mcp_known_issues.c.mcp_slug)
Index("ix_telemetry_events_mcp_created", telemetry_events.c.mcp_slug, telemetry_events.c.created_at)
Index("ix_telemetry_events_mcp_success", telemetry_events.c.mcp_slug, telemetry_events.c.success)
Index("ix_telemetry_events_mcp_error", telemetry_events.c.mcp_slug, telemetry_events.c.error_code)
Index("ix_telemetry_events_instance_hash", telemetry_events.c.instance_hash)
Index("ix_mcp_stack_items_stack_slug", mcp_stack_items.c.stack_slug)
Index("ix_mcp_stack_items_mcp_slug", mcp_stack_items.c.mcp_slug)
Index("ix_showcase_entries_category", showcase_entries.c.category)
Index("ix_mcp_submissions_status", mcp_submissions.c.status)


HUB_RUNTIME_DEFAULTS: dict[str, Any] = {
    "telemetry_ingest_enabled": True,
    "api_token_writes_enabled": True,
    "recommendation_mode": "balanced",
    "featured_min_trust_score": 7.5,
    "featured_min_signal_count": 3,
    "discover_cache_ttl_seconds": 20,
    "overview_cache_ttl_seconds": 30,
    "default_gui_url": "",
}


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
        "verified": True,
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
        "verified": True,
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
        "verified": True,
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
        "verified": True,
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
        "verified": True,
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
        "verified": False,
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

SEED_MCP_INSTALL_BASELINES: dict[str, int] = {
    str(entry["slug"]): int(entry.get("installs", 0) or 0) for entry in SEED_MCPS
}
SEED_STACK_IMPORT_BASELINES: dict[str, int] = {
    str(entry["slug"]): int(entry.get("imports_count", 0) or 0) for entry in SEED_STACKS
}
SEED_SHOWCASE_IMPORT_BASELINES: dict[str, int] = {
    str(entry["slug"]): int(entry.get("imports_count", 0) or 0) for entry in SEED_SHOWCASE
}

_GITHUB_REPO_RE = re.compile(r"^https://github\.com/(?P<owner>[^/]+)/(?P<repo>[^/]+)$", re.IGNORECASE)
_SECRET_RE = re.compile(
    r"(sk-[A-Za-z0-9_\-]{12,}|ghp_[A-Za-z0-9]{12,}|github_pat_[A-Za-z0-9_]+|fc-[A-Za-z0-9]{12,}|AIza[0-9A-Za-z\-_]{20,}|Bearer\s+[A-Za-z0-9._\-]+)",
    re.IGNORECASE,
)


@dataclass(slots=True)
class HubStore:
    database_url: str
    engine: Engine = field(init=False, repr=False)
    backend: str = field(init=False)
    _cache: dict[str, tuple[float, Any]] = field(init=False, repr=False, default_factory=dict)

    def __post_init__(self) -> None:
        database_url = str(self.database_url or "").strip()
        if not database_url:
            raise ValueError("database_url is required.")
        self.backend = "postgresql" if database_url.startswith("postgresql") else "sqlite"
        if self.backend == "sqlite":
            self._ensure_sqlite_parent(database_url)
        self.engine = create_engine(
            database_url,
            future=True,
            pool_pre_ping=True,
            connect_args={"check_same_thread": False} if self.backend == "sqlite" else {},
        )

    def init(self) -> None:
        metadata.create_all(self.engine)
        self._ensure_schema_extras()
        with self.engine.begin() as conn:
            count = conn.execute(select(func.count()).select_from(mcp_servers)).scalar_one()
            if int(count or 0) == 0:
                self._seed(conn)

    def _cache_get(self, key: str) -> Any | None:
        entry = self._cache.get(key)
        if entry is None:
            return None
        expires_at, value = entry
        if monotonic() >= expires_at:
            self._cache.pop(key, None)
            return None
        return value

    def _cache_set(self, key: str, value: Any, ttl_seconds: int) -> Any:
        ttl = max(1, int(ttl_seconds or 1))
        self._cache[key] = (monotonic() + ttl, value)
        return value

    def _invalidate_cache(self, *prefixes: str) -> None:
        if not prefixes:
            self._cache.clear()
            return
        for key in list(self._cache.keys()):
            if any(key.startswith(prefix) for prefix in prefixes):
                self._cache.pop(key, None)

    def get_runtime_settings(self) -> dict[str, Any]:
        cached = self._cache_get("runtime_settings")
        if isinstance(cached, dict):
            return dict(cached)
        values = dict(HUB_RUNTIME_DEFAULTS)
        with self.engine.connect() as conn:
            rows = conn.execute(select(hub_runtime_settings)).mappings().all()
        for row in rows:
            key = str(row["key"]).strip()
            if key not in HUB_RUNTIME_DEFAULTS:
                continue
            try:
                parsed = json.loads(str(row["value_json"]))
            except json.JSONDecodeError:
                continue
            values[key] = parsed
        normalized = self._normalize_runtime_settings(values)
        self._cache_set("runtime_settings", dict(normalized), 15)
        return normalized

    def update_runtime_settings(self, payload: dict[str, Any]) -> dict[str, Any]:
        current = self.get_runtime_settings()
        merged = {**current, **payload}
        normalized = self._normalize_runtime_settings(merged)
        now = _utc_now()
        with self.engine.begin() as conn:
            for key, value in normalized.items():
                existing = conn.execute(
                    select(hub_runtime_settings.c.key).where(hub_runtime_settings.c.key == key)
                ).scalar_one_or_none()
                serialized = json.dumps(value)
                if existing is None:
                    conn.execute(
                        hub_runtime_settings.insert().values(
                            key=key,
                            value_json=serialized,
                            updated_at=now,
                        )
                    )
                else:
                    conn.execute(
                        update(hub_runtime_settings)
                        .where(hub_runtime_settings.c.key == key)
                        .values(value_json=serialized, updated_at=now)
                    )
        self._invalidate_cache("runtime_settings", "marketplace:", "overview:")
        return normalized

    def _ensure_schema_extras(self) -> None:
        inspector = inspect(self.engine)
        columns_by_table = {
            table_name: {column["name"] for column in inspector.get_columns(table_name)}
            for table_name in ("mcp_stacks", "showcase_entries")
        }
        statements: list[str] = []

        stack_columns = columns_by_table["mcp_stacks"]
        if "status" not in stack_columns:
            statements.append(self._alter_add_column("mcp_stacks", "status", "VARCHAR(120) NOT NULL DEFAULT 'published'"))
        if "is_public" not in stack_columns:
            statements.append(self._alter_add_column("mcp_stacks", "is_public", self._boolean_default_sql(True)))
        if "created_by" not in stack_columns:
            statements.append(self._alter_add_column("mcp_stacks", "created_by", "VARCHAR(255) NOT NULL DEFAULT ''"))
        if "updated_at" not in stack_columns:
            statements.append(self._alter_add_column("mcp_stacks", "updated_at", "VARCHAR(40) NOT NULL DEFAULT ''"))

        showcase_columns = columns_by_table["showcase_entries"]
        if "use_case" not in showcase_columns:
            statements.append(self._alter_add_column("showcase_entries", "use_case", "TEXT NOT NULL DEFAULT ''"))
        if "status" not in showcase_columns:
            statements.append(self._alter_add_column("showcase_entries", "status", "VARCHAR(120) NOT NULL DEFAULT 'published'"))
        if "is_public" not in showcase_columns:
            statements.append(self._alter_add_column("showcase_entries", "is_public", self._boolean_default_sql(True)))
        if "created_by" not in showcase_columns:
            statements.append(self._alter_add_column("showcase_entries", "created_by", "VARCHAR(255) NOT NULL DEFAULT ''"))
        if "updated_at" not in showcase_columns:
            statements.append(self._alter_add_column("showcase_entries", "updated_at", "VARCHAR(40) NOT NULL DEFAULT ''"))

        if not statements:
            return
        with self.engine.begin() as conn:
            for statement in statements:
                conn.exec_driver_sql(statement)

    def _alter_add_column(self, table_name: str, column_name: str, column_sql: str) -> str:
        return f"ALTER TABLE {table_name} ADD COLUMN {column_name} {column_sql}"

    def _boolean_default_sql(self, value: bool) -> str:
        if self.backend == "postgresql":
            return f"BOOLEAN NOT NULL DEFAULT {'TRUE' if value else 'FALSE'}"
        return f"BOOLEAN NOT NULL DEFAULT {1 if value else 0}"

    def _seed(self, conn) -> None:
        now = _utc_now()
        for entry in SEED_MCPS:
            conn.execute(
                mcp_servers.insert().values(
                    slug=entry["slug"],
                    name=entry["name"],
                    repo_url=entry["repo_url"],
                    description=entry["description"],
                    category=entry["category"],
                    language=entry["language"],
                    tags_json=json.dumps(entry["tags"]),
                    install_method=entry["install_method"],
                    verified=bool(entry["verified"]),
                    status=entry["status"],
                    active_instances=int(entry["active_instances"]),
                    installs=int(entry["installs"]),
                    success_rate=float(entry["success_rate"]),
                    avg_latency_ms=float(entry["avg_latency_ms"]),
                    created_at=now,
                    updated_at=now,
                )
            )
            for tool_name in entry["tools"]:
                conn.execute(mcp_tools.insert().values(mcp_slug=entry["slug"], tool_name=tool_name))
            for issue_text in entry["known_issues"]:
                conn.execute(mcp_known_issues.insert().values(mcp_slug=entry["slug"], issue_text=issue_text))
            recommendation = entry["recommended_config"]
            conn.execute(
                mcp_recommended_configs.insert().values(
                    mcp_slug=entry["slug"],
                    transport=recommendation["transport"],
                    timeout=int(recommendation["timeout"]),
                    retries=int(recommendation["retries"]),
                    confidence_score=float(recommendation["confidence_score"]),
                    based_on_instances=int(recommendation["based_on_instances"]),
                    updated_at=now,
                )
            )

        for stack in SEED_STACKS:
            conn.execute(
                mcp_stacks.insert().values(
                    slug=stack["slug"],
                    title=stack["title"],
                    description=stack["description"],
                    use_case=stack["use_case"],
                    recommended_model=stack["recommended_model"],
                    example_prompt=stack["example_prompt"],
                    rating=float(stack["rating"]),
                    imports_count=int(stack["imports_count"]),
                    status="published",
                    is_public=True,
                    created_by="seed",
                    created_at=now,
                    updated_at=now,
                )
            )
            for index, item_slug in enumerate(stack["items"]):
                conn.execute(
                    mcp_stack_items.insert().values(
                        stack_slug=stack["slug"],
                        mcp_slug=item_slug,
                        required=True,
                        sort_order=index,
                    )
                )

        for showcase in SEED_SHOWCASE:
            conn.execute(
                showcase_entries.insert().values(
                    slug=showcase["slug"],
                    title=showcase["title"],
                    description=showcase["description"],
                    category=showcase["category"],
                    use_case=showcase["description"],
                    example_prompt=showcase["example_prompt"],
                    stack_slug=showcase["stack_slug"],
                    imports_count=int(showcase["imports_count"]),
                    upvotes_count=int(showcase["upvotes_count"]),
                    status="published",
                    is_public=True,
                    created_by="seed",
                    created_at=now,
                    updated_at=now,
                )
            )

    def list_mcps(
        self,
        *,
        search: str = "",
        category: str = "",
        language: str = "",
        runtime: str = "",
        min_reliability: int = 0,
        sort: str = "trending",
        include_private: bool = False,
    ) -> list[dict[str, Any]]:
        runtime_settings = self.get_runtime_settings()
        cache_key = (
            "marketplace:"
            f"{search.strip().lower()}|{category.strip()}|{language.strip()}|{runtime.strip()}|{int(min_reliability or 0)}|{sort.strip().lower()}|{int(include_private)}"
        )
        cached = self._cache_get(cache_key)
        if isinstance(cached, list):
            return [dict(item) for item in cached]
        telemetry = self._telemetry_stats_by_slug()
        minimum_percent = max(0, min(100, int(min_reliability or 0)))
        with self.engine.connect() as conn:
            stmt = select(mcp_servers)
            conditions = []
            if not include_private:
                conditions.append(mcp_servers.c.status != "rejected")
            if search:
                query = f"%{search.lower()}%"
                conditions.append(
                    or_(
                        func.lower(mcp_servers.c.name).like(query),
                        func.lower(mcp_servers.c.description).like(query),
                        func.lower(mcp_servers.c.repo_url).like(query),
                    )
                )
            if category:
                conditions.append(mcp_servers.c.category == category)
            if language:
                conditions.append(mcp_servers.c.language == language)
            if conditions:
                stmt = stmt.where(and_(*conditions))
            rows = conn.execute(stmt).mappings().all()
            if runtime.strip():
                rows = [
                    row
                    for row in rows
                    if self._matches_runtime_engine(
                        install_method=str(row["install_method"]),
                        language=str(row["language"]),
                        runtime=runtime,
                    )
                ]
            slugs = [str(row["slug"]) for row in rows]
            tools_map = self._prefetch_tools_map(conn, slugs)
            recommendation_map = self._prefetch_recommendation_map(
                conn,
                slugs,
                telemetry,
                runtime_settings=runtime_settings,
            )
            error_cluster_map = self._build_error_clusters_for_slugs(
                conn,
                [slug for slug in slugs if int(telemetry.get(slug, {}).get("error_count", 0) or 0) > 0],
            )
            items = []
            for row in rows:
                slug = str(row["slug"])
                item = self._build_mcp_summary(
                    conn,
                    row,
                    telemetry.get(slug, {}),
                    prefetched_tools=tools_map,
                    prefetched_recommendations=recommendation_map,
                    runtime_settings=runtime_settings,
                )
                if int(item.get("recent_errors", 0) or 0) > 0:
                    item["error_clusters"] = error_cluster_map.get(slug, [])
                    item["known_fixes"] = self._build_known_fix_summaries(item)
                else:
                    item["error_clusters"] = []
                    item["known_fixes"] = []
                items.append(item)
            if minimum_percent > 0:
                items = [
                    item
                    for item in items
                    if int(
                        (
                            item.get("reliability", {}).get("percent", 0)
                            if item.get("has_live_telemetry")
                            else item.get("catalog_reliability_percent", 0)
                        )
                        or 0
                    )
                    >= minimum_percent
                ]

        sort_key = str(sort or "trending").lower()
        if sort_key == "new":
            items.sort(
                key=lambda item: (
                    item["created_at"],
                    float(item.get("trust_score", {}).get("score", 0.0) or 0.0),
                ),
                reverse=True,
            )
        elif sort_key == "reliable":
            items.sort(
                key=lambda item: (
                    (
                        float(item.get("trust_score", {}).get("score", 0.0) or 0.0)
                        * (float(item.get("install_confidence", {}).get("score", 0.0) or 0.0) / 10.0)
                    ),
                    float(item.get("trust_score", {}).get("score", 0.0) or 0.0),
                    float(item.get("install_confidence", {}).get("score", 0.0) or 0.0),
                    int(
                        (
                            item.get("reliability", {}).get("percent", 0)
                            if item.get("has_live_telemetry")
                            else item.get("catalog_reliability_percent", 0)
                        )
                        or 0
                    ),
                    int(item.get("active_instances", 0) or 0),
                ),
                reverse=True,
            )
        elif sort_key == "installed":
            items.sort(
                key=lambda item: (
                    int(item.get("installs", 0) or 0),
                    float(item.get("trust_score", {}).get("score", 0.0) or 0.0),
                    int(item.get("active_instances", 0) or 0),
                ),
                reverse=True,
            )
        else:
            items.sort(
                key=lambda item: (
                    int(item.get("usage_trend", {}).get("runs_24h", 0) or 0),
                    float(item.get("trust_score", {}).get("score", 0.0) or 0.0),
                    int(item.get("active_instances", 0) or 0),
                    int(item.get("installs", 0) or 0),
                ),
                reverse=True,
            )
        return self._cache_set(cache_key, items, int(runtime_settings.get("discover_cache_ttl_seconds", 20) or 20))

    def get_mcp(self, slug: str, *, include_private: bool = False) -> dict[str, Any] | None:
        runtime_settings = self.get_runtime_settings()
        telemetry = self._telemetry_stats_by_slug().get(slug, {})
        with self.engine.connect() as conn:
            stmt = select(mcp_servers).where(mcp_servers.c.slug == slug)
            if not include_private:
                stmt = stmt.where(mcp_servers.c.status != "rejected")
            row = conn.execute(stmt).mappings().first()
            if row is None:
                return None
            recommendation_map = self._prefetch_recommendation_map(
                conn,
                [slug],
                {slug: telemetry},
                runtime_settings=runtime_settings,
            )
            item = self._build_mcp_summary(
                conn,
                row,
                telemetry,
                detailed=True,
                prefetched_tools=self._prefetch_tools_map(conn, [slug]),
                prefetched_recommendations=recommendation_map,
                runtime_settings=runtime_settings,
            )
            item["recommended_config"] = recommendation_map.get(slug)
            issue_rows = conn.execute(
                select(mcp_known_issues.c.issue_text)
                .where(mcp_known_issues.c.mcp_slug == slug)
                .order_by(mcp_known_issues.c.id.asc())
            ).all()
            item["known_issues"] = [str(issue[0]) for issue in issue_rows]
            item["error_clusters"] = self._build_error_clusters(conn, slug)
            item["known_fixes"] = self._build_known_fix_summaries(item)
            item["common_combinations"] = self._build_common_combinations(conn, slug)
            return item

    def get_mcp_fix_suggestions(
        self,
        slug: str,
        *,
        error_code: str = "",
        current_transport: str = "",
        current_timeout: int = 0,
        missing_runtimes: list[str] | None = None,
    ) -> list[dict[str, Any]]:
        item = self.get_mcp(slug, include_private=True)
        if item is None:
            raise ValueError("MCP server not found.")

        normalized_error = str(error_code or "").strip().lower()
        observed_transport = str(current_transport or "").strip()
        observed_timeout = max(0, int(current_timeout or 0))
        missing = {
            str(name).strip().lower()
            for name in (missing_runtimes or [])
            if str(name).strip()
        }
        fixes: list[dict[str, Any]] = []
        recommendation = item.get("recommended_config") if isinstance(item.get("recommended_config"), dict) else {}
        dependencies = [str(entry).strip() for entry in item.get("dependencies", []) if str(entry).strip()]
        known_issues = [str(entry).strip() for entry in item.get("known_issues", []) if str(entry).strip()]
        error_clusters = item.get("error_clusters") if isinstance(item.get("error_clusters"), list) else []

        if recommendation:
            config_changes: dict[str, Any] = {}
            summary_parts: list[str] = []
            recommended_transport = str(recommendation.get("transport", "")).strip()
            recommended_timeout = int(recommendation.get("timeout", 0) or 0)
            recommended_retries = int(recommendation.get("retries", 0) or 0)
            if recommended_transport and recommended_transport != observed_transport:
                config_changes["transport"] = recommended_transport
                summary_parts.append(f"switch transport to {recommended_transport}")
            if recommended_timeout > 0 and recommended_timeout != observed_timeout:
                config_changes["tool_timeout"] = recommended_timeout
                summary_parts.append(f"increase timeout to {recommended_timeout}s")
            if config_changes:
                fixes.append(
                    {
                        "id": "apply-recommended-config",
                        "title": "Apply the community-recommended config",
                        "summary": ", then ".join(summary_parts).capitalize() + ".",
                        "action_type": "apply_recommended_config",
                        "config_changes": config_changes,
                        "recommended_config": {
                            "transport": recommended_transport,
                            "timeout": recommended_timeout,
                            "retries": recommended_retries,
                        },
                        "confidence_score": float(recommendation.get("confidence_score", 0.0) or 0.0),
                        "based_on_instances": int(recommendation.get("based_on_instances", 0) or 0),
                    }
                )

        dependency_text = " ".join(dependencies).lower()
        if ("node" in missing or "npx" in missing or "npm" in missing) and "node.js" in dependency_text:
            fixes.append(
                {
                    "id": "repair-install-node",
                    "title": "Install the Node.js runtime",
                    "summary": "This MCP depends on Node.js and currently reports a missing runtime.",
                    "action_type": "repair_recipe",
                    "repair_recipe": "install_node",
                    "confidence_score": 0.86,
                    "based_on_instances": 0,
                }
            )
        if ("python" in missing or "uv" in missing or "pip" in missing) and "python" in dependency_text:
            fixes.append(
                {
                    "id": "repair-install-uv",
                    "title": "Install the Python runtime helpers",
                    "summary": "This MCP depends on Python tooling and currently reports a missing runtime.",
                    "action_type": "repair_recipe",
                    "repair_recipe": "install_uv",
                    "confidence_score": 0.82,
                    "based_on_instances": 0,
                }
            )

        if normalized_error == "timeout" and recommendation:
            recommended_timeout = int(recommendation.get("timeout", 0) or 0)
            if recommended_timeout and recommended_timeout > observed_timeout:
                fixes.append(
                    {
                        "id": "timeout-community-default",
                        "title": f"Increase timeout to {recommended_timeout} seconds",
                        "summary": "Community telemetry shows this MCP is more reliable with a longer timeout.",
                        "action_type": "apply_recommended_config",
                        "config_changes": {"tool_timeout": recommended_timeout},
                        "recommended_config": {
                            "transport": str(recommendation.get("transport", "")).strip(),
                            "timeout": recommended_timeout,
                            "retries": int(recommendation.get("retries", 0) or 0),
                        },
                        "confidence_score": float(recommendation.get("confidence_score", 0.0) or 0.0),
                        "based_on_instances": int(recommendation.get("based_on_instances", 0) or 0),
                    }
                )

        if not fixes and known_issues:
            issue_hint = known_issues[0]
            fixes.append(
                {
                    "id": "review-known-issues",
                    "title": "Review the known community issue",
                    "summary": issue_hint,
                    "action_type": "manual_review",
                    "config_changes": {},
                    "confidence_score": 0.5,
                    "based_on_instances": 0,
                }
            )

        for cluster in error_clusters:
            if not isinstance(cluster, dict):
                continue
            code = str(cluster.get("error_code", "")).strip()
            if code == normalized_error and code == "timeout" and recommendation:
                recommended_timeout = int(recommendation.get("timeout", 0) or 0)
                if recommended_timeout and recommended_timeout > observed_timeout:
                    fixes.append(
                        {
                            "id": "cluster-timeout-fix",
                            "title": f"Match the community timeout profile ({recommended_timeout}s)",
                            "summary": str(cluster.get("summary", "")).strip()
                            or "Timeout errors are commonly resolved by using the community timeout profile.",
                            "action_type": "apply_recommended_config",
                            "config_changes": {"tool_timeout": recommended_timeout},
                            "recommended_config": {
                                "transport": str(recommendation.get("transport", "")).strip(),
                                "timeout": recommended_timeout,
                                "retries": int(recommendation.get("retries", 0) or 0),
                            },
                            "confidence_score": float(cluster.get("confidence_score", 0.0) or 0.0),
                            "based_on_instances": int(cluster.get("event_count", 0) or 0),
                        }
                    )

        deduped: list[dict[str, Any]] = []
        seen_ids: set[str] = set()
        for item_fix in fixes:
            fix_id = str(item_fix.get("id", "")).strip()
            if not fix_id or fix_id in seen_ids:
                continue
            seen_ids.add(fix_id)
            deduped.append(item_fix)
        return deduped

    def resolve_repo(self, repo_url: str) -> dict[str, Any] | None:
        normalized = _normalize_repo_url(repo_url)
        if not normalized:
            return None
        with self.engine.connect() as conn:
            row = conn.execute(
                select(mcp_servers.c.slug, mcp_servers.c.name, mcp_servers.c.repo_url).where(
                    mcp_servers.c.repo_url == normalized
                )
            ).mappings().first()
            if row is None:
                return None
            recommendation = conn.execute(
                select(
                    mcp_recommended_configs.c.transport,
                    mcp_recommended_configs.c.timeout,
                    mcp_recommended_configs.c.retries,
                    mcp_recommended_configs.c.confidence_score,
                    mcp_recommended_configs.c.based_on_instances,
                ).where(mcp_recommended_configs.c.mcp_slug == str(row["slug"]))
            ).mappings().first()
            return {
                "slug": row["slug"],
                "name": row["name"],
                "repo_url": row["repo_url"],
                "recommended_config": dict(recommendation) if recommendation else None,
            }

    def list_stacks(self, *, search: str = "", include_private: bool = False) -> list[dict[str, Any]]:
        with self.engine.connect() as conn:
            stmt = select(mcp_stacks)
            conditions = []
            if not include_private:
                conditions.append(mcp_stacks.c.is_public.is_(True))
            if search:
                query = f"%{search.lower()}%"
                conditions.append(
                    or_(
                        func.lower(mcp_stacks.c.title).like(query),
                        func.lower(mcp_stacks.c.description).like(query),
                    )
                )
            if conditions:
                stmt = stmt.where(and_(*conditions))
            rows = conn.execute(stmt).mappings().all()
            items = [self._build_stack_summary(conn, row) for row in rows]
            items.sort(
                key=lambda item: (
                    float(item.get("rating", 0.0) or 0.0),
                    int(item.get("imports_count", 0) or 0),
                    str(item.get("title", "")),
                ),
                reverse=True,
            )
            return items

    def get_stack(self, slug: str, *, include_private: bool = False) -> dict[str, Any] | None:
        with self.engine.connect() as conn:
            stmt = select(mcp_stacks).where(mcp_stacks.c.slug == slug)
            if not include_private:
                stmt = stmt.where(mcp_stacks.c.is_public.is_(True))
            row = conn.execute(stmt).mappings().first()
            if row is None:
                return None
            return self._build_stack_summary(conn, row, detailed=True)

    def list_showcase(
        self,
        *,
        search: str = "",
        category: str = "",
        include_private: bool = False,
    ) -> list[dict[str, Any]]:
        with self.engine.connect() as conn:
            stmt = select(showcase_entries)
            conditions = []
            if not include_private:
                conditions.append(showcase_entries.c.is_public.is_(True))
            if search:
                query = f"%{search.lower()}%"
                conditions.append(
                    or_(
                        func.lower(showcase_entries.c.title).like(query),
                        func.lower(showcase_entries.c.description).like(query),
                    )
                )
            if category:
                conditions.append(showcase_entries.c.category == category)
            if conditions:
                stmt = stmt.where(and_(*conditions))
            rows = conn.execute(stmt).mappings().all()
            items: list[dict[str, Any]] = []
            for row in rows:
                stack_stmt = select(
                    mcp_stacks.c.slug,
                    mcp_stacks.c.title,
                    mcp_stacks.c.recommended_model,
                    mcp_stacks.c.use_case,
                ).where(mcp_stacks.c.slug == row["stack_slug"])
                if not include_private:
                    stack_stmt = stack_stmt.where(mcp_stacks.c.is_public.is_(True))
                stack_row = conn.execute(stack_stmt).mappings().first()
                stack_items: list[dict[str, Any]] = []
                if stack_row is not None:
                    stack_items = conn.execute(
                        select(mcp_servers.c.slug, mcp_servers.c.name, mcp_servers.c.category)
                        .select_from(
                            mcp_stack_items.join(mcp_servers, mcp_servers.c.slug == mcp_stack_items.c.mcp_slug)
                        )
                        .where(mcp_stack_items.c.stack_slug == row["stack_slug"])
                        .order_by(mcp_stack_items.c.sort_order.asc(), mcp_stack_items.c.id.asc())
                    ).mappings().all()
                stack_categories = [str(item.get("category", "")) for item in stack_items]
                items.append(
                    {
                        **dict(row),
                        "catalog_imports_count": max(0, int(row["imports_count"] or 0)),
                        "imports_count": max(
                            0,
                            int(row["imports_count"] or 0)
                            - int(SEED_SHOWCASE_IMPORT_BASELINES.get(str(row["slug"]), 0) or 0),
                        ),
                        "stack": dict(stack_row) if stack_row else None,
                        "stack_items": [dict(item) for item in stack_items],
                        "best_for": self._build_stack_best_for_payload(stack_categories),
                        "demo_ready": bool(stack_row and stack_items and str(row.get("example_prompt", "")).strip()),
                    }
                )
            items.sort(
                key=lambda item: (
                    int(item.get("imports_count", 0) or 0),
                    int(item.get("upvotes_count", 0) or 0),
                    str(item.get("title", "")),
                ),
                reverse=True,
            )
            return items

    def list_recent_mcp_submissions(self, limit: int = 20) -> list[dict[str, Any]]:
        with self.engine.connect() as conn:
            rows = conn.execute(
                select(mcp_submissions)
                .order_by(mcp_submissions.c.submitted_at.desc(), mcp_submissions.c.id.desc())
                .limit(max(1, int(limit)))
            ).mappings().all()
            items: list[dict[str, Any]] = []
            for row in rows:
                item = dict(row)
                try:
                    item["details"] = json.loads(str(item.pop("details_json", "{}")))
                except json.JSONDecodeError:
                    item["details"] = {}
                items.append(item)
            return items

    def list_error_hotspots(self, limit: int = 6) -> list[dict[str, Any]]:
        with self.engine.connect() as conn:
            rows = conn.execute(
                select(
                    telemetry_events.c.mcp_slug,
                    telemetry_events.c.error_code,
                    func.count().label("event_count"),
                    func.count(func.distinct(telemetry_events.c.instance_hash)).label("instance_count"),
                    func.avg(func.nullif(telemetry_events.c.latency_ms, 0)).label("avg_latency_ms"),
                )
                .where(
                    and_(
                        telemetry_events.c.success.is_(False),
                        telemetry_events.c.error_code != "",
                    )
                )
                .group_by(telemetry_events.c.mcp_slug, telemetry_events.c.error_code)
                .order_by(func.count().desc(), telemetry_events.c.mcp_slug.asc(), telemetry_events.c.error_code.asc())
                .limit(max(1, int(limit)))
            ).mappings().all()
            results: list[dict[str, Any]] = []
            for row in rows:
                server = conn.execute(
                    select(mcp_servers.c.name).where(mcp_servers.c.slug == row["mcp_slug"])
                ).mappings().first()
                results.append(
                    {
                        "slug": str(row["mcp_slug"]),
                        "name": str(server["name"]) if server else str(row["mcp_slug"]),
                        "error_code": str(row["error_code"]),
                        "event_count": int(row["event_count"] or 0),
                        "instance_count": int(row["instance_count"] or 0),
                        "avg_latency_ms": round(float(row["avg_latency_ms"] or 0.0), 1),
                        "summary": self._summarize_error_cluster(
                            str(row["error_code"]),
                            event_count=int(row["event_count"] or 0),
                            instance_count=int(row["instance_count"] or 0),
                        ),
                    }
                )
            return results

    def get_overview_stats(self) -> dict[str, Any]:
        runtime_settings = self.get_runtime_settings()
        cached = self._cache_get("overview:default")
        if isinstance(cached, dict):
            return dict(cached)
        telemetry = self._telemetry_stats_by_slug()
        marketplace = self.list_mcps()
        telemetry_window_start = (datetime.now(timezone.utc) - timedelta(days=30)).isoformat(timespec="seconds")
        today_start = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
        with self.engine.connect() as conn:
            total_events_today = conn.execute(
                select(func.count()).select_from(telemetry_events).where(
                    telemetry_events.c.created_at >= today_start.isoformat(timespec="seconds")
                )
            ).scalar_one()
            total_active_instances = conn.execute(
                select(func.count(func.distinct(telemetry_events.c.instance_hash))).where(
                    telemetry_events.c.created_at >= telemetry_window_start
                )
            ).scalar_one()
            telemetry_category_rows = conn.execute(
                select(
                    mcp_servers.c.category.label("category"),
                    func.count().label("run_count"),
                    func.count(func.distinct(telemetry_events.c.instance_hash)).label("active_instances"),
                )
                .select_from(
                    telemetry_events.join(mcp_servers, telemetry_events.c.mcp_slug == mcp_servers.c.slug)
                )
                .where(telemetry_events.c.created_at >= telemetry_window_start)
                .group_by(mcp_servers.c.category)
            ).mappings().all()
            common_combinations = self._build_overview_combinations(conn)
        featured_min_trust = float(runtime_settings.get("featured_min_trust_score", 7.5) or 0.0)
        featured_marketplace = [
            item for item in marketplace if float(item.get("trust_score", {}).get("score", 0.0) or 0.0) >= featured_min_trust
        ] or list(marketplace)
        category_counts: dict[str, int] = {}
        for item in marketplace:
            category = str(item.get("category", "")).strip() or "Other"
            category_counts[category] = category_counts.get(category, 0) + 1
        telemetry_top_category = ""
        if telemetry_category_rows:
            telemetry_top_category = str(
                max(
                    telemetry_category_rows,
                    key=lambda row: (
                        int(row["run_count"] or 0),
                        int(row["active_instances"] or 0),
                        str(row["category"] or ""),
                    ),
                )["category"]
            )
        trending_mcps = sorted(
            featured_marketplace,
            key=lambda item: (
                int(item.get("usage_trend", {}).get("runs_24h", 0) or 0),
                float(item.get("trust_score", {}).get("score", 0.0) or 0.0),
                int(item.get("recent_runs", 0) or 0),
                int(item.get("active_instances", 0) or 0),
            ),
            reverse=True,
        )[:3]
        most_reliable_mcps = sorted(
            marketplace,
            key=lambda item: (
                (
                    float(item.get("trust_score", {}).get("score", 0.0) or 0.0)
                    * (float(item.get("install_confidence", {}).get("score", 0.0) or 0.0) / 10.0)
                ),
                float(item.get("trust_score", {}).get("score", 0.0) or 0.0),
                float(item.get("install_confidence", {}).get("score", 0.0) or 0.0),
                int(item.get("reliability", {}).get("percent", 0) or 0),
                int(item.get("active_instances", 0) or 0),
            ),
            reverse=True,
        )[:3]
        total_runs_30d = sum(int(item.get("run_count", 0) or 0) for item in telemetry.values())
        total_successes_30d = sum(int(item.get("success_count", 0) or 0) for item in telemetry.values())
        weighted_latency_sum = sum(
            float(item.get("avg_latency_ms", 0.0) or 0.0) * int(item.get("run_count", 0) or 0)
            for item in telemetry.values()
            if int(item.get("run_count", 0) or 0) > 0
        )
        average_success_rate = round((total_successes_30d / total_runs_30d) if total_runs_30d else 0.0, 4)
        average_latency_ms = round((weighted_latency_sum / total_runs_30d) if total_runs_30d else 0.0, 1)
        top_categories = [
            {
                "name": str(row["category"] or "Other"),
                "count": int(row["run_count"] or 0),
                "active_instances": int(row["active_instances"] or 0),
            }
            for row in sorted(
                telemetry_category_rows,
                key=lambda entry: (
                    -int(entry["run_count"] or 0),
                    -int(entry["active_instances"] or 0),
                    str(entry["category"] or ""),
                ),
            )[:4]
        ]
        fastest_growing_mcps = sorted(
            marketplace,
            key=lambda item: (
                int(item.get("usage_trend", {}).get("runs_24h", 0) or 0),
                float(item.get("trust_score", {}).get("score", 0.0) or 0.0),
                int(item.get("recent_runs", 0) or 0),
            ),
            reverse=True,
        )[:3]
        most_installed_mcps = sorted(
            marketplace,
            key=lambda item: (
                int(item.get("installs", 0) or 0),
                float(item.get("trust_score", {}).get("score", 0.0) or 0.0),
                int(item.get("active_instances", 0) or 0),
            ),
            reverse=True,
        )[:3]
        overview = {
            "registry_count": len(marketplace),
            "verified_count": sum(1 for item in marketplace if item["verified"]),
            "active_instances": int(total_active_instances or 0),
            "runs_today": int(total_events_today or 0),
            "top_category": telemetry_top_category,
            "catalog_top_category": max(category_counts.items(), key=lambda entry: (entry[1], entry[0]))[0] if category_counts else "",
            "unique_categories": len(telemetry_category_rows),
            "top_mcps": sorted(
                featured_marketplace,
                key=lambda item: (
                    float(item.get("trust_score", {}).get("score", 0.0) or 0.0),
                    int(item.get("active_instances", 0) or 0),
                    int(item.get("installs", 0) or 0),
                ),
                reverse=True,
            )[:5],
            "trending_mcps": trending_mcps,
            "most_reliable_mcps": most_reliable_mcps,
            "fastest_growing_mcps": fastest_growing_mcps,
            "most_installed_mcps": most_installed_mcps,
            "average_success_rate": average_success_rate,
            "average_latency_ms": average_latency_ms,
            "top_categories": top_categories,
            "network_health": self._build_network_health_payload(
                average_success_rate=average_success_rate,
                average_latency_ms=average_latency_ms,
                runs_today=int(total_events_today or 0),
                telemetry_active=bool(total_runs_30d),
            ),
            "common_combinations": common_combinations,
            "telemetry_active": bool(total_runs_30d),
        }
        return self._cache_set(
            "overview:default",
            overview,
            int(runtime_settings.get("overview_cache_ttl_seconds", 30) or 30),
        )

    def record_telemetry_event(self, payload: dict[str, Any]) -> dict[str, Any]:
        slug = str(payload.get("mcp_slug", "")).strip()
        if not slug:
            raise ValueError("mcp_slug is required.")
        instance_raw = str(payload.get("instance_hash", "")).strip() or "anonymous"
        instance_hash = hashlib.sha256(instance_raw.encode("utf-8")).hexdigest()[:16]
        created_at = str(payload.get("created_at", "")).strip() or _utc_now()
        with self.engine.begin() as conn:
            exists = conn.execute(
                select(mcp_servers.c.slug).where(mcp_servers.c.slug == slug)
            ).scalar_one_or_none()
            if exists is None:
                raise ValueError(f"Unknown MCP slug: {slug}")
            conn.execute(
                telemetry_events.insert().values(
                    mcp_slug=slug,
                    version=str(payload.get("version", "")),
                    success=bool(payload.get("success")),
                    error_code=str(payload.get("error_code", "")),
                    latency_ms=int(payload.get("latency_ms", 0) or 0),
                    transport=str(payload.get("transport", "")),
                    timeout_bucket=str(payload.get("timeout_bucket", "")),
                    retries=int(payload.get("retries", 0) or 0),
                    instance_hash=instance_hash,
                    nanobot_version=str(payload.get("nanobot_version", "")),
                    created_at=created_at,
                )
            )
        self._invalidate_cache("marketplace:", "overview:")
        return {
            "mcp_slug": slug,
            "instance_hash": instance_hash,
            "created_at": created_at,
        }

    def submit_mcp_submission(self, payload: dict[str, Any]) -> dict[str, Any]:
        repo_url = _normalize_repo_url(payload.get("repo_url", ""))
        match = _GITHUB_REPO_RE.match(repo_url)
        if not match:
            raise ValueError("Only GitHub repository URLs are supported for community MCP submissions.")

        proposed_slug = self._normalize_submission_slug(str(payload.get("slug", "")).strip())
        repo_name = match.group("repo")
        owner_name = match.group("owner")
        name = str(payload.get("name", "")).strip() or repo_name.replace("-", " ").replace("_", " ").title()
        description = str(payload.get("description", "")).strip() or "Community-submitted MCP server."
        category = str(payload.get("category", "")).strip() or self._infer_category(payload)
        install_method = str(payload.get("install_method", "")).strip() or self._infer_install_method(payload)
        language = str(payload.get("language", "")).strip() or self._infer_language(install_method)
        submitted_by = str(payload.get("submitted_by", "")).strip()
        source_instance = str(payload.get("source_instance", "")).strip()
        source_public_url = str(payload.get("source_public_url", "")).strip()
        tags = self._normalize_text_list(payload.get("tags", []))
        tools = self._normalize_text_list(payload.get("tools", []))
        known_issues = self._normalize_text_list(payload.get("known_issues", []))

        self._guard_submission_text(repo_url, name, description, submitted_by, source_instance, source_public_url, *tags, *tools, *known_issues)

        now = _utc_now()
        with self.engine.begin() as conn:
            existing = conn.execute(
                select(mcp_servers.c.slug).where(mcp_servers.c.repo_url == repo_url)
            ).mappings().first()
            if existing is not None:
                submission = self._insert_submission(
                    conn,
                    proposed_slug=proposed_slug,
                    published_slug=str(existing["slug"]),
                    repo_url=repo_url,
                    name=name,
                    submitted_by=submitted_by,
                    source_instance=source_instance,
                    source_public_url=source_public_url,
                    status="duplicate",
                    details={
                        "category": category,
                        "install_method": install_method,
                        "language": language,
                        "tags": tags,
                        "tools": tools,
                        "known_issues": known_issues,
                    },
                    submitted_at=now,
                )
                item = self.get_mcp(str(existing["slug"]))
                return {
                    "created": False,
                    "duplicate": True,
                    "submission": submission,
                    "item": item,
                }

            slug = self._allocate_submission_slug(conn, repo_url, proposed_slug, owner_name=owner_name, repo_name=repo_name)
            conn.execute(
                mcp_servers.insert().values(
                    slug=slug,
                    name=name,
                    repo_url=repo_url,
                    description=description,
                    category=category,
                    language=language,
                    tags_json=json.dumps(tags),
                    install_method=install_method,
                    verified=False,
                    status="submitted",
                    active_instances=0,
                    installs=0,
                    success_rate=0.0,
                    avg_latency_ms=0.0,
                    created_at=now,
                    updated_at=now,
                )
            )
            for tool_name in tools:
                conn.execute(mcp_tools.insert().values(mcp_slug=slug, tool_name=tool_name))
            for issue_text in known_issues:
                conn.execute(mcp_known_issues.insert().values(mcp_slug=slug, issue_text=issue_text))

            recommendation = payload.get("recommended_config")
            if isinstance(recommendation, dict) and recommendation:
                transport = str(recommendation.get("transport", "")).strip()
                timeout = int(recommendation.get("timeout", 0) or 0)
                retries = int(recommendation.get("retries", 0) or 0)
                confidence = float(recommendation.get("confidence_score", 0.0) or 0.0)
                based_on_instances = int(recommendation.get("based_on_instances", 0) or 0)
                if transport and timeout > 0:
                    conn.execute(
                        mcp_recommended_configs.insert().values(
                            mcp_slug=slug,
                            transport=transport,
                            timeout=timeout,
                            retries=max(0, retries),
                            confidence_score=max(0.0, confidence),
                            based_on_instances=max(0, based_on_instances),
                            updated_at=now,
                        )
                    )

            submission = self._insert_submission(
                conn,
                proposed_slug=proposed_slug,
                published_slug=slug,
                repo_url=repo_url,
                name=name,
                submitted_by=submitted_by,
                source_instance=source_instance,
                source_public_url=source_public_url,
                status="published",
                details={
                    "description": description,
                    "category": category,
                    "install_method": install_method,
                    "language": language,
                    "tags": tags,
                    "tools": tools,
                    "known_issues": known_issues,
                },
                submitted_at=now,
            )

        item = self.get_mcp(slug)
        self._invalidate_cache("marketplace:", "overview:")
        return {
            "created": True,
            "duplicate": False,
            "submission": submission,
            "item": item,
        }

    def create_stack_submission(self, payload: dict[str, Any]) -> dict[str, Any]:
        title = str(payload.get("title", "")).strip()
        description = str(payload.get("description", "")).strip()
        use_case = str(payload.get("use_case", "")).strip()
        recommended_model = str(payload.get("recommended_model", "")).strip()
        example_prompt = str(payload.get("example_prompt", "")).strip()
        created_by = str(payload.get("created_by", "")).strip()
        proposed_slug = self._normalize_submission_slug(str(payload.get("slug", "")).strip())
        is_public = bool(payload.get("is_public"))

        if not title or not description or not use_case:
            raise ValueError("Title, description, and use case are required.")

        if not recommended_model:
            raise ValueError("Recommended model is required.")

        items = self._normalize_stack_items(payload.get("items", []))
        self._guard_submission_text(title, description, use_case, recommended_model, example_prompt, created_by, *items)

        with self.engine.begin() as conn:
            slug = self._allocate_generic_slug(conn, mcp_stacks, proposed_slug or title, fallback="stack")
            item_slugs = self._resolve_stack_item_slugs(conn, items)
            if not item_slugs:
                raise ValueError("At least one valid MCP slug or GitHub repository URL is required for a stack.")

            now = _utc_now()
            conn.execute(
                mcp_stacks.insert().values(
                    slug=slug,
                    title=title,
                    description=description,
                    use_case=use_case,
                    recommended_model=recommended_model,
                    example_prompt=example_prompt or use_case,
                    rating=0.0,
                    imports_count=0,
                    status="published" if is_public else "draft",
                    is_public=is_public,
                    created_by=created_by,
                    created_at=now,
                    updated_at=now,
                )
            )
            for index, item_slug in enumerate(item_slugs):
                conn.execute(
                    mcp_stack_items.insert().values(
                        stack_slug=slug,
                        mcp_slug=item_slug,
                        required=True,
                        sort_order=index,
                    )
                )
        created = self.get_stack(slug, include_private=True)
        self._invalidate_cache("overview:")
        return {
            "created": True,
            "item": created,
        }

    def create_showcase_submission(self, payload: dict[str, Any]) -> dict[str, Any]:
        title = str(payload.get("title", "")).strip()
        description = str(payload.get("description", "")).strip()
        use_case = str(payload.get("use_case", "")).strip()
        example_prompt = str(payload.get("example_prompt", "")).strip()
        category = str(payload.get("category", "")).strip() or "Automation"
        stack_slug = str(payload.get("stack_slug", "")).strip()
        created_by = str(payload.get("created_by", "")).strip()
        proposed_slug = self._normalize_submission_slug(str(payload.get("slug", "")).strip())
        is_public = bool(payload.get("is_public"))

        if not title or not description or not use_case or not example_prompt:
            raise ValueError("Title, description, use case, and example prompt are required.")
        if not stack_slug:
            raise ValueError("A stack slug is required for showcase submissions.")

        self._guard_submission_text(title, description, use_case, example_prompt, category, created_by, stack_slug)

        with self.engine.begin() as conn:
            stack_exists = conn.execute(
                select(mcp_stacks.c.slug).where(mcp_stacks.c.slug == stack_slug)
            ).scalar_one_or_none()
            if stack_exists is None:
                raise ValueError("The referenced stack does not exist.")

            slug = self._allocate_generic_slug(conn, showcase_entries, proposed_slug or title, fallback="showcase")
            now = _utc_now()
            conn.execute(
                showcase_entries.insert().values(
                    slug=slug,
                    title=title,
                    description=description,
                    category=category,
                    use_case=use_case,
                    example_prompt=example_prompt,
                    stack_slug=stack_slug,
                    imports_count=0,
                    upvotes_count=0,
                    status="published" if is_public else "draft",
                    is_public=is_public,
                    created_by=created_by,
                    created_at=now,
                    updated_at=now,
                )
            )
        item = self.get_showcase(slug, include_private=True)
        self._invalidate_cache("overview:")
        return {"created": True, "item": item}

    def get_showcase(self, slug: str, *, include_private: bool = False) -> dict[str, Any] | None:
        with self.engine.connect() as conn:
            stmt = select(showcase_entries).where(showcase_entries.c.slug == slug)
            if not include_private:
                stmt = stmt.where(showcase_entries.c.is_public.is_(True))
            row = conn.execute(stmt).mappings().first()
            if row is None:
                return None
            stack_row = conn.execute(
                select(
                    mcp_stacks.c.slug,
                    mcp_stacks.c.title,
                    mcp_stacks.c.recommended_model,
                    mcp_stacks.c.use_case,
                ).where(mcp_stacks.c.slug == row["stack_slug"])
            ).mappings().first()
            stack_items = conn.execute(
                select(mcp_servers.c.slug, mcp_servers.c.name)
                .select_from(
                    mcp_stack_items.join(mcp_servers, mcp_servers.c.slug == mcp_stack_items.c.mcp_slug)
                )
                .where(mcp_stack_items.c.stack_slug == row["stack_slug"])
                .order_by(mcp_stack_items.c.sort_order.asc(), mcp_stack_items.c.id.asc())
            ).mappings().all()
            stack_install_methods = conn.execute(
                select(mcp_servers.c.install_method)
                .select_from(
                    mcp_stack_items.join(mcp_servers, mcp_servers.c.slug == mcp_stack_items.c.mcp_slug)
                )
                .where(mcp_stack_items.c.stack_slug == row["stack_slug"])
            ).all()
            stack_categories = [
                str(item.get("category", ""))
                for item in conn.execute(
                    select(mcp_servers.c.category)
                    .select_from(
                        mcp_stack_items.join(mcp_servers, mcp_servers.c.slug == mcp_stack_items.c.mcp_slug)
                    )
                    .where(mcp_stack_items.c.stack_slug == row["stack_slug"])
                ).mappings().all()
            ]
            return {
                **dict(row),
                "catalog_imports_count": max(0, int(row["imports_count"] or 0)),
                "imports_count": max(
                    0,
                    int(row["imports_count"] or 0)
                    - int(SEED_SHOWCASE_IMPORT_BASELINES.get(str(row["slug"]), 0) or 0),
                ),
                "stack": dict(stack_row) if stack_row else None,
                "stack_items": [dict(item) for item in stack_items],
                "best_for": self._build_stack_best_for_payload(stack_categories),
                "demo_ready": bool(stack_row and stack_items and str(row.get("example_prompt", "")).strip()),
                "stack_difficulty": self._build_stack_difficulty_payload(
                    item_count=len(stack_items),
                    install_methods=[str(item[0]) for item in stack_install_methods],
                ),
                "diagram_nodes": [
                    str(stack_row["recommended_model"]).strip() if stack_row else "",
                    *[str(item.get("name", "")) for item in stack_items if str(item.get("name", "")).strip()],
                    "Result",
                ],
            }

    def list_mcp_moderation_queue(self) -> list[dict[str, Any]]:
        telemetry = self._telemetry_stats_by_slug()
        with self.engine.connect() as conn:
            rows = conn.execute(
                select(mcp_servers)
                .where(or_(mcp_servers.c.status == "submitted", mcp_servers.c.status == "rejected", mcp_servers.c.verified.is_(False)))
                .order_by(mcp_servers.c.updated_at.desc(), mcp_servers.c.created_at.desc())
            ).mappings().all()
            return [self._build_mcp_summary(conn, row, telemetry.get(str(row["slug"]), {}), detailed=True) for row in rows]

    def list_stack_moderation_queue(self) -> list[dict[str, Any]]:
        with self.engine.connect() as conn:
            rows = conn.execute(
                select(mcp_stacks)
                .where(or_(mcp_stacks.c.is_public.is_(False), mcp_stacks.c.status != "published"))
                .order_by(mcp_stacks.c.updated_at.desc(), mcp_stacks.c.created_at.desc())
            ).mappings().all()
            return [self._build_stack_summary(conn, row, detailed=True) for row in rows]

    def list_showcase_moderation_queue(self) -> list[dict[str, Any]]:
        with self.engine.connect() as conn:
            rows = conn.execute(
                select(showcase_entries)
                .where(or_(showcase_entries.c.is_public.is_(False), showcase_entries.c.status != "published"))
                .order_by(showcase_entries.c.updated_at.desc(), showcase_entries.c.created_at.desc())
            ).mappings().all()
            items: list[dict[str, Any]] = []
            for row in rows:
                stack_row = conn.execute(
                    select(
                        mcp_stacks.c.slug,
                        mcp_stacks.c.title,
                        mcp_stacks.c.recommended_model,
                        mcp_stacks.c.use_case,
                    ).where(mcp_stacks.c.slug == row["stack_slug"])
                ).mappings().first()
                stack_items = conn.execute(
                    select(mcp_servers.c.slug, mcp_servers.c.name)
                    .select_from(
                        mcp_stack_items.join(mcp_servers, mcp_servers.c.slug == mcp_stack_items.c.mcp_slug)
                    )
                    .where(mcp_stack_items.c.stack_slug == row["stack_slug"])
                    .order_by(mcp_stack_items.c.sort_order.asc(), mcp_stack_items.c.id.asc())
                ).mappings().all()
                items.append(
                    {
                        **dict(row),
                        "stack": dict(stack_row) if stack_row else None,
                        "stack_items": [dict(item) for item in stack_items],
                    }
                )
            return items

    def moderate_mcp(self, slug: str, *, action: str) -> dict[str, Any]:
        normalized = str(action).strip().lower()
        if normalized not in {"verify", "reject"}:
            raise ValueError("Unsupported moderation action.")
        with self.engine.begin() as conn:
            row = conn.execute(select(mcp_servers).where(mcp_servers.c.slug == slug)).mappings().first()
            if row is None:
                raise ValueError("MCP server not found.")
            conn.execute(
                update(mcp_servers)
                .where(mcp_servers.c.slug == slug)
                .values(
                    verified=(normalized == "verify"),
                    status="active" if normalized == "verify" else "rejected",
                    updated_at=_utc_now(),
                )
            )
        item = self.get_mcp(slug, include_private=True)
        if item is None:
            raise ValueError("MCP server not found.")
        self._invalidate_cache("marketplace:", "overview:")
        return item

    def moderate_stack(self, slug: str, *, action: str) -> dict[str, Any]:
        normalized = str(action).strip().lower()
        if normalized not in {"publish", "hide"}:
            raise ValueError("Unsupported moderation action.")
        with self.engine.begin() as conn:
            row = conn.execute(select(mcp_stacks).where(mcp_stacks.c.slug == slug)).mappings().first()
            if row is None:
                raise ValueError("Stack not found.")
            conn.execute(
                update(mcp_stacks)
                .where(mcp_stacks.c.slug == slug)
                .values(
                    is_public=(normalized == "publish"),
                    status="published" if normalized == "publish" else "draft",
                    updated_at=_utc_now(),
                )
            )
        item = self.get_stack(slug, include_private=True)
        if item is None:
            raise ValueError("Stack not found.")
        self._invalidate_cache("overview:")
        return item

    def moderate_showcase(self, slug: str, *, action: str) -> dict[str, Any]:
        normalized = str(action).strip().lower()
        if normalized not in {"publish", "hide"}:
            raise ValueError("Unsupported moderation action.")
        with self.engine.begin() as conn:
            row = conn.execute(select(showcase_entries).where(showcase_entries.c.slug == slug)).mappings().first()
            if row is None:
                raise ValueError("Showcase entry not found.")
            conn.execute(
                update(showcase_entries)
                .where(showcase_entries.c.slug == slug)
                .values(
                    is_public=(normalized == "publish"),
                    status="published" if normalized == "publish" else "draft",
                    updated_at=_utc_now(),
                )
            )
        item = self.get_showcase(slug, include_private=True)
        if item is None:
            raise ValueError("Showcase entry not found.")
        self._invalidate_cache("overview:")
        return item

    def increment_mcp_install(self, slug: str) -> dict[str, int]:
        result = self._increment_counter(mcp_servers, slug, "installs")
        raw_count = int(result.get("installs", 0) or 0)
        result["catalog_installs"] = raw_count
        result["installs"] = max(0, raw_count - int(SEED_MCP_INSTALL_BASELINES.get(slug, 0) or 0))
        self._invalidate_cache("marketplace:", "overview:")
        return result

    def increment_stack_import(self, slug: str) -> dict[str, int]:
        result = self._increment_counter(mcp_stacks, slug, "imports_count")
        raw_count = int(result.get("imports_count", 0) or 0)
        result["catalog_imports_count"] = raw_count
        result["imports_count"] = max(0, raw_count - int(SEED_STACK_IMPORT_BASELINES.get(slug, 0) or 0))
        self._invalidate_cache("overview:")
        return result

    def increment_showcase_import(self, slug: str) -> dict[str, int]:
        result = self._increment_counter(showcase_entries, slug, "imports_count")
        raw_count = int(result.get("imports_count", 0) or 0)
        result["catalog_imports_count"] = raw_count
        result["imports_count"] = max(0, raw_count - int(SEED_SHOWCASE_IMPORT_BASELINES.get(slug, 0) or 0))
        self._invalidate_cache("overview:")
        return result

    def categories(self) -> list[str]:
        with self.engine.connect() as conn:
            rows = conn.execute(
                select(mcp_servers.c.category).distinct().order_by(mcp_servers.c.category.asc())
            ).all()
            return [str(row[0]) for row in rows]

    def languages(self) -> list[str]:
        with self.engine.connect() as conn:
            rows = conn.execute(
                select(mcp_servers.c.language).distinct().order_by(mcp_servers.c.language.asc())
            ).all()
            return [str(row[0]) for row in rows]

    @staticmethod
    def runtime_options() -> list[str]:
        return ["Node", "Python", "Docker", "Remote/API"]

    def showcase_categories(self) -> list[str]:
        with self.engine.connect() as conn:
            rows = conn.execute(
                select(showcase_entries.c.category).distinct().order_by(showcase_entries.c.category.asc())
            ).all()
            return [str(row[0]) for row in rows]

    def _build_mcp_summary(
        self,
        conn,
        row: dict[str, Any],
        telemetry: dict[str, Any],
        *,
        detailed: bool = False,
        prefetched_tools: dict[str, list[str]] | None = None,
        prefetched_recommendations: dict[str, dict[str, Any] | None] | None = None,
        runtime_settings: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        slug = str(row["slug"])
        tools = list((prefetched_tools or {}).get(slug, []))
        if prefetched_tools is None or slug not in prefetched_tools:
            tool_rows = conn.execute(
                select(mcp_tools.c.tool_name)
                .where(mcp_tools.c.mcp_slug == slug)
                .order_by(mcp_tools.c.tool_name.asc())
            ).all()
            tools = [str(tool[0]) for tool in tool_rows]
        recommendation_known = prefetched_recommendations is not None and slug in prefetched_recommendations
        recommendation = (prefetched_recommendations or {}).get(slug)
        if not recommendation_known:
            recommendation = conn.execute(
                select(mcp_recommended_configs).where(mcp_recommended_configs.c.mcp_slug == slug)
            ).mappings().first()
            recommendation = dict(recommendation) if recommendation else None
        catalog_active_instances = max(0, int(row["active_instances"] or 0))
        telemetry_active_instances = max(0, int(telemetry.get("active_instances", 0) or 0))
        telemetry_run_count = max(0, int(telemetry.get("run_count", 0) or 0))
        has_live_telemetry = telemetry_run_count > 0 or telemetry_active_instances > 0
        effective_success = float(telemetry.get("success_rate", 0.0) or 0.0) if telemetry_run_count > 0 else 0.0
        effective_latency = (
            float(telemetry.get("avg_latency_ms", 0.0) or 0.0)
            if telemetry_run_count > 0 and float(telemetry.get("avg_latency_ms", 0.0) or 0.0) > 0
            else None
        )
        catalog_installs = max(0, int(row["installs"] or 0))
        observed_installs = max(0, catalog_installs - int(SEED_MCP_INSTALL_BASELINES.get(slug, 0) or 0))
        dependencies = self._build_dependency_payload(
            install_method=str(row["install_method"]),
            language=str(row["language"]),
            status=str(row["status"]),
        )
        permission_hints = self._build_permission_hints_payload(
            install_method=str(row["install_method"]),
            language=str(row["language"]),
            status=str(row["status"]),
            tags=json.loads(str(row["tags_json"])),
            tools=tools,
        )
        install_confidence = self._build_install_confidence_payload(
            success_rate=effective_success,
            verified=bool(row["verified"]),
            recommendation=dict(recommendation) if recommendation else None,
            active_instances=telemetry_active_instances,
            installs=observed_installs,
            signal_count=telemetry_run_count,
            telemetry=telemetry,
        )
        trust_score = self._build_trust_score_payload(
            success_rate=effective_success,
            install_confidence=install_confidence,
            verified=bool(row["verified"]),
            telemetry=telemetry,
            recommendation=dict(recommendation) if recommendation else None,
            has_live_telemetry=has_live_telemetry,
        )
        summary = {
            **dict(row),
            "repo_url": _normalize_repo_url(str(row["repo_url"])),
            "tags": json.loads(str(row["tags_json"])),
            "tools": tools,
            "tool_count": len(tools),
            "has_live_telemetry": has_live_telemetry,
            "catalog_active_instances": catalog_active_instances,
            "telemetry_active_instances": telemetry_active_instances,
            "active_instances": telemetry_active_instances,
            "catalog_success_rate": round(float(row["success_rate"] or 0.0), 4),
            "catalog_reliability_percent": max(0, min(100, round(float(row["success_rate"] or 0.0) * 100))),
            "catalog_avg_latency_ms": round(float(row["avg_latency_ms"] or 0.0), 1),
            "catalog_installs": catalog_installs,
            "success_rate": round(effective_success, 4) if has_live_telemetry else None,
            "avg_latency_ms": round(float(effective_latency), 1) if effective_latency is not None else None,
            "recent_runs": telemetry_run_count,
            "recent_errors": int(telemetry.get("error_count", 0) or 0),
            "verified": bool(row["verified"]),
            "reliability": self._build_reliability_payload(
                success_rate=effective_success,
                status=str(row["status"]),
                verified=bool(row["verified"]),
                has_live_telemetry=has_live_telemetry,
            ),
            "best_for": self._build_best_for_payload(
                category=str(row["category"]),
                tags=json.loads(str(row["tags_json"])),
                tools=tools,
            ),
            "dependencies": dependencies,
            "permission_hints": permission_hints,
            "runtime_engine": self._build_runtime_engine_payload(
                install_method=str(row["install_method"]),
                language=str(row["language"]),
            ),
            "mcp_type": self._build_mcp_type_payload(
                install_method=str(row["install_method"]),
                language=str(row["language"]),
            ),
            "install_confidence": install_confidence,
            "trust_score": trust_score,
            "usage_trend": self._build_usage_trend_payload(
                slug=slug,
                telemetry=telemetry,
                recent_runs=int(telemetry.get("run_count", 0) or 0),
            ),
            "community_signals": {
                "runs_30d": telemetry_run_count,
                "instances_30d": int(telemetry.get("active_instances", 0) or 0),
                "config_consensus": round(float(telemetry.get("config_consensus", 0.0) or 0.0), 4),
                "repair_rate": round(float(telemetry.get("repair_rate", 0.0) or 0.0), 4),
                "repair_opportunities": int(telemetry.get("repair_opportunities", 0) or 0),
                "repaired_instances": int(telemetry.get("repaired_instances", 0) or 0),
                "top_config_fingerprint": str(telemetry.get("top_config_fingerprint", "") or ""),
                "top_config_share": round(float(telemetry.get("top_config_share", 0.0) or 0.0), 4),
            },
            "difficulty": self._build_mcp_difficulty_payload(
                install_method=str(row["install_method"]),
                verified=bool(row["verified"]),
                status=str(row["status"]),
            ),
            "installs": observed_installs,
        }
        if detailed:
            summary["recent_telemetry"] = telemetry
        return summary

    def _build_stack_summary(
        self,
        conn,
        row: dict[str, Any],
        *,
        detailed: bool = False,
    ) -> dict[str, Any]:
        item_rows = conn.execute(
            select(
                mcp_servers.c.slug,
                mcp_servers.c.name,
                mcp_servers.c.repo_url,
                mcp_servers.c.install_method,
                mcp_servers.c.status,
                mcp_servers.c.category,
            )
            .select_from(
                mcp_stack_items.join(mcp_servers, mcp_servers.c.slug == mcp_stack_items.c.mcp_slug)
            )
            .where(mcp_stack_items.c.stack_slug == row["slug"])
            .order_by(mcp_stack_items.c.sort_order.asc(), mcp_stack_items.c.id.asc())
        ).mappings().all()
        summary = {
            **dict(row),
            "catalog_imports_count": max(0, int(row["imports_count"] or 0)),
            "imports_count": max(
                0,
                int(row["imports_count"] or 0) - int(SEED_STACK_IMPORT_BASELINES.get(str(row["slug"]), 0) or 0),
            ),
            "items": [dict(item) for item in item_rows],
            "difficulty": self._build_stack_difficulty_payload(
                item_count=len(item_rows),
                install_methods=[str(item.get("install_method", "")) for item in item_rows],
            ),
            "best_for": self._build_stack_best_for_payload([str(item.get("category", "")) for item in item_rows]),
            "diagram": " -> ".join(str(item.get("name", "")) for item in item_rows if str(item.get("name", "")).strip()),
            "diagram_nodes": [
                str(row["recommended_model"]).strip(),
                *[str(item.get("name", "")) for item in item_rows if str(item.get("name", "")).strip()],
                "Result",
            ],
        }
        if detailed:
            summary["mcp_count"] = len(item_rows)
        return summary

    def _build_common_combinations(self, conn, slug: str) -> list[dict[str, Any]]:
        signals = self._build_combination_signal_map(conn)
        related: list[dict[str, Any]] = []
        for pair_key, signal in signals.items():
            if slug not in pair_key:
                continue
            other_slug = signal["slugs"][0] if signal["slugs"][1] == slug else signal["slugs"][1]
            other_name = signal["names"][0] if signal["slugs"][0] == other_slug else signal["names"][1]
            related.append(
                {
                    "slug": other_slug,
                    "name": other_name,
                    "stack_count": int(signal.get("stack_count", 0) or 0),
                    "telemetry_instances": int(signal.get("telemetry_instances", 0) or 0),
                    "strength_score": round(float(signal.get("strength_score", 0.0) or 0.0), 2),
                }
            )
        related.sort(
            key=lambda item: (
                float(item.get("strength_score", 0.0) or 0.0),
                int(item.get("telemetry_instances", 0) or 0),
                int(item.get("stack_count", 0) or 0),
                str(item.get("name", "")),
            ),
            reverse=True,
        )
        return related[:4]

    def _build_overview_combinations(self, conn) -> list[dict[str, Any]]:
        ranked = sorted(
            self._build_combination_signal_map(conn).values(),
            key=lambda item: (
                float(item.get("strength_score", 0.0) or 0.0),
                int(item.get("telemetry_instances", 0) or 0),
                int(item.get("stack_count", 0) or 0),
                " + ".join(item["names"]),
            ),
            reverse=True,
        )[:4]
        for item in ranked:
            item["label"] = " + ".join(item["names"])
        return ranked

    def _build_combination_signal_map(self, conn) -> dict[tuple[str, str], dict[str, Any]]:
        name_rows = conn.execute(select(mcp_servers.c.slug, mcp_servers.c.name)).all()
        name_map = {str(row[0]): str(row[1]) for row in name_rows}
        counts: dict[tuple[str, str], dict[str, Any]] = {}

        stack_rows = conn.execute(
            select(mcp_stacks.c.slug)
            .where(mcp_stacks.c.is_public.is_(True))
            .order_by(mcp_stacks.c.slug.asc())
        ).all()
        for stack_row in stack_rows:
            stack_slug = str(stack_row[0])
            items = conn.execute(
                select(mcp_servers.c.slug, mcp_servers.c.name)
                .select_from(
                    mcp_stack_items.join(mcp_servers, mcp_servers.c.slug == mcp_stack_items.c.mcp_slug)
                )
                .where(mcp_stack_items.c.stack_slug == stack_slug)
                .order_by(mcp_stack_items.c.sort_order.asc(), mcp_stack_items.c.id.asc())
            ).mappings().all()
            normalized_items = [(str(item["slug"]), str(item["name"])) for item in items]
            for first, second in combinations(normalized_items, 2):
                ordered = sorted((first, second), key=lambda entry: entry[0])
                pair_key = (ordered[0][0], ordered[1][0])
                if pair_key not in counts:
                    counts[pair_key] = {
                        "slugs": [ordered[0][0], ordered[1][0]],
                        "names": [ordered[0][1], ordered[1][1]],
                        "stack_count": 0,
                        "telemetry_instances": 0,
                    }
                counts[pair_key]["stack_count"] += 1

        since_30d = (datetime.now(timezone.utc) - timedelta(days=30)).isoformat(timespec="seconds")
        telemetry_rows = conn.execute(
            select(telemetry_events.c.instance_hash, telemetry_events.c.mcp_slug)
            .where(
                and_(
                    telemetry_events.c.created_at >= since_30d,
                    telemetry_events.c.success.is_(True),
                )
            )
            .distinct()
            .order_by(telemetry_events.c.instance_hash.asc(), telemetry_events.c.mcp_slug.asc())
        ).all()
        by_instance: dict[str, list[str]] = defaultdict(list)
        for instance_hash, mcp_slug in telemetry_rows:
            slug = str(mcp_slug)
            if slug not in by_instance[str(instance_hash)]:
                by_instance[str(instance_hash)].append(slug)
        for slugs in by_instance.values():
            if len(slugs) < 2:
                continue
            unique_slugs = sorted({slug for slug in slugs if slug})
            for first_slug, second_slug in combinations(unique_slugs, 2):
                ordered = tuple(sorted((first_slug, second_slug)))
                if ordered not in counts:
                    counts[ordered] = {
                        "slugs": [ordered[0], ordered[1]],
                        "names": [name_map.get(ordered[0], ordered[0]), name_map.get(ordered[1], ordered[1])],
                        "stack_count": 0,
                        "telemetry_instances": 0,
                    }
                counts[ordered]["telemetry_instances"] += 1

        for item in counts.values():
            stack_signal = min(int(item.get("stack_count", 0) or 0) / 5.0, 1.0)
            telemetry_signal = min(int(item.get("telemetry_instances", 0) or 0) / 20.0, 1.0)
            item["strength_score"] = round(((0.6 * stack_signal) + (0.4 * telemetry_signal)) * 10.0, 1)
        return counts

    def _telemetry_stats_by_slug(self) -> dict[str, dict[str, Any]]:
        window_start = (datetime.now(timezone.utc) - timedelta(days=30)).isoformat(timespec="seconds")
        since_24h = (datetime.now(timezone.utc) - timedelta(days=1)).isoformat(timespec="seconds")
        since_7d = (datetime.now(timezone.utc) - timedelta(days=7)).isoformat(timespec="seconds")
        with self.engine.connect() as conn:
            rows = conn.execute(
                select(
                    telemetry_events.c.mcp_slug.label("mcp_slug"),
                    func.count().label("run_count"),
                    func.sum(case((telemetry_events.c.success.is_(True), 1), else_=0)).label("success_count"),
                    func.sum(case((telemetry_events.c.success.is_(False), 1), else_=0)).label("error_count"),
                    func.avg(func.nullif(telemetry_events.c.latency_ms, 0)).label("avg_latency_ms"),
                    func.count(func.distinct(telemetry_events.c.instance_hash)).label("active_instances"),
                    func.sum(case((telemetry_events.c.created_at >= since_24h, 1), else_=0)).label("runs_24h"),
                    func.sum(case((telemetry_events.c.created_at >= since_7d, 1), else_=0)).label("runs_7d"),
                )
                .where(telemetry_events.c.created_at >= window_start)
                .group_by(telemetry_events.c.mcp_slug)
            ).mappings().all()
            event_rows = conn.execute(
                select(
                    telemetry_events.c.mcp_slug,
                    telemetry_events.c.instance_hash,
                    telemetry_events.c.success,
                    telemetry_events.c.created_at,
                    telemetry_events.c.transport,
                    telemetry_events.c.timeout_bucket,
                    telemetry_events.c.retries,
                )
                .where(telemetry_events.c.created_at >= window_start)
                .order_by(
                    telemetry_events.c.mcp_slug.asc(),
                    telemetry_events.c.instance_hash.asc(),
                    telemetry_events.c.created_at.asc(),
                    telemetry_events.c.id.asc(),
                )
            ).mappings().all()
        stats: dict[str, dict[str, Any]] = {}
        for row in rows:
            run_count = int(row["run_count"] or 0)
            success_count = int(row["success_count"] or 0)
            stats[str(row["mcp_slug"])] = {
                "run_count": run_count,
                "success_count": success_count,
                "error_count": int(row["error_count"] or 0),
                "success_rate": (success_count / run_count) if run_count else 0.0,
                "avg_latency_ms": float(row["avg_latency_ms"] or 0.0),
                "active_instances": int(row["active_instances"] or 0),
                "runs_24h": int(row["runs_24h"] or 0),
                "runs_7d": int(row["runs_7d"] or 0),
                "config_consensus": 0.0,
                "repair_rate": 0.0,
                "repair_opportunities": 0,
                "repaired_instances": 0,
                "top_config_fingerprint": "",
                "top_config_share": 0.0,
            }
        successful_fingerprints: dict[str, dict[str, int]] = defaultdict(lambda: defaultdict(int))
        instance_streams: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
        for row in event_rows:
            slug = str(row["mcp_slug"])
            instance_key = str(row["instance_hash"])
            fingerprint = self._build_config_fingerprint(
                transport=str(row["transport"] or ""),
                timeout_bucket=str(row["timeout_bucket"] or ""),
                retries=int(row["retries"] or 0),
            )
            event = {
                "success": bool(row["success"]),
                "fingerprint": fingerprint,
            }
            instance_streams[(slug, instance_key)].append(event)
            if event["success"]:
                successful_fingerprints[slug][fingerprint] += 1

        for slug, fingerprint_counts in successful_fingerprints.items():
            total_successes = sum(int(count) for count in fingerprint_counts.values())
            if slug not in stats or total_successes <= 0:
                continue
            top_fingerprint, top_count = max(
                fingerprint_counts.items(),
                key=lambda item: (int(item[1]), item[0]),
            )
            stats[slug]["config_consensus"] = round(top_count / total_successes, 4)
            stats[slug]["top_config_fingerprint"] = top_fingerprint
            stats[slug]["top_config_share"] = round(top_count / total_successes, 4)

        repair_opportunities_by_slug: dict[str, int] = defaultdict(int)
        repaired_instances_by_slug: dict[str, int] = defaultdict(int)
        for (slug, _instance_key), events in instance_streams.items():
            saw_failure = False
            last_failed_fingerprint = ""
            repaired = False
            for event in events:
                fingerprint = str(event.get("fingerprint", "") or "")
                if not bool(event.get("success")):
                    saw_failure = True
                    if fingerprint:
                        last_failed_fingerprint = fingerprint
                    continue
                if not saw_failure:
                    continue
                if fingerprint and fingerprint != last_failed_fingerprint:
                    repaired = True
                    break
            if saw_failure:
                repair_opportunities_by_slug[slug] += 1
                if repaired:
                    repaired_instances_by_slug[slug] += 1

        for slug, item in stats.items():
            opportunities = int(repair_opportunities_by_slug.get(slug, 0) or 0)
            repaired = int(repaired_instances_by_slug.get(slug, 0) or 0)
            item["repair_opportunities"] = opportunities
            item["repaired_instances"] = repaired
            item["repair_rate"] = round((repaired / opportunities) if opportunities else 0.0, 4)
        return stats

    def _prefetch_tools_map(self, conn, slugs: list[str]) -> dict[str, list[str]]:
        if not slugs:
            return {}
        rows = conn.execute(
            select(mcp_tools.c.mcp_slug, mcp_tools.c.tool_name)
            .where(mcp_tools.c.mcp_slug.in_(slugs))
            .order_by(mcp_tools.c.mcp_slug.asc(), mcp_tools.c.tool_name.asc())
        ).all()
        tools_map: dict[str, list[str]] = defaultdict(list)
        for mcp_slug, tool_name in rows:
            tools_map[str(mcp_slug)].append(str(tool_name))
        return dict(tools_map)

    def _prefetch_recommendation_map(
        self,
        conn,
        slugs: list[str],
        telemetry_by_slug: dict[str, dict[str, Any]],
        *,
        runtime_settings: dict[str, Any] | None = None,
    ) -> dict[str, dict[str, Any] | None]:
        if not slugs:
            return {}
        static_rows = conn.execute(
            select(mcp_recommended_configs).where(mcp_recommended_configs.c.mcp_slug.in_(slugs))
        ).mappings().all()
        static_map = {str(row["mcp_slug"]): dict(row) for row in static_rows}
        dynamic_map = self._build_dynamic_recommendation_map(
            conn,
            slugs,
            runtime_settings=runtime_settings or self.get_runtime_settings(),
        )
        merged: dict[str, dict[str, Any] | None] = {}
        mode = str((runtime_settings or {}).get("recommendation_mode", "balanced")).strip().lower() or "balanced"
        for slug in slugs:
            static_item = static_map.get(slug)
            dynamic_item = dynamic_map.get(slug)
            merged[slug] = self._merge_recommendation_sources(static_item, dynamic_item, mode=mode)
        return merged

    def _build_dynamic_recommendation_map(
        self,
        conn,
        slugs: list[str],
        *,
        runtime_settings: dict[str, Any],
    ) -> dict[str, dict[str, Any]]:
        if not slugs:
            return {}
        min_signals = max(1, int(runtime_settings.get("featured_min_signal_count", 3) or 3))
        since_30d = (datetime.now(timezone.utc) - timedelta(days=30)).isoformat(timespec="seconds")
        rows = conn.execute(
            select(
                telemetry_events.c.mcp_slug,
                telemetry_events.c.transport,
                telemetry_events.c.timeout_bucket,
                telemetry_events.c.retries,
                func.count().label("run_count"),
                func.sum(case((telemetry_events.c.success.is_(True), 1), else_=0)).label("success_count"),
                func.count(func.distinct(telemetry_events.c.instance_hash)).label("active_instances"),
                func.avg(func.nullif(telemetry_events.c.latency_ms, 0)).label("avg_latency_ms"),
            )
            .where(
                and_(
                    telemetry_events.c.mcp_slug.in_(slugs),
                    telemetry_events.c.created_at >= since_30d,
                    telemetry_events.c.transport != "",
                )
            )
            .group_by(
                telemetry_events.c.mcp_slug,
                telemetry_events.c.transport,
                telemetry_events.c.timeout_bucket,
                telemetry_events.c.retries,
            )
        ).mappings().all()
        grouped: dict[str, dict[str, Any]] = {}
        for row in rows:
            run_count = int(row["run_count"] or 0)
            if run_count < min_signals:
                continue
            timeout = self._timeout_from_bucket(str(row["timeout_bucket"] or "").strip())
            if timeout <= 0:
                continue
            success_rate = (int(row["success_count"] or 0) / run_count) if run_count else 0.0
            score = self._bayesian_success_score(success_rate=success_rate, signal_count=run_count)
            latency = float(row["avg_latency_ms"] or 0.0)
            confidence = min(
                0.99,
                0.45
                + score * 0.35
                + min(int(row["active_instances"] or 0), 20) * 0.01
                + min(run_count, 50) * 0.003
                - (0.05 if latency > 3500 else 0.0),
            )
            item = {
                "mcp_slug": str(row["mcp_slug"]),
                "transport": str(row["transport"]),
                "timeout": timeout,
                "retries": int(row["retries"] or 0),
                "confidence_score": round(confidence, 3),
                "based_on_instances": int(row["active_instances"] or 0),
                "based_on_runs": run_count,
                "source": "telemetry",
            }
            current = grouped.get(item["mcp_slug"])
            if current is None or (
                float(item["confidence_score"]) > float(current.get("confidence_score", 0.0))
                or (
                    float(item["confidence_score"]) == float(current.get("confidence_score", 0.0))
                    and int(item["based_on_runs"]) > int(current.get("based_on_runs", 0))
                )
            ):
                grouped[item["mcp_slug"]] = item
        return grouped

    def _build_error_clusters_for_slugs(self, conn, slugs: list[str]) -> dict[str, list[dict[str, Any]]]:
        if not slugs:
            return {}
        rows = conn.execute(
            select(
                telemetry_events.c.mcp_slug,
                telemetry_events.c.error_code,
                func.count().label("event_count"),
                func.avg(func.nullif(telemetry_events.c.latency_ms, 0)).label("avg_latency_ms"),
                func.count(func.distinct(telemetry_events.c.instance_hash)).label("instance_count"),
            )
            .where(
                and_(
                    telemetry_events.c.mcp_slug.in_(slugs),
                    telemetry_events.c.success.is_(False),
                    telemetry_events.c.error_code != "",
                )
            )
            .group_by(telemetry_events.c.mcp_slug, telemetry_events.c.error_code)
            .order_by(telemetry_events.c.mcp_slug.asc(), func.count().desc(), telemetry_events.c.error_code.asc())
        ).mappings().all()
        grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
        for row in rows:
            slug = str(row["mcp_slug"])
            bucket = grouped[slug]
            if len(bucket) >= 4:
                continue
            error_code = str(row["error_code"]).strip()
            event_count = int(row["event_count"] or 0)
            instance_count = int(row["instance_count"] or 0)
            confidence = min(0.95, 0.45 + min(event_count, 12) * 0.03 + min(instance_count, 6) * 0.02)
            bucket.append(
                {
                    "error_code": error_code,
                    "event_count": event_count,
                    "instance_count": instance_count,
                    "avg_latency_ms": float(row["avg_latency_ms"] or 0.0),
                    "summary": self._summarize_error_cluster(error_code, event_count=event_count, instance_count=instance_count),
                    "confidence_score": round(confidence, 2),
                }
            )
        return dict(grouped)

    @staticmethod
    def _timeout_from_bucket(bucket: str) -> int:
        text = str(bucket or "").strip()
        if not text:
            return 0
        range_match = re.match(r"^(\d+)\s*-\s*(\d+)$", text)
        if range_match:
            return int(range_match.group(2))
        lower_bound_match = re.match(r"^[>≥]\s*(\d+)$", text)
        if lower_bound_match:
            return int(lower_bound_match.group(1))
        exact_match = re.match(r"^(\d+)$", text)
        if exact_match:
            return int(exact_match.group(1))
        return 0

    @staticmethod
    def _build_config_fingerprint(*, transport: str, timeout_bucket: str, retries: int) -> str:
        normalized_transport = str(transport or "").strip().lower() or "unknown"
        normalized_timeout = str(timeout_bucket or "").strip().lower() or "unknown"
        normalized_retries = max(0, int(retries or 0))
        return f"{normalized_transport}:{normalized_timeout}:{normalized_retries}"

    @staticmethod
    def _merge_recommendation_sources(
        static_item: dict[str, Any] | None,
        dynamic_item: dict[str, Any] | None,
        *,
        mode: str,
    ) -> dict[str, Any] | None:
        if static_item is None:
            return dynamic_item
        if dynamic_item is None:
            return static_item
        static_conf = float(static_item.get("confidence_score", 0.0) or 0.0)
        dynamic_conf = float(dynamic_item.get("confidence_score", 0.0) or 0.0)
        if mode == "conservative":
            return dynamic_item if dynamic_conf >= (static_conf + 0.12) else static_item
        return dynamic_item if dynamic_conf >= static_conf else static_item

    @staticmethod
    def _ensure_sqlite_parent(database_url: str) -> None:
        if not database_url.startswith("sqlite:///"):
            return
        db_file = Path(database_url.removeprefix("sqlite:///")).expanduser()
        db_file.parent.mkdir(parents=True, exist_ok=True)

    def _insert_submission(
        self,
        conn,
        *,
        proposed_slug: str,
        published_slug: str,
        repo_url: str,
        name: str,
        submitted_by: str,
        source_instance: str,
        source_public_url: str,
        status: str,
        details: dict[str, Any],
        submitted_at: str,
    ) -> dict[str, Any]:
        result = conn.execute(
            mcp_submissions.insert().values(
                proposed_slug=proposed_slug,
                published_slug=published_slug,
                repo_url=repo_url,
                name=name,
                submitted_by=submitted_by,
                source_instance=source_instance,
                source_public_url=source_public_url,
                status=status,
                details_json=json.dumps(details),
                submitted_at=submitted_at,
            )
        )
        submission_id = result.inserted_primary_key[0] if result.inserted_primary_key else None
        return {
            "id": submission_id,
            "proposed_slug": proposed_slug,
            "published_slug": published_slug,
            "repo_url": repo_url,
            "name": name,
            "submitted_by": submitted_by,
            "source_instance": source_instance,
            "source_public_url": source_public_url,
            "status": status,
            "submitted_at": submitted_at,
        }

    def _allocate_submission_slug(self, conn, repo_url: str, proposed_slug: str, *, owner_name: str, repo_name: str) -> str:
        existing_by_url = conn.execute(
            select(mcp_servers.c.slug).where(mcp_servers.c.repo_url == repo_url)
        ).scalar_one_or_none()
        if existing_by_url:
            return str(existing_by_url)

        candidates = [
            proposed_slug,
            self._normalize_submission_slug(repo_name),
            self._normalize_submission_slug(f"{owner_name}-{repo_name}"),
        ]
        seen: set[str] = set()
        for candidate in candidates:
            if not candidate or candidate in seen:
                continue
            seen.add(candidate)
            if conn.execute(select(mcp_servers.c.slug).where(mcp_servers.c.slug == candidate)).scalar_one_or_none() is None:
                return candidate

        base = self._normalize_submission_slug(f"{owner_name}-{repo_name}") or "community-mcp"
        suffix = 2
        while True:
            candidate = f"{base}-{suffix}"
            if conn.execute(select(mcp_servers.c.slug).where(mcp_servers.c.slug == candidate)).scalar_one_or_none() is None:
                return candidate
            suffix += 1

    def _allocate_generic_slug(self, conn, table: Table, raw_value: str, *, fallback: str) -> str:
        base = self._normalize_submission_slug(raw_value) or fallback
        candidate = base
        suffix = 2
        while conn.execute(select(table.c.slug).where(table.c.slug == candidate)).scalar_one_or_none() is not None:
            candidate = f"{base}-{suffix}"
            suffix += 1
        return candidate

    def _resolve_stack_item_slugs(self, conn, items: list[str]) -> list[str]:
        resolved: list[str] = []
        for item in items:
            value = str(item).strip()
            if not value:
                continue
            candidate = value
            if value.startswith("http"):
                candidate = _normalize_repo_url(value)
                row = conn.execute(
                    select(mcp_servers.c.slug).where(mcp_servers.c.repo_url == candidate)
                ).mappings().first()
            else:
                row = conn.execute(
                    select(mcp_servers.c.slug).where(mcp_servers.c.slug == candidate)
                ).mappings().first()
            if row is None:
                raise ValueError(f"Unknown MCP reference in stack: {value}")
            slug = str(row["slug"])
            if slug not in resolved:
                resolved.append(slug)
        return resolved

    def _increment_counter(self, table: Table, slug: str, column_name: str) -> dict[str, int]:
        column = getattr(table.c, column_name)
        with self.engine.begin() as conn:
            row = conn.execute(select(column).where(table.c.slug == slug)).first()
            if row is None:
                raise ValueError("Tracked item not found.")
            current = int(row[0] or 0)
            conn.execute(
                update(table)
                .where(table.c.slug == slug)
                .values(**{column_name: current + 1, "updated_at": _utc_now()} if "updated_at" in table.c else {column_name: current + 1})
            )
        return {"slug": slug, column_name: current + 1}

    @staticmethod
    def _normalize_submission_slug(raw: str) -> str:
        value = re.sub(r"[^a-z0-9]+", "-", str(raw or "").strip().lower()).strip("-")
        return value[:120]

    @staticmethod
    def _normalize_text_list(value: Any) -> list[str]:
        if isinstance(value, str):
            items = [item.strip() for item in value.split(",")]
        elif isinstance(value, (list, tuple, set)):
            items = [str(item).strip() for item in value]
        else:
            items = []
        normalized: list[str] = []
        seen: set[str] = set()
        for item in items:
            if not item:
                continue
            key = item.lower()
            if key in seen:
                continue
            seen.add(key)
            normalized.append(item[:160])
        return normalized

    @staticmethod
    def _normalize_runtime_settings(payload: dict[str, Any]) -> dict[str, Any]:
        mode = str(payload.get("recommendation_mode", HUB_RUNTIME_DEFAULTS["recommendation_mode"])).strip().lower()
        if mode not in {"conservative", "balanced"}:
            mode = "balanced"
        return {
            "telemetry_ingest_enabled": bool(payload.get("telemetry_ingest_enabled", HUB_RUNTIME_DEFAULTS["telemetry_ingest_enabled"])),
            "api_token_writes_enabled": bool(payload.get("api_token_writes_enabled", HUB_RUNTIME_DEFAULTS["api_token_writes_enabled"])),
            "recommendation_mode": mode,
            "featured_min_trust_score": round(
                max(0.0, min(10.0, float(payload.get("featured_min_trust_score", HUB_RUNTIME_DEFAULTS["featured_min_trust_score"]) or 0.0))),
                1,
            ),
            "featured_min_signal_count": max(
                1,
                min(100, int(payload.get("featured_min_signal_count", HUB_RUNTIME_DEFAULTS["featured_min_signal_count"]) or 1)),
            ),
            "discover_cache_ttl_seconds": max(
                5,
                min(600, int(payload.get("discover_cache_ttl_seconds", HUB_RUNTIME_DEFAULTS["discover_cache_ttl_seconds"]) or 5)),
            ),
            "overview_cache_ttl_seconds": max(
                5,
                min(600, int(payload.get("overview_cache_ttl_seconds", HUB_RUNTIME_DEFAULTS["overview_cache_ttl_seconds"]) or 5)),
            ),
            "default_gui_url": str(payload.get("default_gui_url", HUB_RUNTIME_DEFAULTS["default_gui_url"]) or "").strip().rstrip("/"),
        }

    @classmethod
    def _normalize_stack_items(cls, value: Any) -> list[str]:
        return cls._normalize_text_list(value)

    @staticmethod
    def _guard_submission_text(*values: str) -> None:
        for value in values:
            text = str(value or "").strip()
            if text and _SECRET_RE.search(text):
                raise ValueError("Submission contains secret-like content. Remove API keys, tokens, and passwords before publishing.")

    @staticmethod
    def _infer_category(payload: dict[str, Any]) -> str:
        text = " ".join(HubStore._normalize_text_list(payload.get("tools", []))).lower()
        if any(token in text for token in ("github", "repo", "pull", "issue", "code", "devtools")):
            return "Coding"
        if any(token in text for token in ("search", "crawl", "extract", "browser", "doc", "context")):
            return "Research"
        return "Automation"

    @staticmethod
    def _infer_install_method(payload: dict[str, Any]) -> str:
        repo_type = str(payload.get("repo_type", "")).strip().lower()
        if repo_type:
            return repo_type
        return str(payload.get("install_method", "")).strip() or "unknown"

    @staticmethod
    def _infer_language(install_method: str) -> str:
        method = str(install_method or "").strip().lower()
        if method in {"npm", "workspace_package", "monorepo"}:
            return "Node.js"
        if method in {"python", "pip", "uv"}:
            return "Python"
        if method in {"remote", "http", "sse"}:
            return "Remote"
        if method == "docker":
            return "Docker"
        return "Unknown"

    @staticmethod
    def _build_reliability_payload(*, success_rate: float, status: str, verified: bool, has_live_telemetry: bool) -> dict[str, Any]:
        normalized_status = str(status or "").strip().lower()
        if not has_live_telemetry:
            return {"label": "No live telemetry", "tone": "muted", "percent": 0, "bar_width": 0}
        percent = max(0, min(100, round(float(success_rate or 0.0) * 100)))
        if normalized_status == "rejected":
            return {"label": "Rejected", "tone": "bad", "percent": percent, "bar_width": percent}
        if percent >= 95 and verified:
            return {"label": "Stable", "tone": "good", "percent": percent, "bar_width": percent}
        if percent >= 88:
            return {"label": "Reliable", "tone": "good", "percent": percent, "bar_width": percent}
        if percent >= 76:
            return {"label": "Experimental", "tone": "warn", "percent": percent, "bar_width": percent}
        return {"label": "Unstable", "tone": "bad", "percent": percent, "bar_width": percent}

    @staticmethod
    def _build_best_for_payload(*, category: str, tags: list[str], tools: list[str]) -> list[str]:
        tags_lower = {str(tag).strip().lower() for tag in tags}
        tools_lower = {str(tool).strip().lower() for tool in tools}
        category_lower = str(category or "").strip().lower()
        result: list[str] = []
        if category_lower == "coding":
            result.extend(["Coding agents", "Repository analysis"])
        elif category_lower == "research":
            result.extend(["Research agents", "Knowledge workflows"])
        elif category_lower == "automation":
            result.extend(["Automation agents", "Workflow assistants"])
        if {"browser", "devtools", "playwright"} & tags_lower:
            result.append("Browser workflows")
        if {"docs", "doc", "search", "retrieval"} & tags_lower:
            result.append("Documentation lookup")
        if any("repo" in tool or "pull" in tool or "issue" in tool for tool in tools_lower):
            result.append("Code review loops")
        deduped: list[str] = []
        seen: set[str] = set()
        for item in result:
            key = item.lower()
            if key in seen:
                continue
            seen.add(key)
            deduped.append(item)
        return deduped[:4]

    @staticmethod
    def _build_stack_best_for_payload(categories: list[str]) -> list[str]:
        normalized = {str(item).strip().lower() for item in categories if str(item).strip()}
        result: list[str] = []
        if "coding" in normalized:
            result.append("Code analysis")
        if "research" in normalized:
            result.append("Research workflows")
        if "automation" in normalized:
            result.append("Automation pipelines")
        if "browser" in normalized:
            result.append("Browser workflows")
        return result[:3]

    @staticmethod
    def _build_dependency_payload(*, install_method: str, language: str, status: str) -> list[str]:
        method = str(install_method or "").strip().lower()
        language_label = str(language or "").strip().lower()
        dependencies: list[str] = []
        if method in {"npm", "workspace_package", "monorepo"} or language_label == "node.js":
            dependencies.extend(["Node.js >= 18", "npm / npx"])
        elif method in {"python", "pip", "uv"} or language_label == "python":
            dependencies.extend(["Python 3.11+", "uv / pip"])
        elif method == "docker" or language_label == "docker":
            dependencies.append("Docker runtime")
        elif method in {"remote", "http", "sse"} or language_label == "remote":
            dependencies.append("Remote MCP endpoint")
        if str(status or "").strip().lower() == "needs_configuration":
            dependencies.append("Runtime secrets")
        return dependencies[:4]

    @staticmethod
    def _build_runtime_engine_payload(*, install_method: str, language: str) -> dict[str, str]:
        method = str(install_method or "").strip().lower()
        language_label = str(language or "").strip().lower()
        if method in {"npm", "workspace_package", "monorepo"} or language_label == "node.js":
            return {"label": "Node", "tone": "good"}
        if method in {"python", "pip", "uv"} or language_label == "python":
            return {"label": "Python", "tone": "good"}
        if method == "docker" or language_label == "docker":
            return {"label": "Docker", "tone": "warn"}
        if method in {"remote", "http", "sse"} or language_label == "remote":
            return {"label": "Remote/API", "tone": "warn"}
        return {"label": "Other", "tone": "muted"}

    @classmethod
    def _matches_runtime_engine(cls, *, install_method: str, language: str, runtime: str) -> bool:
        expected = str(runtime or "").strip().lower()
        if not expected:
            return True
        engine = cls._build_runtime_engine_payload(install_method=install_method, language=language)
        return str(engine.get("label", "")).strip().lower() == expected

    @staticmethod
    def _build_permission_hints_payload(
        *,
        install_method: str,
        language: str,
        status: str,
        tags: list[str],
        tools: list[str],
    ) -> list[str]:
        method = str(install_method or "").strip().lower()
        language_label = str(language or "").strip().lower()
        tags_lower = {str(item).strip().lower() for item in tags if str(item).strip()}
        tools_lower = [str(item).strip().lower() for item in tools if str(item).strip()]
        joined_tools = " ".join(tools_lower)

        hints: list[str] = []

        if method in {"remote", "http", "sse"} or language_label == "remote":
            hints.append("Makes outbound network requests")

        if method in {"npm", "workspace_package", "monorepo", "python", "pip", "uv", "docker"}:
            hints.append("Runs local runtime processes")

        if (
            {"browser", "devtools", "playwright"} & tags_lower
            or any(token in joined_tools for token in ["browser", "devtools", "playwright", "page", "screenshot"])
        ):
            hints.append("Controls browser or devtools runtime")

        if any(token in joined_tools for token in ["read_file", "write_file", "workspace", "filesystem", "fs_"]):
            hints.append("Can access local workspace files")

        if str(status or "").strip().lower() == "needs_configuration":
            hints.append("Needs secrets or API credentials")

        deduped: list[str] = []
        seen: set[str] = set()
        for hint in hints:
            key = hint.lower()
            if key in seen:
                continue
            seen.add(key)
            deduped.append(hint)
        return deduped[:4]

    @staticmethod
    def _build_mcp_type_payload(*, install_method: str, language: str) -> dict[str, Any]:
        method = str(install_method or "").strip().lower()
        language_label = str(language or "").strip().lower()
        if method in {"remote", "http", "sse"} or language_label == "remote":
            return {"label": "Remote MCP", "tone": "warn"}
        if method == "docker" or language_label == "docker":
            return {"label": "Hybrid MCP", "tone": "warn"}
        return {"label": "Local MCP", "tone": "good"}

    @staticmethod
    def _build_install_confidence_payload(
        *,
        success_rate: float,
        verified: bool,
        recommendation: dict[str, Any] | None,
        active_instances: int,
        installs: int,
        signal_count: int,
        telemetry: dict[str, Any],
    ) -> dict[str, Any]:
        telemetry_runs = max(0, int(telemetry.get("run_count", signal_count) or 0))
        telemetry_instances = max(0, int(telemetry.get("active_instances", active_instances) or 0))
        if telemetry_runs == 0 and telemetry_instances == 0:
            return {
                "score": 0.0,
                "label": "No live telemetry",
                "tone": "muted",
                "based_on_instances": 0,
                "runs_30d": 0,
                "instances_30d": 0,
                "volume_score": 0.0,
                "diversity_score": 0.0,
                "diversity_cap": 0.0,
            }
        target_runs = 1000
        target_instances = 50
        volume = min(1.0, math.log10(telemetry_runs + 1) / math.log10(target_runs + 1))
        diversity = min(1.0, telemetry_instances / target_instances) if target_instances else 0.0
        base = 0.7 * volume + 0.3 * diversity
        diversity_cap = 0.35 + (0.65 * diversity)
        score = round(min(1.0, base, diversity_cap) * 10.0, 1)
        if score >= 9.0:
            label = "High confidence"
            tone = "good"
        elif score >= 7.5:
            label = "Medium confidence"
            tone = "warn"
        else:
            label = "Needs review"
            tone = "bad"
        return {
            "score": score,
            "label": label,
            "tone": tone,
            "based_on_instances": telemetry_instances,
            "runs_30d": telemetry_runs,
            "instances_30d": telemetry_instances,
            "volume_score": round(volume, 4),
            "diversity_score": round(diversity, 4),
            "diversity_cap": round(min(1.0, diversity_cap), 4),
        }

    @staticmethod
    def _build_trust_score_payload(
        *,
        success_rate: float,
        install_confidence: dict[str, Any],
        verified: bool,
        telemetry: dict[str, Any],
        recommendation: dict[str, Any] | None,
        has_live_telemetry: bool,
    ) -> dict[str, Any]:
        if not has_live_telemetry:
            return {
                "score": 0.0,
                "label": "Needs telemetry",
                "tone": "muted",
                "performance_component": 0.0,
                "config_consensus": 0.0,
                "repair_rate": 0.0,
                "verified_bonus": 1.0 if verified else 0.0,
            }
        confidence_norm = max(0.0, min(1.0, float(install_confidence.get("score", 0.0) or 0.0) / 10.0))
        performance = HubStore._bayesian_success_score(
            success_rate=float(success_rate or 0.0),
            signal_count=max(0, int(telemetry.get("run_count", 0) or 0)),
        ) * confidence_norm
        consensus = float(telemetry.get("config_consensus", 0.0) or 0.0)
        if consensus <= 0.0:
            if isinstance(recommendation, dict) and recommendation:
                consensus = 0.8 if verified else 0.7
            else:
                consensus = 0.7 if verified else 0.5
        repair_opportunities = int(telemetry.get("repair_opportunities", 0) or 0)
        repair_rate = float(telemetry.get("repair_rate", 0.0) or 0.0)
        if repair_opportunities == 0:
            repair_rate = 0.75 if verified else 0.65
        verified_factor = 1.0 if verified else 0.0
        score = round(
            (
                (0.4 * performance)
                + (0.3 * max(0.0, min(1.0, consensus)))
                + (0.2 * max(0.0, min(1.0, repair_rate)))
                + (0.1 * verified_factor)
            )
            * 10.0,
            1,
        )
        score = max(1.0, min(10.0, score))
        if score >= 9.0:
            label = "Trusted"
            tone = "good"
        elif score >= 7.5:
            label = "Reviewable"
            tone = "warn"
        else:
            label = "Experimental"
            tone = "bad"
        return {
            "score": score,
            "label": label,
            "tone": tone,
            "performance_component": round(performance, 4),
            "config_consensus": round(max(0.0, min(1.0, consensus)), 4),
            "repair_rate": round(max(0.0, min(1.0, repair_rate)), 4),
            "verified_bonus": verified_factor,
        }

    def _build_usage_trend_payload(self, *, slug: str, telemetry: dict[str, Any], recent_runs: int) -> dict[str, Any]:
        runs_24h = int(telemetry.get("runs_24h", 0) or 0)
        runs_7d = int(telemetry.get("runs_7d", 0) or 0)
        if runs_24h >= 5 or (runs_24h and recent_runs >= 10):
            label = "Growing"
            tone = "good"
        elif runs_24h > 0:
            label = "Observed"
            tone = "warn"
        else:
            label = "Quiet"
            tone = "muted"
        return {
            "runs_24h": runs_24h,
            "runs_7d": runs_7d,
            "label": label,
            "tone": tone,
        }

    @staticmethod
    def _bayesian_success_score(*, success_rate: float, signal_count: int, prior_mean: float = 0.78, prior_weight: float = 4.0) -> float:
        signals = max(0, int(signal_count or 0))
        observed_successes = float(success_rate or 0.0) * signals
        return (observed_successes + prior_mean * prior_weight) / (signals + prior_weight) if (signals + prior_weight) else prior_mean

    @staticmethod
    def _build_network_health_payload(
        *,
        average_success_rate: float,
        average_latency_ms: float,
        runs_today: int,
        telemetry_active: bool,
    ) -> dict[str, Any]:
        if not telemetry_active:
            return {
                "score": 0.0,
                "label": "No live telemetry",
                "tone": "muted",
                "summary": "No live telemetry has been observed yet.",
            }
        score = float(average_success_rate or 0.0) * 100.0
        latency = float(average_latency_ms or 0.0)
        if latency > 3000:
            score -= 6.0
        elif latency > 2200:
            score -= 3.0
        if runs_today < 5:
            score -= 2.0
        score = round(max(0.0, min(100.0, score)), 1)
        if score >= 92.0:
            label = "Healthy"
            tone = "good"
        elif score >= 80.0:
            label = "Watch"
            tone = "warn"
        else:
            label = "Fragile"
            tone = "bad"
        return {
            "score": score,
            "label": label,
            "tone": tone,
            "summary": (
                "Community telemetry looks stable overall."
                if tone == "good"
                else "Some MCP combinations need closer review."
                if tone == "warn"
                else "The network is currently seeing elevated failure or latency signals."
            ),
        }

    @staticmethod
    def _build_mcp_difficulty_payload(*, install_method: str, verified: bool, status: str) -> dict[str, Any]:
        score = 1
        method = str(install_method or "").strip().lower()
        normalized_status = str(status or "").strip().lower()
        if method in {"workspace_package", "monorepo", "docker"}:
            score += 1
        if not verified:
            score += 1
        if normalized_status == "needs_configuration":
            score += 1
        if score <= 1:
            return {"label": "Beginner", "tone": "good"}
        if score == 2:
            return {"label": "Intermediate", "tone": "warn"}
        return {"label": "Advanced", "tone": "bad"}

    @staticmethod
    def _build_stack_difficulty_payload(*, item_count: int, install_methods: list[str]) -> dict[str, Any]:
        score = 1
        methods = {str(item).strip().lower() for item in install_methods if str(item).strip()}
        if item_count >= 3:
            score += 1
        if {"workspace_package", "monorepo", "docker", "remote"} & methods:
            score += 1
        if score <= 1:
            return {"label": "Beginner", "tone": "good"}
        if score == 2:
            return {"label": "Intermediate", "tone": "warn"}
        return {"label": "Advanced", "tone": "bad"}

    def _build_error_clusters(self, conn, slug: str) -> list[dict[str, Any]]:
        rows = conn.execute(
            select(
                telemetry_events.c.error_code,
                func.count().label("event_count"),
                func.avg(func.nullif(telemetry_events.c.latency_ms, 0)).label("avg_latency_ms"),
                func.count(func.distinct(telemetry_events.c.instance_hash)).label("instance_count"),
            )
            .where(
                and_(
                    telemetry_events.c.mcp_slug == slug,
                    telemetry_events.c.success.is_(False),
                    telemetry_events.c.error_code != "",
                )
            )
            .group_by(telemetry_events.c.error_code)
            .order_by(func.count().desc(), telemetry_events.c.error_code.asc())
        ).mappings().all()
        clusters: list[dict[str, Any]] = []
        for row in rows[:4]:
            error_code = str(row["error_code"]).strip()
            event_count = int(row["event_count"] or 0)
            instance_count = int(row["instance_count"] or 0)
            confidence = min(0.95, 0.45 + min(event_count, 12) * 0.03 + min(instance_count, 6) * 0.02)
            clusters.append(
                {
                    "error_code": error_code,
                    "event_count": event_count,
                    "instance_count": instance_count,
                    "avg_latency_ms": float(row["avg_latency_ms"] or 0.0),
                    "summary": self._summarize_error_cluster(error_code, event_count=event_count, instance_count=instance_count),
                    "confidence_score": round(confidence, 2),
                }
            )
        return clusters

    @staticmethod
    def _summarize_error_cluster(error_code: str, *, event_count: int, instance_count: int) -> str:
        code = str(error_code or "").strip().lower()
        if code == "timeout":
            return f"Observed timeout failures {event_count} times across {instance_count} instance fingerprints."
        if code == "missing_env":
            return f"Missing secret or env configuration appeared {event_count} times across {instance_count} instances."
        if code == "unauthorized":
            return f"Authentication failures appeared {event_count} times across {instance_count} instances."
        if code == "connection":
            return f"Connection issues appeared {event_count} times across {instance_count} instances."
        return f"Community telemetry recorded '{error_code}' {event_count} times across {instance_count} instances."

    def _build_known_fix_summaries(self, item: dict[str, Any]) -> list[dict[str, Any]]:
        recommendation = item.get("recommended_config") if isinstance(item.get("recommended_config"), dict) else {}
        clusters = item.get("error_clusters") if isinstance(item.get("error_clusters"), list) else []
        fixes: list[dict[str, Any]] = []
        for cluster in clusters:
            if not isinstance(cluster, dict):
                continue
            code = str(cluster.get("error_code", "")).strip().lower()
            if code == "timeout" and recommendation:
                fixes.append(
                    {
                        "title": "Increase timeout to the community default",
                        "summary": f"Community telemetry links timeout errors to shorter timeouts. Recommended timeout: {int(recommendation.get('timeout', 0) or 0)}s.",
                        "evidence_count": int(cluster.get("event_count", 0) or 0),
                    }
                )
            elif code == "missing_env":
                fixes.append(
                    {
                        "title": "Review required environment variables",
                        "summary": "Community telemetry shows that missing secrets are a common cause of failure for this MCP.",
                        "evidence_count": int(cluster.get("event_count", 0) or 0),
                    }
                )
            elif code == "unauthorized":
                fixes.append(
                    {
                        "title": "Re-check provider or MCP credentials",
                        "summary": "Authentication errors are common for this MCP when the service token is invalid or missing.",
                        "evidence_count": int(cluster.get("event_count", 0) or 0),
                    }
                )
        deduped: list[dict[str, Any]] = []
        seen_titles: set[str] = set()
        for fix in fixes:
            title = str(fix.get("title", "")).strip()
            if not title or title in seen_titles:
                continue
            seen_titles.add(title)
            deduped.append(fix)
        return deduped[:4]
