"""Database-backed store for the community hub."""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from sqlalchemy import (
    Boolean,
    Column,
    Float,
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
        sort: str = "trending",
        include_private: bool = False,
    ) -> list[dict[str, Any]]:
        telemetry = self._telemetry_stats_by_slug()
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
            if conditions:
                stmt = stmt.where(and_(*conditions))
            rows = conn.execute(stmt).mappings().all()
            items = [self._build_mcp_summary(conn, row, telemetry.get(str(row["slug"]), {})) for row in rows]

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

    def get_mcp(self, slug: str, *, include_private: bool = False) -> dict[str, Any] | None:
        telemetry = self._telemetry_stats_by_slug().get(slug, {})
        with self.engine.connect() as conn:
            stmt = select(mcp_servers).where(mcp_servers.c.slug == slug)
            if not include_private:
                stmt = stmt.where(mcp_servers.c.status != "rejected")
            row = conn.execute(stmt).mappings().first()
            if row is None:
                return None
            item = self._build_mcp_summary(conn, row, telemetry, detailed=True)
            recommendation = conn.execute(
                select(mcp_recommended_configs).where(mcp_recommended_configs.c.mcp_slug == slug)
            ).mappings().first()
            item["recommended_config"] = dict(recommendation) if recommendation else None
            issue_rows = conn.execute(
                select(mcp_known_issues.c.issue_text)
                .where(mcp_known_issues.c.mcp_slug == slug)
                .order_by(mcp_known_issues.c.id.asc())
            ).all()
            item["known_issues"] = [str(issue[0]) for issue in issue_rows]
            return item

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
            stmt = stmt.order_by(mcp_stacks.c.rating.desc(), mcp_stacks.c.imports_count.desc())
            rows = conn.execute(stmt).mappings().all()
            return [self._build_stack_summary(conn, row) for row in rows]

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
            stmt = stmt.order_by(showcase_entries.c.imports_count.desc(), showcase_entries.c.upvotes_count.desc())
            rows = conn.execute(stmt).mappings().all()
            items: list[dict[str, Any]] = []
            for row in rows:
                stack_stmt = select(mcp_stacks.c.slug, mcp_stacks.c.title).where(mcp_stacks.c.slug == row["stack_slug"])
                if not include_private:
                    stack_stmt = stack_stmt.where(mcp_stacks.c.is_public.is_(True))
                stack_row = conn.execute(stack_stmt).mappings().first()
                items.append(
                    {
                        **dict(row),
                        "stack": dict(stack_row) if stack_row else None,
                    }
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

    def get_overview_stats(self) -> dict[str, Any]:
        telemetry = self._telemetry_stats_by_slug()
        marketplace = self.list_mcps()
        today_start = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
        with self.engine.connect() as conn:
            total_events_today = conn.execute(
                select(func.count()).select_from(telemetry_events).where(
                    telemetry_events.c.created_at >= today_start.isoformat(timespec="seconds")
                )
            ).scalar_one()
        active_instances = sum(int(item["active_instances"]) for item in marketplace)
        return {
            "registry_count": len(marketplace),
            "verified_count": sum(1 for item in marketplace if item["verified"]),
            "active_instances": active_instances,
            "runs_today": int(total_events_today or 0),
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
                select(mcp_stacks.c.slug, mcp_stacks.c.title).where(mcp_stacks.c.slug == row["stack_slug"])
            ).mappings().first()
            return {
                **dict(row),
                "stack": dict(stack_row) if stack_row else None,
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
                    select(mcp_stacks.c.slug, mcp_stacks.c.title).where(mcp_stacks.c.slug == row["stack_slug"])
                ).mappings().first()
                items.append({**dict(row), "stack": dict(stack_row) if stack_row else None})
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
        return item

    def increment_mcp_install(self, slug: str) -> dict[str, int]:
        return self._increment_counter(mcp_servers, slug, "installs")

    def increment_stack_import(self, slug: str) -> dict[str, int]:
        return self._increment_counter(mcp_stacks, slug, "imports_count")

    def increment_showcase_import(self, slug: str) -> dict[str, int]:
        return self._increment_counter(showcase_entries, slug, "imports_count")

    def categories(self) -> list[str]:
        with self.engine.connect() as conn:
            rows = conn.execute(
                select(mcp_servers.c.category).distinct().order_by(mcp_servers.c.category.asc())
            ).all()
            return [str(row[0]) for row in rows]

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
    ) -> dict[str, Any]:
        tool_rows = conn.execute(
            select(mcp_tools.c.tool_name)
            .where(mcp_tools.c.mcp_slug == row["slug"])
            .order_by(mcp_tools.c.tool_name.asc())
        ).all()
        tools = [str(tool[0]) for tool in tool_rows]
        effective_active = max(int(row["active_instances"]), int(telemetry.get("active_instances", 0) or 0))
        effective_success = (
            float(telemetry.get("success_rate"))
            if telemetry.get("run_count", 0) >= 3
            else float(row["success_rate"])
        )
        effective_latency = (
            float(telemetry.get("avg_latency_ms"))
            if telemetry.get("run_count", 0) >= 3
            else float(row["avg_latency_ms"])
        )
        summary = {
            **dict(row),
            "repo_url": _normalize_repo_url(str(row["repo_url"])),
            "tags": json.loads(str(row["tags_json"])),
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
        conn,
        row: dict[str, Any],
        *,
        detailed: bool = False,
    ) -> dict[str, Any]:
        item_rows = conn.execute(
            select(mcp_servers.c.slug, mcp_servers.c.name, mcp_servers.c.repo_url)
            .select_from(
                mcp_stack_items.join(mcp_servers, mcp_servers.c.slug == mcp_stack_items.c.mcp_slug)
            )
            .where(mcp_stack_items.c.stack_slug == row["slug"])
            .order_by(mcp_stack_items.c.sort_order.asc(), mcp_stack_items.c.id.asc())
        ).mappings().all()
        summary = {
            **dict(row),
            "items": [dict(item) for item in item_rows],
        }
        if detailed:
            summary["mcp_count"] = len(item_rows)
        return summary

    def _telemetry_stats_by_slug(self) -> dict[str, dict[str, Any]]:
        window_start = (datetime.now(timezone.utc) - timedelta(days=7)).isoformat(timespec="seconds")
        with self.engine.connect() as conn:
            rows = conn.execute(
                select(
                    telemetry_events.c.mcp_slug.label("mcp_slug"),
                    func.count().label("run_count"),
                    func.sum(case((telemetry_events.c.success.is_(True), 1), else_=0)).label("success_count"),
                    func.sum(case((telemetry_events.c.success.is_(False), 1), else_=0)).label("error_count"),
                    func.avg(func.nullif(telemetry_events.c.latency_ms, 0)).label("avg_latency_ms"),
                    func.count(func.distinct(telemetry_events.c.instance_hash)).label("active_instances"),
                )
                .where(telemetry_events.c.created_at >= window_start)
                .group_by(telemetry_events.c.mcp_slug)
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
            }
        return stats

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
