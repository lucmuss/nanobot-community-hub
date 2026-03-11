"""FastAPI application for nanobot-community-hub."""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from nanobot_hub import __version__
from nanobot_hub.store import HubStore


_TEMPLATES = Jinja2Templates(directory=str(Path(__file__).resolve().parent / "templates"))


@dataclass(slots=True)
class HubSettings:
    db_path: Path
    public_url: str = ""
    instance_name: str = "nanobot-community-hub"


def create_app() -> FastAPI:
    db_path = Path(os.getenv("NANOBOT_HUB_DB_PATH", "./data/nanobot-community-hub.sqlite3")).expanduser()
    settings = HubSettings(
        db_path=db_path,
        public_url=os.getenv("NANOBOT_HUB_PUBLIC_URL", "").strip(),
        instance_name=os.getenv("NANOBOT_HUB_INSTANCE_NAME", "nanobot-community-hub").strip() or "nanobot-community-hub",
    )
    store = HubStore(settings.db_path)
    store.init()

    app = FastAPI(title="nanobot-community-hub", version=__version__)
    app.mount("/static", StaticFiles(directory=str(Path(__file__).resolve().parent / "static")), name="static")
    app.state.settings = settings
    app.state.store = store

    @app.get("/", response_class=HTMLResponse)
    async def root() -> RedirectResponse:
        return RedirectResponse("/discover", status_code=302)

    @app.get("/health")
    async def health() -> dict[str, Any]:
        return {
            "ok": True,
            "service": settings.instance_name,
            "version": __version__,
            "public_url": settings.public_url,
        }

    @app.get("/discover", response_class=HTMLResponse)
    async def discover_page(
        request: Request,
        q: str = Query(""),
        category: str = Query(""),
        sort: str = Query("trending"),
    ) -> HTMLResponse:
        items = store.list_mcps(search=q.strip(), category=category.strip(), sort=sort.strip())
        return _render(
            request,
            "discover.html",
            {
                "title": "Discover MCP",
                "nav_active": "discover",
                "query": q.strip(),
                "category": category.strip(),
                "sort": sort.strip() or "trending",
                "categories": store.categories(),
                "items": items,
                "overview": store.get_overview_stats(),
            },
        )

    @app.get("/partials/discover-results", response_class=HTMLResponse)
    async def discover_results(
        request: Request,
        q: str = Query(""),
        category: str = Query(""),
        sort: str = Query("trending"),
    ) -> HTMLResponse:
        items = store.list_mcps(search=q.strip(), category=category.strip(), sort=sort.strip())
        return _render(
            request,
            "partials/discover_results.html",
            {
                "items": items,
            },
        )

    @app.get("/mcp/{slug}", response_class=HTMLResponse)
    async def mcp_detail_page(request: Request, slug: str) -> HTMLResponse:
        item = store.get_mcp(slug)
        if item is None:
            raise HTTPException(status_code=404, detail="MCP server not found.")
        return _render(
            request,
            "mcp_detail.html",
            {
                "title": item["name"],
                "nav_active": "discover",
                "item": item,
            },
        )

    @app.get("/stacks", response_class=HTMLResponse)
    async def stacks_page(request: Request, q: str = Query("")) -> HTMLResponse:
        items = store.list_stacks(search=q.strip())
        return _render(
            request,
            "stacks.html",
            {
                "title": "MCP Stacks",
                "nav_active": "stacks",
                "query": q.strip(),
                "items": items,
            },
        )

    @app.get("/partials/stacks-results", response_class=HTMLResponse)
    async def stack_results(request: Request, q: str = Query("")) -> HTMLResponse:
        return _render(
            request,
            "partials/stacks_results.html",
            {
                "items": store.list_stacks(search=q.strip()),
            },
        )

    @app.get("/stacks/{slug}", response_class=HTMLResponse)
    async def stack_detail_page(request: Request, slug: str) -> HTMLResponse:
        item = store.get_stack(slug)
        if item is None:
            raise HTTPException(status_code=404, detail="Stack not found.")
        return _render(
            request,
            "stack_detail.html",
            {
                "title": item["title"],
                "nav_active": "stacks",
                "item": item,
            },
        )

    @app.get("/showcase", response_class=HTMLResponse)
    async def showcase_page(
        request: Request,
        q: str = Query(""),
        category: str = Query(""),
    ) -> HTMLResponse:
        return _render(
            request,
            "showcase.html",
            {
                "title": "Showcase",
                "nav_active": "showcase",
                "query": q.strip(),
                "category": category.strip(),
                "categories": store.showcase_categories(),
                "items": store.list_showcase(search=q.strip(), category=category.strip()),
            },
        )

    @app.get("/partials/showcase-results", response_class=HTMLResponse)
    async def showcase_results(
        request: Request,
        q: str = Query(""),
        category: str = Query(""),
    ) -> HTMLResponse:
        return _render(
            request,
            "partials/showcase_results.html",
            {
                "items": store.list_showcase(search=q.strip(), category=category.strip()),
            },
        )

    @app.get("/community-stats", response_class=HTMLResponse)
    async def community_stats_page(request: Request) -> HTMLResponse:
        return _render(
            request,
            "community_stats.html",
            {
                "title": "Community Stats",
                "nav_active": "stats",
                "overview": store.get_overview_stats(),
            },
        )

    @app.get("/api/v1/health")
    async def api_health() -> dict[str, Any]:
        return await health()

    @app.get("/api/v1/marketplace")
    async def api_marketplace(
        q: str = Query(""),
        category: str = Query(""),
        sort: str = Query("trending"),
    ) -> dict[str, Any]:
        return {
            "items": store.list_mcps(search=q.strip(), category=category.strip(), sort=sort.strip()),
            "query": q.strip(),
            "category": category.strip(),
            "sort": sort.strip() or "trending",
        }

    @app.get("/api/v1/marketplace/resolve")
    async def api_marketplace_resolve(repo_url: str = Query("")) -> dict[str, Any]:
        resolved = store.resolve_repo(repo_url)
        return {"match": resolved}

    @app.get("/api/v1/marketplace/{slug}")
    async def api_marketplace_detail(slug: str) -> dict[str, Any]:
        item = store.get_mcp(slug)
        if item is None:
            raise HTTPException(status_code=404, detail="MCP server not found.")
        return item

    @app.get("/api/v1/marketplace/{slug}/recommendation")
    async def api_marketplace_recommendation(slug: str) -> dict[str, Any]:
        item = store.get_mcp(slug)
        if item is None:
            raise HTTPException(status_code=404, detail="MCP server not found.")
        return {
            "slug": item["slug"],
            "recommended_config": item["recommended_config"],
        }

    @app.get("/api/v1/stacks")
    async def api_stacks(q: str = Query("")) -> dict[str, Any]:
        return {"items": store.list_stacks(search=q.strip())}

    @app.get("/api/v1/stacks/{slug}")
    async def api_stack_detail(slug: str) -> dict[str, Any]:
        item = store.get_stack(slug)
        if item is None:
            raise HTTPException(status_code=404, detail="Stack not found.")
        return item

    @app.get("/api/v1/showcase")
    async def api_showcase(q: str = Query(""), category: str = Query("")) -> dict[str, Any]:
        return {
            "items": store.list_showcase(search=q.strip(), category=category.strip()),
        }

    @app.get("/api/v1/stats/overview")
    async def api_stats_overview() -> dict[str, Any]:
        return store.get_overview_stats()

    @app.post("/api/v1/telemetry/events")
    async def api_telemetry_event(request: Request) -> JSONResponse:
        payload = await request.json()
        try:
            event = store.record_telemetry_event(payload)
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        return JSONResponse({"ok": True, "event": event}, status_code=202)

    return app


def _render(request: Request, template_name: str, context: dict[str, Any]) -> HTMLResponse:
    settings: HubSettings = request.app.state.settings
    shell_context = {
        "instance_name": settings.instance_name,
        "public_url": settings.public_url,
        "current_version": __version__,
        "request_path": request.url.path,
    }
    return _TEMPLATES.TemplateResponse(
        request=request,
        name=template_name,
        context={**shell_context, **context},
    )
