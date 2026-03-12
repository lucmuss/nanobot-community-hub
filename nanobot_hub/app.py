"""FastAPI application for nanobot-community-hub."""

from __future__ import annotations

import html
import re
import os
import hmac
import secrets
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from starlette.middleware.sessions import SessionMiddleware

from nanobot_hub import __version__
from nanobot_hub.auth import HubAuthService, HubAdminUser
from nanobot_hub.store import HubStore


_TEMPLATES = Jinja2Templates(directory=str(Path(__file__).resolve().parent / "templates"))


@dataclass(slots=True)
class HubSettings:
    database_url: str
    public_url: str = ""
    instance_name: str = "nanobot-community-hub"
    session_secret: str = ""
    api_token: str = ""


def create_app() -> FastAPI:
    _TEMPLATES.env.globals["render_markdown"] = _render_markdown_preview
    database_url = os.getenv("NANOBOT_HUB_DATABASE_URL", "").strip()
    if not database_url:
        db_path = Path(os.getenv("NANOBOT_HUB_DB_PATH", "./data/nanobot-community-hub.sqlite3")).expanduser()
        database_url = f"sqlite:///{db_path}"
    settings = HubSettings(
        database_url=database_url,
        public_url=os.getenv("NANOBOT_HUB_PUBLIC_URL", "").strip(),
        instance_name=os.getenv("NANOBOT_HUB_INSTANCE_NAME", "nanobot-community-hub").strip() or "nanobot-community-hub",
        session_secret=os.getenv("NANOBOT_HUB_SESSION_SECRET", "").strip() or secrets.token_urlsafe(48),
        api_token=os.getenv("NANOBOT_HUB_API_TOKEN", "").strip(),
    )
    store = HubStore(settings.database_url)
    store.init()
    auth_service = HubAuthService(store.engine)

    app = FastAPI(title="nanobot-community-hub", version=__version__)
    app.add_middleware(
        SessionMiddleware,
        secret_key=settings.session_secret,
        session_cookie="nanobot_hub_session",
        same_site="lax",
        https_only=False,
    )
    app.mount("/static", StaticFiles(directory=str(Path(__file__).resolve().parent / "static")), name="static")
    app.state.settings = settings
    app.state.store = store
    app.state.auth_service = auth_service

    def render_discover(
        request: Request,
        *,
        q: str = "",
        category: str = "",
        language: str = "",
        runtime: str = "",
        min_reliability: int = 0,
        sort: str = "trending",
        status_code: int = 200,
    ) -> HTMLResponse:
        runtime_settings = store.get_runtime_settings()
        items = store.list_mcps(
            search=q.strip(),
            category=category.strip(),
            language=language.strip(),
            runtime=runtime.strip(),
            min_reliability=min_reliability,
            sort=sort.strip(),
        )
        return _render(
            request,
            "discover.html",
            {
                "title": "Discover MCP",
                "nav_active": "discover",
                "query": q.strip(),
                "category": category.strip(),
                "language": language.strip(),
                "runtime": runtime.strip(),
                "min_reliability": max(0, min(100, int(min_reliability or 0))),
                "sort": sort.strip() or "trending",
                "categories": store.categories(),
                "languages": store.languages(),
                "runtime_options": store.runtime_options(),
                "reliability_options": [0, 80, 90, 95],
                "items": items,
                "overview": store.get_overview_stats(),
                "local_gui_url": str(runtime_settings.get("default_gui_url", "")).strip(),
            },
            status_code=status_code,
        )

    @app.get("/", response_class=HTMLResponse)
    async def root() -> RedirectResponse:
        if not auth_service.has_admin():
            return RedirectResponse("/setup/admin", status_code=302)
        return RedirectResponse("/discover", status_code=302)

    @app.get("/health")
    async def health() -> dict[str, Any]:
        runtime_settings = store.get_runtime_settings()
        return {
            "ok": True,
            "service": settings.instance_name,
            "version": __version__,
            "public_url": settings.public_url,
            "database_backend": store.backend,
            "has_admin": auth_service.has_admin(),
            "admin_write_api": bool(settings.api_token),
            "runtime_settings": runtime_settings,
        }

    @app.get("/setup/admin", response_class=HTMLResponse)
    async def setup_admin_page(request: Request) -> HTMLResponse:
        admin = _current_admin(request, auth_service)
        if auth_service.has_admin():
            if admin is not None:
                return RedirectResponse("/admin", status_code=303)
            return RedirectResponse("/login", status_code=303)
        return _render(
            request,
            "setup_admin.html",
            {
                "title": "Create Admin",
                "hide_shell": True,
            },
        )

    @app.post("/setup/admin", response_class=HTMLResponse)
    async def setup_admin_submit(request: Request) -> HTMLResponse:
        if auth_service.has_admin():
            return RedirectResponse("/login", status_code=303)
        form = await request.form()
        username = str(form.get("username", "")).strip()
        email = str(form.get("email", "")).strip()
        password = str(form.get("password", ""))
        password_confirm = str(form.get("password_confirm", ""))
        if password != password_confirm:
            return _render(
                request,
                "setup_admin.html",
                {
                    "title": "Create Admin",
                    "hide_shell": True,
                    "error": "Passwords do not match.",
                    "form_data": {"username": username, "email": email},
                },
                status_code=400,
            )
        try:
            admin = auth_service.create_admin(username=username, email=email, password=password)
        except ValueError as exc:
            return _render(
                request,
                "setup_admin.html",
                {
                    "title": "Create Admin",
                    "hide_shell": True,
                    "error": str(exc),
                    "form_data": {"username": username, "email": email},
                },
                status_code=400,
            )
        request.session["hub_admin_id"] = admin.id
        _set_flash(request, "Hub admin created.")
        return RedirectResponse("/admin", status_code=303)

    @app.get("/login", response_class=HTMLResponse)
    async def login_page(request: Request) -> HTMLResponse:
        if not auth_service.has_admin():
            return RedirectResponse("/setup/admin", status_code=303)
        admin = _current_admin(request, auth_service)
        if admin is not None:
            return RedirectResponse("/admin", status_code=303)
        return _render(
            request,
            "login.html",
            {
                "title": "Hub Login",
                "hide_shell": True,
            },
        )

    @app.post("/login", response_class=HTMLResponse)
    async def login_submit(request: Request) -> HTMLResponse:
        if not auth_service.has_admin():
            return RedirectResponse("/setup/admin", status_code=303)
        form = await request.form()
        identifier = str(form.get("identifier", "")).strip()
        password = str(form.get("password", ""))
        admin = auth_service.authenticate(identifier, password)
        if admin is None:
            return _render(
                request,
                "login.html",
                {
                    "title": "Hub Login",
                    "hide_shell": True,
                    "error": "Invalid login credentials.",
                    "form_data": {"identifier": identifier},
                },
                status_code=400,
            )
        request.session["hub_admin_id"] = admin.id
        _set_flash(request, "Logged in successfully.")
        return RedirectResponse("/admin", status_code=303)

    @app.post("/logout")
    async def logout(request: Request) -> RedirectResponse:
        request.session.clear()
        return RedirectResponse("/login", status_code=303)

    @app.get("/admin", response_class=HTMLResponse)
    async def admin_page(request: Request) -> HTMLResponse:
        admin = _require_admin(request, auth_service)
        if admin is None:
            return RedirectResponse("/login", status_code=303)
        mcp_queue = store.list_mcp_moderation_queue()
        stack_queue = store.list_stack_moderation_queue()
        showcase_queue = store.list_showcase_moderation_queue()
        recent_submissions = store.list_recent_mcp_submissions()
        error_hotspots = store.list_error_hotspots()
        overview = store.get_overview_stats()
        runtime_settings = store.get_runtime_settings()
        return _render(
            request,
            "admin.html",
            {
                "title": "Hub Admin",
                "nav_active": "admin",
                "admin_user": admin,
                "mcp_queue": mcp_queue,
                "stack_queue": stack_queue,
                "showcase_queue": showcase_queue,
                "recent_submissions": recent_submissions,
                "error_hotspots": error_hotspots,
                "admin_overview": {
                    "mcp_queue": len(mcp_queue),
                    "stack_queue": len(stack_queue),
                    "showcase_queue": len(showcase_queue),
                    "recent_submissions": len(recent_submissions),
                    "error_hotspots": len(error_hotspots),
                    "registry_count": int(overview.get("registry_count", 0) or 0),
                    "runs_today": int(overview.get("runs_today", 0) or 0),
                    "telemetry_active": bool(overview.get("telemetry_active")),
                },
                "ops_info": {
                    "instance_name": settings.instance_name,
                    "public_url": settings.public_url,
                    "database_backend": store.backend,
                    "api_write_enabled": bool(settings.api_token),
                    "has_admin": auth_service.has_admin(),
                    "network_health": overview.get("network_health", {}),
                    "top_category": overview.get("top_category", ""),
                },
                "runtime_settings": runtime_settings,
                "mcp_form": {
                    "repo_url": str(request.query_params.get("repo_url", "")).strip(),
                    "name": str(request.query_params.get("name", "")).strip(),
                    "description": str(request.query_params.get("description", "")).strip(),
                    "category": str(request.query_params.get("category", "")).strip(),
                    "install_method": str(request.query_params.get("install_method", "")).strip(),
                    "tags": str(request.query_params.get("tags", "")).strip(),
                },
                "stack_form": {
                    "title": "",
                    "description": "",
                    "use_case": "",
                    "recommended_model": "",
                    "example_prompt": "",
                    "items": "",
                    "is_public": False,
                },
                "showcase_form": {
                    "title": "",
                    "description": "",
                    "use_case": "",
                    "category": "",
                    "example_prompt": "",
                    "stack_slug": "",
                    "is_public": False,
                },
                "all_stacks": store.list_stacks(include_private=True),
            },
        )

    @app.post("/discover/submit-mcp", response_class=HTMLResponse)
    async def submit_mcp_page(request: Request) -> HTMLResponse:
        admin = _require_admin(request, auth_service)
        if admin is None:
            return RedirectResponse("/login", status_code=303)
        form = await request.form()
        payload = {
            "repo_url": str(form.get("repo_url", "")).strip(),
            "name": str(form.get("name", "")).strip(),
            "description": str(form.get("description", "")).strip(),
            "category": str(form.get("category", "")).strip(),
            "install_method": str(form.get("install_method", "")).strip(),
            "tags": _split_csv(str(form.get("tags", "")).strip()),
            "submitted_by": admin.username,
        }
        try:
            result = store.submit_mcp_submission(payload)
        except ValueError as exc:
            _set_flash(request, str(exc), level="error")
            return RedirectResponse("/admin", status_code=303)
        _set_flash(
            request,
            f"MCP '{result['item']['name']}' {'published' if result.get('created') else 'already existed in'} the hub.",
        )
        return RedirectResponse("/admin", status_code=303)

    @app.post("/admin/settings")
    async def admin_update_settings(request: Request) -> RedirectResponse:
        admin = _require_admin(request, auth_service)
        if admin is None:
            return RedirectResponse("/login", status_code=303)
        form = await request.form()
        payload = {
            "telemetry_ingest_enabled": bool(form.get("telemetry_ingest_enabled")),
            "api_token_writes_enabled": bool(form.get("api_token_writes_enabled")),
            "recommendation_mode": str(form.get("recommendation_mode", "balanced")).strip(),
            "featured_min_trust_score": str(form.get("featured_min_trust_score", "7.5")).strip(),
            "featured_min_signal_count": str(form.get("featured_min_signal_count", "3")).strip(),
            "discover_cache_ttl_seconds": str(form.get("discover_cache_ttl_seconds", "20")).strip(),
            "overview_cache_ttl_seconds": str(form.get("overview_cache_ttl_seconds", "30")).strip(),
            "default_gui_url": str(form.get("default_gui_url", "")).strip(),
        }
        try:
            store.update_runtime_settings(payload)
        except (TypeError, ValueError) as exc:
            _set_flash(request, f"Settings update failed: {exc}", level="error")
            return RedirectResponse("/admin", status_code=303)
        _set_flash(request, "Hub runtime settings saved.")
        return RedirectResponse("/admin", status_code=303)

    @app.get("/discover", response_class=HTMLResponse)
    async def discover_page(
        request: Request,
        q: str = Query(""),
        category: str = Query(""),
        language: str = Query(""),
        runtime: str = Query(""),
        min_reliability: int = Query(0),
        sort: str = Query("trending"),
    ) -> HTMLResponse:
        return render_discover(
            request,
            q=q,
            category=category,
            language=language,
            runtime=runtime,
            min_reliability=min_reliability,
            sort=sort,
        )

    @app.get("/partials/discover-results", response_class=HTMLResponse)
    async def discover_results(
        request: Request,
        q: str = Query(""),
        category: str = Query(""),
        language: str = Query(""),
        runtime: str = Query(""),
        min_reliability: int = Query(0),
        sort: str = Query("trending"),
    ) -> HTMLResponse:
        items = store.list_mcps(
            search=q.strip(),
            category=category.strip(),
            language=language.strip(),
            runtime=runtime.strip(),
            min_reliability=min_reliability,
            sort=sort.strip(),
        )
        runtime_settings = store.get_runtime_settings()
        return _render(
            request,
            "partials/discover_results.html",
            {
                "items": items,
                "local_gui_url": str(runtime_settings.get("default_gui_url", "")).strip(),
            },
        )

    @app.get("/mcp/{slug}", response_class=HTMLResponse)
    async def mcp_detail_page(request: Request, slug: str) -> HTMLResponse:
        item = store.get_mcp(slug)
        if item is None:
            raise HTTPException(status_code=404, detail="MCP server not found.")
        runtime_settings = store.get_runtime_settings()
        return _render(
            request,
            "mcp_detail.html",
            {
                "title": item["name"],
                "nav_active": "discover",
                "item": item,
                "fixes": store.get_mcp_fix_suggestions(slug),
                "local_gui_url": str(runtime_settings.get("default_gui_url", "")).strip(),
            },
        )

    @app.post("/mcp/{slug}/vote")
    async def mcp_vote_submit(request: Request, slug: str) -> RedirectResponse:
        form = await request.form()
        vote_type = str(form.get("vote_type", "")).strip().lower()
        voter_key = _get_or_create_voter_key(request)
        try:
            store.record_vote("mcp", slug, voter_key, vote_type)
        except ValueError as exc:
            _set_flash(request, str(exc), level="error")
        else:
            _set_flash(request, "Community vote saved.")
        response = RedirectResponse(f"/mcp/{slug}", status_code=303)
        _attach_voter_cookie(response, voter_key)
        return response

    @app.get("/stacks", response_class=HTMLResponse)
    async def stacks_page(request: Request, q: str = Query("")) -> HTMLResponse:
        items = store.list_stacks(search=q.strip())
        runtime_settings = store.get_runtime_settings()
        return _render(
            request,
            "stacks.html",
            {
                "title": "MCP Stacks",
                "nav_active": "stacks",
                "query": q.strip(),
                "items": items,
                "local_gui_url": str(runtime_settings.get("default_gui_url", "")).strip(),
            },
        )

    @app.get("/partials/stacks-results", response_class=HTMLResponse)
    async def stack_results(request: Request, q: str = Query("")) -> HTMLResponse:
        runtime_settings = store.get_runtime_settings()
        return _render(
            request,
            "partials/stacks_results.html",
            {
                "items": store.list_stacks(search=q.strip()),
                "local_gui_url": str(runtime_settings.get("default_gui_url", "")).strip(),
            },
        )

    @app.get("/stacks/{slug}", response_class=HTMLResponse)
    async def stack_detail_page(request: Request, slug: str) -> HTMLResponse:
        item = store.get_stack(slug)
        if item is None:
            raise HTTPException(status_code=404, detail="Stack not found.")
        runtime_settings = store.get_runtime_settings()
        return _render(
            request,
            "stack_detail.html",
            {
                "title": item["title"],
                "nav_active": "stacks",
                "item": item,
                "local_gui_url": str(runtime_settings.get("default_gui_url", "")).strip(),
            },
        )

    @app.post("/stacks/{slug}/vote")
    async def stack_vote_submit(request: Request, slug: str) -> RedirectResponse:
        form = await request.form()
        vote_type = str(form.get("vote_type", "")).strip().lower()
        voter_key = _get_or_create_voter_key(request)
        try:
            store.record_vote("stack", slug, voter_key, vote_type)
        except ValueError as exc:
            _set_flash(request, str(exc), level="error")
        else:
            _set_flash(request, "Community vote saved.")
        response = RedirectResponse(f"/stacks/{slug}", status_code=303)
        _attach_voter_cookie(response, voter_key)
        return response

    @app.get("/showcase", response_class=HTMLResponse)
    async def showcase_page(
        request: Request,
        q: str = Query(""),
        category: str = Query(""),
    ) -> HTMLResponse:
        runtime_settings = store.get_runtime_settings()
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
                "local_gui_url": str(runtime_settings.get("default_gui_url", "")).strip(),
            },
        )

    @app.get("/partials/showcase-results", response_class=HTMLResponse)
    async def showcase_results(
        request: Request,
        q: str = Query(""),
        category: str = Query(""),
    ) -> HTMLResponse:
        runtime_settings = store.get_runtime_settings()
        return _render(
            request,
            "partials/showcase_results.html",
            {
                "items": store.list_showcase(search=q.strip(), category=category.strip()),
                "local_gui_url": str(runtime_settings.get("default_gui_url", "")).strip(),
            },
        )

    @app.get("/showcase/{slug}", response_class=HTMLResponse)
    async def showcase_detail_page(request: Request, slug: str) -> HTMLResponse:
        item = store.get_showcase(slug)
        if item is None:
            raise HTTPException(status_code=404, detail="Showcase entry not found.")
        runtime_settings = store.get_runtime_settings()
        return _render(
            request,
            "showcase_detail.html",
            {
                "title": item["title"],
                "nav_active": "showcase",
                "item": item,
                "local_gui_url": str(runtime_settings.get("default_gui_url", "")).strip(),
            },
        )

    @app.get("/community-stats", response_class=HTMLResponse)
    async def community_stats_page(request: Request) -> HTMLResponse:
        runtime_settings = store.get_runtime_settings()
        return _render(
            request,
            "community_stats.html",
            {
                "title": "Community Stats",
                "nav_active": "stats",
                "overview": store.get_overview_stats(),
                "local_gui_url": str(runtime_settings.get("default_gui_url", "")).strip(),
            },
        )

    @app.post("/admin/submit/stack")
    async def admin_submit_stack(request: Request) -> RedirectResponse:
        admin = _require_admin(request, auth_service)
        if admin is None:
            return RedirectResponse("/login", status_code=303)
        form = await request.form()
        payload = {
            "title": str(form.get("title", "")).strip(),
            "description": str(form.get("description", "")).strip(),
            "use_case": str(form.get("use_case", "")).strip(),
            "recommended_model": str(form.get("recommended_model", "")).strip(),
            "example_prompt": str(form.get("example_prompt", "")).strip(),
            "items": _split_csv(str(form.get("items", "")).strip()),
            "is_public": bool(form.get("is_public")),
            "created_by": admin.username,
        }
        try:
            result = store.create_stack_submission(payload)
        except ValueError as exc:
            _set_flash(request, str(exc), level="error")
            return RedirectResponse("/admin", status_code=303)
        _set_flash(request, f"Stack '{result['item']['title']}' saved.")
        return RedirectResponse("/admin", status_code=303)

    @app.post("/admin/submit/showcase")
    async def admin_submit_showcase(request: Request) -> RedirectResponse:
        admin = _require_admin(request, auth_service)
        if admin is None:
            return RedirectResponse("/login", status_code=303)
        form = await request.form()
        payload = {
            "title": str(form.get("title", "")).strip(),
            "description": str(form.get("description", "")).strip(),
            "use_case": str(form.get("use_case", "")).strip(),
            "category": str(form.get("category", "")).strip(),
            "example_prompt": str(form.get("example_prompt", "")).strip(),
            "stack_slug": str(form.get("stack_slug", "")).strip(),
            "is_public": bool(form.get("is_public")),
            "created_by": admin.username,
        }
        try:
            result = store.create_showcase_submission(payload)
        except ValueError as exc:
            _set_flash(request, str(exc), level="error")
            return RedirectResponse("/admin", status_code=303)
        _set_flash(request, f"Showcase '{result['item']['title']}' saved.")
        return RedirectResponse("/admin", status_code=303)

    @app.post("/admin/moderate/mcp/{slug}")
    async def admin_moderate_mcp(request: Request, slug: str) -> RedirectResponse:
        admin = _require_admin(request, auth_service)
        if admin is None:
            return RedirectResponse("/login", status_code=303)
        form = await request.form()
        action = str(form.get("action", "")).strip()
        try:
            item = store.moderate_mcp(slug, action=action)
        except ValueError as exc:
            _set_flash(request, str(exc), level="error")
            return RedirectResponse("/admin", status_code=303)
        _set_flash(request, f"MCP '{item['name']}' updated to {item['status']}.")
        return RedirectResponse("/admin", status_code=303)

    @app.post("/admin/moderate/stack/{slug}")
    async def admin_moderate_stack(request: Request, slug: str) -> RedirectResponse:
        admin = _require_admin(request, auth_service)
        if admin is None:
            return RedirectResponse("/login", status_code=303)
        form = await request.form()
        action = str(form.get("action", "")).strip()
        try:
            item = store.moderate_stack(slug, action=action)
        except ValueError as exc:
            _set_flash(request, str(exc), level="error")
            return RedirectResponse("/admin", status_code=303)
        _set_flash(request, f"Stack '{item['title']}' updated to {item['status']}.")
        return RedirectResponse("/admin", status_code=303)

    @app.post("/admin/moderate/showcase/{slug}")
    async def admin_moderate_showcase(request: Request, slug: str) -> RedirectResponse:
        admin = _require_admin(request, auth_service)
        if admin is None:
            return RedirectResponse("/login", status_code=303)
        form = await request.form()
        action = str(form.get("action", "")).strip()
        try:
            item = store.moderate_showcase(slug, action=action)
        except ValueError as exc:
            _set_flash(request, str(exc), level="error")
            return RedirectResponse("/admin", status_code=303)
        _set_flash(request, f"Showcase '{item['title']}' updated to {item['status']}.")
        return RedirectResponse("/admin", status_code=303)

    @app.get("/api/v1/health")
    async def api_health() -> dict[str, Any]:
        return await health()

    @app.get("/api/v1/marketplace")
    async def api_marketplace(
        q: str = Query(""),
        category: str = Query(""),
        language: str = Query(""),
        runtime: str = Query(""),
        min_reliability: int = Query(0),
        sort: str = Query("trending"),
    ) -> dict[str, Any]:
        return {
            "items": store.list_mcps(
                search=q.strip(),
                category=category.strip(),
                language=language.strip(),
                runtime=runtime.strip(),
                min_reliability=min_reliability,
                sort=sort.strip(),
            ),
            "query": q.strip(),
            "category": category.strip(),
            "language": language.strip(),
            "runtime": runtime.strip(),
            "min_reliability": max(0, min(100, int(min_reliability or 0))),
            "sort": sort.strip() or "trending",
            "categories": store.categories(),
            "languages": store.languages(),
            "runtime_options": store.runtime_options(),
            "reliability_options": [0, 80, 90, 95],
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

    @app.post("/api/v1/marketplace/{slug}/vote")
    async def api_marketplace_vote(request: Request, slug: str) -> JSONResponse:
        payload = await request.json()
        vote_type = str(payload.get("vote_type", "")).strip().lower()
        voter_key = str(payload.get("voter_key", "")).strip() or _get_or_create_voter_key(request)
        try:
            result = store.record_vote("mcp", slug, voter_key, vote_type)
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        response = JSONResponse({"ok": True, "result": result}, status_code=202)
        _attach_voter_cookie(response, voter_key)
        return response

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

    @app.post("/api/v1/stacks/{slug}/vote")
    async def api_stack_vote(request: Request, slug: str) -> JSONResponse:
        payload = await request.json()
        vote_type = str(payload.get("vote_type", "")).strip().lower()
        voter_key = str(payload.get("voter_key", "")).strip() or _get_or_create_voter_key(request)
        try:
            result = store.record_vote("stack", slug, voter_key, vote_type)
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        response = JSONResponse({"ok": True, "result": result}, status_code=202)
        _attach_voter_cookie(response, voter_key)
        return response

    @app.get("/api/v1/showcase")
    async def api_showcase(q: str = Query(""), category: str = Query("")) -> dict[str, Any]:
        return {
            "items": store.list_showcase(search=q.strip(), category=category.strip()),
            "categories": store.showcase_categories(),
        }

    @app.get("/api/v1/showcase/{slug}")
    async def api_showcase_detail(slug: str) -> dict[str, Any]:
        item = store.get_showcase(slug)
        if item is None:
            raise HTTPException(status_code=404, detail="Showcase entry not found.")
        return item

    @app.get("/api/v1/stats/overview")
    async def api_stats_overview() -> dict[str, Any]:
        return store.get_overview_stats()

    @app.get("/api/v1/marketplace/{slug}/fixes")
    async def api_marketplace_fixes(
        slug: str,
        error_code: str = Query(""),
        current_transport: str = Query(""),
        current_timeout: int = Query(0),
        missing_runtimes: str = Query(""),
    ) -> dict[str, Any]:
        item = store.get_mcp(slug)
        if item is None:
            raise HTTPException(status_code=404, detail="MCP server not found.")
        fixes = store.get_mcp_fix_suggestions(
            slug,
            error_code=error_code,
            current_transport=current_transport,
            current_timeout=current_timeout,
            missing_runtimes=_split_csv(missing_runtimes),
        )
        return {"slug": slug, "fixes": fixes}

    @app.post("/api/v1/submissions/mcp")
    async def api_submit_mcp(request: Request) -> JSONResponse:
        _require_write_access(request, auth_service, settings, store)
        payload = await request.json()
        try:
            result = store.submit_mcp_submission(payload)
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        return JSONResponse(result, status_code=201 if result.get("created") else 200)

    @app.post("/api/v1/submissions/stack")
    async def api_submit_stack(request: Request) -> JSONResponse:
        _require_write_access(request, auth_service, settings, store)
        payload = await request.json()
        try:
            result = store.create_stack_submission(payload)
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        return JSONResponse(result, status_code=201)

    @app.post("/api/v1/submissions/showcase")
    async def api_submit_showcase(request: Request) -> JSONResponse:
        _require_write_access(request, auth_service, settings, store)
        payload = await request.json()
        try:
            result = store.create_showcase_submission(payload)
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        return JSONResponse(result, status_code=201)

    @app.post("/api/v1/marketplace/{slug}/installs")
    async def api_mark_install(slug: str) -> JSONResponse:
        try:
            result = store.increment_mcp_install(slug)
        except ValueError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        return JSONResponse({"ok": True, "result": result}, status_code=202)

    @app.post("/api/v1/stacks/{slug}/imports")
    async def api_mark_stack_import(slug: str) -> JSONResponse:
        try:
            result = store.increment_stack_import(slug)
        except ValueError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        return JSONResponse({"ok": True, "result": result}, status_code=202)

    @app.post("/api/v1/showcase/{slug}/imports")
    async def api_mark_showcase_import(slug: str) -> JSONResponse:
        try:
            result = store.increment_showcase_import(slug)
        except ValueError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        return JSONResponse({"ok": True, "result": result}, status_code=202)

    @app.post("/api/v1/telemetry/events")
    async def api_telemetry_event(request: Request) -> JSONResponse:
        runtime_settings = store.get_runtime_settings()
        if not bool(runtime_settings.get("telemetry_ingest_enabled", True)):
            raise HTTPException(status_code=403, detail="Telemetry ingest is disabled by hub settings.")
        payload = await request.json()
        try:
            event = store.record_telemetry_event(payload)
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        return JSONResponse({"ok": True, "event": event}, status_code=202)

    return app


def _render(request: Request, template_name: str, context: dict[str, Any], status_code: int = 200) -> HTMLResponse:
    settings: HubSettings = request.app.state.settings
    auth_service: HubAuthService = request.app.state.auth_service
    admin = _current_admin(request, auth_service)
    shell_context = {
        "instance_name": settings.instance_name,
        "public_url": settings.public_url,
        "current_version": __version__,
        "request_path": request.url.path,
        "hub_admin": admin,
        "has_hub_admin": auth_service.has_admin(),
        "flash": _get_flash(request),
    }
    return _TEMPLATES.TemplateResponse(
        request=request,
        name=template_name,
        context={**shell_context, **context},
        status_code=status_code,
    )


def _split_csv(value: str) -> list[str]:
    return [item.strip() for item in str(value or "").split(",") if item.strip()]


def _render_markdown_preview(content: str) -> str:
    """Render a small markdown subset for hub descriptions."""
    return _render_markdown_html(content, empty_html="<p class='muted'>No description yet.</p>")


def _render_markdown_html(content: str, *, empty_html: str) -> str:
    raw = str(content or "").replace("\r\n", "\n").strip()
    if not raw:
        return empty_html

    blocks: list[str] = []
    paragraph: list[str] = []
    list_items: list[str] = []
    in_code_block = False
    code_lines: list[str] = []

    def flush_paragraph() -> None:
        nonlocal paragraph
        if paragraph:
            blocks.append(f"<p>{_render_inline_markdown(' '.join(paragraph))}</p>")
            paragraph = []

    def flush_list() -> None:
        nonlocal list_items
        if list_items:
            blocks.append("<ul>" + "".join(list_items) + "</ul>")
            list_items = []

    def flush_code_block() -> None:
        nonlocal code_lines
        escaped = html.escape("\n".join(code_lines))
        blocks.append(f"<pre><code>{escaped}</code></pre>")
        code_lines = []

    for raw_line in raw.split("\n"):
        line = raw_line.rstrip()
        stripped = line.strip()

        if stripped.startswith("```"):
            flush_paragraph()
            flush_list()
            if in_code_block:
                flush_code_block()
                in_code_block = False
            else:
                in_code_block = True
            continue

        if in_code_block:
            code_lines.append(line)
            continue

        if not stripped:
            flush_paragraph()
            flush_list()
            continue

        heading = re.sub(r"^#{1,3}\s+", "", stripped)
        if heading != stripped and stripped.startswith("#"):
            flush_paragraph()
            flush_list()
            level = min(4, max(2, len(stripped) - len(stripped.lstrip("#")) + 1))
            blocks.append(f"<h{level}>{_render_inline_markdown(heading)}</h{level}>")
            continue

        if stripped.startswith(("- ", "* ")):
            flush_paragraph()
            list_items.append(f"<li>{_render_inline_markdown(stripped[2:].strip())}</li>")
            continue

        paragraph.append(stripped)

    if in_code_block:
        flush_code_block()
    flush_paragraph()
    flush_list()
    return "".join(blocks) if blocks else empty_html


def _render_inline_markdown(text: str) -> str:
    escaped = html.escape(str(text or ""))
    escaped = re.sub(r"`([^`]+)`", r"<code>\1</code>", escaped)
    escaped = re.sub(r"\*\*([^*]+)\*\*", r"<strong>\1</strong>", escaped)
    escaped = re.sub(r"(?<!\*)\*([^*]+)\*(?!\*)", r"<em>\1</em>", escaped)
    escaped = re.sub(
        r"\[([^\]]+)\]\((https?://[^)\s]+)\)",
        lambda match: (
            f'<a href="{html.escape(match.group(2), quote=True)}" target="_blank" rel="noreferrer">'
            f"{match.group(1)}</a>"
        ),
        escaped,
    )
    return escaped


def _current_admin(request: Request, auth_service: HubAuthService) -> HubAdminUser | None:
    session_admin_id = request.session.get("hub_admin_id")
    try:
        admin_id = int(session_admin_id) if session_admin_id is not None else None
    except (TypeError, ValueError):
        admin_id = None
    return auth_service.get_admin(admin_id)


def _require_admin(request: Request, auth_service: HubAuthService) -> HubAdminUser | None:
    if not auth_service.has_admin():
        return None
    return _current_admin(request, auth_service)


def _set_flash(request: Request, message: str, *, level: str = "info") -> None:
    request.session["hub_flash"] = {"message": message, "level": level}


def _get_flash(request: Request) -> dict[str, str]:
    flash = request.session.pop("hub_flash", None)
    if not isinstance(flash, dict):
        return {}
    message = str(flash.get("message", "")).strip()
    level = str(flash.get("level", "info")).strip() or "info"
    if not message:
        return {}
    return {"message": message, "level": level}


def _require_write_access(request: Request, auth_service: HubAuthService, settings: HubSettings, store: HubStore) -> None:
    if _current_admin(request, auth_service) is not None:
        return
    runtime_settings = store.get_runtime_settings()
    if not bool(runtime_settings.get("api_token_writes_enabled", True)):
        raise HTTPException(status_code=403, detail="Service-to-service writes are disabled by hub settings.")
    token = _extract_api_token(request)
    if settings.api_token and token and hmac.compare_digest(token, settings.api_token):
        return
    raise HTTPException(status_code=403, detail="Admin authentication required.")


def _extract_api_token(request: Request) -> str:
    auth_header = str(request.headers.get("authorization", "")).strip()
    if auth_header.lower().startswith("bearer "):
        return auth_header[7:].strip()
    return str(request.headers.get("x-nanobot-hub-token", "")).strip()


def _get_or_create_voter_key(request: Request) -> str:
    existing = str(request.cookies.get("nanobot_hub_voter", "")).strip()
    if existing:
        return existing
    return secrets.token_urlsafe(18)


def _attach_voter_cookie(response: JSONResponse | RedirectResponse, voter_key: str) -> None:
    if not str(voter_key or "").strip():
        return
    response.set_cookie(
        key="nanobot_hub_voter",
        value=str(voter_key).strip(),
        max_age=60 * 60 * 24 * 365,
        httponly=False,
        samesite="lax",
    )
