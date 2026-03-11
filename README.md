# nanobot-community-hub

`nanobot-community-hub` is the first V1 cut of the Nanobot ecosystem backend.

It provides:

- MCP discovery and marketplace metadata
- MCP detail pages with tools, known issues, and recommended config
- community stats and overview metrics
- admin-controlled MCP, stack, and showcase submissions
- simple moderation for MCP, stack, and showcase visibility
- stack presets
- basic showcase entries
- telemetry ingest for anonymous MCP runtime signals

Tech stack:

- FastAPI
- Jinja2 templates
- HTMX
- PostgreSQL for the shared stack deployment
- SQLite fallback for local development and tests

## Run locally

```bash
uv venv
source .venv/bin/activate
uv pip install -e .[dev]
uvicorn nanobot_hub.app:create_app --factory --host 0.0.0.0 --port 18811
```

## Environment

- `NANOBOT_HUB_DATABASE_URL` preferred, for example: `postgresql+psycopg://user:pass@postgres:5432/nanobot_community_hub_db`
- `NANOBOT_HUB_DB_PATH` fallback for local SQLite mode: `./data/nanobot-community-hub.sqlite3`
- `NANOBOT_HUB_PUBLIC_URL` default: empty
- `NANOBOT_HUB_INSTANCE_NAME` default: `nanobot-community-hub`
- `NANOBOT_HUB_SESSION_SECRET` required for stable admin sessions in deployed environments
- `NANOBOT_HUB_API_TOKEN` optional but recommended for service-to-service write access from trusted GUI instances

In the deployed stack, `nanobot-community-hub` is expected to use the shared `apps-stack` PostgreSQL instance over the `apps-shared` Docker network.

## API

Base path:

```text
/api/v1
```

Important endpoints:

- `GET /api/v1/health`
- `GET /api/v1/marketplace`
- `GET /api/v1/marketplace/{slug}`
- `GET /api/v1/marketplace/{slug}/recommendation`
- `GET /api/v1/marketplace/resolve?repo_url=...`
- `POST /api/v1/submissions/mcp`
- `POST /api/v1/submissions/stack`
- `POST /api/v1/submissions/showcase`
- `GET /api/v1/stacks`
- `GET /api/v1/stacks/{slug}`
- `GET /api/v1/showcase`
- `GET /api/v1/stats/overview`
- `POST /api/v1/telemetry/events`

## Admin Authentication

The Hub now uses a single-admin bootstrap model similar to the WebGUI:

- first visit: `/setup/admin`
- later logins: `/login`
- internal moderation and write flows: `/admin`

The first admin is created once. After that, public users can browse discovery pages, while controlled write paths remain restricted to:

- logged-in Hub admins
- trusted service-to-service callers using `NANOBOT_HUB_API_TOKEN`

## Submission and Moderation Flow

The Hub now supports controlled write flows for:

- MCP repository entries
- MCP stacks
- showcase entries

Current rules:

- MCP submissions accept GitHub repository URLs only
- secret-like strings are rejected
- known repository URLs are deduplicated
- MCP submissions start as `verified=false`
- stacks and showcase entries can be created as `draft` or `published`
- moderation is intentionally simple in V1 through the Hub admin page

This keeps the first version small while still allowing:

- a real public marketplace
- admin-only curation
- service-to-service publishing from trusted Nanobot GUI instances

## GUI Integration

`nanobot-webgui` can now use the Hub for:

- `Install from Community`
- `Import Stack`
- community detail pages with recommended config
- publishing local MCP repositories into the Hub when the GUI instance is explicitly allowed to do so

For GUI-side publishing, two things must be true:

1. the GUI setting `Allow this GUI to publish MCP repository entries to the community hub` is enabled
2. the GUI instance has a valid `NANOBOT_GUI_COMMUNITY_API_TOKEN`

This keeps the read flows public and simple, while write flows remain controlled.
