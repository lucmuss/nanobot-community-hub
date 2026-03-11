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

## Runtime Settings Center

The Hub admin page now includes a lightweight persistent runtime settings center.

These values are stored in the Hub database so they survive container restarts and do not require a redeploy for every operational tweak.

Current runtime settings:

- `telemetry_ingest_enabled`
- `api_token_writes_enabled`
- `recommendation_mode`
- `featured_min_trust_score`
- `featured_min_signal_count`
- `discover_cache_ttl_seconds`
- `overview_cache_ttl_seconds`

This keeps infrastructure-level configuration in environment variables, while allowing day-to-day operational tuning from the admin UI.

Examples:

- temporarily disable telemetry ingest without changing container env
- disable service-token writes while keeping admin moderation access
- switch recommendation behavior between a more `balanced` and more `conservative` profile
- tune Discover and overview cache TTLs for larger registries

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

## Performance Notes

The current V1 backend is still intentionally simple, but the hot paths are no longer purely per-item query driven.

Current optimizations:

- Discover results are cached for a short configurable TTL
- overview stats are cached for a short configurable TTL
- tools and recommendations are prefetched in batches for list pages
- error clusters are built in grouped queries instead of one query per MCP card
- cache invalidation is triggered automatically on submissions, moderation actions, install/import counters, and telemetry writes

For the current V1 scale this keeps the Hub responsive without introducing Redis or a background worker yet.

If the registry becomes much larger later, the next step should be materialized daily stats tables or a lightweight background aggregation job.

## Recommendation Logic

The Hub recommendation system is deliberately lightweight and explainable.

Today it combines:

- seeded recommended configs
- recent telemetry profiles
- Bayesian-smoothed success scoring
- instance and run counts
- latency penalties for unstable profiles

The recommendation source can be:

- `seed`
- `telemetry`
- a merged result when both sources exist

This is not a full recommendation engine yet, but it is already much more stable than a naive "highest success rate wins" rule because it avoids over-trusting tiny sample sizes.

## Community Metrics

The Hub now treats the core marketplace signals as three separate metrics:

- `Reliability`
  - the smoothed success rate of MCP executions
  - this answers: "How often does this MCP work?"
- `Confidence`
  - a community evidence score based on 30-day run volume and independent instance diversity
  - this answers: "How much evidence do we have for this reliability number?"
- `Trust Score`
  - a broader quality score based on:
    - performance
    - config consensus
    - repair rate
    - verification status
  - this answers: "How safe is this MCP as a real ecosystem building block?"

Practical meaning:

- a new MCP can show `100% reliability` but still have low `confidence`
- a high `trust score` now requires more than raw success rate
- single-instance local bias is penalized explicitly in the confidence model

This keeps the marketplace from over-promoting brand new or fragile MCPs just because they worked in one local environment.

## Ranking and Featured Lists

Marketplace and overview lists are now more trust-aware than before.

Examples:

- `Most Reliable` is ranked primarily by `Trust Score`, then by reliability percent
- `Trending` now favors recent usage, but still gives significant weight to trust
- `Top MCPs` use the admin-configurable `featured_min_trust_score` threshold before falling back to the full registry

This keeps discovery useful for developers who care more about dependable setups than about raw install counts alone.

## Compatibility Graph

The Hub now derives `Common Combinations` from two sources:

- public stack co-occurrence
- successful telemetry co-usage across independent instances

This is still a lightweight V1 compatibility graph, but it is already stronger than a static curated list because it reflects both:

- what people publish as public stacks
- what people actually run together in practice

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
