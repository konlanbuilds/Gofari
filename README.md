# Gofari

A Postgres-backed background job queue for Go.

See [docs/PLAN.md](docs/PLAN.md) for the product scope, milestones, database
design, failure scenarios, and design review. See
[docs/DECISIONS.md](docs/DECISIONS.md) for the resolved implementation contract,
SQL semantics, API details, and configuration defaults.

Configuration is environment-based (`GOFARI_*`). Copy `.env.example` to `.env`
and load it into your shell (for example `set -a; source .env; set +a`); the
commands do not read `.env` automatically.
