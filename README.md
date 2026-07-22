# Grainlify Backend

[![CI](https://github.com/jagadeesh/Grainlify-Backend/actions/workflows/ci.yml/badge.svg)](https://github.com/jagadeesh/Grainlify-Backend/actions/workflows/ci.yml)

[![CI](https://github.com/jagadeesh/Grainlify-Backend/actions/workflows/ci.yml/badge.svg)](https://github.com/jagadeesh/Grainlify-Backend/actions/workflows/ci.yml)

Grainlify Backend is a Go-based API server that connects open-source developers with projects through GitHub integration, ecosystem tracking, and contribution management.

## Overview

Grainlify Backend provides:

- GitHub OAuth authentication
- GitHub App integration for repository management
- Project ecosystem organization (Starknet, Ethereum, etc.)
- User profile tracking with contribution statistics
- KYC verification via Didit integration
- Admin endpoints for ecosystem management
- GitHub webhooks for syncing issues and pull requests
- PostgreSQL database with migration support
- Optional NATS event bus for async processing
- Optional Redis for caching

## Tech Stack

| Component | Technology |
|-----------|------------|
| Language | Go 1.24+ |
| HTTP Framework | Fiber (fasthttp) |
| Database | PostgreSQL with pgx driver |
| Migrations | golang-migrate |
| Event Bus | NATS (optional) |
| Cache | Redis (optional) |
| Authentication | JWT + GitHub OAuth |
| KYC Provider | Didit |

## Architecture

```mermaid
flowchart TB

    FE["Frontend"]
    API["Backend API<br>Go + Fiber"]
    DB[(PostgreSQL)]
    GH[GitHub API / Webhooks]
    DIDIT[Didit KYC]

    FE --> API
    GH --> API
    DIDIT --> API
    API --> DB
```

## Project Structure

```text
Grainlify-Backend/
â”œâ”€â”€ cmd/
â”‚   â”œâ”€â”€ api/          # Main API server
â”‚   â”œâ”€â”€ migrate/      # Database migration runner
â”‚   â””â”€â”€ worker/       # Background worker (optional)
â”œâ”€â”€ internal/
â”‚   â”œâ”€â”€ api/          # HTTP handlers and routing
â”‚   â”œâ”€â”€ auth/         # JWT authentication
â”‚   â”œâ”€â”€ bus/          # Event bus interface (NATS)
â”‚   â”œâ”€â”€ config/       # Configuration management
â”‚   â”œâ”€â”€ db/           # Database connection
â”‚   â”œâ”€â”€ github/       # GitHub API client
â”‚   â”œâ”€â”€ handlers/     # HTTP endpoint handlers
â”‚   â”œâ”€â”€ soroban/      # Stellar blockchain integration
â”‚   â””â”€â”€ worker/       # Background job processing
â”œâ”€â”€ migrations/       # SQL migration files
â”œâ”€â”€ .env.example      # Environment variables template
â”œâ”€â”€ go.mod            # Go dependencies
â””â”€â”€ Makefile          # Build commands
```

## Core Features

### Authentication
- GitHub OAuth login/signup flow
- JWT token-based authentication
- Role-based access control (contributor, maintainer, admin)

### GitHub Integration
- GitHub App for repository management
- Webhook handling for issues and pull requests
- Automatic repository verification
- Project syncing with GitHub data

### Project Management
- Register GitHub repositories as projects
- Organize projects by ecosystems (Starknet, Ethereum, etc.)
- Project verification and webhook setup
- Issue and PR tracking

### User Profiles
- Contribution statistics
- Activity calendar (heatmap)
- Language and ecosystem breakdowns
- Paginated activity feed

### KYC Verification
- Didit integration for identity verification
- KYC session management
- Verification status tracking

### Admin Features
- Bootstrap first admin user
- Manage user roles
- Create and manage ecosystems
- View system statistics

## Getting Started

### Prerequisites

- Go 1.24+
- PostgreSQL 12+
- (Optional) NATS server
- (Optional) Redis server

### Installation

```bash
# Clone the repository
git clone https://github.com/jagadeesh/grainlify/backend.git
cd Grainlify-Backend

# Install dependencies
go mod download

# Copy environment template
cp .env.example .env

# Edit .env with your configuration
# Set DB_URL, GitHub OAuth credentials, etc.
```

### Running the Server

**Development with auto-reload (recommended):**
```bash
./run-dev.sh
# or
make dev
```

**Standard run:**
```bash
go run ./cmd/api
```

**Build binary:**
```bash
go build -o ./api ./cmd/api
./api
```

### Running Migrations

```bash
go run ./cmd/migrate
```

Migrations run automatically on startup if `AUTO_MIGRATE=true`.

## Configuration

Key environment variables (see `.env.example`):

```bash
# Database
DB_URL=postgresql://user:password@localhost/dbname
AUTO_MIGRATE=true

# Authentication
JWT_SECRET=your-secret-key-min-32-chars
ADMIN_BOOTSTRAP_TOKEN=your-bootstrap-token-min-32-chars

# GitHub OAuth
GITHUB_OAUTH_CLIENT_ID=your_client_id
GITHUB_OAUTH_CLIENT_SECRET=your_client_secret
GITHUB_OAUTH_REDIRECT_URL=http://localhost:8080/auth/github/callback
GITHUB_LOGIN_SUCCESS_REDIRECT_URL=http://localhost:5173

# GitHub App
GITHUB_APP_ID=123456
GITHUB_APP_SLUG=grainlify
GITHUB_APP_PRIVATE_KEY=<base64-encoded-private-key>
GITHUB_WEBHOOK_SECRET=your-webhook-secret

# URLs
PUBLIC_BASE_URL=http://localhost:8080
FRONTEND_BASE_URL=http://localhost:5173

# Optional Services
NATS_URL=nats://localhost:4222
REDIS_URL=redis://localhost:6379

# KYC (Didit)
DIDIT_API_KEY=your_didit_api_key
DIDIT_WORKFLOW_ID=your_workflow_id
```

## API Documentation

See [API Endpoints](docs/reference/api-endpoints.md) for the complete REST reference.

Interactive API docs are served at `/docs` (Swagger UI) and the raw OpenAPI 3.1 spec at `/openapi.yaml`.

- Local: http://localhost:8080/docs
- Spec: http://localhost:8080/openapi.yaml

## Deployment

### Railway

See [Railway Deployment](docs/deployment/railway.md) for detailed Railway deployment instructions.

### Other Platforms

1. Set environment variables
2. Run migrations: `go run ./cmd/migrate`
3. Build binary: `go build -o ./api ./cmd/api`
4. Start server: `./api`

## Operations

### Graceful Shutdown

Both `cmd/api` and `cmd/worker` use a cancelable root context for background
workers. On `SIGINT` or `SIGTERM`, the API process first stops accepting HTTP
requests, then cancels the sync worker context, waits for in-flight worker work
within the shutdown deadline, and only then lets deferred DB/NATS cleanup run.

The standalone worker process passes the same root context to the NATS webhook
consumer and sync worker. Canceling that context unsubscribes the consumer and
lets the sync worker finish or safely requeue work before `Close()` drains NATS
and closes the database pool.

## Development

### Running Tests

```bash
make test
# or directly:
go test -race -short ./...
```

### Fuzz Testing (XDR decode helpers)

The `internal/soroban` package includes Go native fuzz targets for every
exported `Decode*` function.  These guard against a remote-DoS scenario where
a misbehaving Soroban RPC endpoint returns truncated, type-confused, or
otherwise adversarial XDR that could panic a request handler or worker goroutine.

**Run the corpus seeds only (fast, no randomisation — identical to CI):**

```bash
go test ./internal/soroban/... -run 'TestDecodeNeverPanics|TestDecodeRoundTrip'
```

**30-second fuzz smoke run across all targets:**

```bash
go test ./internal/soroban/... -run='^$' -fuzz=FuzzDecode -fuzztime=30s
```

**Targeted long run (e.g. for the struct/map decoder):**

```bash
go test ./internal/soroban/... -run='^$' \
  -fuzz='^FuzzDecodeScValStruct$' \
  -fuzztime=5m
```

If the fuzzer finds a crash it writes a reproducer to
`testdata/fuzz/<TargetName>/<hash>`.  Commit that file as a regression test
and fix the underlying panic to return an error instead.

Fuzz targets and their coverage:

| Target | Decoder under test | Corpus seeds |
|---|---|---|
| `FuzzDecodeScValInt64` | `DecodeScValInt64` | 9 |
| `FuzzDecodeScValString` | `DecodeScValString` | 7 |
| `FuzzDecodeScValSymbol` | `DecodeScValSymbol` | 7 |
| `FuzzDecodeScValAddress` | `DecodeScValAddress` | 7 |
| `FuzzDecodeScValStruct` | `DecodeScValStruct` | 8 |
| `FuzzDecodeScValRoundTrip` | encode→unmarshal→decode round-trip | 6 |

### Code Style

- Standard Go formatting
- No ORM (use pgx directly)
- Minimal external dependencies
- Fast HTTP responses (no blocking calls in request path)

## Troubleshooting

See [Troubleshooting Guide](docs/troubleshooting/index.md) for common issues and solutions.

## Operations

### Health and Readiness Endpoints

The API exposes two standard observability endpoints:

- `GET /health` — Liveness probe. Always returns 200 with build metadata and uptime. Never reflects dependency state (safe for load-balancer health checks).
- `GET /ready` — Readiness probe. Returns 200 only when **all configured dependencies** (PostgreSQL, NATS) are healthy; returns 503 with a per-dependency breakdown otherwise.

**Example `/health` response:**

```json
{
  "ok": true,
  "service": "grainlify-api",
  "version": "1.2.3",
  "commit": "abc1234",
  "build_time": "2025-06-22T12:00:00Z",
  "uptime": "3h12m45s"
}
```

**Example `/ready` response (healthy):**

```json
{
  "ok": true,
  "deps": [
    { "name": "database", "ready": true, "status": "ok" },
    { "name": "nats", "ready": true, "status": "CONNECTED" }
  ]
}
```

**Example `/ready` response (degraded):**

```json
{
  "ok": false,
  "deps": [
    { "name": "database", "ready": true, "status": "ok" },
    { "name": "nats", "ready": false, "status": "DISCONNECTED" }
  ]
}
```

> **Security:** Neither endpoint leaks DB URLs, NATS credentials, JWT secrets, or internal hostnames. The health endpoint only returns build-time metadata and uptime.

### Build Metadata

Build metadata is injected at compile time via `-ldflags`. Example build command:

```sh
go build -ldflags="\
  -X main.Version=$(git describe --tags --always 2>/dev/null || echo dev) \
  -X main.Commit=$(git rev-parse --short HEAD) \
  -X main.BuildTime=$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
  -o bin/grainlify-api ./cmd/api
```

When these flags are omitted, the defaults (`dev`, `none`, `unknown`) are used.

## Additional Documentation

Full documentation index: **[docs/README.md](docs/README.md)**

- [Quick Start](docs/setup/quick-start.md) — Auto-reload development setup
- [Development Guide](docs/setup/development.md) — Dev commands and logging
- [GitHub App Setup](docs/github-app/setup.md) — GitHub App configuration
- [API Endpoints](docs/reference/api-endpoints.md) — Complete API reference

## License

[Add your license here]
