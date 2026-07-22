package config

import (
	"encoding/base64"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"os"
	"strconv"
	"strings"
	"time"
)

type Config struct {
	// Env is the deployment environment (APP_ENV, default "dev").
	Env      string
	// HTTPAddr is the HTTP listen address (HTTP_ADDR, or :PORT if HTTP_ADDR is empty, default ":8080").
	HTTPAddr string
	// Log is the log level (LOG_LEVEL, default "info").
	Log      string

	// DBURL is the PostgreSQL connection string (DB_URL, required in non-dev environments).
	DBURL       string
	// AutoMigrate enables automatic database migration at startup (AUTO_MIGRATE, default false).
	AutoMigrate bool

	// DBMaxConns is the maximum number of connections in the pool (DB_MAX_CONNS, default 10).
	DBMaxConns int32
	// DBMinConns is the minimum number of idle connections kept open (DB_MIN_CONNS, default 0).
	DBMinConns int32
	// DBMaxConnLifetime is the maximum time a connection may be reused (DB_MAX_CONN_LIFETIME, default 30m).
	DBMaxConnLifetime time.Duration
	// DBMaxConnIdleTime is the maximum idle time before a connection is closed (DB_MAX_CONN_IDLE_TIME, default 5m).
	DBMaxConnIdleTime time.Duration

	// JWTSecret is the secret key for signing JWTs (JWT_SECRET, required ≥32 characters in non-dev).
	JWTSecret string

	// NATSURL is the NATS server URL (NATS_URL, optional, default "").
	NATSURL string

	// JetStream configuration for durable GitHub webhook event delivery.
	// JetStreamEnabled enables JetStream-backed publishing and consumption (JS_ENABLED, default false).
	JetStreamEnabled bool
	// JetStreamStreamName is the name of the JetStream stream (JS_STREAM_NAME, default "GITHUB_WEBHOOKS").
	JetStreamStreamName string
	// JetStreamConsumerName is the durable consumer name (JS_CONSUMER_NAME, default "grainlify-workers").
	JetStreamConsumerName string
	// JetStreamMaxDeliver is the maximum number of delivery attempts before dead-lettering (JS_MAX_DELIVER, default 5).
	JetStreamMaxDeliver int
	// JetStreamAckWait is how long the server waits for an ack before redelivering (JS_ACK_WAIT, default 30s).
	JetStreamAckWait time.Duration
	// JetStreamMaxAge is the maximum age of messages retained in the stream (JS_MAX_AGE, default 24h).
	JetStreamMaxAge time.Duration

	// GitHubOAuthClientID is the GitHub OAuth app client ID (GITHUB_OAUTH_CLIENT_ID, optional).
	GitHubOAuthClientID           string
	// GitHubOAuthClientSecret is the GitHub OAuth app client secret (GITHUB_OAUTH_CLIENT_SECRET, optional).
	GitHubOAuthClientSecret       string
	// GitHubOAuthRedirectURL is the full OAuth callback URL (GITHUB_OAUTH_REDIRECT_URL, optional).
	GitHubOAuthRedirectURL        string // Full callback URL (e.g., http://localhost:8080/auth/github/login/callback)
	// GitHubOAuthSuccessRedirectURL is the URL to redirect to after successful OAuth (GITHUB_OAUTH_SUCCESS_REDIRECT_URL, optional).
	GitHubOAuthSuccessRedirectURL string
	// GitHubLoginRedirectURL is the alternative callback URL (GITHUB_LOGIN_REDIRECT_URL, deprecated, use GitHubOAuthRedirectURL, optional).
	GitHubLoginRedirectURL        string // Alternative callback URL (deprecated, use GitHubOAuthRedirectURL)
	// GitHubLoginSuccessRedirectURL is the URL to redirect to after successful login (GITHUB_LOGIN_SUCCESS_REDIRECT_URL, optional).
	GitHubLoginSuccessRedirectURL string

	// GitHub App configuration (for organization installations)
	// GitHubAppID is the GitHub App ID (GITHUB_APP_ID, numeric, optional).
	GitHubAppID         string // GitHub App ID (numeric)
	// GitHubAppSlug is the GitHub App slug (GITHUB_APP_SLUG, e.g., "grainlify", optional).
	GitHubAppSlug       string // GitHub App slug (e.g., "grainlify")
	// GitHubAppPrivateKey is the GitHub App private key (GITHUB_APP_PRIVATE_KEY, PEM format, base64 encoded, optional).
	GitHubAppPrivateKey string // GitHub App private key (PEM format, base64 encoded)

	// GitHubWebhookSecret is used to validate GitHub webhook signatures (GITHUB_WEBHOOK_SECRET, X-Hub-Signature-256, optional).
	GitHubWebhookSecret string

	// PublicBaseURL is the public base URL of this backend (PUBLIC_BASE_URL, used when registering GitHub webhooks, optional).
	PublicBaseURL string

	// FrontendBaseURL is the frontend base URL (FRONTEND_BASE_URL, e.g., http://localhost:5173 or https://yourdomain.com, optional).
	// Used for OAuth redirects and CORS configuration
	FrontendBaseURL string

	// CORSOrigins is a comma-separated list of allowed CORS origins (CORS_ORIGINS, optional, defaults to FrontendBaseURL).
	// Example: "http://localhost:5173,https://grainlify.figma.site"
	CORSOrigins string

	// CORSAllowPreview enables wildcard matching for *.vercel.app and *.0xo.in origins (CORS_ALLOW_PREVIEW, default false).
	// Off by default; only enable when preview deployments need credentialed CORS access.
	CORSAllowPreview bool

	// TokenEncKeyB64 is used to encrypt stored OAuth access tokens at rest (TOKEN_ENC_KEY_B64, must be 32 bytes base64, required in non-dev).
	TokenEncKeyB64 string

	// AdminBootstrapToken is a dev/admin convenience shared token for promoting users to admin (ADMIN_BOOTSTRAP_TOKEN, optional).
	AdminBootstrapToken string

	// Didit KYC verification
	// DiditAPIKey is the Didit API key (DIDIT_API_KEY, optional).
	DiditAPIKey        string
	// DiditWorkflowID is the Didit workflow ID (DIDIT_WORKFLOW_ID, optional).
	DiditWorkflowID    string
	// DiditWebhookSecret is the Didit webhook secret (DIDIT_WEBHOOK_SECRET, optional).
	DiditWebhookSecret string

	// Soroban configuration
	// SorobanRPCURL is the Soroban RPC URL (SOROBAN_RPC_URL, optional as a group with other Soroban fields).
	SorobanRPCURL            string
	// SorobanNetworkPassphrase is the Soroban network passphrase (SOROBAN_NETWORK_PASSPHRASE, optional).
	SorobanNetworkPassphrase string
	// SorobanNetwork is "testnet" or "mainnet" (SOROBAN_NETWORK, default "testnet").
	SorobanNetwork           string // "testnet" or "mainnet"
	// SorobanSourceSecret is the Soroban source secret (SOROBAN_SOURCE_SECRET, optional as a group).
	SorobanSourceSecret      string
	// EscrowContractID is the escrow contract ID (ESCROW_CONTRACT_ID, optional as a group).
	EscrowContractID         string
	// ProgramEscrowContractID is the program escrow contract ID (PROGRAM_ESCROW_CONTRACT_ID, optional as a group).
	ProgramEscrowContractID  string
	// TokenContractID is the token contract ID (TOKEN_CONTRACT_ID, optional as a group).
	TokenContractID          string

	// SyncJobsMaxAttempts is the maximum number of attempts before a sync job is dead-lettered (SYNC_JOBS_MAX_ATTEMPTS, default 5).
	SyncJobsMaxAttempts int
	// SyncJobsBackoffBase is the base duration for exponential backoff between retries (SYNC_JOBS_BACKOFF_BASE, default 30s).
	SyncJobsBackoffBase time.Duration
	// SyncJobsBackoffMax is the maximum duration for exponential backoff between retries.
	// Controlled by SYNC_JOBS_BACKOFF_MAX, default 1h.
	SyncJobsBackoffMax time.Duration
	// SyncJobsFailureAttentionThreshold is the consecutive failure count after which
	// a sync job is marked dead and requires manual attention.
	// Controlled by SYNC_JOBS_FAILURE_ATTENTION_THRESHOLD, default 5.
	SyncJobsFailureAttentionThreshold int

	// ShutdownTimeout is the graceful shutdown drain window before forceful exit.
	// Controlled by SHUTDOWN_TIMEOUT, default 10s.
	ShutdownTimeout time.Duration

	// MaxBodyBytes is the maximum request body size in bytes (MAX_BODY_BYTES, default 1048576 / 1MB).
	MaxBodyBytes int
	// WebhookMaxBodyBytes is the maximum request body size in bytes for webhook routes
	// (WEBHOOK_MAX_BODY_BYTES, default 10485760 / 10MB).
	WebhookMaxBodyBytes int

	// MetricsToken is the bearer token required to access /metrics (METRICS_TOKEN, optional, default "").
	// If empty, the endpoint is unauthenticated — only acceptable when /metrics is firewalled at the network level.
	MetricsToken string

	// GitHubRepoMetadataCacheTTL is how long a cached GitHub repo-metadata response is
	// considered fresh before it is evicted and re-fetched (GITHUB_REPO_CACHE_TTL, default 60s).
	//
	// Security trade-off: a longer TTL reduces GitHub API traffic but may cause the
	// application to act on stale visibility/permission information (e.g. a repo that
	// was made private after the cache entry was written). Keep this value short (≤5min)
	// in production. Set to 0 to disable caching entirely.
	GitHubRepoMetadataCacheTTL time.Duration

	// RateLimitAuthPerMin is the per-minute limit for auth and webhook endpoints (RATE_LIMIT_AUTH_PER_MIN, default 60 requests/minute).
	RateLimitAuthPerMin int
	// RateLimitPublicPerMin is the per-minute limit for public read endpoints (RATE_LIMIT_PUBLIC_PER_MIN, default 300 requests/minute).
	RateLimitPublicPerMin int
	// TrustedProxies contains the IPs or CIDRs that are allowed to supply X-Forwarded-For values (TRUSTED_PROXIES, default "127.0.0.1,::1").
	TrustedProxies []string
}

func Load() Config {
	env := getEnv("APP_ENV", "dev")
	logLevel := getEnv("LOG_LEVEL", "info")

	// Prefer HTTP_ADDR if provided, otherwise build it from PORT.
	httpAddr := os.Getenv("HTTP_ADDR")
	if strings.TrimSpace(httpAddr) == "" {
		port := getEnv("PORT", "8080")
		httpAddr = ":" + port
	}

	return Config{
		Env:      env,
		HTTPAddr: httpAddr,
		Log:      logLevel,

		DBURL:       getEnv("DB_URL", ""),
		AutoMigrate: getEnvBool("AUTO_MIGRATE", false),

		DBMaxConns:        getEnvInt32("DB_MAX_CONNS", 10),
		DBMinConns:        getEnvInt32("DB_MIN_CONNS", 0),
		DBMaxConnLifetime: getEnvDuration("DB_MAX_CONN_LIFETIME", 30*time.Minute),
		DBMaxConnIdleTime: getEnvDuration("DB_MAX_CONN_IDLE_TIME", 5*time.Minute),

		JWTSecret: getEnv("JWT_SECRET", ""),

		NATSURL: getEnv("NATS_URL", ""),

		JetStreamEnabled:      getEnvBool("JS_ENABLED", false),
		JetStreamStreamName:   getEnv("JS_STREAM_NAME", "GITHUB_WEBHOOKS"),
		JetStreamConsumerName: getEnv("JS_CONSUMER_NAME", "grainlify-workers"),
		JetStreamMaxDeliver:   getEnvInt("JS_MAX_DELIVER", 5),
		JetStreamAckWait:      getEnvDuration("JS_ACK_WAIT", 30*time.Second),
		JetStreamMaxAge:       getEnvDuration("JS_MAX_AGE", 24*time.Hour),

		GitHubOAuthClientID:           getEnv("GITHUB_OAUTH_CLIENT_ID", ""),
		GitHubOAuthClientSecret:       getEnv("GITHUB_OAUTH_CLIENT_SECRET", ""),
		GitHubOAuthRedirectURL:        getEnv("GITHUB_OAUTH_REDIRECT_URL", ""),
		GitHubOAuthSuccessRedirectURL: getEnv("GITHUB_OAUTH_SUCCESS_REDIRECT_URL", ""),
		GitHubLoginRedirectURL:        getEnv("GITHUB_LOGIN_REDIRECT_URL", ""),
		GitHubLoginSuccessRedirectURL: getEnv("GITHUB_LOGIN_SUCCESS_REDIRECT_URL", ""),

		GitHubAppID:         getEnv("GITHUB_APP_ID", ""),
		GitHubAppSlug:       getEnv("GITHUB_APP_SLUG", ""),
		GitHubAppPrivateKey: getEnv("GITHUB_APP_PRIVATE_KEY", ""),

		GitHubWebhookSecret: getEnv("GITHUB_WEBHOOK_SECRET", ""),

		PublicBaseURL: getEnv("PUBLIC_BASE_URL", ""),

		FrontendBaseURL:  getEnv("FRONTEND_BASE_URL", ""),
		CORSOrigins:      getEnv("CORS_ORIGINS", ""),
		CORSAllowPreview: getEnvBool("CORS_ALLOW_PREVIEW", false),

		TokenEncKeyB64: getEnv("TOKEN_ENC_KEY_B64", ""),

		AdminBootstrapToken: strings.TrimSpace(getEnv("ADMIN_BOOTSTRAP_TOKEN", "")),

		DiditAPIKey:        getEnv("DIDIT_API_KEY", ""),
		DiditWorkflowID:    getEnv("DIDIT_WORKFLOW_ID", ""),
		DiditWebhookSecret: getEnv("DIDIT_WEBHOOK_SECRET", ""),

		// Soroban configuration
		SorobanRPCURL:            getEnv("SOROBAN_RPC_URL", ""),
		SorobanNetworkPassphrase: getEnv("SOROBAN_NETWORK_PASSPHRASE", ""),
		SorobanNetwork:           getEnv("SOROBAN_NETWORK", "testnet"),
		SorobanSourceSecret:      getEnv("SOROBAN_SOURCE_SECRET", ""),
		EscrowContractID:         getEnv("ESCROW_CONTRACT_ID", ""),
		ProgramEscrowContractID:  getEnv("PROGRAM_ESCROW_CONTRACT_ID", ""),
		TokenContractID:          getEnv("TOKEN_CONTRACT_ID", ""),

		SyncJobsMaxAttempts:               getEnvInt("SYNC_JOBS_MAX_ATTEMPTS", 5),
		SyncJobsBackoffBase:               getEnvDuration("SYNC_JOBS_BACKOFF_BASE", 30*time.Second),
		SyncJobsBackoffMax:                getEnvDuration("SYNC_JOBS_BACKOFF_MAX", time.Hour),
		SyncJobsFailureAttentionThreshold: getEnvInt("SYNC_JOBS_FAILURE_ATTENTION_THRESHOLD", 5),
		ShutdownTimeout:                   getEnvDuration("SHUTDOWN_TIMEOUT", 10*time.Second),

		MaxBodyBytes:          getEnvInt("MAX_BODY_BYTES", 1048576),
		WebhookMaxBodyBytes:   getEnvInt("WEBHOOK_MAX_BODY_BYTES", 10*1024*1024),
		RateLimitAuthPerMin:   getEnvInt("RATE_LIMIT_AUTH_PER_MIN", 60),
		RateLimitPublicPerMin: getEnvInt("RATE_LIMIT_PUBLIC_PER_MIN", 300),
		TrustedProxies:        parseTrustedProxies(getEnv("TRUSTED_PROXIES", "127.0.0.1,::1")),
		MetricsToken:          strings.TrimSpace(getEnv("METRICS_TOKEN", "")),
		GitHubRepoMetadataCacheTTL: getEnvDuration("GITHUB_REPO_CACHE_TTL", 60*time.Second),
	}
}

// IsDev reports whether the app runs in local development mode.
func (c Config) IsDev() bool {
	return strings.EqualFold(strings.TrimSpace(c.Env), "dev")
}

// Validate checks that all security-critical configuration values are present
// and well-formed. In non-dev environments it returns an error (never logging
// secret values) so that the process can exit with an actionable message
// before accepting any traffic.
//
// Rules applied in every environment:
//   - SorobanNetwork must be "testnet" or "mainnet" when set.
//   - HTTPAddr must be parseable as a TCP address.
//
// Additional rules applied outside dev:
//   - JWT_SECRET must be at least 32 characters.
//   - TOKEN_ENC_KEY_B64 must base64-decode to exactly 32 bytes (AES-256-GCM).
//   - DB_URL must be non-empty.
//   - When any Soroban field (SorobanRPCURL, SorobanSourceSecret, EscrowContractID,
//     ProgramEscrowContractID, TokenContractID) is set, all of them must be set.
func (c Config) Validate() error {
	var errs []string

	// --- HTTPAddr ---
	if addr := strings.TrimSpace(c.HTTPAddr); addr != "" {
		// Normalise ":8080" → "0.0.0.0:8080" for net.ResolveTCPAddr.
		if strings.HasPrefix(addr, ":") {
			addr = "0.0.0.0" + addr
		}
		if _, err := net.ResolveTCPAddr("tcp", addr); err != nil {
			errs = append(errs, fmt.Sprintf("HTTP_ADDR %q is not a valid TCP address: %v", c.HTTPAddr, err))
		}
	}

	// --- SorobanNetwork ---
	if net := strings.TrimSpace(c.SorobanNetwork); net != "" {
		if net != "testnet" && net != "mainnet" {
			errs = append(errs, fmt.Sprintf("SOROBAN_NETWORK must be \"testnet\" or \"mainnet\", got %q", net))
		}
	}

	if !c.IsDev() {
		// --- DB_URL ---
		if strings.TrimSpace(c.DBURL) == "" {
			errs = append(errs, "DB_URL is required in non-dev environments")
		}

		// --- JWT_SECRET ---
		if len(strings.TrimSpace(c.JWTSecret)) < 32 {
			errs = append(errs, "JWT_SECRET must be at least 32 characters (set JWT_SECRET)")
		}

		// --- TOKEN_ENC_KEY_B64 ---
		if strings.TrimSpace(c.TokenEncKeyB64) == "" {
			errs = append(errs, "TOKEN_ENC_KEY_B64 is required in non-dev environments (set TOKEN_ENC_KEY_B64)")
		} else {
			decoded, err := base64.StdEncoding.DecodeString(c.TokenEncKeyB64)
			if err != nil {
				// Try URL-safe variant as well.
				decoded, err = base64.URLEncoding.DecodeString(c.TokenEncKeyB64)
			}
			if err != nil {
				errs = append(errs, "TOKEN_ENC_KEY_B64 is not valid base64 (set TOKEN_ENC_KEY_B64 to a base64-encoded 32-byte key)")
			} else if len(decoded) != 32 {
				errs = append(errs, fmt.Sprintf("TOKEN_ENC_KEY_B64 must decode to exactly 32 bytes for AES-256-GCM (got %d bytes)", len(decoded)))
			}
		}

		// --- Soroban group: all-or-nothing ---
		sorobanFields := map[string]string{
			"SOROBAN_RPC_URL":            c.SorobanRPCURL,
			"SOROBAN_SOURCE_SECRET":      c.SorobanSourceSecret,
			"ESCROW_CONTRACT_ID":         c.EscrowContractID,
			"PROGRAM_ESCROW_CONTRACT_ID": c.ProgramEscrowContractID,
			"TOKEN_CONTRACT_ID":          c.TokenContractID,
		}
		anySet := false
		var missing []string
		for key, val := range sorobanFields {
			if strings.TrimSpace(val) != "" {
				anySet = true
			} else {
				missing = append(missing, key)
			}
		}
		if anySet && len(missing) > 0 {
			errs = append(errs, fmt.Sprintf(
				"incomplete Soroban configuration: when any Soroban field is set, all must be set; missing: %s",
				strings.Join(missing, ", "),
			))
		}
	}

	if len(errs) == 0 {
		return nil
	}
	return errors.New("invalid configuration:\n  - " + strings.Join(errs, "\n  - "))
}

func (c Config) LogLevel() slog.Leveler {
	switch strings.ToLower(strings.TrimSpace(c.Log)) {
	case "debug":
		return slog.LevelDebug
	case "warn", "warning":
		return slog.LevelWarn
	case "error":
		return slog.LevelError
	case "info", "":
		return slog.LevelInfo
	default:
		// Allow numeric levels for easy tweaking (-4 debug, 0 info, 4 warn, 8 error).
		if n, err := strconv.Atoi(c.Log); err == nil {
			return slog.Level(n)
		}
		return slog.LevelInfo
	}
}

func getEnv(key, fallback string) string {
	v := os.Getenv(key)
	if strings.TrimSpace(v) == "" {
		return fallback
	}
	return v
}

func getEnvInt32(key string, fallback int32) int32 {
	v := strings.TrimSpace(os.Getenv(key))
	if v == "" {
		return fallback
	}
	n, err := strconv.ParseInt(v, 10, 32)
	if err != nil || n <= 0 {
		return fallback
	}
	return int32(n)
}

func getEnvDuration(key string, fallback time.Duration) time.Duration {
	v := strings.TrimSpace(os.Getenv(key))
	if v == "" {
		return fallback
	}
	d, err := time.ParseDuration(v)
	if err != nil || d <= 0 {
		return fallback
	}
	return d
}

func getEnvBool(key string, fallback bool) bool {
	v := strings.ToLower(strings.TrimSpace(os.Getenv(key)))
	if v == "" {
		return fallback
	}
	switch v {
	case "1", "true", "t", "yes", "y", "on":
		return true
	case "0", "false", "f", "no", "n", "off":
		return false
	default:
		return fallback
	}
}

func getEnvInt(key string, fallback int) int {
	v := strings.TrimSpace(os.Getenv(key))
	if v == "" {
		return fallback
	}
	n, err := strconv.Atoi(v)
	if err != nil || n <= 0 {
		return fallback
	}
	return n
}

func parseTrustedProxies(value string) []string {
	parts := strings.Split(value, ",")
	proxies := make([]string, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part != "" {
			proxies = append(proxies, part)
		}
	}
	return proxies
}