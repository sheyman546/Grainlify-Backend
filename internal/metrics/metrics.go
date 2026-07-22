// Package metrics provides Prometheus instrumentation for the Grainlify backend.
// It exposes HTTP latency histograms, sync_jobs queue depth gauges, and NATS
// publish failure counters.
package metrics

import (
	"regexp"
	"strconv"
	"time"

	"github.com/gofiber/fiber/v2"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/valyala/fasthttp/fasthttpadaptor"
)

// pathParamRE matches UUID and numeric path segments so they can be replaced
// with a placeholder to avoid unbounded label cardinality.
var pathParamRE = regexp.MustCompile(`/[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}|/\d+`)

var (
	// HTTPRequestDuration tracks request latency, labeled by normalized route,
	// HTTP method, and status class (2xx, 4xx, 5xx, …).
	HTTPRequestDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "grainlify",
		Subsystem: "http",
		Name:      "request_duration_seconds",
		Help:      "HTTP request latency in seconds.",
		Buckets:   prometheus.DefBuckets,
	}, []string{"route", "method", "status_class"})

	// SyncJobsQueueDepth is a gauge set periodically to the number of pending
	// sync_jobs rows in the database.
	SyncJobsQueueDepth = promauto.NewGauge(prometheus.GaugeOpts{
		Namespace: "grainlify",
		Subsystem: "syncjobs",
		Name:      "queue_depth",
		Help:      "Number of sync_jobs rows with status='pending'.",
	})

	// SyncJobsProcessed counts successfully completed sync jobs.
	SyncJobsProcessed = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: "grainlify",
		Subsystem: "syncjobs",
		Name:      "processed_total",
		Help:      "Total number of sync jobs completed successfully.",
	})

	// SyncJobsFailed counts sync jobs that ended in a failed state.
	SyncJobsFailed = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: "grainlify",
		Subsystem: "syncjobs",
		Name:      "failed_total",
		Help:      "Total number of sync jobs that failed.",
	})

	// SyncJobsConsecutiveFailures exposes the current consecutive failure count
	// for each sync job row so alerting can detect repositories needing attention.
	SyncJobsConsecutiveFailures = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "grainlify",
		Subsystem: "syncjobs",
		Name:      "consecutive_failures",
		Help:      "Current consecutive failure count for a sync job.",
	}, []string{"job_id", "project_id", "job_type"})

	// WebhooksReceived counts incoming GitHub webhook requests.
	WebhooksReceived = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: "grainlify",
		Subsystem: "webhooks",
		Name:      "received_total",
		Help:      "Total number of GitHub webhook requests received.",
	})

	// NATSPublishFailures counts bus.Publish errors for webhook events.
	NATSPublishFailures = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: "grainlify",
		Subsystem: "webhooks",
		Name:      "nats_publish_failures_total",
		Help:      "Total number of NATS publish failures for GitHub webhook events.",
	})

	// LandingStatsCache counts cache hits and misses for the public landing stats endpoint.
	LandingStatsCache = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "grainlify",
		Subsystem: "stats",
		Name:      "landing_cache_total",
		Help:      "Total number of landing stats cache lookups by result.",
	}, []string{"result"})

	// RepoMetadataCache counts cache hits and misses for GitHub repo-metadata lookups.
	// The "result" label is either "hit" or "miss".
	// A "bypass" label value is recorded when the caller explicitly requests a fresh
	// fetch, so hit/miss ratios for normal traffic can be computed independently of
	// forced refreshes.
	RepoMetadataCache = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "grainlify",
		Subsystem: "github",
		Name:      "repo_metadata_cache_total",
		Help:      "Total number of GitHub repo-metadata cache lookups by result (hit, miss, bypass).",
	}, []string{"result"})
)

// NormalizePath replaces dynamic path segments (UUIDs, numeric IDs) with ":id"
// to keep label cardinality bounded.
func NormalizePath(path string) string {
	return pathParamRE.ReplaceAllString(path, "/:id")
}

// StatusClass converts an HTTP status code to a string like "2xx", "4xx", etc.
func StatusClass(code int) string {
	return strconv.Itoa(code/100) + "xx"
}

// LatencyMiddleware is a Fiber middleware that records HTTP request duration in
// the HTTPRequestDuration histogram.
func LatencyMiddleware() fiber.Handler {
	return func(c *fiber.Ctx) error {
		start := time.Now()
		err := c.Next()
		elapsed := time.Since(start).Seconds()

		route := NormalizePath(c.Path())
		method := c.Method()
		status := c.Response().StatusCode()

		HTTPRequestDuration.WithLabelValues(route, method, StatusClass(status)).Observe(elapsed)
		return err
	}
}

// Handler returns a Fiber handler that serves the Prometheus text exposition format.
// It should be mounted at /metrics and protected by an internal token or network policy.
func Handler() fiber.Handler {
	h := fasthttpadaptor.NewFastHTTPHandler(promhttp.Handler())
	return func(c *fiber.Ctx) error {
		h(c.Context())
		return nil
	}
}

// TokenGate returns a Fiber middleware that requires the caller to supply the
// configured bearer token via the Authorization header. If token is empty the
// middleware is a no-op (useful when /metrics is protected at the network level).
func TokenGate(token string) fiber.Handler {
	return func(c *fiber.Ctx) error {
		if token == "" {
			return c.Next()
		}
		auth := c.Get(fiber.HeaderAuthorization)
		if auth == "Bearer "+token {
			return c.Next()
		}
		return c.Status(fiber.StatusUnauthorized).JSON(fiber.Map{"error": "unauthorized"})
	}
}
