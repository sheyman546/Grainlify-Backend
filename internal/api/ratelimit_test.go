package api

import (
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/gofiber/fiber/v2"
	"github.com/gofiber/fiber/v2/utils"
	"github.com/jagadeesh/grainlify/backend/internal/config"
	"github.com/valyala/fasthttp"
)

func TestRateLimitMiddleware_AuthRoutesPerIP(t *testing.T) {
	cfg := config.Config{
		RateLimitAuthPerMin:   2,
		RateLimitPublicPerMin: 3,
		TrustedProxies:        []string{"127.0.0.1"},
	}

	app := newRateLimitTestApp(cfg, "/auth/nonce")

	for i := 0; i < 2; i++ {
		status := rateLimitTestRequest(t, app, "/auth/nonce", "203.0.113.10", "")
		if status != fiber.StatusOK {
			t.Fatalf("request %d status = %d, want %d", i+1, status, fiber.StatusOK)
		}
	}

	status := rateLimitTestRequest(t, app, "/auth/nonce", "203.0.113.10", "")
	if status != fiber.StatusTooManyRequests {
		t.Fatalf("exceeding request status = %d, want %d", status, fiber.StatusTooManyRequests)
	}
}

func TestRateLimitMiddleware_PublicRoutesUseHigherBucket(t *testing.T) {
	cfg := config.Config{
		RateLimitAuthPerMin:   2,
		RateLimitPublicPerMin: 3,
		TrustedProxies:        []string{"127.0.0.1"},
	}

	app := newRateLimitTestApp(cfg, "/projects")

	for i := 0; i < 3; i++ {
		status := rateLimitTestRequest(t, app, "/projects", "203.0.113.20", "")
		if status != fiber.StatusOK {
			t.Fatalf("request %d status = %d, want %d", i+1, status, fiber.StatusOK)
		}
	}
}

func TestRateLimitMiddleware_TrustedProxyForwardedFor(t *testing.T) {
	cfg := config.Config{
		RateLimitAuthPerMin:   1,
		RateLimitPublicPerMin: 3,
		TrustedProxies:        []string{"127.0.0.1"},
	}

	app := newRateLimitTestApp(cfg, "/auth/nonce")

	firstStatus := rateLimitTestRequestFromRemote(t, app, "/auth/nonce", "127.0.0.1", "198.51.100.10")
	if firstStatus != fiber.StatusOK {
		t.Fatalf("first request status = %d, want %d", firstStatus, fiber.StatusOK)
	}

	secondStatus := rateLimitTestRequestFromRemote(t, app, "/auth/nonce", "127.0.0.1", "198.51.100.11")
	if secondStatus != fiber.StatusOK {
		t.Fatalf("second request status = %d, want %d", secondStatus, fiber.StatusOK)
	}
}

func TestRateLimitMiddleware_BurstThenRefillWithoutSleeping(t *testing.T) {
	const (
		limit         = 2
		baseTimestamp = uint32(1_900_000_000)
	)

	cfg := config.Config{
		RateLimitAuthPerMin: limit,
		TrustedProxies:      []string{"127.0.0.1"},
	}
	app := newRateLimitTestApp(cfg, "/auth/nonce")

	for i := 0; i < limit; i++ {
		setRateLimitTestTimestamp(baseTimestamp)
		status := rateLimitTestRequest(t, app, "/auth/nonce", "203.0.113.30", "")
		if status != fiber.StatusOK {
			t.Fatalf("request %d inside burst status = %d, want %d", i+1, status, fiber.StatusOK)
		}
	}

	setRateLimitTestTimestamp(baseTimestamp)
	status := rateLimitTestRequest(t, app, "/auth/nonce", "203.0.113.30", "")
	if status != fiber.StatusTooManyRequests {
		t.Fatalf("request immediately after burst status = %d, want %d", status, fiber.StatusTooManyRequests)
	}

	setRateLimitTestTimestamp(baseTimestamp + 59)
	status = rateLimitTestRequest(t, app, "/auth/nonce", "203.0.113.30", "")
	if status != fiber.StatusTooManyRequests {
		t.Fatalf("request one second before refill status = %d, want %d", status, fiber.StatusTooManyRequests)
	}

	setRateLimitTestTimestamp(baseTimestamp + 60)
	status = rateLimitTestRequest(t, app, "/auth/nonce", "203.0.113.30", "")
	if status != fiber.StatusOK {
		t.Fatalf("request exactly at refill boundary status = %d, want %d", status, fiber.StatusOK)
	}
}

func TestRateLimitMiddleware_PerKeyIsolation(t *testing.T) {
	cfg := config.Config{
		RateLimitAuthPerMin: 1,
		TrustedProxies:      []string{"127.0.0.1"},
	}
	app := newRateLimitTestApp(cfg, "/auth/nonce")
	setRateLimitTestTimestamp(1_900_000_100)

	firstClientStatus := rateLimitTestRequestFromRemote(t, app, "/auth/nonce", "127.0.0.1", "203.0.113.40")
	if firstClientStatus != fiber.StatusOK {
		t.Fatalf("first client initial request status = %d, want %d", firstClientStatus, fiber.StatusOK)
	}

	firstClientExceededStatus := rateLimitTestRequestFromRemote(t, app, "/auth/nonce", "127.0.0.1", "203.0.113.40")
	if firstClientExceededStatus != fiber.StatusTooManyRequests {
		t.Fatalf("first client second request status = %d, want %d", firstClientExceededStatus, fiber.StatusTooManyRequests)
	}

	secondClientStatus := rateLimitTestRequestFromRemote(t, app, "/auth/nonce", "127.0.0.1", "203.0.113.41")
	if secondClientStatus != fiber.StatusOK {
		t.Fatalf("second client initial request status = %d, want %d", secondClientStatus, fiber.StatusOK)
	}
}

func TestRateLimitMiddleware_ExactConfiguredLimitBoundary(t *testing.T) {
	const limit = 3

	cfg := config.Config{
		RateLimitPublicPerMin: limit,
		TrustedProxies:        []string{"127.0.0.1"},
	}
	app := newRateLimitTestApp(cfg, "/projects")
	setRateLimitTestTimestamp(1_900_000_200)

	for i := 0; i < limit; i++ {
		status := rateLimitTestRequest(t, app, "/projects", "203.0.113.50", "")
		if status != fiber.StatusOK {
			t.Fatalf("request %d of configured limit status = %d, want %d", i+1, status, fiber.StatusOK)
		}
	}

	status := rateLimitTestRequest(t, app, "/projects", "203.0.113.50", "")
	if status != fiber.StatusTooManyRequests {
		t.Fatalf("request %d over configured limit status = %d, want %d", limit+1, status, fiber.StatusTooManyRequests)
	}
}

func newRateLimitTestApp(cfg config.Config, route string) *fiber.App {
	app := fiber.New(fiber.Config{
		ProxyHeader:             fiber.HeaderXForwardedFor,
		EnableTrustedProxyCheck: true,
		TrustedProxies:          cfg.TrustedProxies,
	})
	app.Use(NewRateLimitMiddleware(cfg))
	app.Get(route, func(c *fiber.Ctx) error {
		return c.SendStatus(fiber.StatusOK)
	})
	return app
}

func rateLimitTestRequest(t *testing.T, app *fiber.App, path, remoteIP, forwardedFor string) int {
	t.Helper()

	req := httptest.NewRequest(http.MethodGet, path, nil)
	req.RemoteAddr = fmt.Sprintf("%s:12345", remoteIP)
	if forwardedFor != "" {
		req.Header.Set(fiber.HeaderXForwardedFor, forwardedFor)
	}

	resp, err := app.Test(req)
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	return resp.StatusCode
}

// rateLimitTestRequestFromRemote fires a single GET directly against the
// app's compiled fasthttp handler with a genuinely-controlled remote
// address. app.Test() round-trips the request through httputil.DumpRequest
// into a raw HTTP byte stream, which does NOT carry Go's
// http.Request.RemoteAddr field — it's local request metadata, not part of
// the wire protocol — so every request through app.Test() lands with the
// same fake remote address regardless of what RemoteAddr is set to. Use
// this instead of rateLimitTestRequest whenever a test depends on the
// middleware seeing a specific real remote IP (e.g. to assert trusted-proxy
// behavior), rather than just on X-Forwarded-For (which travels as a real
// header and does work via app.Test()).
func rateLimitTestRequestFromRemote(t *testing.T, app *fiber.App, path, remoteIP, forwardedFor string) int {
	t.Helper()

	var req fasthttp.Request
	req.Header.SetMethod(fiber.MethodGet)
	req.SetRequestURI(path)
	if forwardedFor != "" {
		req.Header.Set(fiber.HeaderXForwardedFor, forwardedFor)
	}

	ctx := new(fasthttp.RequestCtx)
	ctx.Init(&req, &net.TCPAddr{IP: net.ParseIP(remoteIP), Port: 12345}, nil)

	app.Handler()(ctx)
	return ctx.Response.StatusCode()
}

func setRateLimitTestTimestamp(ts uint32) {
	atomic.StoreUint32(&utils.Timestamp, ts)
}

// ── Ecosystems-specific path routing tests ────────────────────────────────────
// These complement the handler-level tests in internal/handlers/ecosystems_public_test.go
// by directly exercising rateLimitModeForPath at the routing layer so we have a
// unit-level assertion that both /ecosystems (list) and /ecosystems/:id (detail)
// resolve to rateLimitModePublic and not rateLimitModeNone.

func TestRateLimitModeForPath_EcosystemsListIsPublic(t *testing.T) {
	if got := rateLimitModeForPath("/ecosystems"); got != rateLimitModePublic {
		t.Fatalf("rateLimitModeForPath(%q) = %v, want rateLimitModePublic", "/ecosystems", got)
	}
}

func TestRateLimitModeForPath_EcosystemsDetailIsPublic(t *testing.T) {
	paths := []string{
		"/ecosystems/00000000-0000-0000-0000-000000000001",
		"/ecosystems/some-slug",
		"/ecosystems/abc",
	}
	for _, p := range paths {
		if got := rateLimitModeForPath(p); got != rateLimitModePublic {
			t.Errorf("rateLimitModeForPath(%q) = %v, want rateLimitModePublic", p, got)
		}
	}
}

// TestRateLimitMiddleware_EcosystemsListTriggered429 is an integration-style
// test that fires the full middleware stack against /ecosystems and asserts 429.
func TestRateLimitMiddleware_EcosystemsListTriggered429(t *testing.T) {
	const limit = 2
	cfg := config.Config{
		RateLimitPublicPerMin: limit,
		TrustedProxies:        []string{"127.0.0.1"},
	}
	app := newRateLimitTestApp(cfg, "/ecosystems")
	setRateLimitTestTimestamp(1_900_200_000)

	clientIP := "203.0.113.90"
	for i := 0; i < limit; i++ {
		if s := rateLimitTestRequest(t, app, "/ecosystems", clientIP, ""); s != fiber.StatusOK {
			t.Fatalf("request %d: got %d, want 200", i+1, s)
		}
	}
	if s := rateLimitTestRequest(t, app, "/ecosystems", clientIP, ""); s != fiber.StatusTooManyRequests {
		t.Fatalf("request limit+1: got %d, want 429", s)
	}
}

// TestRateLimitMiddleware_EcosystemsDetailTriggered429 is the critical regression
// guard: the detail path /ecosystems/:id must also trigger 429.  Before the fix
// it resolved to rateLimitModeNone and was completely unprotected.
func TestRateLimitMiddleware_EcosystemsDetailTriggered429(t *testing.T) {
	const limit = 2
	cfg := config.Config{
		RateLimitPublicPerMin: limit,
		TrustedProxies:        []string{"127.0.0.1"},
	}
	// Use a dynamic route so Fiber resolves :id correctly.
	app := fiber.New(fiber.Config{
		ProxyHeader:             fiber.HeaderXForwardedFor,
		EnableTrustedProxyCheck: true,
		TrustedProxies:          cfg.TrustedProxies,
	})
	app.Use(NewRateLimitMiddleware(cfg))
	app.Get("/ecosystems/:id", func(c *fiber.Ctx) error {
		return c.SendStatus(fiber.StatusOK)
	})

	setRateLimitTestTimestamp(1_900_200_100)
	clientIP := "203.0.113.91"
	detailPath := "/ecosystems/00000000-0000-0000-0000-000000000099"

	for i := 0; i < limit; i++ {
		if s := rateLimitTestRequest(t, app, detailPath, clientIP, ""); s != fiber.StatusOK {
			t.Fatalf("request %d on detail: got %d, want 200", i+1, s)
		}
	}
	if s := rateLimitTestRequest(t, app, detailPath, clientIP, ""); s != fiber.StatusTooManyRequests {
		t.Fatalf("request limit+1 on detail: got %d, want 429", s)
	}
}
