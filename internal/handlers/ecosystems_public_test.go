// Package handlers_test contains targeted rate-limit tests for the public
// ecosystems endpoints (/ecosystems and /ecosystems/:id).
//
// # Why these tests exist
//
// /ecosystems and /ecosystems/:id are unauthenticated, read-heavy endpoints.
// Without an explicit per-route assertion a global test could pass while the
// detail route (/ecosystems/:id) remained completely unprotected.  These tests
// verify the rate-limiter fires for *this specific route prefix* and cover the
// edge cases called out in the issue:
//
//   - Burst exactly at the limit boundary (last allowed request → 200, next → 429)
//   - Sustained traffic just under the limit (all pass)
//   - Limiter reset window boundary (requests pass again after the window expires)
//   - Proxy-spoofing: X-Forwarded-For is only honoured from a trusted proxy;
//     an untrusted remote cannot bump itself to a fresh bucket by forging the header
//
// Tests do not require a database: the Fiber app uses a nil-DB handler that
// returns 503 from real handler code (db_not_configured path), but the
// rate-limiter fires *before* the handler so the status we care about is 429,
// not 200.  For the "normal traffic passes" cases we register a stub handler
// on the same route so we can assert 200.
package handlers_test

import (
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/gofiber/fiber/v2"
	"github.com/gofiber/fiber/v2/utils"
	"github.com/jagadeesh/grainlify/backend/internal/api"
	"github.com/jagadeesh/grainlify/backend/internal/config"
	"github.com/valyala/fasthttp"
)

// ── helpers ───────────────────────────────────────────────────────────────────

// newEcosystemsRateLimitApp builds a minimal Fiber app that wires the real
// NewRateLimitMiddleware and registers stub 200 handlers on both public
// ecosystems routes.  No database is involved.
func newEcosystemsRateLimitApp(cfg config.Config) *fiber.App {
	app := fiber.New(fiber.Config{
		DisableStartupMessage:   true,
		ProxyHeader:             fiber.HeaderXForwardedFor,
		EnableTrustedProxyCheck: len(cfg.TrustedProxies) > 0,
		TrustedProxies:          cfg.TrustedProxies,
	})
	app.Use(api.NewRateLimitMiddleware(cfg))
	app.Get("/ecosystems", func(c *fiber.Ctx) error {
		return c.SendStatus(fiber.StatusOK)
	})
	app.Get("/ecosystems/:id", func(c *fiber.Ctx) error {
		return c.SendStatus(fiber.StatusOK)
	})
	return app
}

// doEcosystemsRequest fires a single GET to path, optionally setting
// X-Forwarded-For and overriding the TCP remote address.
func doEcosystemsRequest(t *testing.T, app *fiber.App, path, remoteIP, forwardedFor string) int {
	t.Helper()
	req := httptest.NewRequest(http.MethodGet, path, nil)
	req.RemoteAddr = fmt.Sprintf("%s:9999", remoteIP)
	if forwardedFor != "" {
		req.Header.Set(fiber.HeaderXForwardedFor, forwardedFor)
	}
	resp, err := app.Test(req)
	if err != nil {
		t.Fatalf("app.Test: %v", err)
	}
	return resp.StatusCode
}

// setEcosystemsTestTimestamp pins the Fiber utils.Timestamp so we can simulate
// the window boundary without sleeping.
func setEcosystemsTestTimestamp(ts uint32) {
	atomic.StoreUint32(&utils.Timestamp, ts)
}

// doEcosystemsRequestFromRemote fires a single GET directly against the app's
// compiled fasthttp handler with a genuinely-controlled remote address.
//
// app.Test() round-trips the request through httputil.DumpRequest into a raw
// HTTP byte stream, which does NOT carry Go's http.Request.RemoteAddr field —
// it's not part of the wire protocol, only local request metadata. Every
// request that goes through app.Test() therefore lands with the same fake
// "0.0.0.0:0" remote address regardless of what RemoteAddr is set to, which
// silently defeats any test that depends on distinguishing between two real
// remote IPs (as opposed to two X-Forwarded-For values, which travel as a
// real header and DO work via app.Test()). Invoking app.Handler() directly
// against a manually-built fasthttp.RequestCtx sidesteps that round trip
// entirely, so RemoteAddr is exactly what we set it to.
func doEcosystemsRequestFromRemote(t *testing.T, app *fiber.App, path, remoteIP, forwardedFor string) int {
	t.Helper()

	var req fasthttp.Request
	req.Header.SetMethod(fiber.MethodGet)
	req.SetRequestURI(path)
	if forwardedFor != "" {
		req.Header.Set(fiber.HeaderXForwardedFor, forwardedFor)
	}

	ctx := new(fasthttp.RequestCtx)
	ctx.Init(&req, &net.TCPAddr{IP: net.ParseIP(remoteIP), Port: 9999}, nil)

	app.Handler()(ctx)
	return ctx.Response.StatusCode()
}

// ── /ecosystems (list) tests ──────────────────────────────────────────────────

// TestEcosystemsPublic_ListRateLimit_BurstExactBoundary fires exactly cfg.Max
// requests (all should pass) then one more (must be 429).
func TestEcosystemsPublic_ListRateLimit_BurstExactBoundary(t *testing.T) {
	const limit = 3
	cfg := config.Config{
		RateLimitPublicPerMin: limit,
		TrustedProxies:        []string{"127.0.0.1"},
	}
	app := newEcosystemsRateLimitApp(cfg)
	setEcosystemsTestTimestamp(1_900_100_000)

	clientIP := "203.0.113.60"
	for i := 0; i < limit; i++ {
		status := doEcosystemsRequest(t, app, "/ecosystems", clientIP, "")
		if status != fiber.StatusOK {
			t.Fatalf("request %d/%d: got %d, want 200", i+1, limit, status)
		}
	}

	status := doEcosystemsRequest(t, app, "/ecosystems", clientIP, "")
	if status != fiber.StatusTooManyRequests {
		t.Fatalf("request limit+1: got %d, want 429", status)
	}
}

// TestEcosystemsPublic_ListRateLimit_NormalTrafficPasses verifies that a client
// making fewer requests than the limit is never throttled.
func TestEcosystemsPublic_ListRateLimit_NormalTrafficPasses(t *testing.T) {
	const limit = 5
	cfg := config.Config{
		RateLimitPublicPerMin: limit,
		TrustedProxies:        []string{"127.0.0.1"},
	}
	app := newEcosystemsRateLimitApp(cfg)
	setEcosystemsTestTimestamp(1_900_100_100)

	clientIP := "203.0.113.61"
	for i := 0; i < limit-1; i++ {
		status := doEcosystemsRequest(t, app, "/ecosystems", clientIP, "")
		if status != fiber.StatusOK {
			t.Fatalf("request %d: got %d, want 200", i+1, status)
		}
	}
}

// TestEcosystemsPublic_ListRateLimit_WindowReset verifies that the bucket
// refills after the 1-minute window expires (simulated via Timestamp).
func TestEcosystemsPublic_ListRateLimit_WindowReset(t *testing.T) {
	const limit = 2
	const baseTS = uint32(1_900_100_200)

	cfg := config.Config{
		RateLimitPublicPerMin: limit,
		TrustedProxies:        []string{"127.0.0.1"},
	}
	app := newEcosystemsRateLimitApp(cfg)
	clientIP := "203.0.113.62"

	// Exhaust the bucket at baseTS.
	setEcosystemsTestTimestamp(baseTS)
	for i := 0; i < limit; i++ {
		doEcosystemsRequest(t, app, "/ecosystems", clientIP, "")
	}
	if status := doEcosystemsRequest(t, app, "/ecosystems", clientIP, ""); status != fiber.StatusTooManyRequests {
		t.Fatalf("after burst: got %d, want 429", status)
	}

	// One second before the window boundary — still blocked.
	setEcosystemsTestTimestamp(baseTS + 59)
	if status := doEcosystemsRequest(t, app, "/ecosystems", clientIP, ""); status != fiber.StatusTooManyRequests {
		t.Fatalf("59 s in: got %d, want 429", status)
	}

	// Exactly at the window boundary — bucket refills.
	setEcosystemsTestTimestamp(baseTS + 60)
	if status := doEcosystemsRequest(t, app, "/ecosystems", clientIP, ""); status != fiber.StatusOK {
		t.Fatalf("after window reset: got %d, want 200", status)
	}
}

// ── /ecosystems/:id (detail) tests ───────────────────────────────────────────

// TestEcosystemsPublic_DetailRateLimit_BurstExactBoundary is the primary
// regression guard: the detail route was previously unprotected (path == "/ecosystems"
// did not match "/ecosystems/:id").  This test confirms 429 fires on the detail route.
func TestEcosystemsPublic_DetailRateLimit_BurstExactBoundary(t *testing.T) {
	const limit = 3
	cfg := config.Config{
		RateLimitPublicPerMin: limit,
		TrustedProxies:        []string{"127.0.0.1"},
	}
	app := newEcosystemsRateLimitApp(cfg)
	setEcosystemsTestTimestamp(1_900_100_300)

	clientIP := "203.0.113.63"
	detailPath := "/ecosystems/00000000-0000-0000-0000-000000000001"

	for i := 0; i < limit; i++ {
		status := doEcosystemsRequest(t, app, detailPath, clientIP, "")
		if status != fiber.StatusOK {
			t.Fatalf("request %d/%d on detail route: got %d, want 200", i+1, limit, status)
		}
	}

	status := doEcosystemsRequest(t, app, detailPath, clientIP, "")
	if status != fiber.StatusTooManyRequests {
		t.Fatalf("request limit+1 on detail route: got %d, want 429", status)
	}
}

// TestEcosystemsPublic_DetailRateLimit_NormalTrafficPasses verifies that
// well-behaved clients on the detail route are never throttled.
func TestEcosystemsPublic_DetailRateLimit_NormalTrafficPasses(t *testing.T) {
	const limit = 5
	cfg := config.Config{
		RateLimitPublicPerMin: limit,
		TrustedProxies:        []string{"127.0.0.1"},
	}
	app := newEcosystemsRateLimitApp(cfg)
	setEcosystemsTestTimestamp(1_900_100_400)

	clientIP := "203.0.113.64"
	detailPath := "/ecosystems/00000000-0000-0000-0000-000000000002"

	for i := 0; i < limit-1; i++ {
		status := doEcosystemsRequest(t, app, detailPath, clientIP, "")
		if status != fiber.StatusOK {
			t.Fatalf("request %d on detail route: got %d, want 200", i+1, status)
		}
	}
}

// TestEcosystemsPublic_DetailRateLimit_WindowReset verifies the bucket refills
// for the detail route after the window expires.
func TestEcosystemsPublic_DetailRateLimit_WindowReset(t *testing.T) {
	const limit = 2
	const baseTS = uint32(1_900_100_500)

	cfg := config.Config{
		RateLimitPublicPerMin: limit,
		TrustedProxies:        []string{"127.0.0.1"},
	}
	app := newEcosystemsRateLimitApp(cfg)
	clientIP := "203.0.113.65"
	detailPath := "/ecosystems/00000000-0000-0000-0000-000000000003"

	setEcosystemsTestTimestamp(baseTS)
	for i := 0; i < limit; i++ {
		doEcosystemsRequest(t, app, detailPath, clientIP, "")
	}
	if s := doEcosystemsRequest(t, app, detailPath, clientIP, ""); s != fiber.StatusTooManyRequests {
		t.Fatalf("after burst on detail: got %d, want 429", s)
	}

	setEcosystemsTestTimestamp(baseTS + 59)
	if s := doEcosystemsRequest(t, app, detailPath, clientIP, ""); s != fiber.StatusTooManyRequests {
		t.Fatalf("59 s in on detail: got %d, want 429", s)
	}

	setEcosystemsTestTimestamp(baseTS + 60)
	if s := doEcosystemsRequest(t, app, detailPath, clientIP, ""); s != fiber.StatusOK {
		t.Fatalf("after window reset on detail: got %d, want 200", s)
	}
}

// ── shared bucket: list + detail count against the same per-IP quota ─────────

// TestEcosystemsPublic_SharedBucket_ListAndDetailShareQuota verifies that
// requests to /ecosystems and /ecosystems/:id are counted in the same public
// bucket for a given client IP.
func TestEcosystemsPublic_SharedBucket_ListAndDetailShareQuota(t *testing.T) {
	const limit = 3
	cfg := config.Config{
		RateLimitPublicPerMin: limit,
		TrustedProxies:        []string{"127.0.0.1"},
	}
	app := newEcosystemsRateLimitApp(cfg)
	setEcosystemsTestTimestamp(1_900_100_600)

	clientIP := "203.0.113.66"
	detailPath := "/ecosystems/00000000-0000-0000-0000-000000000004"

	// Use limit-1 requests on the list route …
	for i := 0; i < limit-1; i++ {
		status := doEcosystemsRequest(t, app, "/ecosystems", clientIP, "")
		if status != fiber.StatusOK {
			t.Fatalf("list request %d: got %d, want 200", i+1, status)
		}
	}
	// … then one on the detail route (should still pass — bucket not yet full).
	if s := doEcosystemsRequest(t, app, detailPath, clientIP, ""); s != fiber.StatusOK {
		t.Fatalf("detail request at limit: got %d, want 200", s)
	}
	// One more on either route must be 429.
	if s := doEcosystemsRequest(t, app, "/ecosystems", clientIP, ""); s != fiber.StatusTooManyRequests {
		t.Fatalf("list request over shared limit: got %d, want 429", s)
	}
}

// ── per-IP isolation ──────────────────────────────────────────────────────────

// TestEcosystemsPublic_PerIPIsolation confirms that two different clients each
// have their own independent bucket.
func TestEcosystemsPublic_PerIPIsolation(t *testing.T) {
	const limit = 1
	cfg := config.Config{
		RateLimitPublicPerMin: limit,
		TrustedProxies:        []string{"127.0.0.1"},
	}
	app := newEcosystemsRateLimitApp(cfg)
	setEcosystemsTestTimestamp(1_900_100_700)

	ip1 := "203.0.113.70"
	ip2 := "203.0.113.71"

	// ip1 exhausts its bucket.
	if s := doEcosystemsRequestFromRemote(t, app, "/ecosystems", ip1, ""); s != fiber.StatusOK {
		t.Fatalf("ip1 first: got %d, want 200", s)
	}
	if s := doEcosystemsRequestFromRemote(t, app, "/ecosystems", ip1, ""); s != fiber.StatusTooManyRequests {
		t.Fatalf("ip1 second: got %d, want 429", s)
	}

	// ip2 is unaffected.
	if s := doEcosystemsRequestFromRemote(t, app, "/ecosystems", ip2, ""); s != fiber.StatusOK {
		t.Fatalf("ip2 first: got %d, want 200", s)
	}
}

// ── proxy-spoofing ────────────────────────────────────────────────────────────

// TestEcosystemsPublic_ProxySpoofing_UntrustedRemoteIgnoresXFF verifies that
// a client connecting from an untrusted IP cannot forge X-Forwarded-For to
// shift its bucket key to a different IP.  The rate-limiter must key on the
// real TCP remote address, not the forged header.
func TestEcosystemsPublic_ProxySpoofing_UntrustedRemoteIgnoresXFF(t *testing.T) {
	const limit = 1
	cfg := config.Config{
		RateLimitPublicPerMin: limit,
		TrustedProxies:        []string{"10.0.0.1"}, // only 10.0.0.1 is trusted
	}
	app := newEcosystemsRateLimitApp(cfg)
	setEcosystemsTestTimestamp(1_900_100_800)

	// The attacker connects from 203.0.113.80 (untrusted) but claims to be 1.2.3.4.
	attacker := "203.0.113.80"
	forged := "1.2.3.4"

	// First request exhausts the bucket for the real IP.
	if s := doEcosystemsRequestFromRemote(t, app, "/ecosystems", attacker, ""); s != fiber.StatusOK {
		t.Fatalf("attacker first (no XFF): got %d, want 200", s)
	}

	// Subsequent request with forged XFF must still be throttled — because
	// the untrusted remote address is used as the bucket key, not the forged header.
	if s := doEcosystemsRequestFromRemote(t, app, "/ecosystems", attacker, forged); s != fiber.StatusTooManyRequests {
		t.Fatalf("attacker with forged XFF: got %d, want 429 (spoofing must not bypass limiter)", s)
	}
}

// TestEcosystemsPublic_ProxySpoofing_TrustedProxyForwardedForIsHonoured verifies
// that when a request *does* arrive from a trusted proxy, the X-Forwarded-For
// client IP is used as the bucket key (two different forwarded IPs → two buckets).
func TestEcosystemsPublic_ProxySpoofing_TrustedProxyForwardedForIsHonoured(t *testing.T) {
	const limit = 1
	cfg := config.Config{
		RateLimitPublicPerMin: limit,
		TrustedProxies:        []string{"10.0.0.1"},
	}
	app := newEcosystemsRateLimitApp(cfg)
	setEcosystemsTestTimestamp(1_900_100_900)

	trustedProxy := "10.0.0.1"
	client1 := "198.51.100.10"
	client2 := "198.51.100.11"

	// client1 exhausts its bucket through the trusted proxy.
	if s := doEcosystemsRequestFromRemote(t, app, "/ecosystems", trustedProxy, client1); s != fiber.StatusOK {
		t.Fatalf("client1 first: got %d, want 200", s)
	}
	if s := doEcosystemsRequestFromRemote(t, app, "/ecosystems", trustedProxy, client1); s != fiber.StatusTooManyRequests {
		t.Fatalf("client1 second: got %d, want 429", s)
	}

	// client2 through the same proxy is in a separate bucket.
	if s := doEcosystemsRequestFromRemote(t, app, "/ecosystems", trustedProxy, client2); s != fiber.StatusOK {
		t.Fatalf("client2 first: got %d, want 200", s)
	}
}

// ── Rate-limit response headers ───────────────────────────────────────────────

// doEcosystemsRequestFull returns the full http.Response so header values can
// be inspected.
func doEcosystemsRequestFull(t *testing.T, app *fiber.App, path, remoteIP string) *http.Response {
	t.Helper()
	req := httptest.NewRequest(http.MethodGet, path, nil)
	req.RemoteAddr = fmt.Sprintf("%s:9999", remoteIP)
	resp, err := app.Test(req)
	if err != nil {
		t.Fatalf("app.Test: %v", err)
	}
	return resp
}

// TestEcosystemsPublic_RateLimitHeaders_PresentOnAllowedRequest verifies that
// X-RateLimit-Limit and X-RateLimit-Remaining headers are present and correct
// on a request that is within the limit.
func TestEcosystemsPublic_RateLimitHeaders_PresentOnAllowedRequest(t *testing.T) {
	const limit = 5
	cfg := config.Config{
		RateLimitPublicPerMin: limit,
		TrustedProxies:        []string{"127.0.0.1"},
	}
	app := newEcosystemsRateLimitApp(cfg)
	setEcosystemsTestTimestamp(1_900_101_000)

	resp := doEcosystemsRequestFull(t, app, "/ecosystems", "203.0.113.100")

	if resp.StatusCode != fiber.StatusOK {
		t.Fatalf("status: got %d, want 200", resp.StatusCode)
	}

	// Fiber's limiter middleware sets X-RateLimit-Limit and X-RateLimit-Remaining.
	if got := resp.Header.Get("X-Ratelimit-Limit"); got == "" {
		t.Error("X-Ratelimit-Limit header missing on allowed request")
	}
	if got := resp.Header.Get("X-Ratelimit-Remaining"); got == "" {
		t.Error("X-Ratelimit-Remaining header missing on allowed request")
	}
}

// TestEcosystemsPublic_RateLimitHeaders_PresentOn429 verifies that the
// X-RateLimit-Limit header is present on a throttled (429) response too.
func TestEcosystemsPublic_RateLimitHeaders_PresentOn429(t *testing.T) {
	const limit = 1
	cfg := config.Config{
		RateLimitPublicPerMin: limit,
		TrustedProxies:        []string{"127.0.0.1"},
	}
	app := newEcosystemsRateLimitApp(cfg)
	setEcosystemsTestTimestamp(1_900_101_100)

	clientIP := "203.0.113.101"
	// Exhaust the bucket.
	doEcosystemsRequest(t, app, "/ecosystems", clientIP, "")

	// The throttled response must still carry the header.
	resp := doEcosystemsRequestFull(t, app, "/ecosystems", clientIP)
	if resp.StatusCode != fiber.StatusTooManyRequests {
		t.Fatalf("status: got %d, want 429", resp.StatusCode)
	}
	if got := resp.Header.Get("X-Ratelimit-Limit"); got == "" {
		t.Error("X-Ratelimit-Limit header missing on 429 response")
	}
}

// TestEcosystemsPublic_RateLimitHeaders_RemainingDecrementsCorrectly verifies
// that X-RateLimit-Remaining decrements with each successive request.
func TestEcosystemsPublic_RateLimitHeaders_RemainingDecrementsCorrectly(t *testing.T) {
	const limit = 3
	cfg := config.Config{
		RateLimitPublicPerMin: limit,
		TrustedProxies:        []string{"127.0.0.1"},
	}
	app := newEcosystemsRateLimitApp(cfg)
	setEcosystemsTestTimestamp(1_900_101_200)

	clientIP := "203.0.113.102"

	var prevRemaining string
	for i := 0; i < limit; i++ {
		resp := doEcosystemsRequestFull(t, app, "/ecosystems", clientIP)
		if resp.StatusCode != fiber.StatusOK {
			t.Fatalf("request %d: got %d, want 200", i+1, resp.StatusCode)
		}
		curr := resp.Header.Get("X-Ratelimit-Remaining")
		if curr == "" {
			t.Fatalf("request %d: X-Ratelimit-Remaining header missing", i+1)
		}
		if i > 0 && curr >= prevRemaining {
			t.Errorf("request %d: remaining %q should be less than previous %q", i+1, curr, prevRemaining)
		}
		prevRemaining = curr
	}
}
