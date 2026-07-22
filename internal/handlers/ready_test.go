package handlers_test

import (
	"context"
	"encoding/json"
	"errors"
	"net/http/httptest"
	"os"
	"sync"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/gofiber/fiber/v2"
	"github.com/jagadeesh/grainlify/backend/internal/bus"
	"github.com/jagadeesh/grainlify/backend/internal/db"
	"github.com/jagadeesh/grainlify/backend/internal/handlers"
)

// readyTestDB returns a db.DB backed by a live pool from TEST_DB_URL. The
// readiness handler reports the database as ready only when it can ping a real
// pool, so tests that assert a healthy database are skipped when no database is
// available (matching the convention used elsewhere in this package).
func readyTestDB(t *testing.T) *db.DB {
	t.Helper()
	dsn := os.Getenv("TEST_DB_URL")
	if dsn == "" {
		t.Skip("TEST_DB_URL not set; skipping readiness test that needs a live database")
	}
	pool, err := pgxpool.New(context.Background(), dsn)
	if err != nil {
		t.Fatalf("pgxpool.New: %v", err)
	}
	t.Cleanup(pool.Close)
	return &db.DB{Pool: pool}
}

// mockBusReady is a controllable mock bus.Bus for readiness testing.
type mockBusReady struct {
	mu     sync.Mutex
	status string
}

func (b *mockBusReady) Publish(_ context.Context, _ string, _ []byte) error {
	return nil
}

func (b *mockBusReady) Status() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.status
}

func (b *mockBusReady) Close() {}

func (b *mockBusReady) setStatus(s string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.status = s
}

func TestReadyBothHealthy(t *testing.T) {
	d := readyTestDB(t)
	bus := &mockBusReady{status: "CONNECTED"}
	app := fiber.New()
	app.Get("/ready", handlers.NewReady(d, bus))

	resp, err := app.Test(httptest.NewRequest("GET", "/ready", nil))
	if err != nil {
		t.Fatalf("app.Test returned error: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != fiber.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}

	var body struct {
		OK   bool `json:"ok"`
		Deps []struct {
			Name   string `json:"name"`
			Ready  bool   `json:"ready"`
			Status string `json:"status"`
		} `json:"deps"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatalf("decode ready response: %v", err)
	}

	if !body.OK {
		t.Fatal("ready ok = false, want true")
	}
	if len(body.Deps) != 2 {
		t.Fatalf("expected 2 deps, got %d", len(body.Deps))
	}

	for _, dep := range body.Deps {
		if !dep.Ready {
			t.Fatalf("dep %q was not ready (status=%q)", dep.Name, dep.Status)
		}
	}
}

func TestReadyDBNotConfigured(t *testing.T) {
	app := fiber.New()
	app.Get("/ready", handlers.NewReady(nil, &mockBusReady{status: "CONNECTED"}))

	resp, err := app.Test(httptest.NewRequest("GET", "/ready", nil))
	if err != nil {
		t.Fatalf("app.Test returned error: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != fiber.StatusServiceUnavailable {
		t.Fatalf("expected 503, got %d", resp.StatusCode)
	}

	var body struct {
		OK   bool `json:"ok"`
		Deps []struct {
			Name   string `json:"name"`
			Ready  bool   `json:"ready"`
			Status string `json:"status"`
		} `json:"deps"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatalf("decode ready response: %v", err)
	}

	if body.OK {
		t.Fatal("ready ok = true, want false")
	}

	var dbDep, natsDep *struct {
		Name   string `json:"name"`
		Ready  bool   `json:"ready"`
		Status string `json:"status"`
	}
	for i := range body.Deps {
		switch body.Deps[i].Name {
		case "database":
			dbDep = &body.Deps[i]
		case "nats":
			natsDep = &body.Deps[i]
		}
	}

	if dbDep == nil {
		t.Fatal("missing 'database' dep")
	}
	if dbDep.Ready {
		t.Fatal("database should be not ready when nil")
	}
	if dbDep.Status != "not_configured" {
		t.Fatalf("database status = %q, want %q", dbDep.Status, "not_configured")
	}

	if natsDep == nil {
		t.Fatal("missing 'nats' dep")
	}
	if !natsDep.Ready {
		t.Fatal("nats should be ready when configured and connected")
	}
}

func TestReadyNATSDisconnected(t *testing.T) {
	d := &db.DB{}
	bus := &mockBusReady{status: "DISCONNECTED"}
	app := fiber.New()
	app.Get("/ready", handlers.NewReady(d, bus))

	resp, err := app.Test(httptest.NewRequest("GET", "/ready", nil))
	if err != nil {
		t.Fatalf("app.Test returned error: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != fiber.StatusServiceUnavailable {
		t.Fatalf("expected 503, got %d", resp.StatusCode)
	}

	var body struct {
		OK   bool `json:"ok"`
		Deps []struct {
			Name   string `json:"name"`
			Ready  bool   `json:"ready"`
			Status string `json:"status"`
		} `json:"deps"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatalf("decode ready response: %v", err)
	}

	if body.OK {
		t.Fatal("ready ok = true, want false")
	}

	for _, dep := range body.Deps {
		if dep.Name == "nats" {
			if dep.Ready {
				t.Fatal("nats should not be ready when DISCONNECTED")
			}
			if dep.Status != "DISCONNECTED" {
				t.Fatalf("nats status = %q, want %q", dep.Status, "DISCONNECTED")
			}
		}
	}
}

func TestReadyNATSNotConfigured(t *testing.T) {
	d := readyTestDB(t)
	app := fiber.New()
	app.Get("/ready", handlers.NewReady(d, nil))

	resp, err := app.Test(httptest.NewRequest("GET", "/ready", nil))
	if err != nil {
		t.Fatalf("app.Test returned error: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != fiber.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}

	var body struct {
		OK   bool `json:"ok"`
		Deps []struct {
			Name   string `json:"name"`
			Ready  bool   `json:"ready"`
			Status string `json:"status"`
		} `json:"deps"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatalf("decode ready response: %v", err)
	}

	if !body.OK {
		t.Fatal("ready ok = true, want false")
	}

	var natsDep *struct {
		Name   string `json:"name"`
		Ready  bool   `json:"ready"`
		Status string `json:"status"`
	}
	for i := range body.Deps {
		if body.Deps[i].Name == "nats" {
			natsDep = &body.Deps[i]
		}
	}

	if natsDep == nil {
		t.Fatal("missing 'nats' dep")
	}
	if !natsDep.Ready {
		t.Fatal("nats should be considered ready when not configured")
	}
	if natsDep.Status != "not_configured" {
		t.Fatalf("nats status = %q, want %q", natsDep.Status, "not_configured")
	}
}

func TestReadyBothUnavailable(t *testing.T) {
	app := fiber.New()
	app.Get("/ready", handlers.NewReady(nil, &mockBusReady{status: "DISCONNECTED"}))

	resp, err := app.Test(httptest.NewRequest("GET", "/ready", nil))
	if err != nil {
		t.Fatalf("app.Test returned error: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != fiber.StatusServiceUnavailable {
		t.Fatalf("expected 503, got %d", resp.StatusCode)
	}

	var body struct {
		OK   bool `json:"ok"`
		Deps []struct {
			Name   string `json:"name"`
			Ready  bool   `json:"ready"`
			Status string `json:"status"`
		} `json:"deps"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatalf("decode ready response: %v", err)
	}

	if body.OK {
		t.Fatal("ready ok = true, want false")
	}

	for _, dep := range body.Deps {
		if dep.Ready {
			t.Fatalf("dep %q should not be ready", dep.Name)
		}
	}
}

func TestReadyResponseStructure(t *testing.T) {
	app := fiber.New()
	app.Get("/ready", handlers.NewReady(nil, nil))

	resp, err := app.Test(httptest.NewRequest("GET", "/ready", nil))
	if err != nil {
		t.Fatalf("app.Test returned error: %v", err)
	}
	defer resp.Body.Close()

	var raw map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&raw); err != nil {
		t.Fatalf("decode ready response: %v", err)
	}

	if _, ok := raw["ok"]; !ok {
		t.Error("missing 'ok' field")
	}
	if _, ok := raw["deps"]; !ok {
		t.Error("missing 'deps' field")
	}

	deps, ok := raw["deps"].([]any)
	if !ok {
		t.Fatal("'deps' is not an array")
	}
	if len(deps) != 2 {
		t.Fatalf("expected 2 deps, got %d", len(deps))
	}

	for i, dep := range deps {
		depMap, ok := dep.(map[string]any)
		if !ok {
			t.Fatalf("dep[%d] is not an object", i)
		}
		for _, key := range []string{"name", "ready", "status"} {
			if _, exists := depMap[key]; !exists {
				t.Errorf("dep[%d] missing key %q", i, key)
			}
		}
	}
}

func TestReadyDoesNotExposeSecrets(t *testing.T) {
	app := fiber.New()
	app.Get("/ready", handlers.NewReady(nil, &mockBusReady{status: "CONNECTED"}))

	resp, err := app.Test(httptest.NewRequest("GET", "/ready", nil))
	if err != nil {
		t.Fatalf("app.Test returned error: %v", err)
	}
	defer resp.Body.Close()

	var raw map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&raw); err != nil {
		t.Fatalf("decode ready response: %v", err)
	}

	bodyJSON, _ := json.Marshal(raw)
	bodyStr := string(bodyJSON)

	sensitive := []string{"db_url", "nats_url", "password", "secret", "token", "jwt"}
	for _, s := range sensitive {
		if containsSubstring(bodyStr, s) {
			t.Errorf("response leaks %q", s)
		}
	}
}

func containsSubstring(s, substr string) bool {
	return len(s) >= len(substr) && searchSubstring(s, substr)
}

func searchSubstring(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		match := true
		for j := 0; j < len(substr); j++ {
			ci := s[i+j]
			cj := substr[j]
			// case-insensitive
			if ci >= 'A' && ci <= 'Z' {
				ci += 32
			}
			if cj >= 'A' && cj <= 'Z' {
				cj += 32
			}
			if ci != cj {
				match = false
				break
			}
		}
		if match {
			return true
		}
	}
	return false
}

// Ensure mockBusReady satisfies bus.Bus interface at compile time.
var _ bus.Bus = (*mockBusReady)(nil)

type mockFlappyDBPool struct {
	db.DBPool
	mu       sync.Mutex
	pingErrs []error
	pingIdx  int
}

func (m *mockFlappyDBPool) Ping(ctx context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.pingIdx < len(m.pingErrs) {
		err := m.pingErrs[m.pingIdx]
		m.pingIdx++
		return err
	}
	return nil
}

func TestReadyFlapping(t *testing.T) {
	pool := &mockFlappyDBPool{
		pingErrs: []error{
			errors.New("transient db down"), // 1st call fails
			nil,                               // 2nd call succeeds (becomes ready)
			errors.New("transient db blip"), // 3rd call fails, but should not flap
		},
	}
	d := &db.DB{Pool: pool}
	bus := &mockBusReady{status: "CONNECTED"}
	app := fiber.New()
	app.Get("/ready", handlers.NewReady(d, bus))

	// 1st request: should fail because DB is down
	resp1, _ := app.Test(httptest.NewRequest("GET", "/ready", nil))
	if resp1.StatusCode != fiber.StatusServiceUnavailable {
		t.Fatalf("expected 503 on first request, got %d", resp1.StatusCode)
	}

	// 2nd request: should succeed because DB is up
	resp2, _ := app.Test(httptest.NewRequest("GET", "/ready", nil))
	if resp2.StatusCode != fiber.StatusOK {
		t.Fatalf("expected 200 on second request, got %d", resp2.StatusCode)
	}

	// 3rd request: should still succeed (latch) even though Ping would fail
	resp3, _ := app.Test(httptest.NewRequest("GET", "/ready", nil))
	if resp3.StatusCode != fiber.StatusOK {
		t.Fatalf("expected 200 on third request (latched), got %d", resp3.StatusCode)
	}
}
