package liveness

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestNewTrackerDefaults(t *testing.T) {
	threshold := 10 * time.Second
	tr := NewTracker(threshold)
	if tr == nil {
		t.Fatal("NewTracker returned nil")
	}
	if tr.staleThreshold != threshold {
		t.Fatalf("staleThreshold = %v, want %v", tr.staleThreshold, threshold)
	}
}

func TestTickAndStarted(t *testing.T) {
	tr := NewTracker(time.Minute)
	if tr.Started() {
		t.Fatal("Started should be false before first Tick")
	}
	tr.Tick()
	if !tr.Started() {
		t.Fatal("Started should be true after Tick")
	}
}

func TestLastTickAdvances(t *testing.T) {
	tr := NewTracker(time.Minute)
	before := time.Now()
	tr.Tick()
	got := tr.LastTick()
	if got.Before(before) || got.After(time.Now()) {
		t.Fatalf("LastTick = %v, expected ~now (%v)", got, before)
	}
}

func TestStaleBeforeFirstTick(t *testing.T) {
	tr := NewTracker(time.Minute)
	if !tr.Stale() {
		t.Fatal("Stale should be true before first Tick")
	}
}

func TestStaleAfterThreshold(t *testing.T) {
	tr := NewTracker(2 * time.Second)
	tr.Tick()
	time.Sleep(3 * time.Second)
	if !tr.Stale() {
		t.Fatal("Stale should be true after threshold elapsed")
	}
}

func TestNotStaleBeforeThreshold(t *testing.T) {
	tr := NewTracker(2 * time.Second)
	tr.Tick()
	time.Sleep(500 * time.Millisecond)
	if tr.Stale() {
		t.Fatal("Stale should be false before threshold elapsed")
	}
}

func TestTickAdvancesResetsStale(t *testing.T) {
	tr := NewTracker(2 * time.Second)
	tr.Tick()
	time.Sleep(3 * time.Second)
	if !tr.Stale() {
		t.Fatal("expected stale after sleep")
	}
	tr.Tick()
	if tr.Stale() {
		t.Fatal("expected not stale after re-ticking")
	}
}

func TestConcurrentTicks(t *testing.T) {
	tr := NewTracker(time.Minute)
	done := make(chan struct{})
	for i := 0; i < 50; i++ {
		go func() {
			for {
				select {
				case <-done:
					return
				default:
					tr.Tick()
				}
			}
		}()
	}
	time.Sleep(100 * time.Millisecond)
	close(done)
	time.Sleep(50 * time.Millisecond)
	if !tr.Started() {
		t.Fatal("expected Started after concurrent Ticks")
	}
}

func TestConcurrentTicksAndReads(t *testing.T) {
	tr := NewTracker(time.Minute)
	done := make(chan struct{})
	go func() {
		for {
			select {
			case <-done:
				return
			default:
				tr.Tick()
			}
		}
	}()
	for i := 0; i < 1000; i++ {
		_ = tr.LastTick()
		_ = tr.Started()
		_ = tr.Stale()
	}
	close(done)
	if !tr.Started() {
		t.Fatal("expected Started after concurrent access")
	}
}

func TestHealthzBeforeFirstTick(t *testing.T) {
	tr := NewTracker(time.Minute)
	handler := tr.HealthzHandler()
	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503 before first tick", rec.Code)
	}
	if got := rec.Header().Get("Content-Type"); got != "application/json" {
		t.Fatalf("Content-Type = %q, want application/json", got)
	}
}

func TestHealthzReturnsOK(t *testing.T) {
	tr := NewTracker(30 * time.Second)
	tr.Tick()
	handler := tr.HealthzHandler()
	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200 after tick", rec.Code)
	}
}

func TestHealthzReturnsStale(t *testing.T) {
	tr := NewTracker(2 * time.Second)
	tr.Tick()
	time.Sleep(3 * time.Second)
	handler := tr.HealthzHandler()
	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503 when stale", rec.Code)
	}
}

func TestHealthzMethodNotAllowed(t *testing.T) {
	tr := NewTracker(time.Minute)
	tr.Tick()
	handler := tr.HealthzHandler()
	req := httptest.NewRequest(http.MethodPost, "/healthz", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("status = %d, want 405 for POST", rec.Code)
	}
}

func TestHealthzResponseJSON(t *testing.T) {
	tr := NewTracker(30 * time.Second)
	tr.Tick()
	handler := tr.HealthzHandler()
	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	wantSubstring := `"status":"ok"`
	if !bytes.Contains(rec.Body.Bytes(), []byte(wantSubstring)) {
		t.Fatalf("body = %s, want to contain %s", rec.Body.String(), wantSubstring)
	}
}

func TestServeAndShutdown(t *testing.T) {
	tr := NewTracker(30 * time.Second)
	tr.Tick()

	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", tr.HealthzHandler())
	srv := &http.Server{Addr: "127.0.0.1:0", Handler: mux}
	go srv.ListenAndServe()
	time.Sleep(100 * time.Millisecond)

	addr := tr.ServerAddr()
	if addr == "" {
		addr = srv.Addr
	}
	resp, err := http.Get("http://" + addr + "/healthz")
	if err != nil {
		t.Fatalf("failed to GET /healthz: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("/healthz status = %d, want 200", resp.StatusCode)
	}

	if err := srv.Shutdown(context.Background()); err != nil {
		t.Fatalf("server shutdown error: %v", err)
	}
	if err := tr.Shutdown(context.Background()); err != nil {
		t.Fatalf("tracker shutdown error: %v", err)
	}
}

func TestShutdownIdempotent(t *testing.T) {
	tr := NewTracker(30 * time.Second)
	if err := tr.Shutdown(context.Background()); err != nil {
		t.Fatalf("first Shutdown error: %v", err)
	}
	if err := tr.Shutdown(context.Background()); err != nil {
		t.Fatalf("second Shutdown error: %v", err)
	}
}