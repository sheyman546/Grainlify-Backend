// Package liveness provides a thread-safe tracker that records the last time
// the worker loop made progress. It is used by the /healthz HTTP endpoint to
// report whether the worker is healthy or stalled.
package liveness

import (
	"context"
	"encoding/json"
	"net/http"
	"sync"
	"sync/atomic"
	"time"
)

// Tracker records the last-tick timestamp and provides an HTTP handler for
// liveness checks. All exported methods are safe for concurrent use.
type Tracker struct {
	// lastTick is atomically updated so the HTTP handler can read it without
	// acquiring a mutex on every request.
	lastTick atomic.Int64 // unix nanos

	// staleThreshold is how long without a tick before the endpoint returns
	// non-200. Must be set before first use; not modified after.
	staleThreshold time.Duration

	// started is set to true once the first tick has been recorded.
	started atomic.Bool

	mu     sync.Mutex
	server *http.Server
}

// NewTracker creates a Tracker with the given stale threshold. The threshold
// should be at least 2× the expected tick interval to avoid false positives
// during normal operation.
func NewTracker(staleThreshold time.Duration) *Tracker {
	return &Tracker{
		staleThreshold: staleThreshold,
	}
}

// Tick records that the worker has made progress at the current time.
// It is safe to call from multiple goroutines concurrently.
func (t *Tracker) Tick() {
	t.lastTick.Store(time.Now().UnixNano())
	t.started.Store(true)
}

// LastTick returns the last recorded tick time. Returns the zero time if
// Tick has never been called.
func (t *Tracker) LastTick() time.Time {
	nanos := t.lastTick.Load()
	if nanos == 0 {
		return time.Time{}
	}
	return time.Unix(0, nanos)
}

// Started reports whether Tick has been called at least once.
func (t *Tracker) Started() bool {
	return t.started.Load()
}

// Stale returns true if the worker has not ticked within the stale threshold.
// If Tick has never been called, it returns true (not yet healthy).
func (t *Tracker) Stale() bool {
	if !t.started.Load() {
		return true
	}
	elapsed := time.Since(t.LastTick())
	return elapsed > t.staleThreshold
}

// HealthzHandler returns an http.HandlerFunc that serves the liveness check.
//
//   - 200 OK with JSON body {"status":"ok","last_tick":"..."} when the worker
//     has ticked recently (within the stale threshold).
//   - 503 Service Unavailable with JSON body {"status":"stale","last_tick":"..."}
//     when the worker has not ticked within the stale threshold.
//   - 503 Service Unavailable with JSON body {"status":"not_started"} when
//     Tick has never been called.
func (t *Tracker) HealthzHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}

		lastTick := t.LastTick()

		if !t.started.Load() {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusServiceUnavailable)
			_ = json.NewEncoder(w).Encode(map[string]string{
				"status": "not_started",
			})
			return
		}

		if t.Stale() {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusServiceUnavailable)
			_ = json.NewEncoder(w).Encode(map[string]interface{}{
				"status":    "stale",
				"last_tick": lastTick.Format(time.RFC3339Nano),
			})
			return
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(map[string]interface{}{
			"status":    "ok",
			"last_tick": lastTick.Format(time.RFC3339Nano),
		})
	}
}

// Serve starts the HTTP liveness server on the given address in a background
// goroutine. Call Shutdown to stop it.
func (t *Tracker) Serve(addr string) error {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", t.HealthzHandler())

	t.mu.Lock()
	t.server = &http.Server{
		Addr:    addr,
		Handler: mux,
		// Reasonable timeouts to prevent resource leaks.
		ReadHeaderTimeout: 5 * time.Second,
		WriteTimeout:      5 * time.Second,
		IdleTimeout:       30 * time.Second,
	}
	t.mu.Unlock()

	return t.server.ListenAndServe()
}

// Shutdown gracefully stops the HTTP liveness server. It is safe to call
// multiple times.
func (t *Tracker) Shutdown(ctx context.Context) error {
	t.mu.Lock()
	srv := t.server
	t.mu.Unlock()
	if srv == nil {
		return nil
	}
	return srv.Shutdown(ctx)
}

// ServerAddr returns the address the liveness server is listening on.
// Returns empty string if the server is not running.
func (t *Tracker) ServerAddr() string {
	t.mu.Lock()
	srv := t.server
	t.mu.Unlock()
	if srv == nil {
		return ""
	}
	return srv.Addr
}
