// Package main is the entry point for the Grainlify background worker process.
//
// The worker connects to PostgreSQL and NATS, then runs two long-lived jobs
// concurrently:
//
//   - [worker.GitHubWebhookConsumer] — dequeues GitHub webhook events from NATS
//     and ingests them into the database via a queue-group subscription so that
//     multiple worker replicas share the load without duplicate processing.
//
//   - [syncjobs.Worker] — polls the sync_jobs table and executes scheduled
//     repository synchronisation tasks (sync_issues, sync_prs).
//
// # Liveness endpoint
//
// The worker exposes a minimal HTTP endpoint at /healthz on the address
// configured by WORKER_LIVENESS_ADDR (default :9091). The endpoint returns
// 200 OK while the worker loop is making progress and 503 Service Unavailable
// when no progress has been detected within WORKER_LIVENESS_STALE_THRESHOLD
// (default 30s). Set WORKER_LIVENESS_ADDR to empty to disable the server
// entirely (useful for local development without a port conflict).
//
// # Configuration
//
// All configuration is read from environment variables (see .env.example).
// The two required variables for this binary are:
//
//   - DB_URL   — PostgreSQL connection string
//   - NATS_URL — NATS server URL
//
// In non-dev environments (APP_ENV != "dev") the process exits with status 1
// when either variable is absent.
//
// # Graceful shutdown
//
// SIGINT or SIGTERM cancels the root context, which:
//  1. Unsubscribes the NATS subscription (GitHubWebhookConsumer.Subscribe
//     returns when ctx.Done() fires).
//  2. Stops the syncjobs ticker loop (syncjobs.Worker.Run returns on ctx.Done()).
//  3. Shuts down the liveness HTTP server.
//
// The process then waits up to SHUTDOWN_TIMEOUT (default 10s) for in-flight
// work to finish before closing the NATS and database connections and exiting cleanly.
package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/jagadeesh/grainlify/backend/internal/bus/natsbus"
	"github.com/jagadeesh/grainlify/backend/internal/config"
	"github.com/jagadeesh/grainlify/backend/internal/db"
	"github.com/jagadeesh/grainlify/backend/internal/ingest"
	"github.com/jagadeesh/grainlify/backend/internal/liveness"
	shutdownwait "github.com/jagadeesh/grainlify/backend/internal/shutdown"
	"github.com/jagadeesh/grainlify/backend/internal/syncjobs"
	"github.com/jagadeesh/grainlify/backend/internal/worker"
)

func main() {
	// Load environment variables and configuration
	config.LoadDotenv()
	cfg := config.Load()

	// Set up logger
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: cfg.LogLevel(),
	})))

	slog.Info("=== Grainlify Worker Starting ===", "env", cfg.Env, "shutdown_timeout", cfg.ShutdownTimeout.String())

	if err := cfg.Validate(); err != nil {
		slog.Error("startup config validation failed", "error", err)
		os.Exit(1)
	}

	// Fail fast on missing required config.
	if cfg.DBURL == "" {
		if cfg.Env != "dev" {
			slog.Error("DB_URL is required in non-dev environments")
			os.Exit(1)
		}
		slog.Warn("DB_URL not set; worker has nothing to do — exiting")
		os.Exit(1)
	}
	if cfg.NATSURL == "" {
		if cfg.Env != "dev" {
			slog.Error("NATS_URL is required in non-dev environments")
			os.Exit(1)
		}
		slog.Warn("NATS_URL not set; worker has nothing to do — exiting")
		os.Exit(1)
	}

	// ---------- Database connection ----------
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	dbConn, err := db.Connect(ctx, cfg.DBURL, db.PoolConfig{
		MaxConns:        cfg.DBMaxConns,
		MinConns:        cfg.DBMinConns,
		MaxConnLifetime: cfg.DBMaxConnLifetime,
		MaxConnIdleTime: cfg.DBMaxConnIdleTime,
	})
	cancel()
	if err != nil {
		slog.Error("failed to connect to database", "error", err)
		os.Exit(1)
	}
	defer func() {
		slog.Info("closing database connection")
		dbConn.Close()
	}()

	// ---------- NATS connection ----------
	nbus, err := natsbus.Connect(cfg.NATSURL)
	if err != nil {
		slog.Error("failed to connect to NATS", "error", err)
		os.Exit(1)
	}
	defer func() {
		slog.Info("closing NATS connection")
		nbus.Close()
	}()

	// ---------- Liveness tracker (shared) ----------
	livenessTracker := liveness.NewTracker(cfg.WorkerLivenessStaleThreshold)

	// ---------- Liveness HTTP server (optional) ----------
	var livenessWG sync.WaitGroup
	livenessAddr := cfg.WorkerLivenessAddr
	if livenessAddr != "" {
		livenessWG.Add(1)
		go func() {
			defer livenessWG.Done()
			slog.Info("starting liveness HTTP server", "addr", livenessAddr)
			if err := livenessTracker.Serve(livenessAddr); err != nil && !errors.Is(err, context.Canceled) {
				// http.Server.ListenAndServe returns http.ErrServerClosed on
				// graceful shutdown, not context.Canceled. Check for both.
				if !errors.Is(err, http.ErrServerClosed) {
					slog.Error("liveness HTTP server error", "error", err)
				}
			}
			slog.Info("liveness HTTP server stopped")
		}()
	} else {
		slog.Info("liveness HTTP server disabled (WORKER_LIVENESS_ADDR is empty)")
	}

	// Root context — cancelled on shutdown signal.
	workerCtx, stopWorkers := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stopWorkers()

	var workerWG sync.WaitGroup

	// ---------- GitHub webhook consumer ----------
	consumer := &worker.GitHubWebhookConsumer{
		Ingest:          &ingest.GitHubWebhookIngestor{Pool: dbConn.Pool},
		LivenessTracker: livenessTracker,
	}
	if err := consumer.Subscribe(workerCtx, nbus.Conn(), worker.GitHubWebhookQueueGroup); err != nil {
		slog.Error("failed to subscribe to webhook events", "error", err)
		os.Exit(1)
	}
	defer func() {
		if consumer.Sub != nil {
			_ = consumer.Sub.Unsubscribe()
		}
	}()
	slog.Info("github webhook consumer subscribed")

	// ---------- Sync jobs runner (concurrent) ----------
	syncWorker := syncjobs.New(cfg, dbConn.Pool)
	syncWorker.LivenessTracker = livenessTracker
	workerWG.Add(1)
	go func() {
		defer workerWG.Done()
		slog.Info("starting syncjobs worker")
		if err := syncWorker.Run(workerCtx); err != nil && !errors.Is(err, context.Canceled) {
			slog.Error("syncjobs worker exited with error", "error", err)
		}
	}()

	// ---------- Idempotency key cleanup (concurrent) ----------
	// Periodically delete expired idempotency keys (expires_at < now()).
	// Runs every hour; expired keys are also filtered by the lookup query, so this is just
	// cleanup to prevent unbounded table growth.
	workerWG.Add(1)
	go func() {
		defer workerWG.Done()
		slog.Info("starting idempotency key cleanup worker")
		ticker := time.NewTicker(1 * time.Hour)
		defer ticker.Stop()
		for {
			select {
			case <-workerCtx.Done():
				slog.Info("idempotency key cleanup worker stopped")
				return
			case <-ticker.C:
				result, err := dbConn.Pool.Exec(workerCtx, `DELETE FROM idempotency_keys WHERE expires_at < now()`)
				if err != nil {
					slog.Warn("idempotency key cleanup failed", "error", err)
				} else {
					slog.Info("idempotency key cleanup completed", "rows_deleted", result.RowsAffected())
				}
			}
		}
	}()

	slog.Info("=== Grainlify Worker Running ===")

	// Block until signal.
	<-workerCtx.Done()
	slog.Info("shutdown signal received, draining workers")

	// Give goroutines up to the configured shutdown timeout to finish in-flight work.
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), cfg.ShutdownTimeout)
	defer shutdownCancel()

	if err := shutdownwait.Wait(shutdownCtx, &workerWG); err != nil {
		slog.Warn("worker shutdown exceeded deadline", "error", err)
	} else {
		slog.Info("all workers drained cleanly")
	}

	// Shut down the liveness HTTP server after workers have drained.
	if livenessAddr != "" {
		slog.Info("shutting down liveness HTTP server")
		if err := livenessTracker.Shutdown(context.Background()); err != nil {
			slog.Warn("liveness server shutdown error", "error", err)
		}
	}
	livenessWG.Wait()

	slog.Info("worker shutdown complete")
}