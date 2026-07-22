package syncjobs

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"math/rand/v2"
	"os"
	"regexp"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"golang.org/x/time/rate"

	"github.com/jagadeesh/grainlify/backend/internal/config"
	"github.com/jagadeesh/grainlify/backend/internal/db"
	"github.com/jagadeesh/grainlify/backend/internal/github"
	"github.com/jagadeesh/grainlify/backend/internal/liveness"
	"github.com/jagadeesh/grainlify/backend/internal/metrics"
)

type Worker struct {
	cfg      config.Config
	pool     db.DBPool
	limiter  *rate.Limiter
	gh       *github.Client
	workerID string

	// LivenessTracker is updated on every main loop tick so that the /healthz
	// endpoint can detect whether the worker is making progress.
	LivenessTracker *liveness.Tracker
}

type jobState struct {
	status            string
	lastErr           string
	incrementAttempts bool
	runAt             *time.Time // non-nil when rescheduling with backoff
}

// secretPattern matches common secret shapes (tokens, keys, passwords in URLs).
// Used to sanitize last_error before persisting it.
var secretPattern = regexp.MustCompile(`(?i)(token|key|secret|password|auth|bearer)[=: ]+\S+`)

// sanitizeError removes potential secrets from an error string before
// it is written to last_error on the job row.
func sanitizeError(msg string) string {
	return secretPattern.ReplaceAllString(msg, "[REDACTED]")
}

const defaultSyncJobsMaxBackoff = time.Hour

// backoffDuration returns the next retry delay using truncated exponential
// backoff with ±25 % jitter to avoid thundering-herd against the GitHub API.
//
//	delay = base * 2^(attempt-1) * (0.75 + rand*0.5), capped at max
func backoffDuration(base time.Duration, attempt int, max time.Duration) time.Duration {
	if base <= 0 {
		base = 30 * time.Second
	}
	if max <= 0 {
		max = defaultSyncJobsMaxBackoff
	}
	exp := math.Pow(2, float64(attempt-1))
	jitter := 0.75 + rand.Float64()*0.5 // [0.75, 1.25)
	d := time.Duration(float64(base) * exp * jitter)
	if d > max {
		d = max
	}
	return d
}

func New(cfg config.Config, pool db.DBPool) *Worker {
	return &Worker{
		cfg:      cfg,
		pool:     pool,
		limiter:  rate.NewLimiter(rate.Every(250*time.Millisecond), 2), // ~4 req/s, burst 2
		gh:       github.NewClient(),
		workerID: fmt.Sprintf("%s:%d", hostname(), os.Getpid()),
	}
}

// jobCompletionContext gives final status updates a short, non-canceled window
// after the worker root context is canceled during graceful shutdown.
func jobCompletionContext(parent context.Context, timeout time.Duration) (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.WithoutCancel(parent), timeout)
}

// jobFinalState determines the next status for a job after it finishes.
// attempts is the current value stored in the DB (before this run's increment).
// failureAttentionThreshold and backoff settings drive retry/manual-attention behaviour.
func jobFinalState(runErr error, attempts int, failureAttentionThreshold int, backoffBase time.Duration, backoffMax time.Duration) jobState {
	if runErr == nil {
		return jobState{status: "completed", incrementAttempts: true}
	}
	if errors.Is(runErr, context.Canceled) || errors.Is(runErr, context.DeadlineExceeded) {
		return jobState{
			status:            "pending",
			lastErr:           "worker_shutdown_requeued",
			incrementAttempts: false,
		}
	}

	nextAttempt := attempts + 1
	if failureAttentionThreshold <= 0 {
		failureAttentionThreshold = 1
	}
	if nextAttempt >= failureAttentionThreshold {
		return jobState{
			status:            "dead",
			lastErr:           sanitizeError(runErr.Error()),
			incrementAttempts: true,
		}
	}

	delay := backoffDuration(backoffBase, nextAttempt, backoffMax)
	runAt := time.Now().Add(delay)
	return jobState{
		status:            "pending",
		lastErr:           sanitizeError(runErr.Error()),
		incrementAttempts: true,
		runAt:             &runAt,
	}
}

func (w *Worker) Run(ctx context.Context) error {
	if w.pool == nil {
		return fmt.Errorf("db not configured")
	}
	t := time.NewTicker(1 * time.Second)
	defer t.Stop()
	depth := time.NewTicker(15 * time.Second)
	defer depth.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-depth.C:
			w.refreshQueueDepth(ctx)
		case <-t.C:
			if w.LivenessTracker != nil {
				w.LivenessTracker.Tick()
			}
			if err := w.processOne(ctx); err != nil && !errors.Is(err, pgx.ErrNoRows) {
				slog.Error("sync worker error", "error", err)
			}
		}
	}
}

// refreshQueueDepth queries the pending sync_jobs count and updates the gauge.
func (w *Worker) refreshQueueDepth(ctx context.Context) {
	var n float64
	if err := w.pool.QueryRow(ctx,
		`SELECT count(*) FROM sync_jobs WHERE status = 'pending'`,
	).Scan(&n); err != nil {
		slog.Warn("failed to refresh sync_jobs queue depth metric", "error", err)
		return
	}
	metrics.SyncJobsQueueDepth.Set(n)
}

func (w *Worker) processOne(ctx context.Context) error {
	tx, err := w.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	var jobID uuid.UUID
	var projectID uuid.UUID
	var jobType string
	var attempts int
	err = tx.QueryRow(ctx, `
SELECT id, project_id, job_type, attempts
FROM sync_jobs
WHERE status = 'pending'
  AND run_at <= now()
ORDER BY run_at ASC
FOR UPDATE SKIP LOCKED
LIMIT 1
`).Scan(&jobID, &projectID, &jobType, &attempts)
	if err != nil {
		return err
	}

	_, err = tx.Exec(ctx, `
UPDATE sync_jobs
SET status = 'running', locked_at = now(), locked_by = $2, updated_at = now()
WHERE id = $1
`, jobID, w.workerID)
	if err != nil {
		return err
	}

	if err := tx.Commit(ctx); err != nil {
		return err
	}

	runErr := w.runJob(ctx, jobID, projectID, jobType)
	failureAttentionThreshold := w.cfg.SyncJobsFailureAttentionThreshold
	if failureAttentionThreshold == 0 {
		failureAttentionThreshold = w.cfg.SyncJobsMaxAttempts
	}
	state := jobFinalState(runErr, attempts, failureAttentionThreshold, w.cfg.SyncJobsBackoffBase, w.cfg.SyncJobsBackoffMax)
	attemptDelta := 0
	if state.incrementAttempts {
		attemptDelta = 1
	}
	consecutiveFailures := attempts + attemptDelta
	metricLabels := []string{jobID.String(), projectID.String(), jobType}
	if runErr == nil {
		metrics.SyncJobsProcessed.Inc()
		metrics.SyncJobsConsecutiveFailures.WithLabelValues(metricLabels...).Set(0)
	} else if !errors.Is(runErr, context.Canceled) && !errors.Is(runErr, context.DeadlineExceeded) {
		metrics.SyncJobsFailed.Inc()
		metrics.SyncJobsConsecutiveFailures.WithLabelValues(metricLabels...).Set(float64(consecutiveFailures))
	}

	completeCtx, cancel := jobCompletionContext(ctx, 5*time.Second)
	defer cancel()

	if state.runAt != nil {
		// Reschedule with backoff: set run_at to the computed future time.
		_, err = w.pool.Exec(completeCtx, `
UPDATE sync_jobs
SET status = $2,
    attempts = attempts + $3,
    last_error = NULLIF($4, ''),
    run_at = $5,
    locked_at = NULL,
    locked_by = NULL,
    updated_at = now()
WHERE id = $1
`, jobID, state.status, attemptDelta, state.lastErr, *state.runAt)
	} else {
		_, err = w.pool.Exec(completeCtx, `
UPDATE sync_jobs
SET status = $2,
    attempts = attempts + $3,
    last_error = NULLIF($4, ''),
    locked_at = NULL,
    locked_by = NULL,
    updated_at = now()
WHERE id = $1
`, jobID, state.status, attemptDelta, state.lastErr)
	}
	if err != nil {
		return err
	}

	if state.status == "dead" {
		slog.Warn("sync job dead-lettered",
			"job_id", jobID,
			"attempts", attempts+attemptDelta,
			"last_error", state.lastErr,
		)
	}

	return nil
}

func (w *Worker) runJob(ctx context.Context, jobID uuid.UUID, projectID uuid.UUID, jobType string) error {
	// Load project + owner to get GitHub token.
	var fullName string
	var ownerUserID uuid.UUID
	err := w.pool.QueryRow(ctx, `
SELECT github_full_name, owner_user_id
FROM projects
WHERE id = $1
`, projectID).Scan(&fullName, &ownerUserID)
	if err != nil {
		slog.Error("sync job failed: project not found",
			"job_id", jobID,
			"project_id", projectID,
			"error", err,
		)
		return err
	}

	linked, err := github.GetLinkedAccount(ctx, w.pool, ownerUserID, w.cfg.TokenEncKeyB64)
	if err != nil {
		slog.Error("sync job failed: GitHub account not linked",
			"job_id", jobID,
			"project_id", projectID,
			"user_id", ownerUserID,
			"repo", fullName,
			"error", err,
			"hint", "User needs to link their GitHub account via OAuth",
		)
		return fmt.Errorf("github_not_linked: %w", err)
	}

	slog.Info("starting sync job",
		"job_id", jobID,
		"job_type", jobType,
		"project_id", projectID,
		"repo", fullName,
		"user_id", ownerUserID,
	)

	var syncErr error
	switch jobType {
	case "sync_issues":
		syncErr = w.syncIssues(ctx, projectID, fullName, linked.AccessToken)
	case "sync_prs":
		syncErr = w.syncPRs(ctx, projectID, fullName, linked.AccessToken)
	default:
		syncErr = fmt.Errorf("unknown job_type: %s", jobType)
	}

	if syncErr != nil {
		slog.Error("sync job failed",
			"job_id", jobID,
			"job_type", jobType,
			"project_id", projectID,
			"repo", fullName,
			"error", syncErr,
		)
		return syncErr
	}

	slog.Info("sync job completed successfully",
		"job_id", jobID,
		"job_type", jobType,
		"project_id", projectID,
		"repo", fullName,
	)
	return nil
}

func (w *Worker) syncIssues(ctx context.Context, projectID uuid.UUID, fullName string, token string) error {
	totalIssues := 0
	for page := 1; page <= 50; page++ { // safety cap
		if err := w.limiter.Wait(ctx); err != nil {
			return err
		}
		items, err := w.gh.ListIssuesPage(ctx, token, fullName, page)
		if err != nil {
			return err
		}
		if len(items) == 0 {
			return nil
		}

		for _, it := range items {
			// Skip PRs from the issues endpoint.
			if it.PullRequest != nil {
				continue
			}
			totalIssues++
			// Convert assignees to JSONB (array of login strings)
			assigneesJSON, _ := json.Marshal(it.Assignees)
			// Convert labels to JSONB (array of {name, color} objects)
			labelsJSON, _ := json.Marshal(it.Labels)

			// Parse date strings from GitHub API
			var createdAt, updatedAt, closedAt *time.Time
			if it.CreatedAt != nil && *it.CreatedAt != "" {
				if t, err := time.Parse(time.RFC3339, *it.CreatedAt); err == nil {
					createdAt = &t
				} else {
					slog.Warn("failed to parse issue created_at",
						"project_id", projectID,
						"repo", fullName,
						"issue_id", it.ID,
						"created_at", *it.CreatedAt,
						"error", err,
					)
				}
			}
			if it.UpdatedAt != nil && *it.UpdatedAt != "" {
				if t, err := time.Parse(time.RFC3339, *it.UpdatedAt); err == nil {
					updatedAt = &t
				} else {
					slog.Warn("failed to parse issue updated_at",
						"project_id", projectID,
						"repo", fullName,
						"issue_id", it.ID,
						"updated_at", *it.UpdatedAt,
						"error", err,
					)
				}
			}
			if it.ClosedAt != nil && *it.ClosedAt != "" {
				if t, err := time.Parse(time.RFC3339, *it.ClosedAt); err == nil {
					closedAt = &t
				} else {
					slog.Warn("failed to parse issue closed_at",
						"project_id", projectID,
						"repo", fullName,
						"issue_id", it.ID,
						"closed_at", *it.ClosedAt,
						"error", err,
					)
				}
			}

			// Fetch comments for this issue (if comments_count > 0)
			var commentsJSON []byte = []byte("[]")
			if it.Comments > 0 {
				if err := w.limiter.Wait(ctx); err == nil {
					comments, err := w.gh.ListIssueComments(ctx, token, fullName, it.Number)
					if err == nil {
						commentsJSON, _ = json.Marshal(comments)
					}
				}
			}

			_, _ = w.pool.Exec(ctx, `
INSERT INTO github_issues (project_id, github_issue_id, number, state, title, body, author_login, url, assignees, labels, comments_count, comments, created_at_github, updated_at_github, closed_at_github, last_seen_at)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, now())
ON CONFLICT (project_id, github_issue_id) DO UPDATE SET
  number = EXCLUDED.number,
  state = EXCLUDED.state,
  title = EXCLUDED.title,
  body = EXCLUDED.body,
  author_login = EXCLUDED.author_login,
  url = EXCLUDED.url,
  assignees = EXCLUDED.assignees,
  labels = EXCLUDED.labels,
  comments_count = EXCLUDED.comments_count,
  comments = EXCLUDED.comments,
  created_at_github = COALESCE(EXCLUDED.created_at_github, github_issues.created_at_github),
  updated_at_github = COALESCE(EXCLUDED.updated_at_github, github_issues.updated_at_github),
  closed_at_github = COALESCE(EXCLUDED.closed_at_github, github_issues.closed_at_github),
  last_seen_at = now()
`, projectID, it.ID, it.Number, it.State, it.Title, it.Body, it.User.Login, it.HTMLURL, assigneesJSON, labelsJSON, it.Comments, commentsJSON, createdAt, updatedAt, closedAt)
		}
	}

	slog.Info("sync issues completed",
		"project_id", projectID,
		"repo", fullName,
		"total_issues", totalIssues,
	)
	return nil
}

func (w *Worker) syncPRs(ctx context.Context, projectID uuid.UUID, fullName string, token string) error {
	totalPRs := 0
	for page := 1; page <= 50; page++ { // safety cap
		if err := w.limiter.Wait(ctx); err != nil {
			return err
		}
		items, err := w.gh.ListPRsPage(ctx, token, fullName, page)
		if err != nil {
			slog.Error("failed to fetch PRs page",
				"project_id", projectID,
				"repo", fullName,
				"page", page,
				"error", err,
			)
			return err
		}
		if len(items) == 0 {
			slog.Info("sync PRs completed",
				"project_id", projectID,
				"repo", fullName,
				"total_prs", totalPRs,
			)
			return nil
		}

		for _, it := range items {
			totalPRs++

			// Parse date strings from GitHub API
			var createdAt, updatedAt, closedAt, mergedAt *time.Time
			if it.CreatedAt != nil && *it.CreatedAt != "" {
				if t, err := time.Parse(time.RFC3339, *it.CreatedAt); err == nil {
					createdAt = &t
				}
			}
			if it.UpdatedAt != nil && *it.UpdatedAt != "" {
				if t, err := time.Parse(time.RFC3339, *it.UpdatedAt); err == nil {
					updatedAt = &t
				}
			}
			if it.ClosedAt != nil && *it.ClosedAt != "" {
				if t, err := time.Parse(time.RFC3339, *it.ClosedAt); err == nil {
					closedAt = &t
				}
			}
			if it.MergedAt != nil && *it.MergedAt != "" {
				if t, err := time.Parse(time.RFC3339, *it.MergedAt); err == nil {
					mergedAt = &t
				}
			}

			_, _ = w.pool.Exec(ctx, `
INSERT INTO github_pull_requests (project_id, github_pr_id, number, state, title, body, author_login, url, merged, created_at_github, updated_at_github, closed_at_github, merged_at_github, last_seen_at)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, now())
ON CONFLICT (project_id, github_pr_id) DO UPDATE SET
  number = EXCLUDED.number,
  state = EXCLUDED.state,
  title = EXCLUDED.title,
  body = EXCLUDED.body,
  author_login = EXCLUDED.author_login,
  url = EXCLUDED.url,
  merged = EXCLUDED.merged,
  created_at_github = EXCLUDED.created_at_github,
  updated_at_github = EXCLUDED.updated_at_github,
  closed_at_github = EXCLUDED.closed_at_github,
  merged_at_github = EXCLUDED.merged_at_github,
  last_seen_at = now()
`, projectID, it.ID, it.Number, it.State, it.Title, it.Body, it.User.Login, it.HTMLURL, it.Merged, createdAt, updatedAt, closedAt, mergedAt)
		}
	}
	return nil
}

func hostname() string {
	h, _ := os.Hostname()
	if h == "" {
		return "unknown"
	}
	return h
}
