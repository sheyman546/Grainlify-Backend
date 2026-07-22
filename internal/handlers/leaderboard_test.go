package handlers_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gofiber/fiber/v2"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/jagadeesh/grainlify/backend/internal/db"
	"github.com/jagadeesh/grainlify/backend/internal/handlers"
)

type leaderboardResponse struct {
	Leaderboard []struct {
		Rank          int      `json:"rank"`
		Username      string   `json:"username"`
		Contributions int      `json:"contributions"`
		Ecosystems    []string `json:"ecosystems"`
	} `json:"leaderboard"`
	Limit   int  `json:"limit"`
	Offset  int  `json:"offset"`
	Total   int  `json:"total"`
	HasMore bool `json:"has_more"`
}

func newLeaderboardApp(pool *pgxpool.Pool) *fiber.App {
	app := fiber.New(fiber.Config{DisableStartupMessage: true})
	h := handlers.NewLeaderboardHandler(&db.DB{Pool: pool})
	app.Get("/leaderboard", h.Leaderboard())
	return app
}

func getLeaderboard(t *testing.T, app *fiber.App, target string) (int, leaderboardResponse) {
	t.Helper()
	resp, err := app.Test(httptest.NewRequest("GET", target, nil))
	require.NoError(t, err)
	defer resp.Body.Close()

	var body leaderboardResponse
	err = json.NewDecoder(resp.Body).Decode(&body)
	require.NoError(t, err)
	return resp.StatusCode, body
}

func resetLeaderboardTables(t *testing.T, pool *pgxpool.Pool) {
	t.Helper()
	_, err := pool.Exec(t.Context(), `TRUNCATE github_issues, github_pull_requests, projects, ecosystems, github_accounts, wallets, users CASCADE`)
	require.NoError(t, err)
}

func seedLeaderboardDataset(t *testing.T, pool *pgxpool.Pool, contributionsByLogin map[string]int) {
	t.Helper()
	resetLeaderboardTables(t, pool)
	ctx := t.Context()
	suffix := uuid.NewString()

	var ownerID, ecosystemID, projectID uuid.UUID
	require.NoError(t, pool.QueryRow(ctx, `INSERT INTO users (display_name) VALUES ($1) RETURNING id`, "leaderboard owner "+suffix).Scan(&ownerID))
	require.NoError(t, pool.QueryRow(ctx, `INSERT INTO ecosystems (slug, name, status) VALUES ($1, $2, 'active') RETURNING id`, "leaderboard-"+suffix, "Leaderboard Ecosystem "+suffix).Scan(&ecosystemID))
	require.NoError(t, pool.QueryRow(ctx, `
		INSERT INTO projects (owner_user_id, github_full_name, status, ecosystem_id)
		VALUES ($1, $2, 'verified', $3)
		RETURNING id`, ownerID, "grainlify/leaderboard-"+suffix, ecosystemID).Scan(&projectID))

	issueID := int64(time.Now().UnixNano() % 1_000_000_000)
	for login, count := range contributionsByLogin {
		for i := 0; i < count; i++ {
			issueID++
			require.NoError(t, pool.QueryRow(ctx, `
				INSERT INTO github_issues (project_id, github_issue_id, number, state, title, author_login, url, created_at_github, updated_at_github)
				VALUES ($1, $2, $3, 'open', $4, $5, $6, now(), now())
				RETURNING id`, projectID, issueID, int(issueID%1_000_000), fmt.Sprintf("Issue %d", issueID), login, fmt.Sprintf("https://example.test/%d", issueID)).Scan(new(uuid.UUID)))
		}
	}

	t.Cleanup(func() {
		_, _ = pool.Exec(context.Background(), `TRUNCATE github_issues, github_pull_requests, projects, ecosystems, github_accounts, wallets, users CASCADE`)
	})
}

func TestLeaderboardPaginationEdgeCases(t *testing.T) {
	pool := openTestPool(t)
	seedLeaderboardDataset(t, pool, map[string]int{
		"alice": 5,
		"bob":   4,
		"carol": 3,
		"dave":  2,
		"erin":  1,
	})
	app := newLeaderboardApp(pool)

	t.Run("page zero offset returns first page with has more", func(t *testing.T) {
		status, body := getLeaderboard(t, app, "/leaderboard?limit=2&offset=0")
		assert.Equal(t, fiber.StatusOK, status)
		assert.Equal(t, 2, body.Limit)
		assert.Equal(t, 0, body.Offset)
		assert.Equal(t, 5, body.Total)
		assert.True(t, body.HasMore)
		require.Len(t, body.Leaderboard, 2)
		assert.Equal(t, 1, body.Leaderboard[0].Rank)
	})

	t.Run("negative offset is rejected", func(t *testing.T) {
		resp, err := app.Test(httptest.NewRequest("GET", "/leaderboard?offset=-1", nil))
		require.NoError(t, err)
		defer resp.Body.Close()
		assert.Equal(t, fiber.StatusBadRequest, resp.StatusCode)
	})

	t.Run("negative limit falls back to default", func(t *testing.T) {
		status, body := getLeaderboard(t, app, "/leaderboard?limit=-5&offset=0")
		assert.Equal(t, fiber.StatusOK, status)
		assert.Equal(t, 10, body.Limit)
		assert.Equal(t, 0, body.Offset)
		assert.Equal(t, 5, body.Total)
		assert.False(t, body.HasMore)
		require.Len(t, body.Leaderboard, 5)
	})

	t.Run("offset beyond dataset returns empty list", func(t *testing.T) {
		status, body := getLeaderboard(t, app, "/leaderboard?limit=2&offset=100000")
		assert.Equal(t, fiber.StatusOK, status)
		assert.Equal(t, 2, body.Limit)
		assert.Equal(t, 100000, body.Offset)
		assert.Equal(t, 5, body.Total)
		assert.False(t, body.HasMore)
		assert.Empty(t, body.Leaderboard)
	})

	t.Run("oversized limit is clamped", func(t *testing.T) {
		status, body := getLeaderboard(t, app, "/leaderboard?limit=10000&offset=0")
		assert.Equal(t, fiber.StatusOK, status)
		assert.Equal(t, 100, body.Limit)
		assert.Equal(t, 0, body.Offset)
		assert.Equal(t, 5, body.Total)
		assert.False(t, body.HasMore)
	})
}

func TestLeaderboardEmptyDatasetReturnsEmptyResponse(t *testing.T) {
	pool := openTestPool(t)
	resetLeaderboardTables(t, pool)
	t.Cleanup(func() {
		_, _ = pool.Exec(context.Background(), `TRUNCATE github_issues, github_pull_requests, projects, ecosystems, github_accounts, wallets, users CASCADE`)
	})
	app := newLeaderboardApp(pool)

	status, body := getLeaderboard(t, app, "/leaderboard?limit=10&offset=0")
	assert.Equal(t, fiber.StatusOK, status)
	assert.Equal(t, 10, body.Limit)
	assert.Equal(t, 0, body.Offset)
	assert.Equal(t, 0, body.Total)
	assert.Empty(t, body.Leaderboard)
	assert.False(t, body.HasMore)
}

func TestLeaderboardCountQueryAccuracy(t *testing.T) {
	pool := openTestPool(t)
	seedLeaderboardDataset(t, pool, map[string]int{
		"alice": 5,
		"bob":   4,
		"carol": 3,
		"dave":  2,
		"erin":  1,
	})
	app := newLeaderboardApp(pool)

	// Test that count query returns accurate total
	status, body := getLeaderboard(t, app, "/leaderboard?limit=10&offset=0")
	assert.Equal(t, fiber.StatusOK, status)
	assert.Equal(t, 5, body.Total, "count query should return 5 contributors")

	// Verify that fetching all pages matches the total
	status2, body2 := getLeaderboard(t, app, "/leaderboard?limit=2&offset=0")
	assert.Equal(t, fiber.StatusOK, status2)
	assert.Equal(t, 5, body2.Total, "total should be consistent across requests")
	assert.Equal(t, 2, len(body2.Leaderboard))

	status3, body3 := getLeaderboard(t, app, "/leaderboard?limit=2&offset=2")
	assert.Equal(t, fiber.StatusOK, status3)
	assert.Equal(t, 5, body3.Total, "total should be consistent across requests")
	assert.Equal(t, 2, len(body3.Leaderboard))

	status4, body4 := getLeaderboard(t, app, "/leaderboard?limit=2&offset=4")
	assert.Equal(t, fiber.StatusOK, status4)
	assert.Equal(t, 5, body4.Total, "total should be consistent across requests")
	assert.Equal(t, 1, len(body4.Leaderboard))
}
