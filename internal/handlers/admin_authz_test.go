package handlers_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/jagadeesh/grainlify/backend/internal/api"
	"github.com/jagadeesh/grainlify/backend/internal/auth"
	"github.com/jagadeesh/grainlify/backend/internal/config"
	"github.com/jagadeesh/grainlify/backend/internal/db"
	"github.com/jagadeesh/grainlify/backend/internal/handlers"
)

func TestAdminRoutes_AuthorizationCoverage(t *testing.T) {
	pool := openTestPool(t)
	d := &db.DB{Pool: pool}

	cfg := config.Config{
		Env:       "test",
		JWTSecret: "test-secret-for-admin-routes",
	}

	app := api.New(cfg, api.Deps{DB: d}, handlers.BuildInfo{})

	ctx := context.Background()
	var contributorID, adminID, demotedID uuid.UUID

	err := pool.QueryRow(ctx, "INSERT INTO users (role) VALUES ('contributor') RETURNING id").Scan(&contributorID)
	require.NoError(t, err)

	err = pool.QueryRow(ctx, "INSERT INTO users (role) VALUES ('admin') RETURNING id").Scan(&adminID)
	require.NoError(t, err)

	err = pool.QueryRow(ctx, "INSERT INTO users (role) VALUES ('contributor') RETURNING id").Scan(&demotedID)
	require.NoError(t, err)

	contributorToken, err := auth.IssueJWT(cfg.JWTSecret, contributorID, "contributor", "evm", "0x123", time.Hour)
	require.NoError(t, err)

	adminToken, err := auth.IssueJWT(cfg.JWTSecret, adminID, "admin", "evm", "0x456", time.Hour)
	require.NoError(t, err)

	expiredAdminToken, err := auth.IssueJWT(cfg.JWTSecret, adminID, "admin", "evm", "0x456", -time.Hour)
	require.NoError(t, err)

	demotedAdminToken, err := auth.IssueJWT(cfg.JWTSecret, demotedID, "admin", "evm", "0x789", time.Hour)
	require.NoError(t, err)

	routesTested := 0

	for _, route := range app.GetRoutes(true) {
		if !strings.HasPrefix(route.Path, "/admin/") {
			continue
		}
		
		if route.Method == "HEAD" || route.Method == "OPTIONS" {
			continue
		}

		if route.Path == "/admin/bootstrap" {
			continue
		}

		routesTested++
		
		t.Run(route.Method+" "+route.Path, func(t *testing.T) {
			testPath := strings.ReplaceAll(route.Path, ":id", contributorID.String())

			// 1. Unauthenticated -> 401
			req := httptest.NewRequest(route.Method, testPath, nil)
			resp, err := app.Test(req)
			require.NoError(t, err)
			assert.Equal(t, http.StatusUnauthorized, resp.StatusCode, "Expected 401 for unauthenticated request")

			// 2. Non-admin (contributor) -> 403
			req = httptest.NewRequest(route.Method, testPath, nil)
			req.Header.Set("Authorization", "Bearer "+contributorToken)
			resp, err = app.Test(req)
			require.NoError(t, err)
			assert.Equal(t, http.StatusForbidden, resp.StatusCode, "Expected 403 for non-admin request")

			// 3. Expired admin token -> 401
			req = httptest.NewRequest(route.Method, testPath, nil)
			req.Header.Set("Authorization", "Bearer "+expiredAdminToken)
			resp, err = app.Test(req)
			require.NoError(t, err)
			assert.Equal(t, http.StatusUnauthorized, resp.StatusCode, "Expected 401 for expired token")

			// 4. Demoted admin (token says admin, DB says contributor) -> 403
			req = httptest.NewRequest(route.Method, testPath, nil)
			req.Header.Set("Authorization", "Bearer "+demotedAdminToken)
			resp, err = app.Test(req)
			require.NoError(t, err)
			assert.Equal(t, http.StatusForbidden, resp.StatusCode, "Expected 403 for demoted admin")

			// 5. Valid admin -> Should NOT be 401 or 403
			if route.Method == "PUT" || route.Method == "POST" {
				req = httptest.NewRequest(route.Method, testPath, strings.NewReader(`{"role":"maintainer","name":"Test Event","date":"2023-01-01T00:00:00Z"}`))
				req.Header.Set("Content-Type", "application/json")
			} else {
				req = httptest.NewRequest(route.Method, testPath, nil)
			}
			req.Header.Set("Authorization", "Bearer "+adminToken)
			resp, err = app.Test(req)
			require.NoError(t, err)
			
			assert.NotEqual(t, http.StatusUnauthorized, resp.StatusCode, "Admin should not get 401")
			assert.NotEqual(t, http.StatusForbidden, resp.StatusCode, "Admin should not get 403")
		})
	}
	
	require.GreaterOrEqual(t, routesTested, 2, "Should have tested at least 2 admin routes (ListUsers, SetUserRole)")
}
