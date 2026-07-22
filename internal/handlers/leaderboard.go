package handlers

import (
	"github.com/jagadeesh/grainlify/backend/internal/httpx"

	"fmt"
	"log/slog"

	"github.com/gofiber/fiber/v2"

	"github.com/jagadeesh/grainlify/backend/internal/db"
)

type LeaderboardHandler struct {
	db *db.DB
}

func NewLeaderboardHandler(d *db.DB) *LeaderboardHandler {
	return &LeaderboardHandler{db: d}
}

// leaderboardBaseQuery returns username, avatar_url, user_id, contribution_count,
// and ecosystems for all contributors in verified projects.  Issue and PR counts
// are pre-computed once in CTEs and reused in both SELECT and WHERE — avoiding
// the repeated correlated-subquery execution of the previous query shape.
const leaderboardBaseQuery = `
WITH all_contributors AS (
  SELECT DISTINCT i.author_login as login
  FROM github_issues i
  INNER JOIN projects p ON i.project_id = p.id
  WHERE i.author_login IS NOT NULL 
    AND i.author_login != ''
    AND p.status = 'verified'
  
  UNION
  
  SELECT DISTINCT pr.author_login as login
  FROM github_pull_requests pr
  INNER JOIN projects p ON pr.project_id = p.id
  WHERE pr.author_login IS NOT NULL 
    AND pr.author_login != ''
    AND p.status = 'verified'
),
issue_counts AS (
  SELECT
    LOWER(i.author_login) AS login_lower,
    COUNT(*) AS cnt
  FROM github_issues i
  INNER JOIN projects p ON i.project_id = p.id
  WHERE i.author_login IS NOT NULL
    AND i.author_login != ''
    AND p.status = 'verified'
  GROUP BY LOWER(i.author_login)
),
pr_counts AS (
  SELECT
    LOWER(pr.author_login) AS login_lower,
    COUNT(*) AS cnt
  FROM github_pull_requests pr
  INNER JOIN projects p ON pr.project_id = p.id
  WHERE pr.author_login IS NOT NULL
    AND pr.author_login != ''
    AND p.status = 'verified'
  GROUP BY LOWER(pr.author_login)
)
SELECT
  ac.login as username,
  COALESCE(ga.avatar_url, '') as avatar_url,
  COALESCE(u.id::text, '') as user_id,
  (COALESCE(ic.cnt, 0) + COALESCE(pc.cnt, 0)) as contribution_count,
  COALESCE(
    (
      SELECT ARRAY_AGG(DISTINCT e.name)
      FROM (
        SELECT DISTINCT p.ecosystem_id
        FROM github_issues i
        INNER JOIN projects p ON i.project_id = p.id
        WHERE LOWER(i.author_login) = LOWER(ac.login) AND p.status = 'verified'
        UNION
        SELECT DISTINCT p.ecosystem_id
        FROM github_pull_requests pr
        INNER JOIN projects p ON pr.project_id = p.id
        WHERE LOWER(pr.author_login) = LOWER(ac.login) AND p.status = 'verified'
      ) contrib_ecosystems
      INNER JOIN ecosystems e ON contrib_ecosystems.ecosystem_id = e.id
      WHERE e.status = 'active'
    ),
    ARRAY[]::TEXT[]
  ) as ecosystems
FROM all_contributors ac
LEFT JOIN issue_counts ic ON LOWER(ac.login) = ic.login_lower
LEFT JOIN pr_counts pc ON LOWER(ac.login) = pc.login_lower
LEFT JOIN github_accounts ga ON LOWER(ga.login) = LOWER(ac.login)
LEFT JOIN users u ON ga.user_id = u.id
WHERE (COALESCE(ic.cnt, 0) + COALESCE(pc.cnt, 0)) > 0
`

// leaderboardCountQuery is a lighter variant that skips the ecosystems
// aggregation and the account/user joins — only the contributor-count
// logic is needed for pagination.
const leaderboardCountQuery = `
WITH all_contributors AS (
  SELECT DISTINCT i.author_login as login
  FROM github_issues i
  INNER JOIN projects p ON i.project_id = p.id
  WHERE i.author_login IS NOT NULL 
    AND i.author_login != ''
    AND p.status = 'verified'
  UNION
  SELECT DISTINCT pr.author_login as login
  FROM github_pull_requests pr
  INNER JOIN projects p ON pr.project_id = p.id
  WHERE pr.author_login IS NOT NULL 
    AND pr.author_login != ''
    AND p.status = 'verified'
),
issue_counts AS (
  SELECT LOWER(i.author_login) AS login_lower, COUNT(*) AS cnt
  FROM github_issues i
  INNER JOIN projects p ON i.project_id = p.id
  WHERE i.author_login IS NOT NULL AND i.author_login != '' AND p.status = 'verified'
  GROUP BY LOWER(i.author_login)
),
pr_counts AS (
  SELECT LOWER(pr.author_login) AS login_lower, COUNT(*) AS cnt
  FROM github_pull_requests pr
  INNER JOIN projects p ON pr.project_id = p.id
  WHERE pr.author_login IS NOT NULL AND pr.author_login != '' AND p.status = 'verified'
  GROUP BY LOWER(pr.author_login)
)
SELECT COUNT(*)
FROM all_contributors ac
LEFT JOIN issue_counts ic ON LOWER(ac.login) = ic.login_lower
LEFT JOIN pr_counts pc ON LOWER(ac.login) = pc.login_lower
WHERE (COALESCE(ic.cnt, 0) + COALESCE(pc.cnt, 0)) > 0
`

// Leaderboard returns top contributors ranked by contributions in verified projects
func (h *LeaderboardHandler) Leaderboard() fiber.Handler {
	return func(c *fiber.Ctx) error {
		if h.db == nil || h.db.Pool == nil {
			return httpx.RespondError(c, fiber.StatusServiceUnavailable, "db_not_configured", "")
		}

		p, err := ParsePagination(c, 10, 100)
		if err != nil {
			// response already written by ParsePagination on error
			return nil
		}

		// Query top contributors by contribution count in verified projects
		rows, err := h.db.Pool.Query(c.Context(), leaderboardBaseQuery+`
ORDER BY contribution_count DESC, ac.login ASC
LIMIT $1 OFFSET $2
`, p.Limit, p.Offset)
		if err != nil {
			slog.Error("failed to fetch leaderboard",
				"error", err,
			)
			return httpx.RespondError(c, fiber.StatusInternalServerError, "leaderboard_fetch_failed", "")
		}
		defer rows.Close()

		var leaderboard []fiber.Map
		rank := p.Offset + 1 // Start rank from offset + 1 for pagination
		for rows.Next() {
			var username string
			var avatarURL *string
			var userID string
			var contributionCount int
			var ecosystems []string

			if err := rows.Scan(&username, &avatarURL, &userID, &contributionCount, &ecosystems); err != nil {
				slog.Error("failed to scan leaderboard row",
					"error", err,
				)
				continue
			}

			avatar := ""
			if avatarURL != nil && *avatarURL != "" {
				avatar = *avatarURL
			} else {
				avatar = fmt.Sprintf("https://github.com/%s.png?size=200", username)
			}

			if ecosystems == nil {
				ecosystems = []string{}
			}

			rankTier := GetRankTier(rank)

			leaderboard = append(leaderboard, fiber.Map{
				"rank":           rank,
				"rank_tier":      string(rankTier),
				"rank_tier_name": GetRankTierDisplayName(rankTier),
				"username":       username,
				"avatar":         avatar,
				"user_id":        userID,
				"contributions":  contributionCount,
				"ecosystems":     ecosystems,
				"score":          contributionCount,
				"trend":          "same",
				"trendValue":     0,
			})
			rank++
		}

		// Get total count for pagination
		var total int
		if err := h.db.Pool.QueryRow(c.Context(), leaderboardCountQuery).Scan(&total); err != nil {
			slog.Error("failed to count leaderboard", "error", err)
			total = len(leaderboard)
		}

		return c.Status(fiber.StatusOK).JSON(PaginatedResponse("leaderboard", leaderboard, p, total))
	}
}
