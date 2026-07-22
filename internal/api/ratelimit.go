package api

import (
	"net"
	"strconv"
	"strings"
	"time"

	"github.com/gofiber/fiber/v2"
	"github.com/gofiber/fiber/v2/middleware/limiter"
	"github.com/jagadeesh/grainlify/backend/internal/auth"
	"github.com/jagadeesh/grainlify/backend/internal/config"
)

type rateLimitMode int

const (
	rateLimitModeNone rateLimitMode = iota
	rateLimitModeAuth
	rateLimitModePublic
)

// NewRateLimitMiddleware creates a request limiter for auth/webhook routes and
// a separate, higher-capacity limiter for public read endpoints. Requests are
// bucketed per client IP unless a valid bearer token is present, in which case
// the bucket falls back to the authenticated user ID.
func NewRateLimitMiddleware(cfg config.Config) fiber.Handler {
	authLimiter := noOpLimiter()
	publicLimiter := noOpLimiter()

	if cfg.RateLimitAuthPerMin > 0 {
		authLimiter = limiter.New(limiter.Config{
			Max:        cfg.RateLimitAuthPerMin,
			Expiration: time.Minute,
			KeyGenerator: func(c *fiber.Ctx) string {
				return rateLimitKey(c, cfg, rateLimitModeAuth)
			},
			LimitReached: func(c *fiber.Ctx) error {
				// Fiber's limiter middleware only sets X-RateLimit-* on the
				// success path, not here on the throttled path — set it
				// ourselves so callers can still read their limit on a 429.
				c.Set("X-RateLimit-Limit", strconv.Itoa(cfg.RateLimitAuthPerMin))
				return WriteErrorEnvelope(c, fiber.StatusTooManyRequests, "too_many_requests", "rate limit exceeded")
			},
		})
	}

	if cfg.RateLimitPublicPerMin > 0 {
		publicLimiter = limiter.New(limiter.Config{
			Max:        cfg.RateLimitPublicPerMin,
			Expiration: time.Minute,
			KeyGenerator: func(c *fiber.Ctx) string {
				return rateLimitKey(c, cfg, rateLimitModePublic)
			},
			LimitReached: func(c *fiber.Ctx) error {
				c.Set("X-RateLimit-Limit", strconv.Itoa(cfg.RateLimitPublicPerMin))
				return WriteErrorEnvelope(c, fiber.StatusTooManyRequests, "too_many_requests", "rate limit exceeded")
			},
		})
	}

	return func(c *fiber.Ctx) error {
		switch rateLimitModeForPath(c.Path()) {
		case rateLimitModeAuth:
			return authLimiter(c)
		case rateLimitModePublic:
			return publicLimiter(c)
		default:
			return c.Next()
		}
	}
}

func noOpLimiter() fiber.Handler {
	return func(c *fiber.Ctx) error {
		return c.Next()
	}
}

func rateLimitModeForPath(path string) rateLimitMode {
	switch {
	case path == "/health", path == "/ready", path == "/":
		return rateLimitModeNone
	case strings.HasPrefix(path, "/auth") || strings.HasPrefix(path, "/webhooks"):
		return rateLimitModeAuth
	case strings.HasPrefix(path, "/projects"), path == "/leaderboard", path == "/stats/landing",
		strings.HasPrefix(path, "/ecosystems"), path == "/open-source-week/events", path == "/profile/public":
		return rateLimitModePublic
	default:
		return rateLimitModeNone
	}
}

func rateLimitKey(c *fiber.Ctx, cfg config.Config, mode rateLimitMode) string {
	if userID := authenticatedUserID(c, cfg); userID != "" {
		return "user:" + userID
	}

	ip := clientIP(c, cfg.TrustedProxies)
	if mode == rateLimitModeAuth {
		return "auth-ip:" + ip
	}
	return "public-ip:" + ip
}

func authenticatedUserID(c *fiber.Ctx, cfg config.Config) string {
	if strings.TrimSpace(cfg.JWTSecret) == "" {
		return ""
	}

	h := strings.TrimSpace(c.Get(fiber.HeaderAuthorization))
	if h == "" || !strings.HasPrefix(strings.ToLower(h), "bearer ") {
		return ""
	}

	token := strings.TrimSpace(h[len("bearer "):])
	if token == "" {
		return ""
	}

	claims, err := auth.ParseJWT(cfg.JWTSecret, token)
	if err != nil {
		return ""
	}
	return strings.TrimSpace(claims.Subject)
}

func clientIP(c *fiber.Ctx, trustedProxies []string) string {
	if len(trustedProxies) > 0 {
		remoteAddr := strings.TrimSpace(c.Context().RemoteAddr().String())
		if isTrustedProxy(remoteAddr, trustedProxies) {
			forwarded := strings.TrimSpace(c.Get(fiber.HeaderXForwardedFor))
			if forwarded != "" {
				parts := strings.Split(forwarded, ",")
				for i := len(parts) - 1; i >= 0; i-- {
					candidate := strings.TrimSpace(parts[i])
					if candidate != "" {
						if parsed := net.ParseIP(candidate); parsed != nil {
							return candidate
						}
					}
				}
			}
		}
	}

	ip := strings.TrimSpace(c.IP())
	if ip == "" {
		return "unknown"
	}
	return ip
}

func isTrustedProxy(remoteAddr string, trustedProxies []string) bool {
	if len(trustedProxies) == 0 {
		return false
	}

	host := remoteAddr
	if parsedHost, _, err := net.SplitHostPort(remoteAddr); err == nil {
		host = parsedHost
	}

	ip := net.ParseIP(host)
	if ip == nil {
		return false
	}

	for _, candidate := range trustedProxies {
		candidate = strings.TrimSpace(candidate)
		if candidate == "" {
			continue
		}
		if ip.Equal(net.ParseIP(candidate)) {
			return true
		}
		if _, cidr, err := net.ParseCIDR(candidate); err == nil && cidr.Contains(ip) {
			return true
		}
	}

	return false
}
