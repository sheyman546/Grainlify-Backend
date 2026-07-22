package handlers

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/gofiber/fiber/v2"

	"github.com/jagadeesh/grainlify/backend/internal/bus"
	"github.com/jagadeesh/grainlify/backend/internal/db"
)

// healthStatus is a per-dependency readiness result.
type healthStatus struct {
	Name   string `json:"name"`
	Ready  bool   `json:"ready"`
	Status string `json:"status"`
}

// NewReady returns a handler that checks both database and (when configured) NATS
// connectivity. It acts as a startup-gate for Kubernetes readiness probes: once
// all dependencies are healthy, it latches to a ready state to prevent the pod
// from being removed from service endpoints during transient blips. Use liveness
// probes (like /health) to monitor ongoing process health.
func NewReady(d *db.DB, b bus.Bus) fiber.Handler {
	var latched atomic.Value

	return func(c *fiber.Ctx) error {
		// Fast path: if startup has successfully completed once,
		// we remain ready (startup-gate behavior). We don't flap on transient blips.
		if cached := latched.Load(); cached != nil {
			return c.Status(fiber.StatusOK).JSON(fiber.Map{
				"ok":   true,
				"deps": cached.([]healthStatus),
			})
		}

		statusCode := fiber.StatusOK
		var deps []healthStatus
		allReady := true

		// Check database.
		dbStatus := healthStatus{Name: "database"}
		if d == nil || d.Pool == nil {
			dbStatus.Ready = false
			dbStatus.Status = "not_configured"
			statusCode = fiber.StatusServiceUnavailable
			allReady = false
		} else {
			ctx, cancel := context.WithTimeout(c.Context(), 1*time.Second)
			defer cancel()

			if err := d.Ping(ctx); err != nil {
				dbStatus.Ready = false
				dbStatus.Status = "unreachable"
				statusCode = fiber.StatusServiceUnavailable
				allReady = false
			} else {
				dbStatus.Ready = true
				dbStatus.Status = "ok"
			}
		}
		deps = append(deps, dbStatus)

		// Check NATS bus (only when configured).
		natsStatus := healthStatus{Name: "nats"}
		if b != nil {
			s := b.Status()
			if s == "CONNECTED" || s == "RECONNECTING" {
				natsStatus.Ready = true
				natsStatus.Status = s
			} else {
				natsStatus.Ready = false
				natsStatus.Status = s
				statusCode = fiber.StatusServiceUnavailable
				allReady = false
			}
		} else {
			natsStatus.Ready = true
			natsStatus.Status = "not_configured"
		}
		deps = append(deps, natsStatus)

		// Latch the readiness state if all dependencies are verified.
		if allReady {
			latched.Store(deps)
		}

		return c.Status(statusCode).JSON(fiber.Map{
			"ok":   statusCode == fiber.StatusOK,
			"deps": deps,
		})
	}
}
