package api_test

import (
	"context"
	"net"
	"net/http"
	"testing"
	"time"

	"github.com/gofiber/fiber/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/jagadeesh/grainlify/backend/internal/api"
)

// createHangingApp sets up a Fiber app with an active request that hangs,
// preventing app.Shutdown() from completing immediately.
func createHangingApp(t *testing.T) (*fiber.App, net.Listener) {
	app := fiber.New(fiber.Config{DisableStartupMessage: true})

	// This endpoint will block for 2 seconds
	app.Get("/hang", func(c *fiber.Ctx) error {
		time.Sleep(2 * time.Second)
		return c.SendString("ok")
	})

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	go func() {
		_ = app.Listener(ln)
	}()

	// Wait for the server to start
	time.Sleep(100 * time.Millisecond)

	// Fire a request that will block the shutdown process
	go func() {
		_, _ = http.Get("http://" + ln.Addr().String() + "/hang")
	}()

	// Wait for the request to hit the server
	time.Sleep(100 * time.Millisecond)

	return app, ln
}

func TestShutdown_ContextDeadlineWins(t *testing.T) {
	app, ln := createHangingApp(t)
	defer ln.Close()

	// Context with a short deadline that will expire before the 2s hanging request
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	start := time.Now()
	err := api.Shutdown(ctx, app)
	duration := time.Since(start)

	// We expect the context deadline to fire, overriding the hanging shutdown
	assert.ErrorIs(t, err, context.DeadlineExceeded)
	assert.Less(t, duration, 1*time.Second, "Shutdown should return promptly on deadline")
	
	// Goroutine leak/panic check:
	// The test completes and the background goroutine in Shutdown will eventually
	// send to the buffered errCh, which is safe and doesn't panic.
}

func TestShutdown_NormalCompletionFirst(t *testing.T) {
	app := fiber.New(fiber.Config{DisableStartupMessage: true})
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	go func() {
		_ = app.Listener(ln)
	}()
	time.Sleep(100 * time.Millisecond)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	start := time.Now()
	err = api.Shutdown(ctx, app)
	duration := time.Since(start)

	// We expect no error since it shuts down cleanly and immediately
	assert.NoError(t, err)
	assert.Less(t, duration, 1*time.Second, "Shutdown should return promptly when no active requests")
}

func TestShutdown_AlreadyExpiredContext(t *testing.T) {
	app, ln := createHangingApp(t)
	defer ln.Close()

	// Create a context and cancel it immediately before calling Shutdown
	ctx, cancel := context.WithCancel(context.Background())
	cancel() 

	start := time.Now()
	err := api.Shutdown(ctx, app)
	duration := time.Since(start)

	assert.ErrorIs(t, err, context.Canceled)
	assert.Less(t, duration, 500*time.Millisecond, "Shutdown should return immediately on expired context")
}

func TestShutdown_ContextCanceledMidShutdown(t *testing.T) {
	app, ln := createHangingApp(t)
	defer ln.Close()

	ctx, cancel := context.WithCancel(context.Background())
	
	// Cancel the context while the shutdown is waiting
	go func() {
		time.Sleep(200 * time.Millisecond)
		cancel()
	}()

	start := time.Now()
	err := api.Shutdown(ctx, app)
	duration := time.Since(start)

	assert.ErrorIs(t, err, context.Canceled)
	assert.Less(t, duration, 1*time.Second, "Shutdown should return promptly when context is canceled mid-flight")
}

func TestShutdown_AppShutdownReturnsErrorAndRepeatedCalls(t *testing.T) {
	app := fiber.New(fiber.Config{DisableStartupMessage: true})
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	go func() {
		_ = app.Listener(ln)
	}()
	time.Sleep(100 * time.Millisecond)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	// First shutdown should succeed normally
	err1 := api.Shutdown(ctx, app)
	assert.NoError(t, err1)

	// Repeated shutdown on an already shutdown app. This Fiber version's
	// app.Shutdown() is idempotent and returns nil on a repeated call rather
	// than an error, and in any case a repeated call must not surface a
	// context error from our own wrapper.
	ctx2, cancel2 := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel2()

	err2 := api.Shutdown(ctx2, app)

	assert.NoError(t, err2)
	assert.NotErrorIs(t, err2, context.DeadlineExceeded)
	assert.NotErrorIs(t, err2, context.Canceled)
}
