package github

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"testing"

	"github.com/gofiber/fiber/v2"
)

type mockTransport struct {
	roundTripFunc func(req *http.Request) (*http.Response, error)
}

func (m *mockTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	return m.roundTripFunc(req)
}

func TestAssignees_ConflictHandling(t *testing.T) {
	client := NewClient()

	t.Run("double-assign no error", func(t *testing.T) {
		client.HTTP.Transport = &mockTransport{
			roundTripFunc: func(req *http.Request) (*http.Response, error) {
				return &http.Response{
					StatusCode: http.StatusCreated,
					Body:       io.NopCloser(bytes.NewBufferString(`{}`)),
				}, nil
			},
		}

		err := client.AddIssueAssignees(context.Background(), "token", "owner/repo", 1, []string{"user"})
		if err != nil {
			t.Fatalf("expected no error for idempotent assignment, got %v", err)
		}
	})

	t.Run("double-remove no error", func(t *testing.T) {
		client.HTTP.Transport = &mockTransport{
			roundTripFunc: func(req *http.Request) (*http.Response, error) {
				return &http.Response{
					StatusCode: http.StatusOK,
					Body:       io.NopCloser(bytes.NewBufferString(`{}`)),
				}, nil
			},
		}

		err := client.RemoveIssueAssignees(context.Background(), "token", "owner/repo", 1, []string{"user"})
		if err != nil {
			t.Fatalf("expected no error for idempotent removal, got %v", err)
		}
	})

	t.Run("assignee lacking repo access", func(t *testing.T) {
		client.HTTP.Transport = &mockTransport{
			roundTripFunc: func(req *http.Request) (*http.Response, error) {
				errBody := `{"message":"Validation Failed","documentation_url":"https://docs.github.com/rest/issues/assignees"}`
				return &http.Response{
					StatusCode: http.StatusUnprocessableEntity,
					Body:       io.NopCloser(bytes.NewBufferString(errBody)),
				}, nil
			},
		}

		err := client.AddIssueAssignees(context.Background(), "token", "owner/repo", 1, []string{"invalid_user"})
		if err == nil {
			t.Fatal("expected error for invalid assignee, got nil")
		}

		fiberErr, ok := err.(*fiber.Error)
		if !ok {
			t.Fatalf("expected *fiber.Error, got %T: %v", err, err)
		}
		if fiberErr.Code != fiber.StatusUnprocessableEntity {
			t.Errorf("expected fiber.StatusUnprocessableEntity (422), got %d", fiberErr.Code)
		}
		if fiberErr.Message != "Validation Failed" {
			t.Errorf("expected 'Validation Failed', got %q", fiberErr.Message)
		}
	})
}
