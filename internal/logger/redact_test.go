package logger

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"
)

func TestRedactMapRedactsSensitiveKeysRegardlessOfCaseAndDepth(t *testing.T) {
	t.Parallel()

	secretValues := []string{
		"0x123456789abcdef",
		"2500000",
		"person@example.com",
		"super-secret-value",
		"ghp_token_value",
	}

	payload := map[string]interface{}{
		"ADDRESS": secretValues[0],
		"metadata": map[string]interface{}{
			"nestedAmount": secretValues[1],
			"profile": map[string]interface{}{
				"ContactEmail": secretValues[2],
				"credentials": map[string]interface{}{
					"apiSECRET":    secretValues[3],
					"SessionToken": secretValues[4],
				},
			},
		},
	}

	redacted := RedactMap(payload)
	redactedJSON, err := json.Marshal(redacted)
	if err != nil {
		t.Fatalf("marshal redacted payload: %v", err)
	}
	redactedText := string(redactedJSON)

	for _, secretValue := range secretValues {
		if strings.Contains(redactedText, secretValue) {
			t.Fatalf("redacted payload contains original secret value %q: %s", secretValue, redactedText)
		}
	}

	assertPathEquals(t, redacted, []interface{}{"ADDRESS"}, "[REDACTED]")
	assertPathEquals(t, redacted, []interface{}{"metadata", "nestedAmount"}, "[REDACTED]")
	assertPathEquals(t, redacted, []interface{}{"metadata", "profile", "ContactEmail"}, "[REDACTED]")
	assertPathEquals(t, redacted, []interface{}{"metadata", "profile", "credentials", "apiSECRET"}, "[REDACTED]")
	assertPathEquals(t, redacted, []interface{}{"metadata", "profile", "credentials", "SessionToken"}, "[REDACTED]")
}

func TestRedactMapLeavesNonSensitiveFieldsUnchanged(t *testing.T) {
	t.Parallel()

	payload := map[string]interface{}{
		"userID":     "user-123",
		"status":     "active",
		"retryCount": 3,
		"metadata": map[string]interface{}{
			"requestID": "req-456",
			"enabled":   true,
		},
	}

	redacted := RedactMap(payload)

	if !reflect.DeepEqual(redacted, payload) {
		t.Fatalf("non-sensitive fields changed: got %#v, want %#v", redacted, payload)
	}
}

func TestRedactMapRedactsSensitiveKeysInsideSliceValues(t *testing.T) {
	t.Parallel()

	secretValues := []string{
		"alice@example.com",
		"bob@example.com",
		"secret-token-123",
	}

	payload := map[string]interface{}{
		"accounts": []interface{}{
			map[string]interface{}{
				"email":    secretValues[0],
				"username": "alice",
			},
			map[string]interface{}{
				"email":    secretValues[1],
				"username": "bob",
			},
		},
		"sessions": []interface{}{
			map[string]interface{}{
				"accessToken": secretValues[2],
				"expiresIn":   3600,
			},
		},
		"metadata": map[string]interface{}{
			"nestedArray": []interface{}{
				map[string]interface{}{
					"secretKey": "nested-secret",
				},
			},
		},
	}

	redacted := RedactMap(payload)
	redactedJSON, err := json.Marshal(redacted)
	if err != nil {
		t.Fatalf("marshal redacted payload: %v", err)
	}
	redactedText := string(redactedJSON)

	for _, secretValue := range secretValues {
		if strings.Contains(redactedText, secretValue) {
			t.Fatalf("redacted payload contains original secret value %q: %s", secretValue, redactedText)
		}
	}

	// Verify nested secret is also redacted
	if strings.Contains(redactedText, "nested-secret") {
		t.Fatalf("redacted payload contains nested secret: %s", redactedText)
	}

	// Verify non-sensitive fields are preserved
	assertPathEquals(t, redacted, []interface{}{"accounts", 0, "username"}, "alice")
	assertPathEquals(t, redacted, []interface{}{"accounts", 1, "username"}, "bob")
	assertPathEquals(t, redacted, []interface{}{"sessions", 0, "expiresIn"}, 3600)
}

func assertPathEquals(t *testing.T, payload map[string]interface{}, path []interface{}, want interface{}) {
	t.Helper()

	var current interface{} = payload
	for _, key := range path {
		if idx, ok := key.(int); ok {
			slice, ok := current.([]interface{})
			if !ok {
				t.Fatalf("path %v expected slice at key %v, got %#v", path, key, current)
			}
			current = slice[idx]
		} else {
			currentMap, ok := current.(map[string]interface{})
			if !ok {
				t.Fatalf("path %v reached non-map value %#v at key %v", path, current, key)
			}
			current = currentMap[key.(string)]
		}
	}

	if current != want {
		t.Fatalf("path %v = %#v, want %#v", path, current, want)
	}
}
