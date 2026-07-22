package logger

import (
	"strings"
)

// RedactString redacts a sensitive string, replacing it with a placeholder.
// It retains the first 4 characters for identification if the string is long enough.
// This is useful for redacting secrets, tokens, or PII.
func RedactString(s string) string {
	if s == "" {
		return ""
	}
	if len(s) > 8 {
		return s[:4] + "...[REDACTED]"
	}
	return "[REDACTED]"
}

// RedactMap takes a map of arguments and returns a new map where values
// associated with sensitive keys (like "address", "amount", "secret", "token", "email")
// are redacted. Nested maps are also processed.
func RedactMap(m map[string]interface{}) map[string]interface{} {
	if m == nil {
		return nil
	}
	out := make(map[string]interface{}, len(m))
	for k, v := range m {
		kLower := strings.ToLower(k)
		if strings.Contains(kLower, "address") ||
			strings.Contains(kLower, "amount") ||
			strings.Contains(kLower, "email") ||
			strings.Contains(kLower, "secret") ||
			strings.Contains(kLower, "token") {
			out[k] = "[REDACTED]"
		} else if nestedMap, ok := v.(map[string]interface{}); ok {
			out[k] = RedactMap(nestedMap)
		} else if slice, ok := v.([]interface{}); ok {
			out[k] = redactSlice(slice)
		} else {
			out[k] = v
		}
	}
	return out
}

// redactSlice processes a slice, recursively redacting any map[string]interface{} elements.
func redactSlice(slice []interface{}) []interface{} {
	if slice == nil {
		return nil
	}
	out := make([]interface{}, len(slice))
	for i, v := range slice {
		if nestedMap, ok := v.(map[string]interface{}); ok {
			out[i] = RedactMap(nestedMap)
		} else if nestedSlice, ok := v.([]interface{}); ok {
			out[i] = redactSlice(nestedSlice)
		} else {
			out[i] = v
		}
	}
	return out
}
