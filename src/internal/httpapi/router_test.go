package httpapi

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
)

// No database instance is required. A failed dependency must not be reported as ready.
func TestHealthChecksDuringDatabaseFailure(t *testing.T) {
	gin.SetMode(gin.TestMode)
	checks := 0
	router := NewRouter(func(ctx context.Context) error {
		checks++
		if _, ok := ctx.Deadline(); !ok {
			t.Error("database readiness check must have a deadline")
		}
		return errors.New("database unavailable: private connection details")
	})

	for _, tc := range []struct {
		path   string
		status int
		body   string
	}{
		{"/healthz", http.StatusOK, `{"status":"ok"}`},
		{"/readyz", http.StatusServiceUnavailable, `{"status":"unavailable"}`},
	} {
		response := httptest.NewRecorder()
		router.ServeHTTP(response, httptest.NewRequest(http.MethodGet, tc.path, nil))
		if response.Code != tc.status || response.Body.String() != tc.body {
			t.Errorf("%s: got %d %s, want %d %s", tc.path,
				response.Code, response.Body.String(), tc.status, tc.body)
		}
	}
	if checks != 1 {
		t.Errorf("got %d database checks; only readiness should access the database", checks)
	}
}
