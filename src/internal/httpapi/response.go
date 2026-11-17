package httpapi

import (
	"context"
	"errors"
	"log/slog"
	"net/http"

	"github.com/SCPZ24/NoteX/src/internal/account"
	"github.com/SCPZ24/NoteX/src/internal/cache"
	"github.com/SCPZ24/NoteX/src/internal/core"
	"github.com/SCPZ24/NoteX/src/internal/follow"
	"github.com/SCPZ24/NoteX/src/internal/post"
	"github.com/gin-gonic/gin"
)

func writeError(c *gin.Context, err error) {
	status, code := http.StatusInternalServerError, "internal_error"
	switch {
	case errors.Is(err, account.ErrInvalid), errors.Is(err, follow.ErrInvalid), errors.Is(err, post.ErrInvalid):
		status, code = http.StatusBadRequest, "invalid_input"
	case errors.Is(err, follow.ErrInvalidCursor), errors.Is(err, post.ErrInvalidCursor):
		status, code = http.StatusBadRequest, "invalid_cursor"
	case errors.Is(err, account.ErrUnauthorized):
		status, code = http.StatusUnauthorized, "unauthorized"
	case errors.Is(err, account.ErrUsernameTaken):
		status, code = http.StatusConflict, "username_taken"
	case errors.Is(err, post.ErrRequestConflict):
		status, code = http.StatusConflict, "idempotency_conflict"
	case errors.Is(err, account.ErrNotFound), errors.Is(err, follow.ErrUserNotFound), errors.Is(err, post.ErrUserNotFound):
		status, code = http.StatusNotFound, "user_not_found"
	case errors.Is(err, post.ErrNotFound):
		status, code = http.StatusNotFound, "post_not_found"
	case errors.Is(err, post.ErrForbidden):
		status, code = http.StatusForbidden, "forbidden"
	case errors.Is(err, post.ErrCacheUnavailable):
		status, code = http.StatusServiceUnavailable, "cache_invalidation_pending"
	case errors.Is(err, cache.ErrBusy):
		status, code = http.StatusServiceUnavailable, "cache_busy"
	case errors.Is(err, follow.ErrBusy):
		status, code = http.StatusServiceUnavailable, "relationship_busy"
	case errors.Is(err, context.DeadlineExceeded), errors.Is(err, context.Canceled), errors.Is(err, core.ErrUnavailable):
		status, code = http.StatusServiceUnavailable, "request_unavailable"
	default:
		slog.Error("API operation failed", "route", c.FullPath(), "error", err)
	}
	c.AbortWithStatusJSON(status, gin.H{"error": code})
}
