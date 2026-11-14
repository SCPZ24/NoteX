package httpapi

import (
	"strings"

	"github.com/SCPZ24/NoteX/src/internal/account"
	"github.com/gin-gonic/gin"
)

func (h *handlers) authenticate(c *gin.Context) {
	parts := strings.Fields(c.GetHeader("Authorization"))
	if len(parts) != 2 || !strings.EqualFold(parts[0], "Bearer") {
		writeError(c, account.ErrUnauthorized)
		return
	}
	user, err := h.accounts.Authenticate(c.Request.Context(), parts[1])
	if err != nil {
		writeError(c, err)
		return
	}
	c.Set("current_user", user)
	c.Set("session_token", parts[1])
	c.Header("Cache-Control", "no-store")
	c.Next()
}

func currentUser(c *gin.Context) account.User {
	return c.MustGet("current_user").(account.User)
}
