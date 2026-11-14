package httpapi

import (
	"net/http"

	"github.com/SCPZ24/NoteX/src/internal/account"
	"github.com/gin-gonic/gin"
)

type credentials struct {
	Username string `json:"username" binding:"required"`
	Password string `json:"password" binding:"required"`
}

func (h *handlers) register(c *gin.Context) {
	var input credentials
	if c.ShouldBindJSON(&input) != nil {
		writeError(c, account.ErrInvalid)
		return
	}
	user, err := h.accounts.Register(c.Request.Context(), input.Username, input.Password)
	if err != nil {
		writeError(c, err)
		return
	}
	c.JSON(http.StatusCreated, user)
}

func (h *handlers) login(c *gin.Context) {
	var input credentials
	if c.ShouldBindJSON(&input) != nil {
		writeError(c, account.ErrInvalid)
		return
	}
	result, err := h.accounts.Login(c.Request.Context(), input.Username, input.Password)
	if err != nil {
		writeError(c, err)
		return
	}
	c.Header("Cache-Control", "no-store")
	c.JSON(http.StatusOK, result)
}

func (h *handlers) logout(c *gin.Context) {
	if err := h.accounts.Logout(c.Request.Context(), c.GetString("session_token")); err != nil {
		writeError(c, err)
		return
	}
	c.JSON(http.StatusOK, gin.H{"status": "ok"})
}

func (h *handlers) findUser(c *gin.Context) {
	user, err := h.accounts.FindByUsername(c.Request.Context(), c.Query("username"))
	if err != nil {
		writeError(c, err)
		return
	}
	c.JSON(http.StatusOK, user)
}
