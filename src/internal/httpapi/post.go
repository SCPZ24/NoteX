package httpapi

import (
	"net/http"
	"strconv"

	"github.com/SCPZ24/NoteX/src/internal/post"
	"github.com/gin-gonic/gin"
)

func (h *handlers) createPost(c *gin.Context) {
	var input struct {
		Content string `json:"content" binding:"required"`
	}
	if c.ShouldBindJSON(&input) != nil {
		writeError(c, post.ErrInvalid)
		return
	}
	keys := c.Request.Header.Values("Idempotency-Key")
	if len(keys) != 1 {
		writeError(c, post.ErrInvalid)
		return
	}
	item, err := h.posts.Create(c.Request.Context(), currentUser(c).ID, keys[0], input.Content)
	if err != nil {
		writeError(c, err)
		return
	}
	c.JSON(http.StatusCreated, item)
}

func postID(c *gin.Context) (int64, error) {
	id, err := strconv.ParseInt(c.Param("id"), 10, 64)
	if err != nil || id <= 0 {
		return 0, post.ErrInvalid
	}
	return id, nil
}

func (h *handlers) getPost(c *gin.Context) {
	id, err := postID(c)
	if err != nil {
		writeError(c, err)
		return
	}
	item, err := h.posts.Get(c.Request.Context(), id)
	if err != nil {
		writeError(c, err)
		return
	}
	c.JSON(http.StatusOK, item)
}

func (h *handlers) deletePost(c *gin.Context) {
	id, err := postID(c)
	if err != nil {
		writeError(c, err)
		return
	}
	if err := h.posts.Delete(c.Request.Context(), currentUser(c).ID, id); err != nil {
		writeError(c, err)
		return
	}
	c.JSON(http.StatusOK, gin.H{"status": "ok"})
}

func (h *handlers) listPosts(c *gin.Context) {
	owner, err := targetUser(c)
	if err != nil {
		writeError(c, err)
		return
	}
	limit, err := postLimit(c)
	if err != nil {
		writeError(c, err)
		return
	}
	page, err := h.posts.List(c.Request.Context(), owner, c.Query("cursor"), limit)
	if err != nil {
		writeError(c, err)
		return
	}
	c.JSON(http.StatusOK, page)
}

func (h *handlers) listFeed(c *gin.Context) {
	limit, err := postLimit(c)
	if err != nil {
		writeError(c, err)
		return
	}
	page, err := h.feed.Feed(c.Request.Context(), currentUser(c).ID, c.Query("cursor"), limit)
	if err != nil {
		writeError(c, err)
		return
	}
	c.JSON(http.StatusOK, page)
}

func postLimit(c *gin.Context) (int, error) {
	value := c.Query("limit")
	if value == "" {
		return 20, nil
	}
	limit, err := strconv.Atoi(value)
	if err != nil || limit < 1 || limit > 100 {
		return 0, post.ErrInvalid
	}
	return limit, nil
}
