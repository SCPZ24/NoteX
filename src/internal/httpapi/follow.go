package httpapi

import (
	"net/http"
	"strconv"

	"github.com/SCPZ24/NoteX/src/internal/follow"
	"github.com/gin-gonic/gin"
)

func targetUser(c *gin.Context) (int64, error) {
	id, err := strconv.ParseInt(c.Param("id"), 10, 64)
	if err != nil || id <= 0 {
		return 0, follow.ErrInvalid
	}
	return id, nil
}

func (h *handlers) followUser(c *gin.Context) {
	target, err := targetUser(c)
	if err != nil {
		writeError(c, err)
		return
	}
	if err := h.follows.Follow(c.Request.Context(), currentUser(c).ID, target); err != nil {
		writeError(c, err)
		return
	}
	c.JSON(http.StatusOK, gin.H{"is_following": true})
}

func (h *handlers) unfollowUser(c *gin.Context) {
	target, err := targetUser(c)
	if err != nil {
		writeError(c, err)
		return
	}
	if err := h.follows.Unfollow(c.Request.Context(), currentUser(c).ID, target); err != nil {
		writeError(c, err)
		return
	}
	c.JSON(http.StatusOK, gin.H{"is_following": false})
}

func (h *handlers) isFollowing(c *gin.Context) {
	target, err := targetUser(c)
	if err != nil {
		writeError(c, err)
		return
	}
	value, err := h.follows.IsFollowing(c.Request.Context(), currentUser(c).ID, target)
	if err != nil {
		writeError(c, err)
		return
	}
	c.JSON(http.StatusOK, gin.H{"is_following": value})
}

func (h *handlers) listFollowing(c *gin.Context) { h.listRelations(c, follow.Following) }
func (h *handlers) listFollowers(c *gin.Context) { h.listRelations(c, follow.Followers) }

func (h *handlers) listRelations(c *gin.Context, direction string) {
	owner, err := targetUser(c)
	if err != nil {
		writeError(c, err)
		return
	}
	limit := 50
	if value, exists := c.GetQuery("limit"); exists {
		limit, err = strconv.Atoi(value)
		if err != nil {
			writeError(c, follow.ErrInvalid)
			return
		}
	}
	page, err := h.follows.List(c.Request.Context(), owner, direction, c.Query("cursor"), limit)
	if err != nil {
		writeError(c, err)
		return
	}
	c.JSON(http.StatusOK, page)
}
