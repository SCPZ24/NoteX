package httpapi

import (
	"context"
	"net/http"
	"time"

	"github.com/SCPZ24/NoteX/src/internal/account"
	"github.com/SCPZ24/NoteX/src/internal/follow"
	"github.com/SCPZ24/NoteX/src/internal/post"
	"github.com/gin-gonic/gin"
)

func NewRouter(pingDB func(context.Context) error) *gin.Engine {
	router := gin.New()
	router.Use(gin.Logger(), gin.Recovery())

	// Liveness checks the HTTP process, independently of database availability.
	router.GET("/healthz", func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{"status": "ok"})
	})

	// Readiness checks whether the application can reach its authoritative store.
	router.GET("/readyz", func(c *gin.Context) {
		ctx, cancel := context.WithTimeout(c.Request.Context(), time.Second)
		defer cancel()
		if err := pingDB(ctx); err != nil {
			c.JSON(http.StatusServiceUnavailable, gin.H{"status": "unavailable"})
			return
		}
		c.JSON(http.StatusOK, gin.H{"status": "ready"})
	})
	return router
}

type handlers struct {
	accounts *account.Service
	follows  *follow.Service
	posts    *post.Service
}

// RegisterAPI leaves the existing health routes available without authentication.
func RegisterAPI(router *gin.Engine, accounts *account.Service, follows *follow.Service, posts *post.Service) {
	h := &handlers{accounts: accounts, follows: follows, posts: posts}
	api := router.Group("/api/v1")
	api.Use(func(c *gin.Context) {
		c.Request.Body = http.MaxBytesReader(c.Writer, c.Request.Body, 4096)
		ctx, cancel := context.WithTimeout(c.Request.Context(), 5*time.Second)
		defer cancel()
		c.Request = c.Request.WithContext(ctx)
		c.Next()
	})
	api.POST("/register", h.register)
	api.POST("/login", h.login)
	private := api.Group("")
	private.Use(h.authenticate)
	private.POST("/logout", h.logout)
	private.GET("/me", func(c *gin.Context) { c.JSON(http.StatusOK, currentUser(c)) })
	private.GET("/users", h.findUser)
	private.PUT("/users/:id/follow", h.followUser)
	private.DELETE("/users/:id/follow", h.unfollowUser)
	private.GET("/users/:id/follow", h.isFollowing)
	private.GET("/users/:id/following", h.listFollowing)
	private.GET("/users/:id/followers", h.listFollowers)
	private.POST("/posts", h.createPost)
	private.GET("/posts/:id", h.getPost)
	private.DELETE("/posts/:id", h.deletePost)
	private.GET("/users/:id/posts", h.listPosts)
	private.GET("/feed", h.listFeed)
}
