package httpapi

import (
	"context"
	"crypto/rand"
	"net/http"
	"time"

	"github.com/SCPZ24/NoteX/src/internal/account"
	"github.com/SCPZ24/NoteX/src/internal/follow"
	"github.com/SCPZ24/NoteX/src/internal/post"
	"github.com/gin-gonic/gin"
	"google.golang.org/grpc/metadata"
)

func NewRouter(pingDB func(context.Context) error) *gin.Engine {
	router := gin.New()
	router.Use(gin.Logger(), gin.Recovery())

	// Liveness checks the HTTP process, independently of database availability.
	router.GET("/healthz", func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{"status": "ok"})
	})

	// Readiness uses the dependencies required by this process role.
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
	feed     FeedReader
	feedAuth Authenticator
}

// Authenticator allows Feed routes to authenticate through Core RPC.
type Authenticator interface {
	Authenticate(context.Context, string) (account.User, error)
}

// FeedReader is implemented by both the SQL baseline and the Redis-backed Feed.
type FeedReader interface {
	Feed(context.Context, int64, string, int) (post.Page, error)
	List(context.Context, int64, string, int) (post.Page, error)
}

// RegisterAPI retains the combined-process entry point.
func RegisterAPI(router *gin.Engine, accounts *account.Service, follows *follow.Service, posts *post.Service, reader FeedReader) {
	RegisterCore(router, accounts, follows, posts)
	var auth Authenticator = accounts
	if remote, ok := reader.(Authenticator); ok {
		auth = remote
	}
	RegisterFeed(router, reader, auth)
}

func apiGroup(router *gin.Engine) *gin.RouterGroup {
	api := router.Group("/api/v1")
	api.Use(func(c *gin.Context) {
		c.Request.Body = http.MaxBytesReader(c.Writer, c.Request.Body, 4096)
		ctx, cancel := context.WithTimeout(c.Request.Context(), 5*time.Second)
		defer cancel()
		requestID := rand.Text()
		ctx = metadata.AppendToOutgoingContext(ctx, "x-request-id", requestID)
		c.Header("X-Request-ID", requestID)
		c.Request = c.Request.WithContext(ctx)
		c.Next()
	})
	return api
}

// RegisterCore exposes account, relationship and post operations.
func RegisterCore(router *gin.Engine, accounts *account.Service, follows *follow.Service, posts *post.Service) {
	h := &handlers{accounts: accounts, follows: follows, posts: posts}
	api := apiGroup(router)
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
	private.PUT("/posts/:id/like", h.setLike)
	private.DELETE("/posts/:id/like", h.setLike)
	private.DELETE("/posts/:id", h.deletePost)
}

// RegisterFeed exposes only Feed and author listing, using Core authentication.
func RegisterFeed(router *gin.Engine, reader FeedReader, auth Authenticator) {
	h := &handlers{feed: reader, feedAuth: auth}
	api := apiGroup(router)
	api.Use(h.authenticateFeed)
	api.GET("/users/:id/posts", h.listPosts)
	api.GET("/feed", h.listFeed)
}
