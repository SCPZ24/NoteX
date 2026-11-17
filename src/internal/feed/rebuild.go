package feed

import (
	"context"
	"errors"
	"time"

	"github.com/SCPZ24/NoteX/src/internal/cache"
	"github.com/SCPZ24/NoteX/src/internal/post"
)

func (s *Service) ensureAuthor(ctx context.Context, source cacheSource) error {
	key := s.cacheKeys(source)[3] + ":build"
	return s.cache.Do(ctx, key+":"+source.signature, func(task context.Context) error {
		sources := []cacheSource{source}
		for attempt := 0; attempt < 2; attempt++ {
			if err := s.touchSources(task, sources); err != nil {
				return err
			}
			if sources[0].state.ready {
				return nil
			}
			token, owned, err := s.cache.Claim(task, key)
			if err != nil {
				return err
			}
			if !owned {
				timeout := time.NewTimer(200 * time.Millisecond)
				tick := time.NewTicker(20 * time.Millisecond)
				defer timeout.Stop()
				defer tick.Stop()
				for {
					select {
					case <-task.Done():
						return task.Err()
					case <-timeout.C:
						return cache.ErrSuperseded
					case <-tick.C:
						if err := s.touchSources(task, sources); err != nil {
							return err
						}
						if sources[0].state.ready {
							return nil
						}
					}
				}
			}
			// Refresh generation/revision after claim, before SQL. Snapshot installation
			// validates these along with the lease in one Redis script.
			err = s.touchSources(task, sources)
			installed := false
			if err == nil && !sources[0].state.ready {
				err = s.warmSnapshot(task, sources[0], token)
				installed = err == nil
			}
			s.cache.Release(task, key, token)
			if err == nil {
				if installed {
					s.cache.Count("author_built")
				}
				return nil
			}
			if task.Err() != nil {
				return task.Err()
			}
			if !errors.Is(err, cache.ErrSuperseded) {
				return err
			}
			s.cache.Count("author_commit_rejected")
		}
		return cache.ErrSuperseded
	})
}

// List uses the same lazy author index as hybrid Feed. SQL remains authoritative
// for deletion and historical pagination; cache coverage never means full history.
func (s *Service) List(ctx context.Context, owner int64, after string, limit int) (post.Page, error) {
	if owner <= 0 || limit < 1 || limit > 100 {
		return post.Page{}, post.ErrInvalid
	}
	var before *Entry
	if after != "" {
		cursor, err := post.DecodeCursor(after, owner, "user_posts")
		if err != nil {
			return post.Page{}, err
		}
		before = &Entry{ID: cursor.ID, CreatedOn: cursor.CreatedOn}
		if cursor.Source == "sql" {
			return s.authorSQL(ctx, owner, after, limit)
		}
	}
	// Author existence is distinct from an existing author with no posts.
	var exists bool
	err := s.cache.SQL(ctx, func(query context.Context) error {
		return s.db.QueryRowContext(query, "SELECT EXISTS(SELECT 1 FROM users WHERE id=?)", owner).Scan(&exists)
	})
	if err != nil {
		return post.Page{}, err
	}
	if !exists {
		return post.Page{}, post.ErrUserNotFound
	}
	cacheCtx, cancel := context.WithTimeout(ctx, cache.BuildTimeout)
	defer cancel()
	sources := []cacheSource{{owner: owner, author: true, signature: s.policy}}
	if err := s.touchSources(cacheCtx, sources); err != nil {
		return s.authorSQL(ctx, owner, after, limit)
	}
	if !sources[0].state.ready {
		if err := s.ensureAuthor(cacheCtx, sources[0]); err != nil {
			if errors.Is(err, cache.ErrBusy) {
				return post.Page{}, err
			}
			return s.authorSQL(ctx, owner, after, limit)
		}
		if err := s.touchSources(cacheCtx, sources); err != nil {
			return s.authorSQL(ctx, owner, after, limit)
		}
	}
	if before != nil && sources[0].state.floor != "" && member(*before) <= sources[0].state.floor {
		return s.authorSQL(ctx, owner, after, limit)
	}
	page, fallback, err := s.cachedPage(cacheCtx, owner, before, limit, sources)
	if err != nil || fallback != "" {
		return s.authorSQL(ctx, owner, after, limit)
	}
	// cachedPage uses Feed's cursor namespace; the public author endpoint retains
	// its existing owner/kind contract.
	if page.HasMore {
		last := page.Items[len(page.Items)-1]
		page.NextCursor, err = post.EncodeCursor(post.Cursor{Owner: owner, Kind: "user_posts", CreatedOn: last.CreatedOn, ID: last.ID})
	}
	s.cache.Count("author_list_hit")
	return page, err
}

func (s *Service) authorSQL(ctx context.Context, owner int64, after string, limit int) (post.Page, error) {
	var page post.Page
	err := s.cache.SQL(ctx, func(query context.Context) error {
		var e error
		page, e = s.sql.List(query, owner, after, limit)
		return e
	})
	if err == nil && page.HasMore {
		cursor, e := post.DecodeCursor(page.NextCursor, owner, "user_posts")
		if e != nil {
			return post.Page{}, e
		}
		cursor.Source = "sql"
		page.NextCursor, err = post.EncodeCursor(cursor)
	}
	s.cache.Count("author_sql_fallback")
	return page, err
}
