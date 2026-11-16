package feed

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"database/sql"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/SCPZ24/NoteX/src/internal/post"
	"github.com/redis/go-redis/v9"
)

// Stats returns process-local path counts for functional/performance evidence.
func (s *Service) Stats() map[string]uint64 {
	result := make(map[string]uint64)
	s.stats.Range(func(key, value any) bool {
		result[key.(string)] = value.(*atomic.Uint64).Load()
		return true
	})
	return result
}
func (s *Service) recordPath(path string) {
	value, _ := s.stats.LoadOrStore(path, &atomic.Uint64{})
	value.(*atomic.Uint64).Add(1)
}

// Feed preserves the SQL Feed contract. Cache work has a separate two-second
// budget, leaving time for authoritative SQL fallback in the HTTP deadline.
func (s *Service) Feed(ctx context.Context, actor int64, after string, limit int) (post.Page, error) {
	if actor <= 0 || limit < 1 || limit > 100 {
		return post.Page{}, post.ErrInvalid
	}
	var boundary *Entry
	historical := false
	if after != "" {
		cursor, err := post.DecodeCursor(after, actor, "feed")
		if err != nil {
			return post.Page{}, err
		}
		boundary = &Entry{ID: cursor.ID, CreatedOn: cursor.CreatedOn}
		historical = cursor.Source == "sql"
	}
	cacheCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	// Historical requests still renew the existing activity lease without marking
	// missing/incomplete data ready or scanning Redis candidate sources.
	if historical {
		s.renewReader(cacheCtx, actor)
		return s.sqlPage(ctx, actor, after, limit, "history", true)
	}
	sources, reason, err := s.sources(ctx, actor)
	if err != nil {
		return post.Page{}, err
	}
	if reason != "" {
		s.renewReader(cacheCtx, actor)
		return s.sqlPage(ctx, actor, after, limit, reason, false)
	}
	if err := s.touchSources(cacheCtx, sources); err != nil {
		return s.sqlPage(ctx, actor, after, limit, "redis", false)
	}
	cold := false
	for _, source := range sources {
		cold = cold || !source.state.ready
	}
	if cold {
		// Serve this page from SQL first. Warm at most four bounded source snapshots
		// per request; a concurrent update makes installation fail conservatively.
		page, err := s.sqlPage(ctx, actor, after, limit, "cold", false)
		if err != nil {
			return page, err
		}
		warmed := 0
		for _, source := range sources {
			if !source.state.ready && warmed < 4 {
				if err := s.warmSource(cacheCtx, source); err != nil {
					s.recordPath("warm_failed")
				} else {
					s.recordPath("warm_attempt")
				}
				warmed++
			}
		}
		return page, nil
	}
	floor := ""
	for _, source := range sources {
		if source.state.floor > floor {
			floor = source.state.floor
		}
	}
	if boundary != nil && floor != "" && member(*boundary) <= floor {
		return s.sqlPage(ctx, actor, after, limit, "history", true)
	}
	page, fallback, err := s.cachedPage(cacheCtx, actor, boundary, limit, sources)
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
			return s.sqlPage(ctx, actor, after, limit, "budget", false)
		}
		return s.sqlPage(ctx, actor, after, limit, "redis_or_validation", false)
	}
	if fallback != "" {
		return s.sqlPage(ctx, actor, after, limit, fallback, fallback == "history")
	}
	s.recordPath("cache")
	return page, nil
}

var renewActivity = redis.NewScript(`
if redis.call('EXISTS',KEYS[4])==0 then
 redis.call('DEL',unpack(KEYS))
 redis.call('HSET',KEYS[4],'generation',ARGV[2],'signature','pending','ready','0','floor','','count','0','revision','0')
end
for _,key in ipairs(KEYS) do redis.call('PEXPIRE',key,ARGV[1]) end
return 1
`)

func (s *Service) renewReader(ctx context.Context, actor int64) {
	var token [16]byte
	if _, err := rand.Read(token[:]); err != nil {
		s.recordPath("renew_failed")
		return
	}
	if err := renewActivity.Run(ctx, s.redis, s.cacheKeys(cacheSource{owner: actor}), activeWindow.Milliseconds(), fmt.Sprintf("%x", token)).Err(); err != nil {
		s.recordPath("renew_failed")
	}
}
func (s *Service) sqlPage(ctx context.Context, actor int64, after string, limit int, reason string, historical bool) (post.Page, error) {
	s.recordPath("sql_" + reason)
	page, err := s.sql.Feed(ctx, actor, after, limit)
	if err != nil || !page.HasMore || !historical {
		return page, err
	}
	cursor, err := post.DecodeCursor(page.NextCursor, actor, "feed")
	if err != nil {
		return post.Page{}, err
	}
	cursor.Source = "sql"
	page.NextCursor, err = post.EncodeCursor(cursor)
	return page, err
}

// The same bounded SQL result selects pull authors and fingerprints exact
// relationship identities. An unfollow/refollow cannot reuse the old coverage.
func (s *Service) sources(ctx context.Context, actor int64) ([]cacheSource, string, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT f.id,f.follow_id FROM users u LEFT JOIN following f ON f.user_id=u.id WHERE u.id=? ORDER BY f.id LIMIT 4097`, actor)
	if err != nil {
		return nil, "", err
	}
	defer rows.Close()
	found := false
	relations := 0
	fingerprint := sha256.New()
	fmt.Fprintf(fingerprint, "%s/%d", s.policy, actor)
	authors := make(map[int64]bool)
	if s.pullAuthors[actor] {
		authors[actor] = true
	}
	for rows.Next() {
		found = true
		var id, author sql.NullInt64
		if err := rows.Scan(&id, &author); err != nil {
			return nil, "", err
		}
		if id.Valid {
			relations++
			fmt.Fprintf(fingerprint, "/%d", id.Int64)
			if s.pullAuthors[author.Int64] {
				authors[author.Int64] = true
			}
		}
	}
	if err := rows.Err(); err != nil {
		return nil, "", err
	}
	if !found {
		return nil, "", post.ErrUserNotFound
	}
	if relations > 4096 {
		return nil, "relationships", nil
	}
	if len(authors) > maxPullAuthors {
		return nil, "sources", nil
	}
	sources := []cacheSource{{owner: actor, signature: fmt.Sprintf("%x", fingerprint.Sum(nil))}}
	ids := make([]int64, 0, len(authors))
	for id := range authors {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	for _, id := range ids {
		sources = append(sources, cacheSource{owner: id, author: true, signature: s.policy})
	}
	return sources, "", nil
}

func (s *Service) warmSource(ctx context.Context, source cacheSource) error {
	query := `SELECT p.id,p.created_on,0 FROM posts p WHERE p.deleted_on IS NULL AND p.author_id=?`
	args := []any{source.owner}
	if !source.author {
		query = `SELECT p.id,p.created_on,CASE WHEN p.author_id=? THEN 0 ELSE f.id END
 FROM posts p LEFT JOIN following f ON f.user_id=? AND f.follow_id=p.author_id
 WHERE p.deleted_on IS NULL AND (p.author_id=? OR f.id IS NOT NULL)`
		args = []any{source.owner, source.owner, source.owner}
		if len(s.pullAuthors) > 0 {
			ids := make([]int64, 0, len(s.pullAuthors))
			for id := range s.pullAuthors {
				ids = append(ids, id)
			}
			sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
			placeholders := make([]string, len(ids))
			for i, id := range ids {
				placeholders[i] = "?"
				args = append(args, id)
			}
			query += " AND p.author_id NOT IN (" + strings.Join(placeholders, ",") + ")"
		}
	}
	query += " ORDER BY p.created_on DESC,p.id DESC LIMIT 501"
	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return err
	}
	type snapshotEntry struct {
		entry    Entry
		relation int64
	}
	items := make([]snapshotEntry, 0, retainedPosts+1)
	for rows.Next() {
		var item snapshotEntry
		if err := rows.Scan(&item.entry.ID, &item.entry.CreatedOn, &item.relation); err != nil {
			rows.Close()
			return err
		}
		items = append(items, item)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return err
	}
	floor := ""
	if len(items) > retainedPosts {
		items = items[:retainedPosts]
		floor = member(items[len(items)-1].entry)
	}
	values := []any{source.state.generation, source.state.revision, floor, len(items)}
	for _, item := range items {
		if item.entry.ID <= 0 || item.entry.CreatedOn <= 0 {
			return errors.New("invalid SQL snapshot ordering")
		}
		relation := ""
		if !source.author {
			relation = strconv.FormatInt(item.relation, 10)
		}
		values = append(values, member(item.entry), strconv.FormatInt(item.entry.ID, 10), relation)
	}
	applied, err := installCache.Run(ctx, s.redis, s.cacheKeys(source), values...).Int()
	if err != nil {
		return err
	}
	if applied != 1 {
		return errors.New("cache snapshot superseded")
	}
	return nil
}

var readCache = redis.NewScript(`
if redis.call('HGET',KEYS[4],'generation')~=ARGV[1] or redis.call('HGET',KEYS[4],'revision')~=ARGV[2] or redis.call('HGET',KEYS[4],'ready')~='1' then return false end
local n=tonumber(redis.call('HGET',KEYS[4],'count') or '-1')
if redis.call('ZCARD',KEYS[1])~=n then return false end
if ARGV[5]=='reader' and redis.call('HLEN',KEYS[3])~=n then return false end
return redis.call('ZREVRANGEBYLEX',KEYS[1],ARGV[3],'-','LIMIT',0,ARGV[4])
`)

// Each round consumes only the globally safe prefix. If a source may have more
// entries, its last fetched member bounds how far other sources can be used.
func (s *Service) cachedPage(ctx context.Context, actor int64, before *Entry, limit int, sources []cacheSource) (post.Page, string, error) {
	page := post.Page{Items: make([]post.Post, 0, limit+1)}
	maximum := "+"
	if before != nil {
		maximum = "(" + member(*before)
	}
	fetched := 0
	for round := 0; round < maxReadRounds; round++ {
		candidates := make(map[int64]Entry)
		frontier := ""
		coverageFloor := ""
		for start := 0; start < len(sources); start += pipelineSize {
			end := min(start+pipelineSize, len(sources))
			if fetched+(end-start)*(limit+1) > candidateBudget {
				return post.Page{}, "budget", nil
			}
			calls := make([]scriptCall, 0, end-start)
			for _, source := range sources[start:end] {
				kind := "reader"
				if source.author {
					kind = "author"
				}
				calls = append(calls, scriptCall{s.cacheKeys(source), []any{source.state.generation, source.state.revision, maximum, limit + 1, kind}})
			}
			commands, err := s.scriptBatch(ctx, readCache, calls)
			if err != nil {
				return post.Page{}, "", err
			}
			for i, command := range commands {
				values, err := command.StringSlice()
				if err != nil {
					return post.Page{}, "", err
				}
				fetched += len(values)
				source := sources[start+i]
				if source.state.floor > coverageFloor {
					coverageFloor = source.state.floor
				}
				if len(values) == limit+1 && values[len(values)-1] > frontier {
					frontier = values[len(values)-1]
				}
				for _, value := range values {
					entry, err := parseMember(value)
					if err != nil {
						return post.Page{}, "", err
					}
					if prior, ok := candidates[entry.ID]; ok && prior != entry {
						return post.Page{}, "", errors.New("conflicting index ordering")
					}
					candidates[entry.ID] = entry
				}
			}
		}
		if coverageFloor > frontier {
			frontier = coverageFloor
		}
		entries := make([]Entry, 0, len(candidates))
		for _, entry := range candidates {
			if frontier == "" || member(entry) >= frontier {
				entries = append(entries, entry)
			}
		}
		sort.Slice(entries, func(i, j int) bool {
			if entries[i].CreatedOn == entries[j].CreatedOn {
				return entries[i].ID > entries[j].ID
			}
			return entries[i].CreatedOn > entries[j].CreatedOn
		})
		for start := 0; start < len(entries) && len(page.Items) <= limit; start += 256 {
			end := min(start+256, len(entries))
			ids := make([]int64, 0, end-start)
			for _, entry := range entries[start:end] {
				ids = append(ids, entry.ID)
			}
			items, err := s.sql.FeedCandidates(ctx, actor, ids, limit+1-len(page.Items))
			if err != nil {
				return post.Page{}, "", err
			}
			for _, item := range items {
				if candidates[item.ID].CreatedOn != item.CreatedOn {
					return post.Page{}, "", errors.New("candidate time differs from SQL")
				}
				page.Items = append(page.Items, item)
			}
		}
		if len(page.Items) > limit {
			if err := s.validateSources(ctx, sources); err != nil {
				return post.Page{}, "", err
			}
			page.Items = page.Items[:limit]
			page.HasMore = true
			last := page.Items[len(page.Items)-1]
			cursor, err := post.EncodeCursor(post.Cursor{Owner: actor, Kind: "feed", CreatedOn: last.CreatedOn, ID: last.ID})
			page.NextCursor = cursor
			return page, "", err
		}
		if frontier == "" {
			if err := s.validateSources(ctx, sources); err != nil {
				return post.Page{}, "", err
			}
			return page, "", nil
		}
		if frontier == coverageFloor {
			return post.Page{}, "history", nil
		}
		maximum = "(" + frontier
	}
	return post.Page{}, "budget", nil
}

func (s *Service) validateSources(ctx context.Context, sources []cacheSource) error {
	for start := 0; start < len(sources); start += pipelineSize {
		end := min(start+pipelineSize, len(sources))
		pipe := s.redis.Pipeline()
		commands := make([]*redis.SliceCmd, 0, end-start)
		for _, source := range sources[start:end] {
			commands = append(commands, pipe.HMGet(ctx, s.cacheKeys(source)[3], "generation", "revision", "ready"))
		}
		if _, err := pipe.Exec(ctx); err != nil {
			return err
		}
		for i, command := range commands {
			values, err := command.Result()
			if err != nil {
				return err
			}
			state := sources[start+i].state
			if len(values) != 3 || values[0] != state.generation || values[1] != state.revision || values[2] != "1" {
				return errors.New("cache changed during read")
			}
		}
	}
	return nil
}
