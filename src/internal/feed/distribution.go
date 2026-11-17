package feed

import (
	"context"
	"errors"
	"fmt"
	"strconv"

	"github.com/SCPZ24/NoteX/src/internal/outbox"
	"github.com/SCPZ24/NoteX/src/internal/post"
)

func (s *Service) readerKeys(reader int64) []string {
	base := fmt.Sprintf("%s:reader:{%d}", s.prefix, reader)
	return []string{base + ":posts", base + ":deleted", base + ":relations"}
}

func (s *Service) distribute(ctx context.Context, event outbox.Event) error {
	current, err := loadTask(ctx, s.db, event, false)
	if err != nil {
		return err
	}
	targets := current.Body.Recipients
	if current.Kind != "feed.chunk" || len(targets) == 0 || len(targets) > chunkSize {
		return fmt.Errorf("%w: invalid distribution chunk", outbox.ErrPermanent)
	}
	if err := s.readerGenerations(ctx, targets); err != nil {
		return err
	}
	state, err := s.authority.GetPostState(ctx, current.Body.PostID)
	if errors.Is(err, post.ErrNotFound) {
		return fmt.Errorf("%w: distribution post missing", outbox.ErrPermanent)
	}
	if err != nil {
		return err
	}
	entry := Entry{ID: state.PostId, CreatedOn: state.CreatedOn}
	author := state.AuthorId
	if s.pullAuthors[author] {
		return nil
	}
	if author <= 0 || entry.ID <= 0 || entry.CreatedOn <= 0 {
		return fmt.Errorf("%w: invalid distribution post", outbox.ErrPermanent)
	}
	for _, target := range targets {
		if target.UserID <= 0 || target.RelationID < 0 || (target.RelationID == 0 && target.UserID != author) {
			return fmt.Errorf("%w: invalid recipient", outbox.ErrPermanent)
		}
	}
	// Deletion can remove a previously delivered member even after an unfollow.
	// Active additions check the exact currently existing relationship identity.
	if !state.Deleted {
		ids := make([]int64, 0, len(targets))
		for _, target := range targets {
			ids = append(ids, target.RelationID)
		}
		relations, err := s.authority.CheckRelations(ctx, author, ids)
		if err != nil {
			return err
		}
		active := make(map[int64]int64, len(relations.Items))
		for _, relation := range relations.Items {
			active[relation.RelationId] = relation.UserId
		}
		eligible := make([]recipient, 0, len(targets))
		for _, target := range targets {
			if (target.UserID == author && target.RelationID == 0) || active[target.RelationID] == target.UserID {
				eligible = append(eligible, target)
			}
		}
		targets = eligible
	}
	for offset := 0; offset < len(targets); offset += pipelineSize {
		end := min(offset+pipelineSize, len(targets))
		if err := s.writeReaderBatch(ctx, entry, targets[offset:end], state.Deleted); err != nil {
			return err
		}
	}
	return nil
}

// Each Exec contains at most 32 fixed-size scripts. Only NOSCRIPT responses are
// resent with EVAL, in a second equally bounded batch. Uncertain I/O or partial
// errors return to durable chunk retry; successful effects tolerate repetition.
func (s *Service) writeReaderBatch(ctx context.Context, entry Entry, targets []recipient, deleted bool) error {
	if len(targets) == 0 || len(targets) > pipelineSize {
		return fmt.Errorf("%w: invalid pipeline size", outbox.ErrPermanent)
	}
	state := "active"
	if deleted {
		state = "deleted"
	}
	calls := make([]scriptCall, 0, len(targets))
	for _, target := range targets {
		if target.generation == "" {
			continue
		}
		calls = append(calls, scriptCall{s.cacheKeys(cacheSource{owner: target.UserID}), []any{strconv.FormatInt(entry.ID, 10), member(entry), state, strconv.FormatInt(target.RelationID, 10), target.generation, retainedPosts}})
	}
	if len(calls) == 0 {
		return nil
	}
	_, err := s.scriptBatch(ctx, cacheUpdate, calls)
	return err
}
