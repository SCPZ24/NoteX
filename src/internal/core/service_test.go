package core

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"strings"
	"testing"
)

// A failed checkout identifies which pool received a query, without a live server.
type failingConnector struct{ err error }

func (c failingConnector) Connect(context.Context) (driver.Conn, error) { return nil, c.err }
func (c failingConnector) Driver() driver.Driver                        { return nil }

func TestReadPoolRouting(t *testing.T) {
	ordinaryErr := errors.New("ordinary pool")
	readErr := errors.New("read pool")
	ordinary := sql.OpenDB(failingConnector{ordinaryErr})
	defer ordinary.Close()
	read := sql.OpenDB(failingConnector{readErr})
	defer read.Close()
	service := NewWithReadPool(ordinary, read, nil)
	ctx := t.Context()
	if _, err := service.Authenticate(ctx, strings.Repeat("a", 64)); !errors.Is(err, ordinaryErr) {
		t.Fatalf("Authenticate used the wrong pool: %v", err)
	}
	if _, err := service.GetFeedRelations(ctx, 1); !errors.Is(err, readErr) {
		t.Fatalf("GetFeedRelations used the wrong pool: %v", err)
	}
	if _, err := service.ReadFeedPage(ctx, 1, "", 20); !errors.Is(err, readErr) {
		t.Fatalf("ReadFeedPage used the wrong pool: %v", err)
	}
}
