package postgres

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/alkem-io/collaboration-service/internal/domain/model"
)

// fakeRow is a pgx.Row backed by a fixed column slice (or an error).
type fakeRow struct {
	cols []any
	err  error
}

func (r fakeRow) Scan(dest ...any) error {
	if r.err != nil {
		return r.err
	}
	if len(dest) != len(r.cols) {
		return errors.New("column count mismatch")
	}
	for i, d := range dest {
		assign(d, r.cols[i])
	}
	return nil
}

// assign copies a source column value into a typed scan destination pointer.
func assign(dest, src any) {
	switch d := dest.(type) {
	case *string:
		*d = src.(string)
	case *int:
		*d = src.(int)
	case *time.Time:
		*d = src.(time.Time)
	}
}

// fakeTag satisfies pgconnCommandTag.
type fakeTag struct{ n int64 }

func (t fakeTag) RowsAffected() int64 { return t.n }

// fakeQuerier records the SQL and args it was called with and returns scripted
// results, so the adapter's SQL shaping and error mapping are unit-tested
// without a live Postgres.
type fakeQuerier struct {
	row       fakeRow
	execErr   error
	lastExec  string
	lastArgs  []any
	lastQuery string
}

func (q *fakeQuerier) QueryRow(_ context.Context, sql string, args ...any) pgx.Row {
	q.lastQuery = sql
	q.lastArgs = args
	return q.row
}

func (q *fakeQuerier) Exec(_ context.Context, sql string, args ...any) (pgconnCommandTag, error) {
	q.lastExec = sql
	q.lastArgs = args
	return fakeTag{n: 1}, q.execErr
}

func TestLoadMapsRow(t *testing.T) {
	now := time.Now().UTC()
	q := &fakeQuerier{row: fakeRow{cols: []any{
		"doc-1", "whiteboard", 3, "ptr-uuid", "pol-7", "owner-x", now, now,
	}}}
	store := &Store{db: q}

	got, err := store.Load(context.Background(), "doc-1")
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if got.ID != "doc-1" || got.ContentType != model.ContentTypeWhiteboard ||
		got.Version != 3 || got.ContentPointer != "ptr-uuid" ||
		got.AuthorizationPolicyID != "pol-7" || got.OwnerRef != "owner-x" {
		t.Errorf("mapped row = %+v", got)
	}
	if !strings.Contains(q.lastQuery, "FROM collaboration_metadata") {
		t.Errorf("unexpected load SQL: %s", q.lastQuery)
	}
	if len(q.lastArgs) != 1 || q.lastArgs[0] != "doc-1" {
		t.Errorf("load args = %v", q.lastArgs)
	}
}

func TestLoadNoRowsIsNotFound(t *testing.T) {
	q := &fakeQuerier{row: fakeRow{err: pgx.ErrNoRows}}
	store := &Store{db: q}
	_, err := store.Load(context.Background(), "absent")
	if !errors.Is(err, model.ErrNotFound) {
		t.Errorf("Load(absent) err = %v, want ErrNotFound", err)
	}
}

func TestLoadScanErrorSurfaces(t *testing.T) {
	q := &fakeQuerier{row: fakeRow{err: errors.New("connection reset")}}
	store := &Store{db: q}
	_, err := store.Load(context.Background(), "doc")
	if err == nil || errors.Is(err, model.ErrNotFound) {
		t.Errorf("expected a non-NotFound error, got %v", err)
	}
}

func TestSaveUpsertsWithArgs(t *testing.T) {
	q := &fakeQuerier{}
	store := &Store{db: q}
	err := store.Save(context.Background(), model.Metadata{
		ID:                    "doc-2",
		ContentType:           model.ContentTypeMemo,
		ContentPointer:        "ptr",
		AuthorizationPolicyID: "pol-9",
		OwnerRef:              "owner",
	})
	if err != nil {
		t.Fatalf("Save: %v", err)
	}
	if !strings.Contains(q.lastExec, "INSERT INTO collaboration_metadata") ||
		!strings.Contains(q.lastExec, "ON CONFLICT") {
		t.Errorf("unexpected upsert SQL: %s", q.lastExec)
	}
	// id, content_type, content_pointer, authorization_policy_id, owner_ref
	want := []any{"doc-2", "memo", "ptr", "pol-9", "owner"}
	if len(q.lastArgs) != len(want) {
		t.Fatalf("save args = %v, want %v", q.lastArgs, want)
	}
	for i := range want {
		if q.lastArgs[i] != want[i] {
			t.Errorf("arg[%d] = %v, want %v", i, q.lastArgs[i], want[i])
		}
	}
}

func TestSaveErrorSurfaces(t *testing.T) {
	q := &fakeQuerier{execErr: errors.New("deadlock")}
	store := &Store{db: q}
	if err := store.Save(context.Background(), model.Metadata{ID: "d"}); err == nil {
		t.Error("expected Save to surface the exec error")
	}
}

func TestDeleteExecutesAndIsIdempotent(t *testing.T) {
	q := &fakeQuerier{}
	store := &Store{db: q}
	if err := store.Delete(context.Background(), "doc-3"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if !strings.Contains(q.lastExec, "DELETE FROM collaboration_metadata") {
		t.Errorf("unexpected delete SQL: %s", q.lastExec)
	}
	if len(q.lastArgs) != 1 || q.lastArgs[0] != "doc-3" {
		t.Errorf("delete args = %v", q.lastArgs)
	}
}

func TestDeleteErrorSurfaces(t *testing.T) {
	q := &fakeQuerier{execErr: errors.New("db down")}
	store := &Store{db: q}
	if err := store.Delete(context.Background(), "d"); err == nil {
		t.Error("expected Delete to surface the exec error")
	}
}

func TestConnectRejectsEmptyDSN(t *testing.T) {
	if _, _, err := Connect(context.Background(), ""); err == nil {
		t.Error("expected Connect(\"\") to error")
	}
}

func TestMigrateRejectsEmptyDSN(t *testing.T) {
	if err := Migrate(""); err == nil {
		t.Error("expected Migrate(\"\") to error")
	}
}

func TestConnectBadDSNErrors(t *testing.T) {
	// A syntactically invalid DSN fails at pool construction, not at ping.
	if _, _, err := Connect(context.Background(), "://not a dsn"); err == nil {
		t.Error("expected Connect with a malformed DSN to error")
	}
}

func TestMigrateBadDSNErrors(t *testing.T) {
	// A non-empty but unreachable/invalid DSN must error (not panic) — covers
	// the migrate driver-construction path.
	if err := Migrate("postgres://u:p@127.0.0.1:1/none?sslmode=disable&connect_timeout=1"); err == nil {
		t.Error("expected Migrate against an unreachable DB to error")
	}
}

func TestNewWrapsPool(t *testing.T) {
	// New simply wraps a pool in the querier adapter; a nil pool is acceptable
	// for construction (it would only fail on use), proving New does no I/O.
	if s := New(nil); s == nil || s.db == nil {
		t.Error("New should wrap the pool in a querier")
	}
}

// TestConnectPingFailureClosesPool defends Connect's ping-error branch
// (store.go:65): a syntactically valid DSN whose host is unreachable passes the
// lazy pgxpool.New but fails Ping. Connect must surface that error (so startup
// fails loudly instead of handing back a store backed by a dead pool).
func TestConnectPingFailureClosesPool(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	// Port 1 is unreachable; connect_timeout keeps the test fast.
	store, pool, err := Connect(ctx, "postgres://u:p@127.0.0.1:1/db?sslmode=disable&connect_timeout=1")
	if err == nil {
		t.Error("expected Connect to fail pinging an unreachable database")
	}
	if store != nil || pool != nil {
		t.Error("on a ping failure Connect must return nil store and pool (pool already closed)")
	}
}

// TestMigrateRejectsMalformedDSN asserts Migrate fails fast with a clear,
// non-panicking error when the DSN cannot be parsed (§XV: no half-configured
// runs). This pins the fix for the prior latent panic: the old "nil-safe" empty
// config was NOT nil-safe — pgx's connect() panics on an empty ConnConfig — so
// Migrate now propagates the pgx.ParseConfig error instead.
func TestMigrateRejectsMalformedDSN(t *testing.T) {
	if err := Migrate("://not a dsn"); err == nil {
		t.Fatal("Migrate with a malformed DSN must return a parse error, not panic")
	}
}
