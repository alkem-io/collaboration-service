// Package postgres is the standalone MetadataStore (port.MetadataStore): it
// persists the small, queryable document index in Postgres via pgx/v5, with the
// schema managed by golang-migrate. It is the standalone-deployment alternative
// to the Alkemio RabbitMQ metadata store — a deployment that owns its own index
// instead of delegating it to the Alkemio server.
//
// The SQL is hand-written and column-explicit (the sqlc convention of named,
// typed queries rather than dynamic SQL); pgx executes it. Load/Save/Delete
// satisfy the port; Save upserts and bumps the version on every persisted
// snapshot.
package postgres

import (
	"context"
	"embed"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/alkem-io/collaboration-service/internal/domain/model"
	"github.com/alkem-io/collaboration-service/internal/domain/port"
)

//go:embed migrations/*.sql
var migrationsFS embed.FS

// querier is the pgx subset the store uses, so unit tests can fake it without a
// live Postgres (a build-tagged integration test covers the real pool).
type querier interface {
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
	Exec(ctx context.Context, sql string, args ...any) (pgconnCommandTag, error)
}

// pgconnCommandTag is the part of pgconn.CommandTag the store inspects
// (RowsAffected), narrowed so the fake need not construct a real tag.
type pgconnCommandTag interface {
	RowsAffected() int64
}

// Store persists the document index in Postgres.
type Store struct {
	db querier
}

// New constructs a Postgres metadata store over an existing pgx pool. Callers
// own the pool's lifecycle. Use Connect for the common case of building a pool
// from a DSN.
func New(pool *pgxpool.Pool) *Store {
	return &Store{db: poolAdapter{pool}}
}

// Connect builds a pgx pool from a DSN, pings it, and returns a store plus the
// pool (so the caller can Close it on shutdown).
func Connect(ctx context.Context, dsn string) (*Store, *pgxpool.Pool, error) {
	if dsn == "" {
		return nil, nil, fmt.Errorf("postgres metadata store: DSN is required")
	}
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, nil, fmt.Errorf("connect postgres: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, nil, fmt.Errorf("ping postgres: %w", err)
	}
	return New(pool), pool, nil
}

const loadSQL = `
SELECT id, content_type, version, content_pointer,
       authorization_policy_id, owner_ref, created_at, updated_at
FROM collaboration_metadata
WHERE id = $1`

// Load returns the index row for id, or model.ErrNotFound.
func (s *Store) Load(ctx context.Context, id model.DocumentID) (model.Metadata, error) {
	row := s.db.QueryRow(ctx, loadSQL, string(id))
	var (
		m                    model.Metadata
		contentType          string
		idStr                string
		createdAt, updatedAt time.Time
	)
	err := row.Scan(&idStr, &contentType, &m.Version, &m.ContentPointer,
		&m.AuthorizationPolicyID, &m.OwnerRef, &createdAt, &updatedAt)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return model.Metadata{}, model.ErrNotFound
		}
		return model.Metadata{}, fmt.Errorf("load metadata: %w", err)
	}
	m.ID = model.DocumentID(idStr)
	m.ContentType = model.ContentType(contentType)
	m.CreatedAt = createdAt
	m.UpdatedAt = updatedAt
	return m, nil
}

// upsertSQL inserts a new row (version 1) or, on conflict, bumps the existing
// version and refreshes the mutable columns — mirroring the in-memory store's
// version-bump-on-save semantics (one canonical save behavior across backends).
//
// The mutable columns are upserted with "blank = unchanged" semantics
// (COALESCE(NULLIF(EXCLUDED.x,”), existing)) because two callers Save PARTIAL
// rows and must not clobber the columns they do not own:
//
//   - owner_ref / authorization_policy_id are LIFECYCLE metadata set at
//     pre-register (the create/PreRegister path); the per-snapshot persist
//     (Room.persist) historically carried them blank, so a blank value there means
//     "unchanged" — otherwise the first snapshot save would wipe the owner_ref the
//     delete cascade keys off (FR-023).
//   - content_pointer is SNAPSHOT metadata set by the persist path;
//     pre-register (document.created) carries them blank. A REDELIVERED
//     document.created re-runs PreRegister (a blind Save) with a blank
//     content_pointer; with an unconditional `= EXCLUDED` that would clobber the
//     live pointer back to ” AFTER a snapshot already set it — orphaning the
//     persisted blob and bumping the version. Treating blank as "unchanged" makes
//     a redelivered/late pre-register a no-op against a populated row.
//
// A non-blank value always wins (a genuine update). content_type is required on
// every write (never blank), so it stays an unconditional EXCLUDED.
const upsertSQL = `
INSERT INTO collaboration_metadata
    (id, content_type, version, content_pointer,
     authorization_policy_id, owner_ref, created_at, updated_at)
VALUES ($1, $2, 1, $3, $4, $5, now(), now())
ON CONFLICT (id) DO UPDATE SET
    content_type            = EXCLUDED.content_type,
    version                 = collaboration_metadata.version + 1,
    content_pointer         = COALESCE(NULLIF(EXCLUDED.content_pointer, ''), collaboration_metadata.content_pointer),
    authorization_policy_id = COALESCE(NULLIF(EXCLUDED.authorization_policy_id, ''), collaboration_metadata.authorization_policy_id),
    owner_ref               = COALESCE(NULLIF(EXCLUDED.owner_ref, ''), collaboration_metadata.owner_ref),
    updated_at              = now()`

// Save upserts the index row, bumping its version (data-model.md). Called on
// first save and on every persisted snapshot.
func (s *Store) Save(ctx context.Context, meta model.Metadata) error {
	_, err := s.db.Exec(ctx, upsertSQL,
		string(meta.ID), string(meta.ContentType), meta.ContentPointer,
		meta.AuthorizationPolicyID, meta.OwnerRef)
	if err != nil {
		return fmt.Errorf("save metadata: %w", err)
	}
	return nil
}

const deleteSQL = `DELETE FROM collaboration_metadata WHERE id = $1`

// Delete removes the index row for id. Idempotent: deleting an absent row is a
// no-op (lifecycle-events.md).
func (s *Store) Delete(ctx context.Context, id model.DocumentID) error {
	if _, err := s.db.Exec(ctx, deleteSQL, string(id)); err != nil {
		return fmt.Errorf("delete metadata: %w", err)
	}
	return nil
}

// compile-time assertion that Store satisfies the port.
var _ port.MetadataStore = (*Store)(nil)
