package postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/golang-migrate/migrate/v4"
	"github.com/golang-migrate/migrate/v4/database/postgres"
	"github.com/golang-migrate/migrate/v4/source/iofs"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/stdlib"
)

// pgxPool is the slice of *pgxpool.Pool that poolAdapter forwards to.
//
// Declared as an interface rather than taking the concrete pool so the adapter
// itself is reachable in the unit lane: pgxmock's pool satisfies this, which
// makes the forwarding testable without a live Postgres. The store's own querier
// seam already covered the SQL and the row mapping; what was left uncovered was
// exactly this bridge, and a bridge that forwards to the wrong pool method or
// drops the command tag is a real defect that no amount of store-level testing
// would see.
type pgxPool interface {
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
}

// poolAdapter bridges a pgx pool to the store's querier interface, wrapping
// pgx's concrete pgconn.CommandTag in the narrow pgconnCommandTag the store
// reads (so the fake querier in tests need not build a real tag).
type poolAdapter struct {
	pool pgxPool
}

// QueryRow runs a single-row query on the pool.
func (a poolAdapter) QueryRow(ctx context.Context, sql string, args ...any) pgx.Row {
	return a.pool.QueryRow(ctx, sql, args...)
}

// Exec runs a statement on the pool, returning the command tag for row counts.
func (a poolAdapter) Exec(ctx context.Context, sql string, args ...any) (pgconnCommandTag, error) {
	tag, err := a.pool.Exec(ctx, sql, args...)
	return tag, err
}

// Migrate applies the embedded migrations to the database at dsn using
// golang-migrate. It is idempotent (a no-op when already at the latest version)
// and is intended to run once at startup for the standalone Postgres deployment.
func Migrate(dsn string) error {
	if dsn == "" {
		return fmt.Errorf("postgres migrate: DSN is required")
	}
	src, err := iofs.New(migrationsFS, "migrations")
	if err != nil {
		return fmt.Errorf("open migrations: %w", err)
	}

	// golang-migrate's postgres driver runs over database/sql; pgx's stdlib
	// shim adapts the pgx driver to that interface so we keep a single driver.
	connCfg, err := pgx.ParseConfig(dsn)
	if err != nil {
		// A bad DSN is a startup misconfiguration: fail fast with the offending
		// detail (§XV) rather than letting pgx's stdlib panic on an empty config.
		return fmt.Errorf("parse postgres DSN: %w", err)
	}
	db := stdlib.OpenDB(*connCfg)
	defer func() { _ = db.Close() }()

	driver, err := postgres.WithInstance(db, &postgres.Config{})
	if err != nil {
		return fmt.Errorf("init migrate driver: %w", err)
	}

	m, err := migrate.NewWithInstance("iofs", src, "postgres", driver)
	if err != nil {
		return fmt.Errorf("init migrate: %w", err)
	}
	// Close the migrate instance (source + DB handle) so the startup migration does
	// not leak the iofs source / driver connection. We deliberately do not return
	// these close errors — the migration itself already succeeded/failed above.
	defer func() { _, _ = m.Close() }()
	if err := m.Up(); err != nil && !errors.Is(err, migrate.ErrNoChange) {
		return fmt.Errorf("apply migrations: %w", err)
	}
	return nil
}

// ensure database/sql is linked (golang-migrate's postgres.WithInstance needs a
// *sql.DB); referenced for the import to remain meaningful.
var _ = (*sql.DB)(nil)
