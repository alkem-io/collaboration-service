package postgres

import (
	"context"
	"errors"
	"testing"

	"github.com/pashagolub/pgxmock/v5"
)

// TestPoolAdapterForwardsToThePool covers the bridge between the store's querier
// seam and the real pgx pool.
//
// The store's SQL and row mapping were already covered through a fake querier,
// which is why this two-method adapter sat at 0%: every store test replaced it.
// But a bridge that forwards to the wrong pool method, reorders arguments, or
// drops the command tag is a real defect, and no amount of store-level testing
// with the adapter swapped out can see it. pgxmock stands in for the pool so the
// forwarding itself is exercised without a live Postgres.
func TestPoolAdapterForwardsToThePool(t *testing.T) {
	mock, err := pgxmock.NewPool()
	if err != nil {
		t.Fatalf("pgxmock pool: %v", err)
	}
	defer mock.Close()

	ctx := context.Background()
	adapter := poolAdapter{pool: mock}

	t.Run("QueryRow passes sql and args through", func(t *testing.T) {
		mock.ExpectQuery("SELECT id FROM document").
			WithArgs("doc-1").
			WillReturnRows(pgxmock.NewRows([]string{"id"}).AddRow("doc-1"))

		var got string
		if err := adapter.QueryRow(ctx, "SELECT id FROM document WHERE id = $1", "doc-1").Scan(&got); err != nil {
			t.Fatalf("QueryRow/Scan: %v", err)
		}
		if got != "doc-1" {
			t.Fatalf("scanned %q, want doc-1", got)
		}
	})

	t.Run("Exec returns the command tag the store reads row counts from", func(t *testing.T) {
		mock.ExpectExec("DELETE FROM document").
			WithArgs("doc-2").
			WillReturnResult(pgxmock.NewResult("DELETE", 1))

		tag, err := adapter.Exec(ctx, "DELETE FROM document WHERE id = $1", "doc-2")
		if err != nil {
			t.Fatalf("Exec: %v", err)
		}
		// RowsAffected is the ONLY thing the store reads off the tag, and it is what
		// Delete uses to tell "row removed" from "no such document".
		if tag.RowsAffected() != 1 {
			t.Fatalf("RowsAffected = %d, want 1; the store distinguishes a removed row from a missing document by this value", tag.RowsAffected())
		}
	})

	t.Run("Exec surfaces the pool error", func(t *testing.T) {
		boom := errors.New("connection reset")
		mock.ExpectExec("UPDATE document").WillReturnError(boom)

		if _, err := adapter.Exec(ctx, "UPDATE document SET version = 1"); !errors.Is(err, boom) {
			t.Fatalf("Exec error = %v, want it to surface the pool error unwrapped", err)
		}
	})

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet pgxmock expectations: %v", err)
	}
}
