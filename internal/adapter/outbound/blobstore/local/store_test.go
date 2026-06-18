package local

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/alkem-io/collaboration-service/internal/domain/model"
)

func TestPutGetRoundTrip(t *testing.T) {
	store, err := New(t.TempDir())
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	ctx := context.Background()

	want := []byte("snapshot-bytes-v2")
	if _, err := store.Put(ctx, "doc-1", want); err != nil {
		t.Fatalf("Put: %v", err)
	}
	got, err := store.Get(ctx, "doc-1")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if string(got) != string(want) {
		t.Errorf("Get = %q, want %q", got, want)
	}
}

func TestPutOverwrites(t *testing.T) {
	store, _ := New(t.TempDir())
	ctx := context.Background()
	_, _ = store.Put(ctx, "doc", []byte("v1"))
	if _, err := store.Put(ctx, "doc", []byte("v2-longer")); err != nil {
		t.Fatalf("Put overwrite: %v", err)
	}
	got, _ := store.Get(ctx, "doc")
	if string(got) != "v2-longer" {
		t.Errorf("after overwrite Get = %q, want v2-longer", got)
	}
}

func TestGetMissingIsNotFound(t *testing.T) {
	store, _ := New(t.TempDir())
	_, err := store.Get(context.Background(), "absent")
	if !errors.Is(err, model.ErrNotFound) {
		t.Errorf("Get(absent) err = %v, want ErrNotFound", err)
	}
}

func TestDeleteIsIdempotent(t *testing.T) {
	store, _ := New(t.TempDir())
	ctx := context.Background()
	_, _ = store.Put(ctx, "doc", []byte("x"))
	if err := store.Delete(ctx, "doc"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	// Deleting again (absent) must be a no-op, not an error.
	if err := store.Delete(ctx, "doc"); err != nil {
		t.Errorf("Delete(absent) = %v, want nil (idempotent)", err)
	}
	if _, err := store.Get(ctx, "doc"); !errors.Is(err, model.ErrNotFound) {
		t.Errorf("Get after Delete err = %v, want ErrNotFound", err)
	}
}

func TestAtomicWriteLeavesNoTempFiles(t *testing.T) {
	root := t.TempDir()
	store, _ := New(root)
	_, _ = store.Put(context.Background(), "doc", []byte("x"))

	entries, err := os.ReadDir(root)
	if err != nil {
		t.Fatalf("ReadDir: %v", err)
	}
	for _, e := range entries {
		if filepath.Ext(e.Name()) == ".tmp" {
			t.Errorf("left a temp file behind: %s", e.Name())
		}
	}
}

func TestPointerTraversalRejected(t *testing.T) {
	store, _ := New(t.TempDir())
	ctx := context.Background()
	// A pointer that escapes the root must be rejected, not write outside it.
	if _, err := store.Put(ctx, "../escape", []byte("x")); err == nil {
		t.Error("expected Put to reject a traversal pointer")
	}
	if _, err := store.Get(ctx, "../escape"); err == nil {
		t.Error("expected Get to reject a traversal pointer")
	}
	if err := store.Delete(ctx, "../escape"); err == nil {
		t.Error("expected Delete to reject a traversal pointer")
	}
}

func TestNestedPointer(t *testing.T) {
	store, _ := New(t.TempDir())
	ctx := context.Background()
	// A pointer with a subdirectory component is created under the root.
	if _, err := store.Put(ctx, "sub/doc", []byte("nested")); err != nil {
		t.Fatalf("Put nested: %v", err)
	}
	got, err := store.Get(ctx, "sub/doc")
	if err != nil {
		t.Fatalf("Get nested: %v", err)
	}
	if string(got) != "nested" {
		t.Errorf("nested Get = %q", got)
	}
}

func TestNewRejectsEmptyRoot(t *testing.T) {
	if _, err := New(""); err == nil {
		t.Error("expected New(\"\") to error")
	}
}

func TestNewCreatesRoot(t *testing.T) {
	root := filepath.Join(t.TempDir(), "nested", "blobs")
	if _, err := New(root); err != nil {
		t.Fatalf("New should create the root: %v", err)
	}
	if _, err := os.Stat(root); err != nil {
		t.Errorf("root not created: %v", err)
	}
}

func TestPutFailsWhenDirIsAFile(t *testing.T) {
	root := t.TempDir()
	store, _ := New(root)
	// Create a file where Put needs a directory ("sub" is a file, so "sub/doc"
	// cannot be created).
	if err := os.WriteFile(filepath.Join(root, "sub"), []byte("x"), 0o600); err != nil {
		t.Fatalf("setup: %v", err)
	}
	if _, err := store.Put(context.Background(), "sub/doc", []byte("y")); err == nil {
		t.Error("expected Put to fail when the parent path is a file")
	}
}

func TestDeleteRejectsTraversalIsCovered(t *testing.T) {
	store, _ := New(t.TempDir())
	if err := store.Delete(context.Background(), "../escape"); err == nil {
		t.Error("expected Delete to reject a traversal pointer")
	}
}
