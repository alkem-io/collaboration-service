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
	if _, err := store.Put(ctx, "doc-1", "", want); err != nil {
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
	_, _ = store.Put(ctx, "doc", "", []byte("v1"))
	if _, err := store.Put(ctx, "doc", "", []byte("v2-longer")); err != nil {
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
	_, _ = store.Put(ctx, "doc", "", []byte("x"))
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
	_, _ = store.Put(context.Background(), "doc", "", []byte("x"))

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
	if _, err := store.Put(ctx, "../escape", "", []byte("x")); err == nil {
		t.Error("expected Put to reject a traversal pointer")
	}
	if _, err := store.Get(ctx, "../escape"); err == nil {
		t.Error("expected Get to reject a traversal pointer")
	}
	if err := store.Delete(ctx, "../escape"); err == nil {
		t.Error("expected Delete to reject a traversal pointer")
	}
}

// TestEmptyOrRootPointerRejected defends resolve's empty/root guard: an empty (or
// "."/"/"-only) pointer cleans to the root directory itself, so without the guard
// Put/Get/Delete would target the blob ROOT (a directory) instead of a snapshot
// file. resolve must reject it up front.
//
// Non-vacuity: remove the `clean == "." || clean == os.PathSeparator` guard in
// resolve and the empty-pointer cases below stop erroring — resolve returns the
// root path, so the operations target the directory and these assertions fail.
func TestEmptyOrRootPointerRejected(t *testing.T) {
	store, _ := New(t.TempDir())
	ctx := context.Background()
	for _, p := range []string{"", ".", "/", "//"} {
		if _, err := store.Put(ctx, p, "", []byte("x")); err == nil {
			t.Errorf("Put(%q): expected an empty/root pointer rejection", p)
		}
		if _, err := store.Get(ctx, p); err == nil {
			t.Errorf("Get(%q): expected an empty/root pointer rejection", p)
		}
		if err := store.Delete(ctx, p); err == nil {
			t.Errorf("Delete(%q): expected an empty/root pointer rejection", p)
		}
	}
}

func TestNestedPointer(t *testing.T) {
	store, _ := New(t.TempDir())
	ctx := context.Background()
	// A pointer with a subdirectory component is created under the root.
	if _, err := store.Put(ctx, "sub/doc", "", []byte("nested")); err != nil {
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
	if _, err := store.Put(context.Background(), "sub/doc", "", []byte("y")); err == nil {
		t.Error("expected Put to fail when the parent path is a file")
	}
}

func TestDeleteRejectsTraversalIsCovered(t *testing.T) {
	store, _ := New(t.TempDir())
	if err := store.Delete(context.Background(), "../escape"); err == nil {
		t.Error("expected Delete to reject a traversal pointer")
	}
}

// TestNewFailsWhenRootCannotBeCreated defends New's mkdir-error branch
// (store.go:34): if the requested root lives under a read-only directory, root
// creation must fail loudly at construction rather than yielding a store that
// silently cannot persist anything.
func TestNewFailsWhenRootCannotBeCreated(t *testing.T) {
	parent := t.TempDir()
	// 0500 (read+execute, no write) is required to make the dir traversable but
	// not writable so MkdirAll fails; gosec's 0600 ceiling would drop the execute
	// bit and break the fault injection.
	if err := os.Chmod(parent, 0o500); err != nil { //nolint:gosec // test fault injection: read-only dir
		t.Fatalf("chmod: %v", err)
	}
	t.Cleanup(func() { _ = os.Chmod(parent, 0o750) }) //nolint:gosec // restore so TempDir cleanup works
	if _, err := New(filepath.Join(parent, "cannot", "create")); err == nil {
		t.Error("expected New to fail when the root cannot be created under a read-only parent")
	}
}

// TestPutFailsWhenTempCannotBeCreated defends Put's create-temp branch
// (store.go:67): when the target directory is not writable the atomic temp file
// cannot be created, and Put must surface that error rather than report a
// success that never durably wrote the snapshot.
func TestPutFailsWhenTempCannotBeCreated(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("running as root: a read-only directory does not block writes, so this fault injection cannot be simulated")
	}
	root := t.TempDir()
	store, _ := New(root)
	// Make the root (the Put target dir for a top-level pointer) read-only so
	// CreateTemp inside it fails. 0500 keeps the execute bit needed to traverse;
	// gosec's 0600 ceiling would not.
	if err := os.Chmod(root, 0o500); err != nil { //nolint:gosec // test fault injection: read-only dir
		t.Fatalf("chmod: %v", err)
	}
	t.Cleanup(func() { _ = os.Chmod(root, 0o750) }) //nolint:gosec // restore for cleanup
	if _, err := store.Put(context.Background(), "doc", "", []byte("x")); err == nil {
		t.Error("expected Put to fail when a temp file cannot be created in a read-only dir")
	}
}

// TestPutFailsWhenTargetIsADirectory defends Put's rename branch (store.go:85):
// the atomic rename cannot replace a non-empty directory occupying the target
// path, so Put must error rather than leave the snapshot in a temp file.
func TestPutFailsWhenTargetIsADirectory(t *testing.T) {
	root := t.TempDir()
	store, _ := New(root)
	// Occupy the target pointer path with a non-empty directory; rename over it
	// is refused by the OS.
	if err := os.MkdirAll(filepath.Join(root, "doc", "inner"), 0o750); err != nil {
		t.Fatalf("setup: %v", err)
	}
	if _, err := store.Put(context.Background(), "doc", "", []byte("x")); err == nil {
		t.Error("expected Put to fail when the target path is a non-empty directory")
	}
}

// TestGetSurfacesNonNotExistReadError defends Get's read-error branch
// (store.go:103): a read failure that is NOT a missing file (here the pointer
// resolves to a directory) must surface as a real error, never be misreported
// as ErrNotFound.
func TestGetSurfacesNonNotExistReadError(t *testing.T) {
	root := t.TempDir()
	store, _ := New(root)
	if err := os.MkdirAll(filepath.Join(root, "adir"), 0o750); err != nil {
		t.Fatalf("setup: %v", err)
	}
	_, err := store.Get(context.Background(), "adir")
	if err == nil {
		t.Fatal("expected Get to fail reading a directory")
	}
	if errors.Is(err, model.ErrNotFound) {
		t.Error("a read error on an existing path must not be reported as ErrNotFound")
	}
}

// TestDeleteSurfacesNonNotExistError defends Delete's error branch
// (store.go:115): a removal failure that is NOT "already absent" (here a
// non-empty directory) must surface as an error, not be swallowed as the
// idempotent no-op reserved for a missing file.
func TestDeleteSurfacesNonNotExistError(t *testing.T) {
	root := t.TempDir()
	store, _ := New(root)
	if err := os.MkdirAll(filepath.Join(root, "adir", "inner"), 0o750); err != nil {
		t.Fatalf("setup: %v", err)
	}
	if err := store.Delete(context.Background(), "adir"); err == nil {
		t.Error("expected Delete to surface a non-empty-directory removal error")
	}
}
