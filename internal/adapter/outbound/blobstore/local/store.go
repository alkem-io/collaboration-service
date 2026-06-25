// Package local is the local-filesystem BlobStore (port.BlobStore) for
// standalone deployments: the encoded Y.Doc v2 snapshot is written under a
// configured root directory, the content pointer being a path relative to that
// root. Writes are atomic (temp file + rename) so a crash mid-write never leaves
// a half-written snapshot a reload would apply.
package local

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/alkem-io/collaboration-service/internal/domain/model"
	"github.com/alkem-io/collaboration-service/internal/domain/port"
)

// Store writes snapshots under a fixed root directory.
type Store struct {
	root string
}

// New constructs a local blob store rooted at dir, creating it if absent.
func New(dir string) (*Store, error) {
	if dir == "" {
		return nil, errors.New("local blob store root must not be empty")
	}
	abs, err := filepath.Abs(dir)
	if err != nil {
		return nil, fmt.Errorf("resolve root: %w", err)
	}
	if err := os.MkdirAll(abs, 0o750); err != nil {
		return nil, fmt.Errorf("create root: %w", err)
	}
	return &Store{root: abs}, nil
}

// resolve maps a content pointer to an absolute path under the root, rejecting
// any pointer that would escape it (path traversal).
func (s *Store) resolve(pointer string) (string, error) {
	clean := filepath.Clean(pointer)
	full := filepath.Join(s.root, clean)
	// filepath.Join cleans "..", so compare the result against the root prefix.
	if full != s.root && !strings.HasPrefix(full, s.root+string(os.PathSeparator)) {
		return "", fmt.Errorf("invalid content pointer %q: escapes blob root", pointer)
	}
	return full, nil
}

// Put writes data under pointer atomically: a temp file in the same directory is
// written and fsync'd, then renamed over the target (rename is atomic on POSIX).
// It echoes the pointer back — local blobs are addressed by the stable relative
// path the caller supplies. bucketID is ignored: local blobs are rooted at a
// fixed configured directory, not per document.
func (s *Store) Put(_ context.Context, pointer, _ string, data []byte) (string, error) {
	full, err := s.resolve(pointer)
	if err != nil {
		return "", err
	}
	dir := filepath.Dir(full)
	if err := os.MkdirAll(dir, 0o750); err != nil {
		return "", fmt.Errorf("create dir: %w", err)
	}

	tmp, err := os.CreateTemp(dir, "."+filepath.Base(full)+".*.tmp")
	if err != nil {
		return "", fmt.Errorf("create temp: %w", err)
	}
	tmpName := tmp.Name()
	// Best-effort cleanup if we bail before the rename.
	defer func() { _ = os.Remove(tmpName) }()

	if _, err := tmp.Write(data); err != nil {
		_ = tmp.Close()
		return "", fmt.Errorf("write temp: %w", err)
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return "", fmt.Errorf("sync temp: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return "", fmt.Errorf("close temp: %w", err)
	}
	if err := os.Rename(tmpName, full); err != nil {
		return "", fmt.Errorf("rename temp: %w", err)
	}
	// tmp.Sync above only makes the file CONTENTS durable; after the rename the new
	// directory entry can still be lost on a crash/power-loss until the containing
	// directory is itself fsync'd. Sync it so a returned pointer always names a
	// snapshot that survives a restart (no acknowledged-then-vanished writes).
	dirHandle, err := os.Open(dir) //nolint:gosec // dir is derived from resolve()-constrained full.
	if err != nil {
		return "", fmt.Errorf("open dir for sync: %w", err)
	}
	defer func() { _ = dirHandle.Close() }()
	if err := dirHandle.Sync(); err != nil {
		return "", fmt.Errorf("sync dir: %w", err)
	}
	return pointer, nil
}

// Get reads the snapshot bytes under pointer, mapping a missing file to
// model.ErrNotFound.
func (s *Store) Get(_ context.Context, pointer string) ([]byte, error) {
	full, err := s.resolve(pointer)
	if err != nil {
		return nil, err
	}
	data, err := os.ReadFile(full) //nolint:gosec // full is constrained to the blob root by resolve.
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, model.ErrNotFound
		}
		return nil, fmt.Errorf("read blob: %w", err)
	}
	return data, nil
}

// Delete removes the snapshot under pointer. A missing file is a no-op
// (idempotent).
func (s *Store) Delete(_ context.Context, pointer string) error {
	full, err := s.resolve(pointer)
	if err != nil {
		return err
	}
	if err := os.Remove(full); err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("delete blob: %w", err)
	}
	return nil
}

// compile-time assertion that Store satisfies the port.
var _ port.BlobStore = (*Store)(nil)
