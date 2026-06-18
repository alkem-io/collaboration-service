// Package local will implement the BlobStore (port.BlobStore) against the local
// filesystem for standalone deployments; the content pointer is a file path
// under a configured root. Implementation lands with task T005 of
// specs/003-unify-collab-yjs/tasks/collaboration-service.md.
package local
