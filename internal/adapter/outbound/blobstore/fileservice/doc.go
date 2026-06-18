// Package fileservice will implement the BlobStore (port.BlobStore) by
// offloading the encoded Y.Doc snapshot to the existing file-service via its
// PUT/GET API; the content pointer is the file-service object id. Expanding
// file-service is pre-authorized if the blob-store needs a capability it does
// not yet expose (e.g. a versioned-snapshot endpoint). Implementation lands
// with task T005 of specs/003-unify-collab-yjs/tasks/collaboration-service.md.
package fileservice
