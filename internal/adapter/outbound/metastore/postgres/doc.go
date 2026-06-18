// Package postgres will implement the MetadataStore (port.MetadataStore)
// against Postgres (sqlc + pgx/v5, golang-migrate migrations) for standalone
// deployments that own their own document index. Implementation lands with task
// T005 of specs/003-unify-collab-yjs/tasks/collaboration-service.md.
package postgres
