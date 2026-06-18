// Package rabbitmq will implement the MetadataStore (port.MetadataStore) by
// riding the existing Alkemio server save/fetch RabbitMQ pattern, extended with
// content_pointer + blob_store so the server stores the index, not the blob.
// This is the Alkemio-deployment default. Implementation lands with task T005
// of specs/003-unify-collab-yjs/tasks/collaboration-service.md.
package rabbitmq
