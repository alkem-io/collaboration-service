-- The collaboration document index (data-model.md "Document metadata / index").
-- The small, queryable row; never holds the snapshot blob (that lives in the
-- BlobStore behind content_pointer). One row per document id (single namespace
-- across memos and whiteboards).
CREATE TABLE IF NOT EXISTS collaboration_metadata (
    id                      TEXT        PRIMARY KEY,
    content_type            TEXT        NOT NULL,
    version                 INTEGER     NOT NULL DEFAULT 0,
    content_pointer         TEXT        NOT NULL DEFAULT '',
    blob_store              TEXT        NOT NULL DEFAULT 'inline',
    authorization_policy_id TEXT        NOT NULL DEFAULT '',
    owner_ref               TEXT        NOT NULL DEFAULT '',
    created_at              TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at              TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- The delete cascade keys off the owning Alkemio entity (FR-023).
CREATE INDEX IF NOT EXISTS idx_collaboration_metadata_owner_ref
    ON collaboration_metadata (owner_ref);
