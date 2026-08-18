-- Composite indexes as materialized rows, the way Firestore does it.
--
-- One row per (index, document): `key` is the concatenated sort keys of the
-- indexed fields terminated by the document name. value.AppendSortKey's byte
-- order equals Firestore's total order and its encoding is prefix-free, so a
-- range scan over (index_id, key) traverses results in exact query order — which
-- is what lets LIMIT and OFFSET push down to Postgres instead of being applied
-- after sorting every match in Go.
--
-- Keyed by doc_name rather than documents.doc_pk: doc_pk is an identity column
-- with no unique index behind it, so joining or cascading on it would need extra
-- schema. name is already the primary key.
CREATE TABLE index_entries (
    index_id  bigint NOT NULL,
    key       bytea  NOT NULL,
    doc_name  text   NOT NULL COLLATE "C",
    PRIMARY KEY (index_id, key, doc_name)
);

-- The scan path: (index_id, key) is the primary key's leading edge, so range
-- scans are index-only until the document fetch.

-- Maintenance path: every document write clears its old entries first, so this
-- lookup is on the hot write path, not just cleanup.
CREATE INDEX index_entries_doc_idx ON index_entries (doc_name);

-- Registry of the definitions that produced the entries above. index_id is
-- derived from the spec rather than assigned here, so this table is for
-- introspection and for sweeping entries whose definition has since changed or
-- been removed — never a lookup on the write path.
CREATE TABLE index_defs (
    index_id      bigint PRIMARY KEY,
    collection_id text   NOT NULL COLLATE "C",
    spec          text   NOT NULL,
    is_group      boolean NOT NULL DEFAULT false,
    created_at    timestamptz NOT NULL DEFAULT now(),
    -- NULL until a backfill has completed. Registration and readiness are
    -- deliberately separate: between them the index exists but is incomplete,
    -- and serving reads from a partial index silently returns fewer documents
    -- than the collection holds.
    ready_at      timestamptz
);
