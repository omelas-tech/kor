-- Kor core schema. COLLATE "C" everywhere: resource names are compared and
-- range-scanned byte-wise, never linguistically.

CREATE TABLE documents (
    doc_pk        bigint GENERATED ALWAYS AS IDENTITY,
    name          text PRIMARY KEY COLLATE "C",
    parent_path   text NOT NULL COLLATE "C",
    collection_id text NOT NULL COLLATE "C",
    doc_id        text NOT NULL COLLATE "C",
    data          jsonb NOT NULL,
    create_time   timestamptz NOT NULL,
    update_time   timestamptz NOT NULL
);

-- Collection scans (RunQuery over a single parent).
CREATE INDEX documents_parent_idx ON documents (parent_path, collection_id, doc_id);
-- Collection-group scans.
CREATE INDEX documents_cg_idx ON documents (collection_id, name);
-- Equality filters without a composite index (Firestore's automatic
-- single-field behavior, approximated by jsonb containment).
CREATE INDEX documents_gin ON documents USING gin (data jsonb_path_ops);

-- Registry of collection ids per parent, maintained on write; backs
-- ListCollectionIds without a distinct-scan over documents.
CREATE TABLE collections (
    parent_path   text NOT NULL COLLATE "C",
    collection_id text NOT NULL COLLATE "C",
    PRIMARY KEY (parent_path, collection_id)
);
