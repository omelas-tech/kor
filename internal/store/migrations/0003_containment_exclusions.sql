-- Collections exempt from the containment index.
--
-- documents_gin indexes the WHOLE document jsonb to approximate Firestore's
-- automatic single-field indexes. That is the right default, but it is global
-- to the documents table: a collection that is only ever read by document id
-- pays the full write cost and can never benefit. Measured, the index accounts
-- for ~90% of write-ahead volume on large nested documents.
--
-- Firestore has the same escape hatch (single-field index exemptions); this is
-- Kor's, at collection granularity, which is where the cost concentrates.
CREATE TABLE containment_exclusions (
    collection_id text PRIMARY KEY COLLATE "C",
    created_at    timestamptz NOT NULL DEFAULT now()
);
