-- Document images on the change feed, opt-in per collection.
--
-- 0004 kept the feed data-free on purpose: a consumer that only needs "this
-- document changed" (a search index) re-reads current state, and that is both
-- cheaper and self-healing. Firestore triggers cannot work that way. An
-- onDocumentUpdated handler compares the document as it was BEFORE this write
-- with the document as it was AFTER it — not with whatever it is now — and the
-- before-image is gone the moment the write commits. Only the commit itself can
-- capture it.
--
-- So images are captured inside the same transaction as the write, but only for
-- collections someone registered in feed_images. Every other collection keeps
-- 0004's cost profile: a bulk import into an unregistered collection does not
-- double its write volume.
--
-- Both columns hold a google.firestore.v1.Document in protobuf wire format
-- (name, fields, create_time, update_time). NULL means "did not exist" when the
-- collection is registered, and "not captured" when it is not; captured rows
-- say which via has_images.
ALTER TABLE changelog
    ADD COLUMN has_images boolean NOT NULL DEFAULT false,
    ADD COLUMN before     bytea,
    ADD COLUMN after      bytea;

CREATE TABLE feed_images (
    collection_id text PRIMARY KEY COLLATE "C",
    registered_at timestamptz NOT NULL DEFAULT now()
);
