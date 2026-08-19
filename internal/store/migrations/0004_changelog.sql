-- Change feed: what happened to which document, in an order a reader can
-- resume from without missing anything.
--
-- The obvious design — a bigserial, tailed with `seq > last_seen` — is wrong,
-- and wrong in the way that hurts most: it drops events silently. Sequence
-- values are handed out when a row is inserted, but rows become VISIBLE when
-- their transaction commits, and those orders differ. If T1 takes seq 5 and T2
-- takes seq 6, and T2 commits first, a reader sees 6, advances its cursor past
-- 5, and never sees T1's event at all. Nothing errors. Search indexes just
-- quietly drift.
--
-- So each row also records its transaction id, and readers consume only rows
-- whose transaction is older than the oldest one still in flight
-- (pg_snapshot_xmin). That is the low-water mark: below it, no transaction can
-- still appear, so the feed is complete and resumable.
CREATE TABLE changelog (
    seq           bigserial PRIMARY KEY,
    -- xid8, not xid: the 32-bit type wraps around, and a feed that resumes from
    -- a wrapped cursor would replay or skip an epoch's worth of changes.
    xid           xid8   NOT NULL,
    doc_name      text   NOT NULL COLLATE "C",
    collection_id text   NOT NULL COLLATE "C",
    -- write covers create and update alike: consumers re-read current state, so
    -- distinguishing them buys nothing and invites a consumer to skip reloads.
    kind          text   NOT NULL CHECK (kind IN ('write', 'delete')),
    at            timestamptz NOT NULL DEFAULT now()
);

-- The reader's access path: everything at or after a cursor, in commit order.
CREATE INDEX changelog_cursor_idx ON changelog (xid, seq);

-- Lets a consumer subscribe to one collection without scanning the rest.
CREATE INDEX changelog_collection_idx ON changelog (collection_id, xid, seq);
