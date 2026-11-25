# Functions runtime

`kor-functions` runs a `firebase-functions` v2 codebase against Kor, with no
code changes. Firestore triggers fire from Kor's change feed, and scheduled
functions fire on their cron. You point it at the directory you would
`firebase deploy`, and it reads the same export manifest the deploy CLI reads.

```bash
cd functions && npm install           # or copy functions/ next to your project
npx kor-functions --source ../my-app/functions --project my-proj --list
npx kor-functions --source ../my-app/functions --project my-proj \
    --hybrid users,posts --metrics 127.0.0.1:6567
```

`--list` prints every exported function and how it would be served:

```
✓ onPostWritten          firestore written posts/{postId}
✓ nightlyDigest          schedule "every day 02:00" UTC
  generateThumbnail      NOT SUPPORTED (google.cloud.storage.object.v1.finalized) — keep it on Cloud Functions
```

## What it serves

| Trigger | Served | Notes |
|---|---|---|
| `onDocumentCreated` / `Updated` / `Deleted` / `Written` | yes | including the `…WithAuthContext` variants |
| `onSchedule` | yes | unix cron and App Engine syntax, any IANA time zone |
| `onRequest`, `onCall` | no | an HTTP framework already does this; keep them on Cloud Functions or your server |
| Storage, Pub/Sub, Auth blocking, task queues, v1 functions | no | `--list` names them; leave them deployed on Google |

A Firestore trigger needs a literal collection id in its document pattern
(`posts/{id}` or `users/{uid}/posts/{id}`). A pattern whose collection is a
wildcard is refused at startup. The feed is filtered by collection, so such a
trigger would mean reading every write in the database.

## How a trigger fires

1. At startup the runtime registers the collections its triggers watch
   (`POST /feed/v1/images`). From then on, every write to those collections
   stores the document as it was **before** and **after** that write, in the
   same Postgres transaction as the write itself. A trigger needs exactly that
   pair, and no later read can reconstruct it. Collections nobody registered
   keep the data-free feed, so a bulk import into them costs nothing extra.
2. It tails `GET /feed/v1/changes` from its saved cursor. A first start begins
   at the feed head returned by the registration, so it never replays history
   and never sees an event without images.
3. For each event it builds the CloudEvent Eventarc would deliver (JSON
   encoding, `data.value` / `data.oldValue` as REST `Document`s) and calls the
   exported function. `firebase-functions` itself decodes the event and builds
   the snapshots, so `event.data`, `event.params` and `Change` objects are the
   real SDK types.
4. The kind is derived from the images: no before means `created`, no after
   means `deleted`, and both means `updated`. A `written` trigger receives all
   three.
5. The cursor is saved atomically after each page.

**Delivery is at least once**, as on Cloud Functions. A crash replays the
tail of the page it was handling. Events for one document arrive in commit
order. Different documents are handled concurrently (`--concurrency`,
default 8). A failing handler with `retry: true` is retried with backoff up to
5 attempts, then logged and skipped so one poisoned document cannot stall the
feed. A handler without `retry` is attempted once.

The feed itself never drops an event: it is read below the low-water mark of
in-flight transactions (see `0004_changelog.sql`).

## Schedules

The scheduler wakes at each minute boundary and fires every function whose
schedule matches the wall clock **in its own time zone**. It calls the exported
HTTP handler the way Cloud Scheduler would, with the `X-CloudScheduler-*`
headers, so `event.scheduleTime` and `event.jobName` are set.

- Unix cron: five fields, ranges, lists, steps, month and weekday names; when
  both day-of-month and day-of-week are restricted, either may match (Vixie
  semantics, as Cloud Scheduler).
- App Engine: `every N minutes|hours` (aligned to the hour or to midnight),
  `every day HH:MM`, `every monday,thursday HH:MM`.
- DST: a time that does not exist (02:30 on a spring-forward night) does not
  fire. A time that happens twice fires once.
- The minute a job last fired is saved, so a restart within the minute does not
  fire it twice. A slot that passed while the runtime was down is not caught
  up, matching cron.
- If the previous run of a job is still going, the next slot is skipped and
  logged instead of running two copies.
- `retryConfig.retryCount` is honoured.

## Hybrid mode: migrating one collection at a time

Most projects move to Kor in waves, so the functions read and write
collections on both sides. `--hybrid a,b` (or `KOR_COLLECTIONS=a,b`) patches the
Firestore instance your code gets from `getFirestore()`, routing each request
to the store that owns it:

- Ownership is by **root collection**. `users/u1/posts/p1` lives wherever
  `users` lives. Use the same rule in every other client of the project so that
  they all agree.
- **Collection-group queries** name only a collection id, so they go to Kor
  when that id is in the owned set. List subcollections you query as a group
  next to their roots.
- **Transactions are pinned** to the store that served their first read. A
  transaction that then touches the other store fails with
  `FAILED_PRECONDITION`. It is never split across stores.
- Outside a transaction, a `getAll` or a write batch that spans both stores
  is split into two RPCs and the results are merged in request order. That
  write is **not atomic across stores**, and it is logged each time.
- `onSnapshot` listeners always go to Google, because Kor has no `Listen` yet
  (roadmap phase 3).

The router patches the two internal funnels every SDK call goes through
(`Firestore#request` and `#requestStream`), so it covers the whole API
surface without wrapping any of it.

Without `--hybrid` the SDK is untouched. Point it wherever you like, for
example with `FIRESTORE_EMULATOR_HOST` at kord when everything lives in Kor.

## The change-feed API

kord serves the feed on its admin listener (`-metrics`, default
`127.0.0.1:6566`), next to `/metrics`. Like the gRPC port it has no
authentication: reachability is authorization, so keep it on loopback or
behind a private network.

| Endpoint | |
|---|---|
| `GET /feed/v1/changes?after=XID.SEQ&collection=a&collection=b&images=1&limit=500` | events after the cursor, in commit order; `cursor` in the reply is where to resume |
| `GET /feed/v1/head` | the newest readable cursor, for a consumer that only wants new events |
| `GET /feed/v1/images` | collections whose events carry before/after documents |
| `POST /feed/v1/images` `{"register": [...], "unregister": [...]}` | change that set; replies with the set and the head cursor taken after the change |

Each event has `seq`, `xid`, `name`, `collection`, `kind` (`write` or
`delete`), `at` and `hasImages`. With `images=1`, `before` and `after` are
included, as `google.firestore.v1.Document` in REST JSON. Cursors are strings
because a Postgres `xid8` does not fit a JSON number. Pages are capped at
2,000 events.

The registered set lives in the database (`feed_images`), so it survives
restarts, and kord reloads it on the same ticker as indexes.

## Running it as a service

```ini
# /etc/systemd/system/my-functions.service
[Service]
WorkingDirectory=/opt/my-app/functions
Environment=GOOGLE_APPLICATION_CREDENTIALS=/opt/my-app/sa.json
Environment=KOR_COLLECTIONS=users,posts
ExecStart=/usr/bin/node /opt/kor/functions/bin/kor-functions.js \
  --source /opt/my-app/functions --project my-proj \
  --state /var/lib/kor-functions/state.json --metrics 127.0.0.1:6567
Restart=always
```

Deploy the functions it serves **off** Cloud Functions in the same change
(`firebase functions:delete name`). Otherwise both runtimes fire for every
write that still lands on Google, and Google's copy fires on writes Kor never
sees.

Metrics on `--metrics`: `kor_functions_events_total`,
`kor_functions_invocations_total`, `kor_functions_failures_total`,
`kor_functions_events_without_images_total`,
`kor_functions_scheduled_runs_total`, `kor_functions_feed_lag_seconds` and
`kor_functions_hybrid_requests_total{target}`.

## Testing

```bash
cd functions && npm test                       # pattern, cron, discovery units
FIREBASE_NODE_MODULES=/path/to/functions/node_modules functions/test/e2e/run.sh
```

The end-to-end suite builds kord, starts two of them on a throwaway Postgres
(the second stands in for Google Firestore), runs a fixture codebase through
real `firebase-functions` and `firebase-admin`, and asserts on what the
triggers, a transaction, a schedule and the hybrid router actually wrote.
