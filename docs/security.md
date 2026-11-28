# Security: the client endpoint

kord can serve Firebase client SDKs (iOS, Android, Flutter and the browser)
directly, as Firestore does. Those clients are untrusted: they
connect to the **client endpoint**, where every request is authenticated by
its Firebase Auth ID token and decided by your `firestore.rules`, run
verbatim.

```bash
kord -pg-dsn "$DSN" \
  -client-listen 127.0.0.1:6567 \
  -rules /etc/kor/firestore.rules \
  -project my-firebase-project
```

The main endpoint (`-listen`) is unchanged: trusted, unauthenticated, for your
servers and tools, like Firestore's service-account access. Both endpoints
share one store and one change hub, so a server write reaches a phone's
listener in the same ~100 ms.

## Exposing it

ID tokens are bearer credentials, so kord refuses an off-host
`-client-listen` without TLS. Either give it a certificate
(`-client-tls-cert`, `-client-tls-key`) or keep it on loopback behind a
reverse proxy that terminates TLS and speaks h2c to kord:

```caddy
firestore.example.com {
  reverse_proxy h2c://127.0.0.1:6567
}
```

Point the SDKs at that host: Flutter
`FirebaseFirestore.instance.settings = Settings(host: 'firestore.example.com', sslEnabled: true)`,
iOS `settings.host`, Android `FirebaseFirestoreSettings.Builder().setHost(...)`.
The SDKs attach the signed-in user's ID token to every call and refresh it on
their own; nothing else changes in the app.

### Browsers

Browsers cannot speak gRPC. The web SDK uses Firestore's REST API for one-shot
calls (commit, batchGet, runQuery, aggregations, transactions) and WebChannel
for the Listen and Write streams. `-client-web-listen` serves both, as plain
HTTP on loopback behind the same TLS proxy:

```bash
kord ... -client-listen 127.0.0.1:6567 \
  -client-web-listen 127.0.0.1:6568 -client-web-origins https://example.com
```

```caddy
firestore.example.com {
  @web path /v1/* /google.firestore.v1.Firestore/*/channel*
  reverse_proxy @web 127.0.0.1:6568 { flush_interval -1 }
  reverse_proxy h2c://127.0.0.1:6567
}
```

The browser transports hold no authority. Each request is forwarded to the
client endpoint as a gRPC call carrying the caller's ID token, so
authentication and rules are identical to the native SDKs'. Only the listed
origins pass CORS; it is a browser courtesy, not the security boundary (a
script outside a browser can send any Origin, and still needs a valid token).
In the web app:
`initializeFirestore(app, { host: 'firestore.example.com', ssl: true })`.

Serve it over HTTP/2 (any TLS proxy does). Every open listener holds a
request, and over HTTP/1.1 browsers allow six per host.

The client endpoint serves one project. Requests naming another project are
refused, and an ID token is only accepted for the project it was issued for.

## Authentication

Every request may carry `authorization: Bearer <ID token>`. kord verifies:

- the RS256 signature, against Google's published `securetoken` certificates
  (cached for their `Cache-Control` max-age; a failed refresh keeps the
  previous keys rather than locking everyone out)
- `aud` is the project and `iss` is `https://securetoken.google.com/<project>`
- `exp`, `iat` and `auth_time`, with one minute of clock skew
- a non-empty `sub`, which becomes `request.auth.uid`

`request.auth.token` is the full claim set, so custom claims work as on
Firestore. No token means a signed-out caller (`request.auth == null`). A
token that does not verify is `UNAUTHENTICATED`, never treated as signed-out.

A listener or write stream ends with `UNAUTHENTICATED` when its token
expires; the SDKs reconnect with a fresh token, so revoked users drop off
within the hour, as on Firestore.

`-accept-unsigned-test-tokens` accepts the unsigned tokens the Firebase
emulators and the SDKs' `mockUserToken` issue. Anyone can then act as any
user, so kord only allows it on a loopback `-client-listen`. Tests only.

## Rules

The ruleset is parsed at startup and reloaded when the file changes (checked
every 5 s). A file that fails to parse is logged and the previous ruleset
keeps serving, so a typo can neither open nor close the database. kord refuses
to start if the initial file does not parse.

Supported: `rules_version = '2'` (and 1), nested `match` blocks, `{x}` and
`{x=**}` wildcards (anywhere in a path, matching zero or more segments in
v2), `allow` with `read`/`get`/`list`/`write`/`create`/`update`/`delete`,
functions with `let`, `get`/`exists`/`getAfter`/`existsAfter`, the
`request` and `resource` variables, and the standard library: string, list,
map, set, `MapDiff`, timestamp, duration, latlng, math, `int`/`float`/`string`
conversions, `is` type checks, ranges and `in`.

Semantics follow Firestore:

- Any matching `allow` that evaluates to `true` allows; everything else
  denies.
- Errors deny, except where `||` and `&&` make them irrelevant
  (`true || error` is `true`, `false && error` is `false`, either order).
- A request may read at most 10 documents through `get()`/`exists()` (20 for
  a batch or transaction), counted per distinct path.
- In a write, `resource` is the document before the write,
  `request.resource` the document after it, `get()` the committed state and
  `getAfter()` the state the whole batch would leave. A batch or transaction
  commits only if every document in it is allowed.
- `request.time` is the commit time.

### Queries

Rules are not filters: a query is allowed only if the rule holds for every
document it could return, so a denial reveals nothing about documents the
caller may not read. kord decides this before running the query, with
`resource.data` known exactly where the query's top-level filters pin it
(`field == value`, `field array-contains value`) and unknown everywhere else.
Only a rule that is definitely true allows.

That covers how rules are written for queries: owner filters
(`where('owner', '==', uid)`), membership (`array-contains uid`), rules that
look only at the path or at `request.auth`, and limits
(`request.query.limit`). It is never more permissive than Firestore. It can
be stricter where Firestore's solver proves more, for instance a rule that
range-checks a field the query bounds with `<`. Those queries are denied.

Every document a query returns is also checked on its own before any is
sent. If one fails (which would mean the query proof had a gap), the query is
denied and kord logs a warning.

### Realtime

A listener is checked when its target is added. Afterwards, every document
it is about to deliver is checked again. When one fails, for instance because
a document changed hands, the target is removed with `PERMISSION_DENIED` and
the client gets the error. The stream and its other targets carry on.

### Not available on the client endpoint

`ListCollectionIds` and `BatchWrite` (BulkWriter) are admin APIs and are
denied, as on Firestore.

## How it is tested

- Unit tests for the language, the semantics above and the token checks
  (forged, expired, other-project, `alg` confusion).
- End-to-end tests with the official Go SDK against a client endpoint:
  reads, queries, aggregations, batches, transactions, listeners losing
  access, bad tokens.
- `e2e/js/browser.sh`: the unmodified firebase-js web SDK in headless
  Chromium, cross-origin, against `-client-web-listen` with rules on, in both
  streaming and forced long-polling mode: listeners, writes, non-ASCII data
  through the channel framing, server gets and queries, count aggregation,
  transactions, batches and rule denials.
- `TestRulesDifferential` loads the same ruleset into Google's Firestore
  emulator and kord, seeds the same documents, and has random users run
  random reads, writes, near-miss mutations and queries against both.
  Decisions must agree. Kor allowing anything the emulator denies always
  fails. Kor denying a query the emulator allows is reported. The generator
  reads paths and field names from the ruleset, so it runs against any
  `firestore.rules`:

  ```bash
  KOR_FUZZ_EMULATOR=127.0.0.1:8080 KOR_RULES_FILE=path/to/firestore.rules \
    go test ./e2e/ -run TestRulesDifferential -count=1 -v
  ```

## Metrics

`kord_rules_decisions_total{method, result}` counts decisions on the client
endpoint. Denials are logged at debug level with the reason.
