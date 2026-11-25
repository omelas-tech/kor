'use strict';

// Hybrid mode: one Firestore instance, two stores.
//
// A codebase mid-migration has some collections in Kor and the rest still in
// Google Cloud Firestore. Functions code calls getFirestore() and expects one
// database, so the runtime patches the instance's two RPC funnels —
// Firestore#request and Firestore#requestStream, which every SDK operation
// goes through — and sends each request to the store that owns its documents.
//
// Ownership is by ROOT collection: users/u1/posts/p1 lives wherever "users"
// lives. That is the same rule the Go and Python clients of a migrating
// codebase use, so all three agree on where a document is.
//
// Collection-group queries are the one exception: they name only a collection
// id, which may live under any root. They go to Kor when that id is itself in
// the owned set — so a subcollection queried as a group (posts under users/*)
// must be listed alongside its root.
//
// Transactions are pinned. The SDK starts one lazily on its first read
// (newTransaction) and learns the id from the response, so the router records
// which store answered and sends every later request carrying that id there.
// A transaction cannot span stores: touching the other store fails loudly
// (FAILED_PRECONDITION) instead of silently splitting the atomic unit.
//
// Outside transactions, a batch get or a write batch that mixes stores is
// split and the results merged back in request order. That split write is NOT
// atomic across the two stores — the same trade-off the Python router makes,
// and the reason to keep a single batch inside one store where it matters.

const { PassThrough } = require('node:stream');

/** @typedef {'kor'|'google'} Target */

class CrossStoreError extends Error {
  constructor(msg) {
    super(msg);
    this.code = 9; // FAILED_PRECONDITION
  }
}

const TX_TTL_MS = 10 * 60 * 1000;

/**
 * Patch `google` (a Firestore instance) so requests for `collections` go to
 * `kor` (a Firestore instance pointed at kord).
 *
 * @param {any} google
 * @param {any} kor
 * @param {{collections: Iterable<string>, log?: (msg: string) => void}} opts
 * @returns {{stats: Record<string, number>, targetOfName: (name: string) => Target, unpatch: () => void}}
 */
function install(google, kor, { collections, log = () => {} }) {
  const owned = new Set(collections);
  if (google.__korHybrid) throw new Error('kor-functions: hybrid router already installed on this instance');
  const origRequest = google.request.bind(google);
  const origStream = google.requestStream.bind(google);
  /** @type {Map<string, {target: Target, at: number}>} */
  const txs = new Map();
  const stats = { kor: 0, google: 0, split: 0 };

  const root = () => `projects/${google.projectId}/databases/${google.databaseId || '(default)'}/documents`;

  /** @param {string} name @returns {Target} */
  function targetOfName(name) {
    const prefix = root();
    if (!name || !name.startsWith(prefix)) return 'google';
    const rest = name.slice(prefix.length).replace(/^\//, '');
    const first = rest.split('/')[0];
    return first && owned.has(first) ? 'kor' : 'google';
  }

  /** Parent + collection id: a query/list under the root is decided by the collection. */
  function targetOfParent(parent, collectionId) {
    const prefix = root();
    if ((parent === prefix || parent === `${prefix}/`) && collectionId) {
      return owned.has(collectionId) ? 'kor' : 'google';
    }
    return targetOfName(parent);
  }

  const txKey = (t) => (t && t.length ? Buffer.from(t).toString('base64') : '');

  function remember(tx, target) {
    const k = txKey(tx);
    if (!k) return;
    const now = Date.now();
    txs.set(k, { target, at: now });
    if (txs.size > 1000) for (const [key, v] of txs) if (now - v.at > TX_TTL_MS) txs.delete(key);
  }

  function pinned(tx) {
    const hit = txs.get(txKey(tx));
    return hit ? hit.target : null;
  }

  /** @returns {Target[]} the target of each write in a commit/batchWrite */
  function writeTargets(writes) {
    return (writes || []).map((w) => targetOfName(w.update?.name || w.delete || w.transform?.document || ''));
  }

  function queryTarget(req) {
    const sq = req.structuredQuery || req.structuredAggregationQuery?.structuredQuery;
    return targetOfParent(req.parent, sq?.from?.[0]?.collectionId);
  }

  const call = (target, method, req, tag, retry) => {
    stats[target]++;
    return target === 'kor'
      ? kor.initializeIfNeeded(tag).then(() => kor.request(method, req, tag, retry))
      : origRequest(method, req, tag, retry);
  };
  const callStream = (target, method, bidi, req, tag) => {
    stats[target]++;
    return target === 'kor'
      ? kor.initializeIfNeeded(tag).then(() => kor.requestStream(method, bidi, req, tag))
      : origStream(method, bidi, req, tag);
  };

  /** Enforce one store per transaction. */
  function checkPinned(tx, targets, what) {
    const p = pinned(tx);
    const want = p || targets[0];
    if (targets.some((t) => t !== want)) {
      throw new CrossStoreError(`kor-functions: ${what} in a transaction touches both Kor and Google Firestore; ` +
        'a transaction must stay inside one store (owned by Kor: ' + [...owned].join(',') + ')');
    }
    return want;
  }

  google.request = async function routedRequest(method, req, tag, retry) {
    switch (method) {
      case 'commit':
      case 'batchWrite': {
        const targets = writeTargets(req.writes);
        if (req.transaction && req.transaction.length) {
          const t = checkPinned(req.transaction, targets.length ? targets : [pinned(req.transaction) || 'google'], method);
          try {
            return await call(t, method, req, tag, retry);
          } finally {
            txs.delete(txKey(req.transaction));
          }
        }
        const uniq = [...new Set(targets)];
        if (uniq.length <= 1) return call(uniq[0] || 'google', method, req, tag, retry);
        return splitWrites(method, req, targets, tag, retry);
      }
      case 'rollback': {
        const t = pinned(req.transaction) || 'google';
        txs.delete(txKey(req.transaction));
        return call(t, method, req, tag, retry);
      }
      case 'listDocuments':
        return call(targetOfParent(req.parent, req.collectionId), method, req, tag, retry);
      case 'listCollectionIds':
        return call(targetOfName(req.parent), method, req, tag, retry);
      default:
        return call(targetOfName(req?.name || req?.parent || ''), method, req, tag, retry);
    }
  };

  /** A non-transactional write batch spanning stores: two RPCs, merged results. */
  async function splitWrites(method, req, targets, tag, retry) {
    stats.split++;
    log(`kor-functions: ${method} of ${targets.length} writes spans Kor and Google; split (not atomic across stores)`);
    const parts = /** @type {Record<Target, number[]>} */ ({ kor: [], google: [] });
    targets.forEach((t, i) => parts[t].push(i));
    const res = await Promise.all(/** @type {Target[]} */ (['kor', 'google']).map((t) =>
      call(t, method, { ...req, writes: parts[t].map((i) => req.writes[i]) }, tag, retry)));
    const writeResults = new Array(targets.length);
    const status = method === 'batchWrite' ? new Array(targets.length) : undefined;
    let commitTime = null;
    /** @type {Target[]} */ (['kor', 'google']).forEach((t, k) => {
      parts[t].forEach((i, j) => {
        writeResults[i] = res[k].writeResults?.[j];
        if (status) status[i] = res[k].status?.[j];
      });
      const ct = res[k].commitTime;
      if (ct && (!commitTime || Number(ct.seconds) > Number(commitTime.seconds) ||
        (Number(ct.seconds) === Number(commitTime.seconds) && (ct.nanos || 0) > (commitTime.nanos || 0)))) commitTime = ct;
    });
    return status ? { writeResults, status } : { writeResults, commitTime };
  }

  google.requestStream = async function routedStream(method, bidi, req, tag) {
    switch (method) {
      case 'batchGetDocuments': {
        const targets = (req.documents || []).map(targetOfName);
        if (req.transaction && req.transaction.length) {
          return callStream(checkPinned(req.transaction, targets, method), method, bidi, req, tag);
        }
        const uniq = [...new Set(targets)];
        if (req.newTransaction) {
          const t = checkPinned(null, targets, method);
          return recordTx(await callStream(t, method, bidi, req, tag), t);
        }
        if (uniq.length <= 1) return callStream(uniq[0] || 'google', method, bidi, req, tag);
        stats.split++;
        const byT = /** @type {Record<Target, string[]>} */ ({ kor: [], google: [] });
        req.documents.forEach((d, i) => byT[targets[i]].push(d));
        const streams = await Promise.all(/** @type {Target[]} */ (['kor', 'google']).map((t) =>
          callStream(t, method, bidi, { ...req, documents: byT[t] }, tag)));
        return merge(streams);
      }
      case 'runQuery':
      case 'runAggregationQuery':
      case 'partitionQueryStream': {
        const t0 = queryTarget(req);
        if (req.transaction && req.transaction.length) {
          return callStream(checkPinned(req.transaction, [t0], method), method, bidi, req, tag);
        }
        const s = await callStream(t0, method, bidi, req, tag);
        return req.newTransaction ? recordTx(s, t0) : s;
      }
      default:
        // listen (bidirectional; Kor has no Listen yet) and anything unknown.
        return callStream('google', method, bidi, req, tag);
    }
  };

  /** Pass a response stream through, remembering the transaction it opened. */
  function recordTx(src, target) {
    const out = new PassThrough({ objectMode: true });
    out.pause();
    src.on('data', (chunk) => {
      if (chunk?.transaction?.length) remember(chunk.transaction, target);
      if (!out.write(chunk)) {
        src.pause();
        out.once('drain', () => src.resume());
      }
    });
    src.on('end', () => out.end());
    src.on('error', (e) => out.destroy(e));
    out.on('close', () => { if (!src.readableEnded) src.resume(); });
    src.resume();
    return out;
  }

  /** Interleave several response streams into one; ends when all have. */
  function merge(srcs) {
    const out = new PassThrough({ objectMode: true });
    out.pause();
    let open = srcs.length;
    for (const s of srcs) {
      s.on('data', (chunk) => {
        if (!out.write(chunk)) {
          s.pause();
          out.once('drain', () => s.resume());
        }
      });
      s.on('end', () => { if (--open === 0) out.end(); });
      s.on('error', (e) => out.destroy(e));
      s.resume();
    }
    if (open === 0) out.end();
    return out;
  }

  google.__korHybrid = true;
  return {
    stats,
    targetOfName,
    unpatch() {
      google.request = origRequest;
      google.requestStream = origStream;
      delete google.__korHybrid;
    },
  };
}

module.exports = { install, CrossStoreError };
