// Bundled for the browser by browser.sh: the unmodified firebase-js web SDK,
// which reaches kord only through WebChannel (Listen/Write) and REST.
import { initializeApp } from 'firebase/app';
import * as F from 'firebase/firestore';

window.runKorBrowserTest = async (port, longPolling) => {
  const log = [];
  const fails = [];
  const ok = (c, m) => { if (!c) fails.push(m); log.push((c ? 'ok   ' : 'FAIL ') + m); };
  const mk = (name, uid) => {
    const db = F.initializeFirestore(initializeApp({ projectId: 'demo-kor', apiKey: 'x' }, name + longPolling),
      longPolling ? { experimentalForceLongPolling: true } : { experimentalAutoDetectLongPolling: false });
    F.connectFirestoreEmulator(db, '127.0.0.1', port, { mockUserToken: { user_id: uid, sub: uid } });
    return db;
  };
  const T = (p, ms = 10000) => Promise.race([p, new Promise((_, j) => setTimeout(() => j({ code: 'TIMEOUT' }), ms))]);
  const code = async (p) => { try { await T(p); return 'ok'; } catch (e) { return e.code || String(e); } };
  const A = mk('alice', 'alice'), B = mk('watcher', 'alice'), E = mk('eve', 'eve');
  const R = 'r' + Date.now() + (longPolling ? 'lp' : 's');
  try {
    // B listens, A writes: what B sees came through kord's fan-out.
    const got = [];
    let resolve; const want = new Promise((r) => (resolve = r));
    const unsub = F.onSnapshot(F.query(F.collection(B, `rooms/${R}/messages`), F.orderBy('n')), (s) => {
      if (s.metadata.fromCache || s.metadata.hasPendingWrites) return;
      got.push(s.docs.map((d) => d.data().text).join('|'));
      if (s.size === 2) resolve(true);
    }, (e) => got.push('ERR ' + e.code));
    ok(await code(F.setDoc(F.doc(A, `rooms/${R}/messages/m1`), { n: 1, text: 'hello' })) === 'ok', 'write over the Write channel');
    ok(await code(F.setDoc(F.doc(A, `rooms/${R}/messages/m2`), { n: 2, text: 'çağ 🎨 ünïcode' })) === 'ok', 'non-ASCII write');
    ok(await Promise.race([want, new Promise((r) => setTimeout(() => r(false), 8000))]), 'listener sees both writes: ' + JSON.stringify(got));
    ok(got[got.length - 1] === 'hello|çağ 🎨 ünïcode', 'non-ASCII survives the channel framing');
    unsub();

    const snap = await T(F.getDocFromServer(F.doc(B, `rooms/${R}/messages/m1`)));
    ok(snap.exists() && snap.data().text === 'hello', 'getDocFromServer');
    const q = await T(F.getDocsFromServer(F.query(F.collection(B, `rooms/${R}/messages`), F.where('n', '>', 1))));
    ok(q.size === 1, 'getDocsFromServer query');
    const c = await T(F.getCountFromServer(F.collection(B, `rooms/${R}/messages`)));
    ok(c.data().count === 2, 'count aggregation over REST');

    ok(await code(F.runTransaction(A, async (tx) => {
      const d = await tx.get(F.doc(A, `rooms/${R}/messages/m1`));
      tx.update(d.ref, { n: d.data().n + 10 });
    })) === 'ok', 'transaction over REST (beginTransaction/batchGet/commit)');
    ok((await T(F.getDocFromServer(F.doc(B, `rooms/${R}/messages/m1`)))).data().n === 11, 'transaction applied');

    const batch = F.writeBatch(A);
    batch.set(F.doc(A, `rooms/${R}/messages/m3`), { n: 3, text: 'b' });
    batch.delete(F.doc(A, `rooms/${R}/messages/m2`));
    ok(await code(batch.commit()) === 'ok', 'batch write');

    // The rules decide, exactly as for the native SDKs.
    ok(await code(F.setDoc(F.doc(E, `private/alice`), { x: 1 })) === 'permission-denied', 'write to another user refused');
    ok(await code(F.getDocFromServer(F.doc(E, `private/alice`))) === 'permission-denied', 'read of another user refused');
    const own = await code(F.setDoc(F.doc(A, `private/alice`), { x: 1 }));
    ok(own === 'ok', 'own private write ' + own);
    ok(await code(F.getDocsFromServer(F.collection(E, 'private'))) === 'permission-denied', 'unscoped list refused');
  } catch (e) {
    ok(false, 'exception: ' + (e && e.stack || e));
  }
  // Each instance holds hanging requests; over plain HTTP/1.1 (no proxy in
  // this test) Chrome allows six per host, so release them before the next run.
  await Promise.all([A, B, E].map((d) => F.terminate(d)));
  return { log, fails };
};
