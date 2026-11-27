// The unmodified firebase-js SDK under Node speaks gRPC to kord — the same
// Listen/Write streams and client state machine the native mobile SDKs use
// (multi-target streams, get() through Listen, writes on the Write stream).
// Two app instances: B listens while A writes, so every snapshot B sees came
// through Kor's fan-out, not A's local latency compensation.
//
//   FIREBASE_NODE_MODULES=/path/to/node_modules e2e/js/run.sh
const path = require('path');
const req = require('module').createRequire(path.join(process.env.FIREBASE_NODE_MODULES, '..', 'resolve.js'));
const { initializeApp } = req('firebase/app');
const F = req('firebase/firestore');
const port = +process.argv[2];
const mk = (n) => { const d = F.getFirestore(initializeApp({ projectId: 'demo-kor' }, n)); F.connectFirestoreEmulator(d, '127.0.0.1', port); return d; };
const R = 'r' + Date.now();
const A = mk('writer'), B = mk('watcher');
const fails = []; const ok = (c, m) => { if (!c) fails.push(m); console.log((c ? 'ok   ' : 'FAIL ') + m); };
// Resolve when B's listener reaches a server-confirmed state equal to want.
function watch(q, render) {
  let latest = null, waiters = [];
  const unsub = F.onSnapshot(q, (s) => {
    if (s.metadata.fromCache || s.metadata.hasPendingWrites) return;
    latest = render(s); waiters = waiters.filter(([w, r]) => (latest === w ? (r(true), false) : true));
  }, (e) => { latest = 'ERR ' + e.code; });
  const until = (want, ms = 5000) => new Promise((res) => {
    if (latest === want) return res(true);
    waiters.push([want, res]); setTimeout(() => res(latest === want), ms);
  });
  return { until, unsub, get: () => latest };
}
const sj = (o) => JSON.stringify(Object.fromEntries(Object.entries(o).sort()));
const ids = (s) => s.docs.map((d) => d.id + ':' + sj(d.data())).join(' ');
(async () => {
  const msgs = (db) => F.collection(db, `${R}chats/c1/messages`);
  const qB = F.query(msgs(B), F.where('room', '==', 'a'), F.orderBy('n'));
  const lim = F.query(msgs(B), F.orderBy('n', 'desc'), F.limit(2));
  const w1 = watch(qB, ids), w2 = watch(F.doc(B, `${R}presence/u1`), (s) => (s.exists() ? JSON.stringify(s.data()) : 'missing'));
  const w3 = watch(lim, (s) => s.docs.map((d) => d.id).join(','));
  ok(await w1.until(''), 'empty query snapshot');
  ok(await w2.until('missing'), 'missing doc snapshot');

  await F.setDoc(F.doc(msgs(A), 'm1'), { room: 'a', n: 1 });          // Write stream
  await F.setDoc(F.doc(msgs(A), 'm2'), { room: 'b', n: 2 });
  ok(await w1.until('m1:{"n":1,"room":"a"}'), 'cross-client add reaches B: ' + w1.get());
  const b = F.writeBatch(A);                                         // one atomic Write request
  b.set(F.doc(msgs(A), 'm3'), { room: 'a', n: 0 }); b.update(F.doc(msgs(A), 'm2'), { room: 'a' });
  b.set(F.doc(A, `${R}presence/u1`), { online: true });
  await b.commit();
  ok(await w1.until('m3:{"n":0,"room":"a"} m1:{"n":1,"room":"a"} m2:{"n":2,"room":"a"}'), 'batch fans out to query: ' + w1.get());
  ok(await w2.until('{"online":true}'), 'batch fans out to doc listener: ' + w2.get());
  ok(await w3.until('m2,m1'), 'limit window: ' + w3.get());
  await F.deleteDoc(F.doc(msgs(A), 'm2'));
  ok(await w3.until('m1,m3'), 'delete pulls next into window: ' + w3.get());
  ok(await w1.until('m3:{"n":0,"room":"a"} m1:{"n":1,"room":"a"}'), 'delete leaves query: ' + w1.get());

  // Server reads in the full SDK go through Listen targets that are added and removed.
  const g = await F.getDocFromServer(F.doc(B, `${R}chats/c1/messages/m1`));
  ok(g.exists() && g.data().n === 1, 'getDocFromServer');
  const gs = await F.getDocsFromServer(F.query(msgs(B), F.where('n', '>=', 1)));
  ok(gs.size === 1 && gs.docs[0].id === 'm1', 'getDocsFromServer size=' + gs.size);
  const gm = await F.getDocFromServer(F.doc(B, `${R}chats/c1/messages/nope`));
  ok(!gm.exists(), 'getDocFromServer missing');

  // Transactions use BatchGetDocuments + Commit.
  await F.runTransaction(A, async (tx) => { const s = await tx.get(F.doc(msgs(A), 'm1')); tx.update(s.ref, { n: s.data().n + 10 }); });
  ok(await w1.until('m3:{"n":0,"room":"a"} m1:{"n":11,"room":"a"}'), 'transaction result reaches B: ' + w1.get());

  // Unsubscribing removes targets; the remaining ones keep working on the same stream.
  w1.unsub(); w3.unsub();
  await F.setDoc(F.doc(A, `${R}presence/u1`), { online: false });
  ok(await w2.until('{"online":false}'), 'remaining target still live after removes: ' + w2.get());
  // A failed write is rejected cleanly and the stream recovers for the next one.
  let rejected = false;
  try { await F.updateDoc(F.doc(A, `${R}chats/c1/messages/never`), { n: 1 }); } catch (e) { rejected = e.code === 'not-found'; }
  ok(rejected, 'update of missing doc rejected with not-found');
  await F.setDoc(F.doc(A, `${R}presence/u1`), { online: 'again' });
  ok(await w2.until('{"online":"again"}'), 'write stream recovers after a rejected batch: ' + w2.get());
  w2.unsub();
  console.log(fails.length ? `${fails.length} FAILED` : 'ALL PASSED'); process.exit(fails.length ? 1 : 0);
})().catch((e) => { console.error('crash', e); process.exit(2); });
