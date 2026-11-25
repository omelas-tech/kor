// "google" = getFirestore() pointed at kord #2 via the emulator env; "kor" = kord #1.
process.env.FIRESTORE_EMULATOR_HOST = process.env.GOOGLE_STANDIN;
const assert = require('node:assert');
const { initializeApp } = require('firebase-admin/app');
const { getFirestore, Firestore } = require('firebase-admin/firestore');
const hybrid = require(process.env.KOR_FUNCTIONS_LIB + '/hybrid.js');
initializeApp({ projectId: 'demo-p' });
const g = getFirestore();
const kor = new Firestore({ projectId: 'demo-p', host: '127.0.0.1', port: Number(process.env.KOR_GRPC.split(':')[1]), ssl: false });
const direct2 = new Firestore({ projectId: 'demo-p', host: '127.0.0.1', port: Number(process.env.GOOGLE_STANDIN.split(':')[1]), ssl: false });
const r = hybrid.install(g, kor, { collections: ['owned', 'sub'], log: (m) => console.log('LOG', m) });
(async () => {
  // Mixed batch: split, results per write.
  const b = g.batch();
  b.set(g.doc('owned/a'), { v: 1 });
  b.set(g.doc('plain/a'), { v: 2 });
  b.set(g.doc('owned/a/sub/x'), { v: 3 });
  const res = await b.commit();
  assert.strictEqual(res.length, 3);
  assert.strictEqual((await kor.doc('owned/a').get()).data().v, 1);
  assert.strictEqual((await kor.doc('owned/a/sub/x').get()).data().v, 3);
  assert.strictEqual((await kor.doc('plain/a').get()).exists, false);
  assert.strictEqual((await direct2.doc('plain/a').get()).data().v, 2);
  assert.strictEqual((await direct2.doc('owned/a').get()).exists, false);
  // Mixed getAll: merged, in request order.
  const got = await g.getAll(g.doc('plain/a'), g.doc('owned/a'), g.doc('owned/missing'));
  assert.deepStrictEqual(got.map((s) => s.exists && s.data().v), [2, 1, false]);
  // Queries route by collection, including collection-group under the root.
  assert.strictEqual((await g.collection('owned').get()).size, 1);
  assert.strictEqual((await g.collection('plain').get()).size, 1);
  assert.strictEqual((await g.collectionGroup('sub').get()).size, 1);
  assert.strictEqual((await g.collection('owned').count().get()).data().count, 1);
  // Transactions pin to one store.
  await g.runTransaction(async (tx) => {
    const s = await tx.get(g.doc('owned/a'));
    tx.update(g.doc('owned/a'), { v: s.data().v + 10 });
  });
  assert.strictEqual((await kor.doc('owned/a').get()).data().v, 11);
  await g.runTransaction(async (tx) => {
    const s = await tx.get(g.doc('plain/a'));
    tx.update(g.doc('plain/a'), { v: s.data().v + 10 });
  });
  assert.strictEqual((await direct2.doc('plain/a').get()).data().v, 12);
  await assert.rejects(g.runTransaction(async (tx) => {
    await tx.get(g.doc('owned/a'));
    tx.set(g.doc('plain/b'), { v: 1 });
  }, { maxAttempts: 1 }), /both Kor and Google/);
  // listDocuments / listCollections
  // BulkWriter mixed.
  const bw = g.bulkWriter();
  bw.set(g.doc('owned/bw'), { v: 1 }); bw.set(g.doc('plain/bw'), { v: 1 });
  await bw.close();
  assert.ok((await kor.doc('owned/bw').get()).exists && (await direct2.doc('plain/bw').get()).exists);
  console.log('OK', JSON.stringify(r.stats));
  process.exit(0);
})().catch((e) => { console.error('FAIL', e); process.exit(1); });
