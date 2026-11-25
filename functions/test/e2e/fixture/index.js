const { initializeApp } = require('firebase-admin/app');
const { getFirestore, FieldValue } = require('firebase-admin/firestore');
const { onDocumentCreated, onDocumentUpdated, onDocumentDeleted, onDocumentWritten } = require('firebase-functions/v2/firestore');
const { onSchedule } = require('firebase-functions/v2/scheduler');
const { onRequest } = require('firebase-functions/v2/https');
initializeApp({ projectId: 'demo-p' });
const db = () => getFirestore();

exports.created = onDocumentCreated('posts/{id}', async (e) => {
  await db().collection('audit').doc(`c-${e.params.id}`).set({ v: e.data.data().v, exists: e.data.exists });
});
exports.updated = onDocumentUpdated('posts/{id}', async (e) => {
  await db().collection('audit').doc(`u-${e.params.id}`).set({ before: e.data.before.data().v, after: e.data.after.data().v });
  await db().runTransaction(async (tx) => {
    const ref = db().collection('counters').doc('updates');
    const snap = await tx.get(ref);
    tx.set(ref, { n: (snap.exists ? snap.data().n : 0) + 1 });
  });
});
exports.deleted = onDocumentDeleted('posts/{id}', async (e) => {
  await db().collection('audit').doc(`d-${e.params.id}`).set({ v: e.data.data().v });
});
exports.group = { written: onDocumentWritten('posts/{id}', async (e) => {
  await db().collection('counters').doc('writes').set({ n: FieldValue.increment(1) }, { merge: true });
}) };
exports.tick = onSchedule('every 1 minutes', async () => {
  await db().collection('ticks').add({ at: new Date() });
});
exports.api = onRequest((req, res) => res.send('hi'));
