'use strict';
const test = require('node:test');
const assert = require('node:assert');
const { discover, select } = require('../lib/discover');

const fn = (ep) => Object.assign(() => {}, { __endpoint: ep });
const mod = {
  onEvent: fn({ platform: 'gcfv2', eventTrigger: {
    eventType: 'google.cloud.firestore.document.v1.written',
    eventFilters: { database: '(default)', namespace: '(default)' },
    eventFilterPathPatterns: { document: 'events/{id}' }, retry: false } }),
  nightly: fn({ platform: 'gcfv2', scheduleTrigger: { schedule: 'every day 02:00', timeZone: 'UTC', retryConfig: { retryCount: 2 } } }),
  meili: { onArtist: fn({ platform: 'gcfv2', eventTrigger: {
    eventType: 'google.cloud.firestore.document.v1.created.withAuthContext',
    eventFilters: { database: '(default)', document: 'artists/a1' }, retry: true } }) },
  api: fn({ platform: 'gcfv2', httpsTrigger: {} }),
  notAFunction: 3,
};

test('discovers and classifies v2 functions', () => {
  const all = discover(mod);
  assert.deepStrictEqual(all.map((d) => d.name), ['api', 'meili-onArtist', 'nightly', 'onEvent']);
  const by = Object.fromEntries(all.map((d) => [d.name, d]));
  assert.strictEqual(by.onEvent.type, 'firestore');
  assert.strictEqual(by.onEvent.kind, 'written');
  assert.strictEqual(by.onEvent.document, 'events/{id}');
  assert.strictEqual(by['meili-onArtist'].withAuthContext, true);
  assert.strictEqual(by['meili-onArtist'].document, 'artists/a1');
  assert.strictEqual(by['meili-onArtist'].retry, true);
  assert.strictEqual(by.nightly.retryCount, 2);
  assert.strictEqual(by.api.type, 'unsupported');
});

test('selection', () => {
  const all = discover(mod);
  assert.deepStrictEqual(select(all, { only: ['meili-*', 'nightly'] }).map((d) => d.name), ['meili-onArtist', 'nightly']);
  assert.deepStrictEqual(select(all, { except: ['api'] }).length, 3);
  assert.throws(() => select(all, { only: ['nope'] }), /unknown/);
});
