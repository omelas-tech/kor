'use strict';
const test = require('node:test');
const assert = require('node:assert');
const cron = require('../lib/cron');

const w = (iso, tz = 'UTC') => cron.wallClock(new Date(iso), tz);

test('unix cron', () => {
  const s = cron.parse('0 11,23 * * *');
  assert.ok(s.matches(w('2026-11-25T11:00:00Z')));
  assert.ok(s.matches(w('2026-11-25T23:00:00Z')));
  assert.ok(!s.matches(w('2026-11-25T11:01:00Z')));
  const wk = cron.parse('*/15 9-17 * * mon-fri');
  assert.ok(wk.matches(w('2026-11-25T09:45:00Z'))); // Wednesday
  assert.ok(!wk.matches(w('2026-11-28T09:45:00Z'))); // Saturday
  assert.ok(!wk.matches(w('2026-11-25T09:50:00Z')));
});

test('day-of-month OR day-of-week when both restricted', () => {
  const s = cron.parse('0 0 1 * mon');
  assert.ok(s.matches(w('2026-12-01T00:00:00Z'))); // the 1st (a Tuesday)
  assert.ok(s.matches(w('2026-11-30T00:00:00Z'))); // a Monday
  assert.ok(!s.matches(w('2026-12-02T00:00:00Z')));
});

test('App Engine syntax', () => {
  assert.ok(cron.parse('every day 02:00').matches(w('2026-11-25T02:00:00Z')));
  assert.ok(!cron.parse('every day 02:00').matches(w('2026-11-25T02:01:00Z')));
  assert.ok(cron.parse('every 5 minutes').matches(w('2026-11-25T10:35:00Z')));
  assert.ok(!cron.parse('every 5 minutes').matches(w('2026-11-25T10:36:00Z')));
  assert.ok(cron.parse('every 2 hours').matches(w('2026-11-25T14:00:00Z')));
  assert.ok(cron.parse('every monday,thu 06:00').matches(w('2026-11-26T06:00:00Z')));
  assert.throws(() => cron.parse('every second tuesday of month 09:00'));
  assert.throws(() => cron.parse('0 25 * * *'));
});

test('time zones and DST', () => {
  const s = cron.parse('0 9 * * *');
  // 09:00 in Berlin is 08:00 UTC in winter, 07:00 UTC in summer.
  assert.ok(s.matches(w('2026-11-25T08:00:00Z', 'Europe/Berlin')));
  assert.ok(s.matches(w('2026-07-01T07:00:00Z', 'Europe/Berlin')));
  // Spring forward: 02:30 Berlin does not exist on 2026-03-29.
  const night = cron.parse('30 2 * * *');
  const hits = [];
  for (let t = Date.parse('2026-03-28T23:00:00Z'); t < Date.parse('2026-03-29T03:00:00Z'); t += 60000) {
    if (night.matches(w(new Date(t).toISOString(), 'Europe/Berlin'))) hits.push(t);
  }
  assert.strictEqual(hits.length, 0);
  assert.throws(() => cron.checkTimeZone('Mars/Olympus'));
});
