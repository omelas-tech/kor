'use strict';

// Finds the functions in a loaded firebase-functions v2 codebase.
//
// Every v2 function object carries `__endpoint`: the manifest `firebase deploy`
// reads. It says what triggers the function (eventTrigger / scheduleTrigger /
// httpsTrigger / ...), so the runtime needs no configuration of its own to know
// what to serve — the same code, the same manifest, a different executor.
//
// Exports are walked recursively; nested groups are named the way Firebase
// names them on deploy: `{ meili: { onX } }` -> "meili-onX".

const FIRESTORE_PREFIX = 'google.cloud.firestore.document.v1.';

/**
 * @typedef {object} Discovered
 * @property {string} name
 * @property {Function} fn
 * @property {'firestore'|'schedule'|'unsupported'} type
 * @property {string} [kind]           firestore: created|updated|deleted|written; unsupported: trigger label
 * @property {boolean} [withAuthContext]
 * @property {string} [document]
 * @property {string} [database]
 * @property {boolean} [retry]
 * @property {string} [schedule]
 * @property {string} [timeZone]
 * @property {number} [retryCount]
 */

/** @returns {Discovered[]} */
function discover(mod) {
  /** @type {Discovered[]} */
  const out = [];
  walk(mod, '', out, new Set());
  out.sort((a, b) => a.name.localeCompare(b.name));
  return out;
}

function walk(obj, prefix, out, seen) {
  if (!obj || typeof obj !== 'object' && typeof obj !== 'function') return;
  if (seen.has(obj)) return;
  seen.add(obj);
  for (const [k, v] of Object.entries(obj)) {
    const name = prefix ? `${prefix}-${k}` : k;
    if (typeof v === 'function' && v.__endpoint) {
      out.push(classify(name, v));
    } else if (v && typeof v === 'object' && !Array.isArray(v) && isPlain(v)) {
      walk(v, name, out, seen);
    }
  }
}

function isPlain(v) {
  const p = Object.getPrototypeOf(v);
  return p === Object.prototype || p === null;
}

/**
 * Params (defineString etc.) arrive as Expression objects; options left unset
 * arrive as RESET_VALUE markers. Both are resolved here so the rest of the
 * runtime sees plain values.
 */
function val(x) {
  if (x && typeof x === 'object') {
    if (typeof x.value === 'function') return x.value();
    return undefined;
  }
  return x;
}

/** @returns {Discovered} */
function classify(name, fn) {
  const ep = fn.__endpoint;
  const et = ep.eventTrigger;
  if (et && typeof et.eventType === 'string' && et.eventType.startsWith(FIRESTORE_PREFIX)) {
    const [kind, auth] = et.eventType.slice(FIRESTORE_PREFIX.length).split('.');
    const filters = et.eventFilters || {};
    const patterns = et.eventFilterPathPatterns || {};
    return {
      name, fn, type: 'firestore', kind,
      withAuthContext: auth === 'withAuthContext',
      document: val(patterns.document ?? filters.document),
      database: val(filters.database) || '(default)',
      retry: !!val(et.retry),
    };
  }
  if (ep.scheduleTrigger) {
    const st = ep.scheduleTrigger;
    return {
      name, fn, type: 'schedule',
      schedule: val(st.schedule),
      timeZone: val(st.timeZone) || 'UTC',
      retryCount: Number(val(st.retryConfig?.retryCount) || 0),
    };
  }
  let kind = 'https';
  if (ep.callableTrigger) kind = 'callable';
  else if (et) kind = et.eventType;
  else if (ep.blockingTrigger) kind = `blocking:${ep.blockingTrigger.eventType}`;
  else if (ep.taskQueueTrigger) kind = 'taskQueue';
  if (ep.platform === 'gcfv1') kind = `v1:${kind}`;
  return { name, fn, type: 'unsupported', kind };
}

/**
 * @param {Discovered[]} all
 * @param {{only?: string[], except?: string[]}} sel  entries may end in "*" (prefix match)
 */
function select(all, { only = [], except = [] }) {
  const hit = (list, name) => list.some((p) => (p.endsWith('*') ? name.startsWith(p.slice(0, -1)) : p === name));
  const unknown = [...only, ...except].filter((p) => !p.endsWith('*') && !all.some((d) => d.name === p));
  if (unknown.length) throw new Error(`unknown function name(s): ${unknown.join(', ')}`);
  return all.filter((d) => (only.length === 0 || hit(only, d.name)) && !hit(except, d.name));
}

module.exports = { discover, select, FIRESTORE_PREFIX };
