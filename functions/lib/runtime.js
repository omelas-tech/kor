'use strict';

// The functions runtime: tails Kor's change feed and invokes Firestore
// triggers, and fires scheduled functions on their cron.
//
// Delivery is at-least-once, like Cloud Functions. The cursor is saved only
// after a page of events has been handled, so a crash replays the tail of the
// page. Events for one document are delivered in commit order; different
// documents run concurrently. Handlers should be idempotent — Firestore asks
// the same of them.

const fs = require('node:fs');
const http = require('node:http');
const path = require('node:path');
const { compile, match, collectionOf } = require('./pattern');
const cron = require('./cron');
const { FIRESTORE_PREFIX } = require('./discover');

/**
 * @typedef {object} Options
 * @property {string} feedUrl          kord admin listener, e.g. http://127.0.0.1:6566
 * @property {string} stateFile        cursor + scheduler bookkeeping
 * @property {string} project          project id in document names
 * @property {import('./discover').Discovered[]} functions
 * @property {number} [concurrency]    documents handled in parallel (default 8)
 * @property {number} [pageSize]       events per feed request (default 200)
 * @property {number} [pollMs]         idle poll interval (default 500)
 * @property {number} [maxAttempts]    per event, for retry:true triggers (default 5)
 * @property {boolean} [triggers]      run Firestore triggers (default true)
 * @property {boolean} [schedules]     run scheduled functions (default true)
 * @property {(level: string, msg: string, fields?: object) => void} [log]
 */

class Runtime {
  /** @param {Options} opts */
  constructor(opts) {
    this.o = {
      concurrency: 8, pageSize: 200, pollMs: 500, maxAttempts: 5, triggers: true, schedules: true,
      log: defaultLog,
      // An explicit undefined (an unset CLI flag) must not erase a default.
      ...Object.fromEntries(Object.entries(opts).filter(([, v]) => v !== undefined)),
    };
    this.log = this.o.log;
    this.triggers = this.o.functions.filter((f) => f.type === 'firestore').map((f) => {
      const compiled = compile(f.document);
      const coll = collectionOf(compiled);
      if (!coll) throw new Error(`${f.name}: pattern "${f.document}" has no literal collection id; Kor's feed is filtered by collection`);
      return { ...f, compiled, collection: coll, eventType: `${FIRESTORE_PREFIX}${f.kind}${f.withAuthContext ? '.withAuthContext' : ''}` };
    });
    this.schedules = this.o.functions.filter((f) => f.type === 'schedule').map((f) => {
      cron.checkTimeZone(f.timeZone);
      return { ...f, cron: cron.parse(f.schedule) };
    });
    this.collections = [...new Set(this.triggers.map((t) => t.collection))].sort();
    this.state = loadState(this.o.stateFile);
    this.stopped = false;
    /** @type {Set<string>} scheduled functions currently running */
    this.running = new Set();
    this.metrics = { events: 0, invocations: 0, failures: 0, skippedNoImages: 0, scheduled: 0, lagSeconds: 0 };
  }

  async start() {
    const tasks = [];
    if (this.o.triggers && this.triggers.length) tasks.push(this.feedLoop());
    if (this.o.schedules && this.schedules.length) tasks.push(this.cronLoop());
    this.log('info', 'kor-functions started', {
      triggers: this.o.triggers ? this.triggers.map((t) => t.name) : [],
      schedules: this.o.schedules ? this.schedules.map((s) => `${s.name} [${s.schedule} ${s.timeZone}]`) : [],
      collections: this.collections,
    });
    await Promise.all(tasks);
  }

  stop() {
    this.stopped = true;
    if (this.wake) this.wake();
  }

  async feedLoop() {
    // Register the collections we trigger on, so their events carry images.
    // The head returned by registration is the first point from which every
    // event is guaranteed to have them: a fresh install starts there.
    const reg = await this.http('POST', '/feed/v1/images', { register: this.collections });
    if (!this.state.cursor) {
      this.state.cursor = reg.head;
      saveState(this.o.stateFile, this.state);
      this.log('info', 'no saved cursor; starting at the feed head', { cursor: reg.head });
    }
    while (!this.stopped) {
      let page;
      try {
        const qs = new URLSearchParams({ after: this.state.cursor, images: '1', limit: String(this.o.pageSize) });
        for (const c of this.collections) qs.append('collection', c);
        page = await this.http('GET', `/feed/v1/changes?${qs}`);
      } catch (e) {
        this.log('error', 'feed read failed; retrying', { err: String(e) });
        await this.sleep(Math.max(this.o.pollMs, 2000));
        continue;
      }
      if (page.changes.length) {
        await this.handlePage(page.changes);
        this.state.cursor = page.cursor;
        saveState(this.o.stateFile, this.state);
        const last = page.changes[page.changes.length - 1];
        this.metrics.lagSeconds = Math.max(0, (Date.now() - Date.parse(last.at)) / 1000);
      } else {
        this.metrics.lagSeconds = 0;
      }
      if (page.changes.length < this.o.pageSize) await this.sleep(this.o.pollMs);
    }
  }

  /** One page: per-document ordering, bounded concurrency across documents. */
  async handlePage(changes) {
    /** @type {Map<string, any[]>} */
    const byDoc = new Map();
    for (const c of changes) {
      if (!byDoc.has(c.name)) byDoc.set(c.name, []);
      byDoc.get(c.name).push(c);
    }
    const queue = [...byDoc.values()];
    const worker = async () => {
      for (let evs = queue.shift(); evs; evs = queue.shift()) {
        for (const c of evs) await this.handleChange(c);
      }
    };
    await Promise.all(Array.from({ length: Math.min(this.o.concurrency, queue.length) }, worker));
  }

  async handleChange(c) {
    this.metrics.events++;
    const parsed = parseName(c.name);
    if (!parsed) return;
    const matching = [];
    for (const t of this.triggers) {
      if (t.database !== parsed.database) continue;
      const params = match(t.compiled, parsed.path);
      if (params) matching.push(t);
    }
    if (!matching.length) return;
    if (!c.hasImages) {
      // Committed before the collection was registered (or while registration
      // was in flight). A trigger without before/after cannot be honest.
      this.metrics.skippedNoImages++;
      this.log('warn', 'event has no document images; skipped', { name: c.name, seq: c.seq });
      return;
    }
    const before = c.before || null;
    const after = c.after || null;
    const kind = !before && after ? 'created' : before && !after ? 'deleted' : before && after ? 'updated' : null;
    if (!kind) return;
    for (const t of matching) {
      if (t.kind !== kind && t.kind !== 'written') continue;
      await this.invoke(t, buildEvent(t, c, parsed, before, after));
    }
  }

  async invoke(t, event) {
    for (let attempt = 1; ; attempt++) {
      this.metrics.invocations++;
      try {
        await t.fn(event);
        return;
      } catch (e) {
        this.metrics.failures++;
        const last = !t.retry || attempt >= this.o.maxAttempts;
        this.log(last ? 'error' : 'warn', `${t.name} failed`, {
          document: event.document, attempt, retry: t.retry, err: e && e.stack ? e.stack : String(e),
        });
        if (last) return;
        await this.sleep(Math.min(60000, 1000 * 2 ** (attempt - 1)));
      }
    }
  }

  async cronLoop() {
    this.state.lastFired = this.state.lastFired || {};
    while (!this.stopped) {
      const now = new Date();
      for (const s of this.schedules) {
        const w = cron.wallClock(now, s.timeZone);
        if (!s.cron.matches(w)) continue;
        // Keyed on the wall-clock minute in the job's zone: a restart within
        // the minute does not fire twice, and neither does the repeated hour
        // when DST falls back.
        const key = `${w.year}-${w.month}-${w.dom}T${w.hour}:${w.minute}`;
        if (this.state.lastFired[s.name] === key) continue;
        this.state.lastFired[s.name] = key;
        saveState(this.o.stateFile, this.state);
        if (this.running.has(s.name)) {
          this.log('warn', `${s.name} still running from its previous slot; this one is skipped`, { slot: key });
          continue;
        }
        this.running.add(s.name);
        this.runScheduled(s, now).finally(() => this.running.delete(s.name));
      }
      // Sleep to just past the next minute boundary.
      await this.sleep(60000 - (Date.now() % 60000) + 50);
    }
  }

  /** Scheduled functions are HTTP handlers Cloud Scheduler calls; call them the same way. */
  async runScheduled(s, at) {
    for (let attempt = 1; ; attempt++) {
      this.metrics.scheduled++;
      const t0 = Date.now();
      const code = await callHttp(s.fn, {
        'x-cloudscheduler-jobname': `kor-${s.name}`,
        'x-cloudscheduler-scheduletime': at.toISOString(),
      });
      const ms = Date.now() - t0;
      if (code < 400) {
        this.log('info', `${s.name} ran`, { ms, attempt });
        return;
      }
      this.metrics.failures++;
      const last = attempt > s.retryCount;
      this.log(last ? 'error' : 'warn', `${s.name} failed`, { status: code, ms, attempt });
      if (last) return;
      await this.sleep(Math.min(300000, 10000 * 2 ** (attempt - 1)));
    }
  }

  /**
   * node:http, not fetch: the fetch spec blocks a list of "bad ports", and
   * kord's default admin port (6566, the SANE scanner port) is on it.
   */
  http(method, p, body) {
    const url = new URL(this.o.feedUrl.replace(/\/$/, '') + p);
    const payload = body ? JSON.stringify(body) : undefined;
    return new Promise((resolve, reject) => {
      const req = http.request(url, {
        method,
        headers: payload ? { 'content-type': 'application/json', 'content-length': Buffer.byteLength(payload) } : {},
        timeout: 60000,
      }, (res) => {
        const chunks = [];
        res.on('data', (c) => chunks.push(c));
        res.on('end', () => {
          let json = {};
          try {
            json = JSON.parse(Buffer.concat(chunks).toString('utf8'));
          } catch {
            // fall through with an empty object; the status decides
          }
          if (res.statusCode >= 400) reject(new Error(`${method} ${p}: HTTP ${res.statusCode} ${json.error || ''}`));
          else resolve(json);
        });
        res.on('error', reject);
      });
      req.on('timeout', () => req.destroy(new Error(`${method} ${p}: timed out`)));
      req.on('error', reject);
      req.end(payload);
    });
  }

  sleep(ms) {
    return new Promise((resolve) => {
      const t = setTimeout(resolve, ms);
      this.wake = () => { clearTimeout(t); resolve(); };
    });
  }
}

/** projects/P/databases/D/documents/<path> */
function parseName(name) {
  const m = /^projects\/([^/]+)\/databases\/([^/]+)\/documents\/(.+)$/.exec(name);
  return m ? { project: m[1], database: m[2], path: m[3] } : null;
}

/** The CloudEvent Eventarc would deliver, in its JSON encoding. */
function buildEvent(t, c, parsed, before, after) {
  const ev = {
    specversion: '1.0',
    id: `kor-${c.xid}-${c.seq}`,
    source: `//firestore.googleapis.com/projects/${parsed.project}/databases/${parsed.database}`,
    type: t.eventType,
    subject: `documents/${parsed.path}`,
    time: c.at,
    datacontenttype: 'application/json',
    data: { value: after || undefined, oldValue: before || undefined },
    project: parsed.project,
    database: parsed.database,
    namespace: '(default)',
    document: parsed.path,
    location: 'kor',
  };
  if (t.withAuthContext) {
    // Kor does not know who made a write; say so rather than inventing a principal.
    ev.authtype = 'unknown';
    ev.authid = '';
  }
  return ev;
}

/** Invoke an (req, res) handler in-process. Resolves with the status code. */
function callHttp(fn, headers) {
  return new Promise((resolve) => {
    let status = 200;
    const lower = Object.fromEntries(Object.entries(headers).map(([k, v]) => [k.toLowerCase(), v]));
    const req = {
      method: 'POST', url: '/', path: '/', body: {}, query: {}, headers: lower,
      header: (n) => lower[String(n).toLowerCase()],
      get: (n) => lower[String(n).toLowerCase()],
    };
    const res = {
      status(c) { status = c; return res; },
      send() { resolve(status); return res; },
      end() { resolve(status); return res; },
      json() { resolve(status); return res; },
      sendStatus(c) { status = c; resolve(c); return res; },
      set() { return res; },
      setHeader() { return res; },
    };
    Promise.resolve()
      .then(() => fn(req, res))
      .catch(() => resolve(500));
  });
}

function loadState(file) {
  try {
    return JSON.parse(fs.readFileSync(file, 'utf8'));
  } catch (e) {
    if (e.code === 'ENOENT') return {};
    throw new Error(`kor-functions: unreadable state file ${file}: ${e.message}`);
  }
}

/** Atomic: a crash mid-write must not lose the cursor. */
function saveState(file, state) {
  fs.mkdirSync(path.dirname(file), { recursive: true });
  const tmp = `${file}.tmp`;
  fs.writeFileSync(tmp, JSON.stringify(state));
  fs.renameSync(tmp, file);
}

function defaultLog(level, msg, fields) {
  const line = JSON.stringify({ time: new Date().toISOString(), level, msg, ...fields });
  (level === 'error' || level === 'warn' ? process.stderr : process.stdout).write(line + '\n');
}

module.exports = { Runtime, buildEvent, parseName, callHttp };
