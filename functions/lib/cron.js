'use strict';

// Schedules, as Cloud Scheduler accepts them from firebase-functions:
//
//   unix cron       "0 11,23 * * *", "*/15 9-17 * * mon-fri"
//   App Engine      "every 5 minutes", "every 2 hours",
//                   "every day 02:00", "every monday 09:30", "every mon,thu 06:00"
//
// Matching is done on the wall clock of the schedule's time zone, one minute at
// a time. That is also how cron handles DST: a 02:30 job does not run on the
// spring-forward day (02:30 never happens) and runs once on the fall-back day
// (the scheduler remembers the minute it last fired).
//
// "every N minutes|hours" is treated as synchronized to the top of the hour /
// midnight. App Engine's unsynchronized form counts from when the job was
// created, which a restartable process cannot reproduce; synchronized is the
// deterministic reading.

const MONTHS = ['jan', 'feb', 'mar', 'apr', 'may', 'jun', 'jul', 'aug', 'sep', 'oct', 'nov', 'dec'];
const DAYS = ['sun', 'mon', 'tue', 'wed', 'thu', 'fri', 'sat'];
const LONG_DAYS = ['sunday', 'monday', 'tuesday', 'wednesday', 'thursday', 'friday', 'saturday'];

/** @typedef {{minute:number,hour:number,dom:number,month:number,dow:number}} Wall */

/**
 * @param {string} expr
 * @returns {{matches: (w: Wall) => boolean, source: string}}
 */
function parse(expr) {
  const src = String(expr).trim();
  const lower = src.toLowerCase().replace(/\s+/g, ' ');
  if (lower.startsWith('every ')) return parseAppEngine(lower, src);
  return parseCron(lower, src);
}

function parseCron(lower, src) {
  const f = lower.split(' ');
  if (f.length !== 5) throw new Error(`schedule "${src}": want 5 cron fields, got ${f.length}`);
  const minute = field(f[0], 0, 59, null, src);
  const hour = field(f[1], 0, 23, null, src);
  const dom = field(f[2], 1, 31, null, src);
  const month = field(f[3], 1, 12, MONTHS, src);
  const dow = field(f[4].replace(/\b7\b/g, '0'), 0, 6, DAYS, src);
  const domStar = f[2] === '*' || f[2] === '?';
  const dowStar = f[4] === '*' || f[4] === '?';
  return {
    source: src,
    matches: (w) => {
      if (!minute.has(w.minute) || !hour.has(w.hour) || !month.has(w.month)) return false;
      // Vixie cron: when both day fields are restricted, either may match.
      if (!domStar && !dowStar) return dom.has(w.dom) || dow.has(w.dow);
      return dom.has(w.dom) && dow.has(w.dow);
    },
  };
}

function field(s, lo, hi, names, src) {
  /** @type {Set<number>} */
  const out = new Set();
  for (const part of s.split(',')) {
    let [range, stepS] = part.split('/');
    const step = stepS === undefined ? 1 : Number(stepS);
    if (!Number.isInteger(step) || step < 1) throw new Error(`schedule "${src}": bad step in "${part}"`);
    let a = lo;
    let b = hi;
    if (range !== '*' && range !== '?') {
      const [x, y] = range.split('-');
      a = num(x, names, src);
      b = y === undefined ? (stepS === undefined ? a : hi) : num(y, names, src);
    }
    if (a < lo || b > hi || a > b) throw new Error(`schedule "${src}": "${part}" out of range ${lo}-${hi}`);
    for (let v = a; v <= b; v += step) out.add(v);
  }
  return out;
}

function num(s, names, src) {
  if (names) {
    const i = names.indexOf(s.slice(0, 3));
    if (i >= 0 && /^[a-z]+$/.test(s)) return names === MONTHS ? i + 1 : i;
  }
  const n = Number(s);
  if (!Number.isInteger(n)) throw new Error(`schedule "${src}": "${s}" is not a number`);
  return n;
}

function parseAppEngine(lower, src) {
  let m = /^every (\d+) (minute|minutes|mins|hour|hours)$/.exec(lower);
  if (m) {
    const n = Number(m[1]);
    if (n < 1) throw new Error(`schedule "${src}": interval must be positive`);
    if (m[2].startsWith('min')) {
      return { source: src, matches: (w) => (w.hour * 60 + w.minute) % n === 0 };
    }
    return { source: src, matches: (w) => w.minute === 0 && w.hour % n === 0 };
  }
  m = /^every ([a-z, ]+?) (\d{1,2}):(\d{2})$/.exec(lower);
  if (m) {
    const hour = Number(m[2]);
    const minute = Number(m[3]);
    if (hour > 23 || minute > 59) throw new Error(`schedule "${src}": bad time`);
    const which = m[1].trim();
    /** @type {Set<number>|null} */
    let days = null;
    if (which !== 'day') {
      days = new Set();
      for (const d of which.split(/\s*,\s*/)) {
        let i = LONG_DAYS.indexOf(d);
        if (i < 0) i = DAYS.indexOf(d);
        if (i < 0) throw new Error(`schedule "${src}": unknown day "${d}"`);
        days.add(i);
      }
    }
    return {
      source: src,
      matches: (w) => w.hour === hour && w.minute === minute && (days === null || days.has(w.dow)),
    };
  }
  throw new Error(`schedule "${src}": unsupported App Engine syntax (supported: every N minutes|hours, every day HH:MM, every <weekday[,weekday]> HH:MM)`);
}

/** @type {Map<string, Intl.DateTimeFormat>} */
const formatters = new Map();

/**
 * The wall clock of `date` in `timeZone`.
 * @param {Date} date
 * @param {string} timeZone
 * @returns {Wall & {year:number}}
 */
function wallClock(date, timeZone) {
  let fmt = formatters.get(timeZone);
  if (!fmt) {
    fmt = new Intl.DateTimeFormat('en-US', {
      timeZone, hourCycle: 'h23', year: 'numeric', month: 'numeric', day: 'numeric',
      hour: 'numeric', minute: 'numeric', weekday: 'short',
    });
    formatters.set(timeZone, fmt);
  }
  /** @type {Record<string,string>} */
  const p = {};
  for (const part of fmt.formatToParts(date)) p[part.type] = part.value;
  return {
    year: Number(p.year), month: Number(p.month), dom: Number(p.day),
    hour: Number(p.hour) % 24, minute: Number(p.minute),
    dow: DAYS.indexOf(p.weekday.toLowerCase().slice(0, 3)),
  };
}

/** Throws for an unknown IANA zone, so a typo fails at startup. */
function checkTimeZone(tz) {
  wallClock(new Date(), tz);
}

module.exports = { parse, wallClock, checkTimeZone };
