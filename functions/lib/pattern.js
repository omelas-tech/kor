'use strict';

// Firestore trigger document patterns, as firebase-functions writes them:
//   users/{uid}               one wildcard segment
//   users/{uid}/posts/{pid}   nested
//   users/{rest=**}           multi-segment wildcard, last position only
//
// compile() parses once; match() returns the captured params, or null.

/** @typedef {{lit?: string, wild?: string, multi?: boolean}} Segment */

/** @param {string} pattern @returns {Segment[]} */
function compile(pattern) {
  const segs = String(pattern).replace(/^\/+|\/+$/g, '').split('/');
  const out = segs.map((s, i) => {
    const m = /^\{([^}=]+)(=\*\*)?\}$/.exec(s);
    if (!m) {
      if (s.includes('{') || s.includes('}')) throw new Error(`pattern ${pattern}: bad segment "${s}"`);
      return { lit: s };
    }
    if (m[2] && i !== segs.length - 1) throw new Error(`pattern ${pattern}: "**" must be the last segment`);
    return { wild: m[1], multi: !!m[2] };
  });
  if (!out.some((s) => s.multi) && out.length % 2 !== 0) {
    throw new Error(`pattern ${pattern}: a document pattern has an even number of segments`);
  }
  return out;
}

/**
 * @param {Segment[]} compiled
 * @param {string} path document path relative to the database root
 * @returns {Record<string,string>|null}
 */
function match(compiled, path) {
  const segs = path.split('/');
  /** @type {Record<string,string>} */
  const params = {};
  for (let i = 0; i < compiled.length; i++) {
    const c = compiled[i];
    if (c.multi) {
      if (segs.length <= i) return null;
      params[c.wild] = segs.slice(i).join('/');
      return params;
    }
    if (i >= segs.length) return null;
    if (c.lit !== undefined) {
      if (c.lit !== segs[i]) return null;
    } else {
      params[c.wild] = segs[i];
    }
  }
  return segs.length === compiled.length ? params : null;
}

/**
 * The id of the collection a pattern's documents live in, when it is a literal
 * — which is what Kor's feed filters and image registration key on. null for
 * a wildcard collection or a "**" pattern (those need every collection).
 * @param {Segment[]} compiled
 */
function collectionOf(compiled) {
  if (compiled.some((c) => c.multi)) return null;
  const c = compiled[compiled.length - 2];
  return c && c.lit !== undefined ? c.lit : null;
}

module.exports = { compile, match, collectionOf };
