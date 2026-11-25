'use strict';
const test = require('node:test');
const assert = require('node:assert');
const { compile, match, collectionOf } = require('../lib/pattern');

test('single and nested wildcards', () => {
  assert.deepStrictEqual(match(compile('users/{uid}'), 'users/u1'), { uid: 'u1' });
  assert.strictEqual(match(compile('users/{uid}'), 'users/u1/posts/p'), null);
  assert.strictEqual(match(compile('users/{uid}'), 'groups/u1'), null);
  assert.deepStrictEqual(match(compile('users/{uid}/posts/{pid}'), 'users/u1/posts/p9'), { uid: 'u1', pid: 'p9' });
});

test('multi-segment wildcard', () => {
  const p = compile('users/{rest=**}');
  assert.deepStrictEqual(match(p, 'users/u1/posts/p'), { rest: 'u1/posts/p' });
  assert.strictEqual(collectionOf(p), null);
  assert.throws(() => compile('a/{x=**}/b/{y}'));
});

test('collection of a pattern', () => {
  assert.strictEqual(collectionOf(compile('events/{id}')), 'events');
  assert.strictEqual(collectionOf(compile('users/{u}/posts/{p}')), 'posts');
  assert.strictEqual(collectionOf(compile('{c}/{id}')), null);
  assert.throws(() => compile('users/{u}/posts'));
});
