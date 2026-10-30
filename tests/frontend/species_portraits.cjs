const assert = require('node:assert/strict');
const fs = require('node:fs');

(async () => {
  const source = fs.readFileSync(process.argv[2], 'utf8');
  const {lookupSpeciesPortrait: lookup} = await import('data:text/javascript;base64,' + Buffer.from(source).toString('base64'));
  const node = '70ada787-ba19-4dd5-817f-17a1e7456b41';
  const image = '51b1b6e4-129d-41a6-bbbd-c3fab459c25f';
  const makeNode = () => ({uuid: node, names: [[{class: 'scientific', text: 'Vulpes vulpes'}]],
    _links: {primaryImage: {href: `/images/${image}?build=560`}}});
  const makeImage = () => ({uuid: image, attribution: 'Steven Traver', _links: {
    specificNode: {href: `/nodes/${node}?build=560`},
    license: {href: 'https://creativecommons.org/publicdomain/zero/1.0/'},
    thumbnailFiles: [192, 64, 128].map(n => ({type: 'image/png',
      href: `https://images.phylopic.org/images/${image}/thumbnail/${n}x${n}.png`}))}});
  let changes = {}, calls;
  function mock(overrides = {}) {
    changes = overrides; calls = [];
    global.fetch = async (url, options) => {
      const u = new URL(url); calls.push(u);
      assert.equal(options.credentials, 'omit');
      assert.equal(options.referrerPolicy, 'no-referrer');
      assert.equal(u.origin, 'https://api.phylopic.org');
      let data;
      if (u.pathname === '/nodes' && !u.searchParams.has('page')) {
        assert.equal(u.searchParams.get('filter_name'), 'vulpes vulpes');
        data = {totalItems: 2, totalPages: 1, _links: {firstPage: {href: '/nodes?build=560&page=0'}}};
        if (changes.list) data = changes.list;
      } else if (u.pathname === '/nodes') {
        assert.equal(u.searchParams.get('build'), '560');
        assert.equal(u.searchParams.get('embed_items'), 'true');
        data = {_embedded: {items: changes.nodes || [makeNode(), {names: [[{class: 'scientific', text: 'Vulpes'}]]}]}};
      } else {
        data = changes.image || makeImage();
      }
      return {ok: true, json: async () => data};
    };
  }
  mock();
  const found = await lookup('  Vulpes   vulpes ');
  assert.equal(found.credit, 'Steven Traver');
  assert.equal(found.license, 'CC0');
  assert.match(found.url, /128x128\.png$/);
  assert.equal(calls.length, 3);

  // An ancestor's image, fuzzy search result, and duplicate exact matches are omitted.
  let img = makeImage(); img._links.specificNode.href = '/nodes/00000000-0000-0000-0000-000000000000';
  mock({image: img}); assert.equal(await lookup('Vulpes vulpes'), null);
  const genus = makeNode(); genus.names = [[{class: 'scientific', text: 'Vulpes'}]];
  mock({nodes: [genus]}); assert.equal(await lookup('Vulpes vulpes'), null);
  mock({nodes: [makeNode(), makeNode()]}); assert.equal(await lookup('Vulpes vulpes'), null);
  assert.equal(calls.length, 2);

  for (const [kind, expected] of [['by', 'CC BY 4.0'], ['by-sa', 'CC BY-SA 4.0'], ['by-nc', null]]) {
    img = makeImage(); img._links.license.href = `https://creativecommons.org/licenses/${kind}/4.0/`;
    mock({image: img}); assert.equal((await lookup('Vulpes vulpes'))?.license || null, expected);
  }
  img = makeImage(); img._links.thumbnailFiles[0].href = 'https://example.org/not-a-silhouette.png';
  img._links.thumbnailFiles = [img._links.thumbnailFiles[0]];
  mock({image: img}); assert.equal(await lookup('Vulpes vulpes'), null);
  img = makeImage(); delete img._links.license;
  mock({image: img}); assert.equal(await lookup('Vulpes vulpes'), null);
  mock({list: {totalItems: 0}}); assert.equal(await lookup('Vulpes vulpes'), null);
  assert.equal(calls.length, 1);
  mock({list: {totalItems: 100, totalPages: 3}}); assert.equal(await lookup('Vulpes vulpes'), null);
  mock({list: {totalItems: 1, totalPages: 1, _links: {firstPage: {href: 'https://example.org/collect'}}}});
  assert.equal(await lookup('Vulpes vulpes'), null); assert.equal(calls.length, 1);

  // Unexpected provider payloads, HTTP errors, offline mode, and a hung service are harmless.
  mock({nodes: 'malformed'}); assert.equal(await lookup('Vulpes vulpes'), null);
  global.fetch = async () => ({ok: false});
  assert.equal(await lookup('Vulpes vulpes'), null);
  global.fetch = async () => { throw new Error('offline'); };
  assert.equal(await lookup('Vulpes vulpes'), null);
  const timeout = global.setTimeout;
  global.setTimeout = callback => timeout(callback, 5);
  global.fetch = (_, options) => new Promise((resolve, reject) =>
    options.signal.addEventListener('abort', () => reject(new Error('timed out'))));
  assert.equal(await lookup('Vulpes vulpes'), null);
  global.setTimeout = timeout;
  console.log('Species lookup, matching, licensing, URL validation, and failure checks passed');
})().catch(error => { console.error(error); process.exitCode = 1; });
