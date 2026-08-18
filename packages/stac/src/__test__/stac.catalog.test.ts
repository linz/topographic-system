import assert from 'node:assert';
import { before, describe, it } from 'node:test';

import { fsa, FsMemory } from '@chunkd/fs';
import pLimit from 'p-limit';

import { getCollectionsByStrategy } from '../stac.catalog.ts';

// Catalog URLs must use the form memory://{host}/{category}/catalog.json
// so that category can be derived as a path component (e.g. 'data')

describe('getCollectionsByStrategy', () => {
  const mem = new FsMemory();
  const q = pLimit(10);

  before(() => {
    fsa.register('memory://', mem);
  });

  it('should find all commit-specific collections for all layers', async () => {
    const commitSha = 'a30006782f863ae93a19e1f78adffa30c89f6e8f';
    const catalogUrl = new URL('memory://stac/data/catalog.json');

    await fsa.write(
      catalogUrl,
      JSON.stringify({
        type: 'Catalog',
        id: 'data',
        links: [
          { rel: 'child', href: './airport/catalog.json', title: 'airport' },
          { rel: 'child', href: './coastline/catalog.json', title: 'coastline' },
          { rel: 'child', href: './water/catalog.json', title: 'water' },
        ],
      }),
    );

    for (const layer of ['airport', 'coastline', 'water']) {
      await fsa.write(
        new URL(`memory://stac/data/${layer}/catalog.json`),
        JSON.stringify({ type: 'Catalog', id: layer, links: [] }),
      );
      await fsa.write(
        new URL(`memory://stac/data/${layer}/commit_prefix=a/commit=${commitSha}/collection.json`),
        JSON.stringify({ type: 'Collection', id: layer }),
      );
    }

    const result = await getCollectionsByStrategy(catalogUrl, { type: 'commit', commit: commitSha }, q);

    assert.strictEqual(result.size, 3);
    assert.ok(result.has('airport'));
    assert.ok(result.has('coastline'));
    assert.ok(result.has('water'));
    assert.strictEqual(
      result.get('airport')?.href,
      `memory://stac/data/airport/commit_prefix=a/commit=${commitSha}/collection.json`,
    );
  });

  it('should only return layers that have commit-specific data', async () => {
    const commitSha = 'b40006782f863ae93a19e1f78adffa30c89f6e8f';
    const catalogUrl = new URL('memory://stac-fallback/data/catalog.json');

    await fsa.write(
      catalogUrl,
      JSON.stringify({
        type: 'Catalog',
        id: 'data',
        links: [
          { rel: 'child', href: './airport/catalog.json', title: 'airport' },
          { rel: 'child', href: './coastline/catalog.json', title: 'coastline' },
        ],
      }),
    );
    await fsa.write(
      new URL('memory://stac-fallback/data/airport/catalog.json'),
      JSON.stringify({ type: 'Catalog', id: 'airport', links: [] }),
    );
    await fsa.write(
      new URL('memory://stac-fallback/data/coastline/catalog.json'),
      JSON.stringify({ type: 'Catalog', id: 'coastline', links: [] }),
    );

    // Only airport has commit data
    await fsa.write(
      new URL(`memory://stac-fallback/data/airport/commit_prefix=b/commit=${commitSha}/collection.json`),
      JSON.stringify({ type: 'Collection', id: 'airport' }),
    );

    const result = await getCollectionsByStrategy(catalogUrl, { type: 'commit', commit: commitSha }, q);
    assert.strictEqual(result.size, 1);
    assert.ok(result.has('airport'));
    assert.ok(!result.has('coastline'));
  });

  it('should throw if no layers have commit-specific data', async () => {
    const commitSha = 'd99999782f863ae93a19e1f78adffa30c89f6e8f';
    const catalogUrl = new URL('memory://stac-none/data/catalog.json');

    await fsa.write(
      catalogUrl,
      JSON.stringify({
        type: 'Catalog',
        id: 'data',
        links: [{ rel: 'child', href: './airport/catalog.json', title: 'airport' }],
      }),
    );
    await fsa.write(
      new URL('memory://stac-none/data/airport/catalog.json'),
      JSON.stringify({ type: 'Catalog', id: 'airport', links: [] }),
    );

    await assert.rejects(
      getCollectionsByStrategy(catalogUrl, { type: 'commit', commit: commitSha }, q),
      (err: Error) => {
        assert.ok(err.message.includes('No data found for strategy'));
        return true;
      },
    );
  });

  it('should handle different commit SHA prefixes correctly', async () => {
    const commitSha1 = 'a30006782f863ae93a19e1f78adffa30c89f6e8f';
    const commitSha2 = 'c50006782f863ae93a19e1f78adffa30c89f6e8f';
    const catalogUrl = new URL('memory://stac-prefixes/data/catalog.json');

    await fsa.write(
      catalogUrl,
      JSON.stringify({
        type: 'Catalog',
        id: 'data',
        links: [{ rel: 'child', href: './airport/catalog.json', title: 'airport' }],
      }),
    );
    await fsa.write(
      new URL('memory://stac-prefixes/data/airport/catalog.json'),
      JSON.stringify({ type: 'Catalog', id: 'airport', links: [] }),
    );

    await fsa.write(
      new URL(`memory://stac-prefixes/data/airport/commit_prefix=a/commit=${commitSha1}/collection.json`),
      JSON.stringify({ type: 'Collection', id: 'airport' }),
    );
    await fsa.write(
      new URL(`memory://stac-prefixes/data/airport/commit_prefix=c/commit=${commitSha2}/collection.json`),
      JSON.stringify({ type: 'Collection', id: 'airport' }),
    );

    const result1 = await getCollectionsByStrategy(catalogUrl, { type: 'commit', commit: commitSha1 }, q);
    assert.strictEqual(result1.size, 1);
    assert.ok(result1.get('airport')?.href.includes('commit_prefix=a'));

    const result2 = await getCollectionsByStrategy(catalogUrl, { type: 'commit', commit: commitSha2 }, q);
    assert.strictEqual(result2.size, 1);
    assert.ok(result2.get('airport')?.href.includes('commit_prefix=c'));
  });

  it('should handle different date prefixes correctly', async () => {
    const date1 = new Date('2026-05-19T22:18:14.595Z');
    const date2 = new Date('2027-01-02T03:04:05.678Z');
    const date1Path = date1.toISOString().replaceAll(':', '-');
    const date2Path = date2.toISOString().replaceAll(':', '-');
    const catalogUrl = new URL('memory://stac-date-prefixes/data/catalog.json');

    await fsa.write(
      catalogUrl,
      JSON.stringify({
        type: 'Catalog',
        id: 'data',
        links: [{ rel: 'child', href: './airport/catalog.json', title: 'airport' }],
      }),
    );
    await fsa.write(
      new URL('memory://stac-date-prefixes/data/airport/catalog.json'),
      JSON.stringify({ type: 'Catalog', id: 'airport', links: [] }),
    );

    await fsa.write(
      new URL(
        `memory://stac-date-prefixes/data/airport/year=${date1.getUTCFullYear()}/date=${date1Path}/collection.json`,
      ),
      JSON.stringify({ type: 'Collection', id: 'airport' }),
    );
    await fsa.write(
      new URL(
        `memory://stac-date-prefixes/data/airport/year=${date2.getUTCFullYear()}/date=${date2Path}/collection.json`,
      ),
      JSON.stringify({ type: 'Collection', id: 'airport' }),
    );

    const result1 = await getCollectionsByStrategy(catalogUrl, { type: 'date', date: date1 }, q);
    assert.strictEqual(result1.size, 1);
    assert.strictEqual(
      result1.get('airport')?.href,
      `memory://stac-date-prefixes/data/airport/year=${date1.getUTCFullYear()}/date=${date1Path}/collection.json`,
    );

    const result2 = await getCollectionsByStrategy(catalogUrl, { type: 'date', date: date2 }, q);
    assert.strictEqual(result2.size, 1);
    assert.strictEqual(
      result2.get('airport')?.href,
      `memory://stac-date-prefixes/data/airport/year=${date2.getUTCFullYear()}/date=${date2Path}/collection.json`,
    );
  });

  it('should skip non-child and non-catalog links', async () => {
    const commitSha = 'a30006782f863ae93a19e1f78adffa30c89f6e8f';
    const catalogUrl = new URL('memory://stac-skip/data/catalog.json');

    await fsa.write(
      catalogUrl,
      JSON.stringify({
        type: 'Catalog',
        id: 'data',
        links: [
          { rel: 'child', href: './parent' }, // doesn't end with /catalog.json
          { rel: 'child', href: './airport/catalog.json', title: 'airport' },
          { rel: 'parent', href: '../root.json' }, // different rel type
        ],
      }),
    );
    await fsa.write(
      new URL('memory://stac-skip/data/airport/catalog.json'),
      JSON.stringify({ type: 'Catalog', id: 'airport', links: [] }),
    );
    await fsa.write(
      new URL(`memory://stac-skip/data/airport/commit_prefix=a/commit=${commitSha}/collection.json`),
      JSON.stringify({ type: 'Collection', id: 'airport' }),
    );

    const result = await getCollectionsByStrategy(catalogUrl, { type: 'commit', commit: commitSha }, q);
    assert.strictEqual(result.size, 1);
    assert.ok(result.has('airport'));
  });
});
