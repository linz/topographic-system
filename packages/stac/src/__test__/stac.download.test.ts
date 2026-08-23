import assert from 'node:assert';
import { before, describe, it } from 'node:test';

import { fsa, FsMemory } from '@chunkd/fs';
import pLimit from 'p-limit';

import { StacDownloader } from '../stac.downloader.ts';
import type { StacLruCache } from '../stac.lru.ts';

describe('Downloader - Canonical URLs', () => {
  const mem = new FsMemory();

  before(() => {
    fsa.register('memory://', mem);
    fsa.register('https://example.com/', mem);
  });

  it('should hit canonical URL if canonical resolver is active', async () => {
    // 1. Setup paths
    const targetUrl = new URL('memory://target/');
    const sourceCacheUrl = new URL('memory://source-cache/');

    const initialUrl = new URL('memory://source/catalog.json');
    const canonicalUrl = new URL('memory://canonical-source/catalog.json');

    // 2. Setup mock STAC catalogs/collections
    const initialStac = {
      type: 'Collection',
      id: 'initial',
      links: [{ rel: 'canonical', href: 'memory://canonical-source/catalog.json' }],
      assets: {
        data: {
          href: './initial-data.parquet',
          'file:checksum': '1220b94d27b9934d3e08a52e52d7da7dabfac484efe37a5380ee9088f7ace2efcde9',
          'file:size': 11,
        },
      },
    };

    const canonicalStac = {
      type: 'Collection',
      id: 'canonical',
      links: [],
      assets: {
        data: {
          href: './canonical-data.parquet',
          'file:checksum': '1220b94d27b9934d3e08a52e52d7da7dabfac484efe37a5380ee9088f7ace2efcde9',
          'file:size': 11,
        },
      },
    };

    // 3. Write files to FsMemory
    await fsa.write(initialUrl, JSON.stringify(initialStac));
    await fsa.write(canonicalUrl, JSON.stringify(canonicalStac));

    // Asset files content ('hello world' has size 11, sha256 multihash 1220b94...)
    const fileContent = 'hello world';
    const initialAssetUrl = new URL('memory://source/initial-data.parquet');
    const canonicalAssetUrl = new URL('memory://canonical-source/canonical-data.parquet');

    await fsa.write(initialAssetUrl, fileContent);
    await fsa.write(canonicalAssetUrl, fileContent);

    // 4. Test downloader with default canonical resolver
    const downloader = new StacDownloader({ target: targetUrl, cache: sourceCacheUrl, q: pLimit(1) });

    const assets = await downloader.fetchAssets(initialUrl);

    // 5. Verify results
    assert.strictEqual(assets.length, 1);
    // It should have downloaded from canonicalUrl, so source of asset should be canonicalAssetUrl
    assert.strictEqual(assets[0]?.source.href, canonicalAssetUrl.href);

    // We can also verify that the link in target was created for the canonical asset
    const targetAssetPath = new URL('canonical-data.parquet', targetUrl);
    assert.ok(await fsa.exists(targetAssetPath));
    const targetContent = await fsa.read(targetAssetPath);
    assert.strictEqual(targetContent.toString(), fileContent);
  });

  it('should hit original URL if canonical resolver is removed', async () => {
    const targetUrl = new URL('memory://target-original/');
    const sourceCacheUrl = new URL('memory://source-cache-original/');

    const initialUrl = new URL('memory://source/catalog.json');
    const initialAssetUrl = new URL('memory://source/initial-data.parquet');

    const downloader = new StacDownloader({ target: targetUrl, cache: sourceCacheUrl, q: pLimit(1) });
    downloader.resolvers = [];

    const assets = await downloader.fetchAssets(initialUrl);

    // It should have downloaded from initialUrl, so source of asset should be initialAssetUrl
    assert.strictEqual(assets.length, 1);
    assert.strictEqual(assets[0]?.source.href, initialAssetUrl.href);

    const targetAssetPath = new URL('initial-data.parquet', targetUrl);
    assert.ok(await fsa.exists(targetAssetPath));
    const targetContent = await fsa.read(targetAssetPath);
    assert.strictEqual(targetContent.toString(), 'hello world');
  });

  it('should detect circular canonical links and throw an error', async () => {
    const targetUrl = new URL('memory://target-circular/');
    const sourceCacheUrl = new URL('memory://source-cache-circular/');

    const stacAUrl = new URL('memory://source-circular/stacA.json');
    const stacBUrl = new URL('memory://source-circular/stacB.json');

    const stacA = {
      type: 'Collection',
      id: 'stacA',
      links: [{ rel: 'canonical', href: 'memory://source-circular/stacB.json' }],
      assets: {},
    };

    const stacB = {
      type: 'Collection',
      id: 'stacB',
      links: [{ rel: 'canonical', href: 'memory://source-circular/stacA.json' }],
      assets: {},
    };

    await fsa.write(stacAUrl, JSON.stringify(stacA));
    await fsa.write(stacBUrl, JSON.stringify(stacB));

    const downloader = new StacDownloader({ target: targetUrl, cache: sourceCacheUrl, q: pLimit(1) });

    await assert.rejects(downloader.fetchAssets(stacAUrl), (err: Error) => {
      assert.ok(err.message.includes('Circular canonical link detected'));
      return true;
    });
  });

  it('should support multiple files pointing to the same canonical list without downloading twice', async (t) => {
    const targetUrl = new URL('memory://target-same-canonical/');
    const sourceCacheUrl = new URL('memory://source-cache-same-canonical/');

    const stacAUrl = new URL('memory://source-same-canonical/stacA.json');
    const stacBUrl = new URL('memory://source-same-canonical/stacB.json');
    const canonicalUrl = new URL('memory://canonical-same-canonical/catalog.json');

    const stacA = {
      type: 'Collection',
      id: 'stacA',
      links: [{ rel: 'canonical', href: 'memory://canonical-same-canonical/catalog.json' }],
      assets: {},
    };

    const stacB = {
      type: 'Collection',
      id: 'stacB',
      links: [{ rel: 'canonical', href: 'memory://canonical-same-canonical/catalog.json' }],
      assets: {},
    };

    const canonicalStac = {
      type: 'Collection',
      id: 'canonical',
      links: [],
      assets: {
        data: {
          href: './canonical-data.parquet',
          'file:checksum': '1220b94d27b9934d3e08a52e52d7da7dabfac484efe37a5380ee9088f7ace2efcde9',
          'file:size': 11,
        },
      },
    };

    await fsa.write(stacAUrl, JSON.stringify(stacA));
    await fsa.write(stacBUrl, JSON.stringify(stacB));
    await fsa.write(canonicalUrl, JSON.stringify(canonicalStac));

    const fileContent = 'hello world';
    const canonicalAssetUrl = new URL('memory://canonical-same-canonical/canonical-data.parquet');
    await fsa.write(canonicalAssetUrl, fileContent);

    const downloader = new StacDownloader({ target: targetUrl, cache: sourceCacheUrl, q: pLimit(2) });

    const spy = t.mock.method(fsa, 'readStream');

    const assetsA = await downloader.fetchAssets(stacAUrl);
    const assetsB = await downloader.fetchAssets(stacBUrl);

    assert.strictEqual(assetsA.length, 1);
    assert.strictEqual(assetsB.length, 1);
    assert.strictEqual(assetsA[0]?.source.href, canonicalAssetUrl.href);
    assert.strictEqual(assetsB[0]?.source.href, canonicalAssetUrl.href);

    assert.deepEqual(
      spy.mock.calls.map((m) => m.arguments[0].href),
      [
        'memory://canonical-same-canonical/canonical-data.parquet',
        'memory://source-cache-same-canonical/1220b94d27b9934d3e08a52e52d7da7dabfac484efe37a5380ee9088f7ace2efcde9_canonical-data.parquet',
      ],
    );
  });

  it('should return the canonical url when an absolute canonical link exists', async () => {
    const collectionUrl = new URL('memory://source-canonical-abs/collection.json');
    const canonical = 'https://example.com/canonical/collection.json';
    await fsa.write(
      collectionUrl,
      JSON.stringify({ type: 'Collection', id: 'test', links: [{ rel: 'canonical', href: canonical }] }),
    );
    await fsa.write(new URL(canonical), JSON.stringify({ type: 'Collection', id: 'canonical-abs', links: [] }));

    const downloader = new StacDownloader({
      target: new URL('memory://target/'),
      cache: new URL('memory://cache/'),
      q: pLimit(1),
    });
    const resolved = await downloader.resolveUrl(collectionUrl);
    assert.strictEqual(resolved.href, canonical);
  });

  it('should resolve a relative canonical link against the collection url', async () => {
    const collectionUrl = new URL('memory://source-canonical-rel/source/collection.json');
    const canonicalUrl = new URL('memory://source-canonical-rel/canonical/collection.json');
    await fsa.write(
      collectionUrl,
      JSON.stringify({
        type: 'Collection',
        id: 'test',
        links: [{ rel: 'canonical', href: '../canonical/collection.json' }],
      }),
    );
    await fsa.write(canonicalUrl, JSON.stringify({ type: 'Collection', id: 'canonical-rel', links: [] }));

    const downloader = new StacDownloader({
      target: new URL('memory://target/'),
      cache: new URL('memory://cache/'),
      q: pLimit(1),
    });
    const resolved = await downloader.resolveUrl(collectionUrl);
    assert.strictEqual(resolved.href, 'memory://source-canonical-rel/canonical/collection.json');
  });

  it('should return the original url when no canonical link exists', async () => {
    const collectionUrl = new URL('memory://source-no-canonical/collection.json');
    await fsa.write(
      collectionUrl,
      JSON.stringify({ type: 'Collection', id: 'test', links: [{ rel: 'self', href: collectionUrl.href }] }),
    );

    const downloader = new StacDownloader({
      target: new URL('memory://target/'),
      cache: new URL('memory://cache/'),
      q: pLimit(1),
    });
    const resolved = await downloader.resolveUrl(collectionUrl);
    assert.strictEqual(resolved.href, collectionUrl.href);
  });

  it('should return the original url when there are no links', async () => {
    const collectionUrl = new URL('memory://source-empty-links/collection.json');
    await fsa.write(collectionUrl, JSON.stringify({ type: 'Collection', id: 'test', links: [] }));

    const downloader = new StacDownloader({
      target: new URL('memory://target/'),
      cache: new URL('memory://cache/'),
      q: pLimit(1),
    });
    const resolved = await downloader.resolveUrl(collectionUrl);
    assert.strictEqual(resolved.href, collectionUrl.href);
  });
});

describe('Downloader - Resolver Support', () => {
  it('should resolve URL via custom resolver if match exists', async () => {
    const originalUrl = new URL('memory://stac/data/airport/latest/collection.json');
    const overrideUrl = new URL('memory://stac/data/airport/commit=123/collection.json');

    await fsa.write(originalUrl, JSON.stringify({ type: 'Collection', id: 'airport-latest', links: [] }));
    await fsa.write(overrideUrl, JSON.stringify({ type: 'Collection', id: 'airport-commit', links: [] }));
    const resolver = {
      name: 'custom',
      resolve: async (_downloader: StacLruCache, url: URL) => (url.href === originalUrl.href ? overrideUrl : url),
    };

    const downloader = new StacDownloader({
      target: new URL('memory://target/'),
      cache: new URL('memory://cache/'),
      q: pLimit(1),
    });
    downloader.resolvers.push(resolver);

    const resolved = await downloader.resolveUrl(originalUrl);
    assert.strictEqual(resolved.href, overrideUrl.href);

    assert.deepEqual(downloader.resolutionStats.get('custom'), { name: 'custom', invokes: 1, resolves: 1 });
  });

  it('should return original URL if resolver makes no changes', async () => {
    const originalUrl = new URL('memory://stac/data/coastline/latest/collection.json');

    await fsa.write(originalUrl, JSON.stringify({ type: 'Collection', id: 'coastline-latest', links: [] }));

    const resolver = { name: 'custom', resolve: async (_downloader: StacLruCache, url: URL) => url };

    const downloader = new StacDownloader({
      target: new URL('memory://target/'),
      cache: new URL('memory://cache/'),
      q: pLimit(1),
    });
    downloader.resolvers.push(resolver);

    const resolved = await downloader.resolveUrl(originalUrl);
    assert.strictEqual(resolved.href, originalUrl.href);
    assert.deepEqual(downloader.resolutionStats.get('custom'), { name: 'custom', invokes: 1, resolves: 0 });
  });
});
