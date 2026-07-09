import assert from 'node:assert';
import { before, describe, it } from 'node:test';

import { fsa, FsMemory } from '@chunkd/fs';
import pLimit from 'p-limit';

import { Downloader } from '../download.ts';

describe('Downloader - Canonical URLs', () => {
  const mem = new FsMemory();

  before(() => {
    fsa.register('memory://', mem);
  });

  it('should hit canonical URL if useCanonical option is true', async () => {
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

    // 4. Test downloader with useCanonical: true
    const downloader = new Downloader(targetUrl, sourceCacheUrl, pLimit(1));
    downloader.addStac(initialUrl);

    const assets = await downloader.getAsset(initialUrl, { skipIfExists: false, useCanonical: true });

    // 5. Verify results
    assert.strictEqual(assets.length, 1);
    // It should have downloaded from canonicalUrl, so url of asset should be canonicalAssetUrl
    assert.strictEqual(assets[0]!.url.href, canonicalAssetUrl.href);

    // We can also verify that the link in target was created for the canonical asset
    const targetAssetPath = new URL('canonical-data.parquet', targetUrl);
    assert.ok(await fsa.exists(targetAssetPath));
    const targetContent = await fsa.read(targetAssetPath);
    assert.strictEqual(targetContent.toString(), fileContent);
  });

  it('should hit original URL if useCanonical option is false', async () => {
    const targetUrl = new URL('memory://target-original/');
    const sourceCacheUrl = new URL('memory://source-cache-original/');

    const initialUrl = new URL('memory://source/catalog.json');
    const initialAssetUrl = new URL('memory://source/initial-data.parquet');

    const downloader = new Downloader(targetUrl, sourceCacheUrl, pLimit(1));
    downloader.addStac(initialUrl);

    const assets = await downloader.getAsset(initialUrl, { skipIfExists: false, useCanonical: false });

    // It should have downloaded from initialUrl, so url of asset should be initialAssetUrl
    assert.strictEqual(assets.length, 1);
    assert.strictEqual(assets[0]!.url.href, initialAssetUrl.href);

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

    const downloader = new Downloader(targetUrl, sourceCacheUrl, pLimit(1));
    downloader.addStac(stacAUrl);

    await assert.rejects(downloader.getAsset(stacAUrl, { skipIfExists: false, useCanonical: true }), (err: Error) => {
      assert.ok(err.message.includes('Circular canonical link detected'));
      return true;
    });
  });

  it('should support multiple files pointing to the same canonical list without throwing or downloading twice', async (t) => {
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

    const downloader = new Downloader(targetUrl, sourceCacheUrl, pLimit(2));
    downloader.addStac(stacAUrl);
    downloader.addStac(stacBUrl);

    const spy = t.mock.method(fsa, 'readStream');

    const assets = await downloader.getAllAssets({ skipIfExists: false, useCanonical: true });

    // only one asset should be returned
    assert.strictEqual(assets.length, 1);
    assert.strictEqual(assets[0]!.url.href, canonicalAssetUrl.href);

    assert.deepEqual(
      spy.mock.calls.map((m) => m.arguments[0].href),
      [
        'memory://canonical-same-canonical/canonical-data.parquet',
        // writes the parquet into the cache
        'memory://source-cache-same-canonical/1220b94d27b9934d3e08a52e52d7da7dabfac484efe37a5380ee9088f7ace2efcde9_canonical-data.parquet',
      ],
    );
  });
});
