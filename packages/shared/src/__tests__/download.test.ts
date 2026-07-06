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
});
