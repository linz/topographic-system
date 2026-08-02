import assert from 'node:assert';
import { rm } from 'node:fs/promises';
import { beforeEach, describe, it } from 'node:test';

import { fsa } from '@chunkd/fs';

import { getCanonical } from '../url.ts';

describe('getCanonical', () => {
  const testBasePath = '/tmp/test/url-get-canonical-test/';
  const testBaseUrl = fsa.toUrl(testBasePath);
  const collectionUrl = new URL('source/collection.json', testBaseUrl);

  const writeCollection = (url: URL, links: { rel: string; href: string }[]): Promise<void> =>
    fsa.write(url, JSON.stringify({ type: 'Collection', id: 'test', links }));

  beforeEach(async () => {
    await rm(testBasePath, { recursive: true, force: true });
  });

  it('should return the canonical url when an absolute canonical link exists', async () => {
    const canonical = 'https://example.com/canonical/collection.json';
    await writeCollection(collectionUrl, [
      { rel: 'self', href: collectionUrl.href },
      { rel: 'canonical', href: canonical },
    ]);

    const result = await getCanonical(collectionUrl);
    assert.strictEqual(result.href, canonical);
  });

  it('should resolve a relative canonical link against the collection url', async () => {
    await writeCollection(collectionUrl, [{ rel: 'canonical', href: '../canonical/collection.json' }]);

    const result = await getCanonical(collectionUrl);
    assert.strictEqual(result.href, new URL('canonical/collection.json', testBaseUrl).href);
  });

  it('should return the original url when no canonical link exists', async () => {
    await writeCollection(collectionUrl, [{ rel: 'self', href: collectionUrl.href }]);

    const result = await getCanonical(collectionUrl);
    assert.strictEqual(result.href, collectionUrl.href);
  });

  it('should return the original url when there are no links', async () => {
    await writeCollection(collectionUrl, []);

    const result = await getCanonical(collectionUrl);
    assert.strictEqual(result.href, collectionUrl.href);
  });
});
