import assert from 'node:assert';
import { before, describe, it } from 'node:test';

import { fsa, FsMemory } from '@chunkd/fs';

import { parseStrategy } from '../parser.ts';
import { StacPusher } from '../stac.pusher.ts';
import type { StorageContext } from '../stac.storage.ts';
import { StacStorage, storageStrategyFromLatest } from '../stac.storage.ts';

describe('stac.storage', async () => {
  before(() => {
    fsa.register('memory://', new FsMemory());
  });

  const latest = { type: 'latest' } as const;
  const commit = { type: 'commit', commit: 'commit' } as const;
  const prefix = new URL('memory://target/bucket/');

  describe('id', () => {
    for (const s of [latest, commit]) {
      it(`should generate ${s.type} ids`, () => {
        assert.equal(StacStorage.id(s, { prefix, category: 'data', label: 'airport' }), `data_airport_${s.type}`);
        assert.equal(StacStorage.id(s, { prefix, category: 'qgis', label: 'nztopo50' }), `qgis_nztopo50_${s.type}`);
        assert.equal(
          StacStorage.id(s, { prefix, category: 'product', label: 'nztopo50' }),
          `product_nztopo50_${s.type}`,
        );
        assert.equal(
          StacStorage.id(s, { prefix, category: 'product', label: 'nztopo50', item: 'BQ27' }),
          `product_nztopo50_${s.type}-BQ27`,
        );
      });
    }
  });

  describe('strategies', () => {
    it('should sort the strategies by priority', () => {
      const pusher = new StacPusher(new URL('memory://target/bucket/'), 'data');
      pusher.strategy({ type: 'latest' });
      assert.deepEqual(pusher.strategies, [{ type: 'latest' }]);
      pusher.strategy({ type: 'date', date: new Date('2024-01-01') });
      assert.deepEqual(pusher.strategies, [{ type: 'date', date: new Date('2024-01-01') }, { type: 'latest' }]);
      pusher.strategy({ type: 'commit', commit: 'abc' });
      assert.deepEqual(pusher.strategies, [
        { type: 'date', date: new Date('2024-01-01') },
        { type: 'commit', commit: 'abc' },
        { type: 'latest' },
      ]);
    });

    it('should parse an ISO date strategy', () => {
      assert.deepEqual(parseStrategy('date=2026-05-19T22:18:14.595Z'), {
        type: 'date',
        date: new Date('2026-05-19T22:18:14.595Z'),
      });
    });

    it('should parse a path-safe date strategy', () => {
      assert.deepEqual(parseStrategy('date=2026-05-19T22-18-14.595Z'), {
        type: 'date',
        date: new Date('2026-05-19T22:18:14.595Z'),
      });
    });

    it('should throw for empty date strategy', () => {
      assert.throws(() => parseStrategy('date='), /Invalid date/);
    });

    it('should throw for missing date strategy value', () => {
      assert.throws(() => parseStrategy('date'), /Invalid date/);
    });
  });

  describe('storageStrategyFromLatest', () => {
    it('should round-trip StorageContext through StacStorage.url and storageStrategyFromLatest', () => {
      const contexts: StorageContext[] = [
        { prefix: new URL('https://d1jzh93b1t1cv.cloudfront.net/'), category: 'data', label: 'airport' },
        { prefix: new URL('https://example.com/stac/'), category: 'qgis', label: 'nztopo50' },
        { prefix: new URL('memory://target/bucket/'), category: 'product', label: 'nztopo50' },
      ];

      for (const ctx of contexts) {
        const latestUrl = StacStorage.url({ type: 'latest' }, ctx);
        const parsedCtx = storageStrategyFromLatest(latestUrl);

        assert.ok(parsedCtx != null);
        assert.strictEqual(parsedCtx.label, ctx.label);
        assert.strictEqual(parsedCtx.category, ctx.category);
        assert.strictEqual(parsedCtx.prefix.href, ctx.prefix.href);
      }
    });

    it('should round-trip when URL points to a file inside latest directory', () => {
      const ctx: StorageContext = {
        prefix: new URL('https://d1jzh93b1t1cv.cloudfront.net/'),
        category: 'data',
        label: 'airport',
      };
      const latestFolderUrl = StacStorage.url({ type: 'latest' }, ctx);
      const collectionJsonUrl = new URL('collection.json', latestFolderUrl);

      const parsedCtx = storageStrategyFromLatest(collectionJsonUrl);

      assert.ok(parsedCtx != null);
      assert.strictEqual(parsedCtx.label, ctx.label);
      assert.strictEqual(parsedCtx.category, ctx.category);
      assert.strictEqual(parsedCtx.prefix.href, ctx.prefix.href);
    });

    it('should return null for URLs that do not contain /latest/', () => {
      const commitUrl = new URL(
        'https://d1jzh93b1t1cv.cloudfront.net/data/airport/commit_prefix=a/commit=123/collection.json',
      );
      assert.strictEqual(storageStrategyFromLatest(commitUrl), null);
    });
  });
});
