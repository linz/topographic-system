import assert from 'node:assert';
import { describe, it } from 'node:test';

import { parseStrategy } from '../parser.ts';
import { StacPusher } from '../stac.pusher.ts';
import { StacStorage } from '../stac.storage.ts';

describe('stac.storage', async () => {
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
      assert.deepEqual(parseStrategy('date=2026-05-19T22:18:14.595Z'), [
        { type: 'date', date: new Date('2026-05-19T22:18:14.595Z') },
      ]);
    });

    it('should parse a path-safe date strategy', () => {
      assert.deepEqual(parseStrategy('date=2026-05-19T22-18-14.595Z'), [
        { type: 'date', date: new Date('2026-05-19T22:18:14.595Z') },
      ]);
    });

    it('should throw for empty date strategy', () => {
      assert.throws(() => parseStrategy('date='), /Invalid date/);
    });

    it('should throw for missing date strategy value', () => {
      assert.throws(() => parseStrategy('date'), /Invalid date/);
    });
  });
});
