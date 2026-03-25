import assert from 'node:assert';
import { describe, it } from 'node:test';

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
});
