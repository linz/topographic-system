import assert from 'node:assert';
import { describe, it } from 'node:test';

import { lintDatasources } from '../action.lint.qgis.ts';

describe('action.lint.qgis', () => {
  describe('lintDatasources', () => {
    it('should pass for relative parquet datasource paths', () => {
      const xml = {
        qgis: {
          datasource: './buildings.parquet',
        },
      };
      const errors = lintDatasources(xml);
      assert.deepStrictEqual(errors, []);
    });

    it('should pass for non-parquet datasources with absolute paths', () => {
      const xml = {
        qgis: {
          datasource: '/data/layer.gpkg',
        },
      };
      const errors = lintDatasources(xml);
      assert.deepStrictEqual(errors, []);
    });

    it('should error for absolute parquet datasource path', () => {
      const xml = {
        qgis: {
          datasource: '/data/buildings.parquet',
        },
      };
      const errors = lintDatasources(xml);
      assert.strictEqual(errors.length, 1);
    });

    it('should collect multiple errors across layers', () => {
      const xml = {
        qgis: {
          layers: [
            { datasource: '/abs/roads.parquet' },
            { datasource: './ok.parquet' },
            { datasource: 'no-prefix.parquet' },
          ],
        },
      };
      const errors = lintDatasources(xml);
      assert.strictEqual(errors.length, 2);
    });

    it('should handle deeply nested datasources', () => {
      const xml = {
        a: { b: { c: { d: { datasource: 'deep.parquet' } } } },
      };
      const errors = lintDatasources(xml);
      assert.strictEqual(errors.length, 1);
      assert.ok(errors[0]?.includes('deep.parquet'));
    });
  });
});
