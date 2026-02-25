import assert from 'node:assert';
import { describe, it } from 'node:test';

import { buildValidationArgs, parseBbox } from '../action.validate.ts';

describe('action.validate', () => {
  describe('parseBbox', () => {
    it('should parse comma-separated bbox', () => {
      const result = parseBbox('1,2,3,4');
      assert.deepStrictEqual(result, ['1', '2', '3', '4']);
    });

    it('should parse space-separated bbox', () => {
      const result = parseBbox('1 2 3 4');
      assert.deepStrictEqual(result, ['1', '2', '3', '4']);
    });

    it('should parse mixed separator bbox', () => {
      const result = parseBbox('1,2 3,4');
      assert.deepStrictEqual(result, ['1', '2', '3', '4']);
    });

    it('should return empty array for undefined input', () => {
      const result = parseBbox(undefined);
      assert.deepStrictEqual(result, []);
    });

    it('should return empty array for empty string', () => {
      const result = parseBbox('');
      assert.deepStrictEqual(result, []);
    });
  });

  describe('buildValidationArgs', () => {
    it('should build basic args with default values', async () => {
      const args = {
        mode: 'generic',
        'db-path': '/tmp/test.parquet',
        'config-file': '/config.json',
        'output-dir': '/tmp/output',
        'area-crs': 2193,
        'export-parquet': false,
        'export-parquet-by-geometry': false,
        'no-export-gpkg': false,
        'use-date-folder': false,
        'report-only': false,
        'skip-queries': false,
        'skip-features-on-layer': false,
        'skip-self-intersections': false,
        verbose: false,
        bbox: undefined,
        date: undefined,
        weeks: undefined,
      };

      const result = await buildValidationArgs(args);

      assert.ok(result.includes('--db-path'));
      assert.ok(result.includes('/tmp/test.parquet'));
      assert.ok(result.includes('--mode'));
      assert.ok(result.includes('generic'));
      assert.ok(result.includes('--config-file'));
      assert.ok(result.includes('/config.json'));
      assert.ok(result.includes('--output-dir'));
      assert.ok(result.includes('/tmp/output'));
      assert.ok(result.includes('--area-crs'));
      assert.ok(result.includes('2193'));
    });

    it('should include boolean flags when true', async () => {
      const args = {
        mode: 'generic',
        'db-path': '/tmp/test.parquet',
        'config-file': '/config.json',
        'output-dir': '/tmp/output',
        'area-crs': 2193,
        'export-parquet': true,
        'export-parquet-by-geometry': true,
        'no-export-gpkg': true,
        'use-date-folder': false,
        'report-only': true,
        'skip-queries': false,
        'skip-features-on-layer': true,
        'skip-self-intersections': false,
        verbose: true,
        bbox: undefined,
        date: undefined,
        weeks: undefined,
      };

      const result = await buildValidationArgs(args);

      assert.ok(result.includes('--export-parquet'));
      assert.ok(result.includes('--export-parquet-by-geometry'));
      assert.ok(result.includes('--no-export-gpkg'));
      assert.ok(!result.includes('--use-date-folder'));
      assert.ok(result.includes('--report-only'));
      assert.ok(!result.includes('--skip-queries'));
      assert.ok(result.includes('--skip-features-on-layer'));
      assert.ok(!result.includes('--skip-self-intersections'));
      assert.ok(result.includes('--verbose'));
    });

    it('should include bbox when valid 4 values provided', async () => {
      const args = {
        mode: 'generic',
        'db-path': '/tmp/test.parquet',
        'config-file': undefined,
        'output-dir': undefined,
        'area-crs': 2193,
        'export-parquet': false,
        'export-parquet-by-geometry': false,
        'no-export-gpkg': false,
        'use-date-folder': false,
        'report-only': false,
        'skip-queries': false,
        'skip-features-on-layer': false,
        'skip-self-intersections': false,
        verbose: false,
        bbox: '1,2,3,4',
        date: undefined,
        weeks: undefined,
      };

      const result = await buildValidationArgs(args);

      assert.ok(result.includes('--bbox'));
      const bboxIndex = result.indexOf('--bbox');
      assert.strictEqual(result[bboxIndex + 1], '1');
      assert.strictEqual(result[bboxIndex + 2], '2');
      assert.strictEqual(result[bboxIndex + 3], '3');
      assert.strictEqual(result[bboxIndex + 4], '4');
    });

    it('should throw error for invalid bbox (not 4 values)', () => {
      const args = {
        mode: 'generic',
        'db-path': '/tmp/test.parquet',
        'config-file': undefined,
        'output-dir': undefined,
        'area-crs': 2193,
        'export-parquet': false,
        'export-parquet-by-geometry': false,
        'no-export-gpkg': false,
        'use-date-folder': false,
        'report-only': false,
        'skip-queries': false,
        'skip-features-on-layer': false,
        'skip-self-intersections': false,
        verbose: false,
        bbox: '1,2,3',
        date: undefined,
        weeks: undefined,
      };

      assert.throws(async () => await buildValidationArgs(args), {
        message: 'bbox requires exactly 4 values: minX minY maxX maxY',
      });
    });

    it('should include date when provided', async () => {
      const args = {
        mode: 'generic',
        'db-path': '/tmp/test.parquet',
        'config-file': undefined,
        'output-dir': undefined,
        'area-crs': 2193,
        'export-parquet': false,
        'export-parquet-by-geometry': false,
        'no-export-gpkg': false,
        'use-date-folder': false,
        'report-only': false,
        'skip-queries': false,
        'skip-features-on-layer': false,
        'skip-self-intersections': false,
        verbose: false,
        bbox: undefined,
        date: '2025-01-15',
        weeks: undefined,
      };

      const result = await buildValidationArgs(args);

      assert.ok(result.includes('--date'));
      assert.ok(result.includes('2025-01-15'));
    });

    it('should include weeks when provided', async () => {
      const args = {
        mode: 'generic',
        'db-path': '/tmp/test.parquet',
        'config-file': undefined,
        'output-dir': undefined,
        'area-crs': 2193,
        'export-parquet': false,
        'export-parquet-by-geometry': false,
        'no-export-gpkg': false,
        'use-date-folder': false,
        'report-only': false,
        'skip-queries': false,
        'skip-features-on-layer': false,
        'skip-self-intersections': false,
        verbose: false,
        bbox: undefined,
        date: undefined,
        weeks: 4,
      };

      const result = await buildValidationArgs(args);

      assert.ok(result.includes('--weeks'));
      assert.ok(result.includes('4'));
    });

    it('should handle postgis mode', async () => {
      const args = {
        mode: 'postgis',
        'db-path': 'postgresql://localhost/testdb',
        'config-file': undefined,
        'output-dir': undefined,
        'area-crs': 4326,
        'export-parquet': false,
        'export-parquet-by-geometry': false,
        'no-export-gpkg': false,
        'use-date-folder': false,
        'report-only': false,
        'skip-queries': false,
        'skip-features-on-layer': false,
        'skip-self-intersections': false,
        verbose: false,
        bbox: undefined,
        date: undefined,
        weeks: undefined,
      };

      const result = await buildValidationArgs(args);

      assert.ok(result.includes('--mode'));
      assert.ok(result.includes('postgis'));
      assert.ok(result.includes('--db-path'));
      assert.ok(result.includes('postgresql://localhost/testdb'));
      assert.ok(result.includes('--area-crs'));
      assert.ok(result.includes('4326'));
    });
  });
});
