import assert from 'node:assert';
import { mkdir, rm } from 'node:fs/promises';
import { afterEach, beforeEach, describe, it } from 'node:test';

import { fsa } from '@chunkd/fs';

import type { ValidationConfig } from '../action.validate.ts';
import { buildValidationArgs, createFilteredConfig, parseBbox } from '../action.validate.ts';

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
    const testBasePath = '/tmp/test/validate-test';
    const testDbPath = `${testBasePath}/parquet`;
    const testConfigPath = `${testBasePath}/config.json`;

    const testConfig = {
      feature_not_on_layers: [
        {
          table: 'railway_station',
          intersection_table: 'railway_line',
          layername: 'stations-not-on-railway-line',
          message: 'Railway station point features must fall on a railway line',
        },
        {
          table: 'railway_station',
          intersection_table: 'missing_table',
          layername: 'stations-not-on-missing-table',
          message: 'This rule should be excluded in strict mode',
        },
        {
          table: 'missing_table_2',
          intersection_table: 'railway_line',
          layername: 'missing-table-not-on-railway-line',
          message: 'This rule should always be excluded because the main table is missing',
        },
        {
          table: 'railway_line',
          layername: 'railway-line-not-on-anything',
          message: 'This rule should always be included because the main table is present',
        },
      ],
      feature_in_layers: [
        {
          table: 'building_point',
          intersection_table: 'building',
          layername: 'building-points-in-building-polygons',
          message: 'Building point features must not fall within building polygon features',
        },
      ],
    };

    beforeEach(async () => {
      await rm(testBasePath, { recursive: true, force: true });
      await fsa.write(fsa.toUrl(`${testDbPath}/railway_station.parquet`), '');
      await fsa.write(fsa.toUrl(`${testDbPath}/railway_line.parquet`), '');
      await fsa.write(fsa.toUrl(testConfigPath), JSON.stringify(testConfig));
    });

    afterEach(async () => {
      await rm(testBasePath, { recursive: true, force: true });
    });

    it('should build basic args with default values', async () => {
      const args = {
        mode: 'generic',
        'db-path': `${testDbPath}/files.parquet`,
        'config-file': testConfigPath,
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
      assert.ok(result.includes(`${testDbPath}/files.parquet`));
      assert.ok(result.includes('--mode'));
      assert.ok(result.includes('generic'));
      assert.ok(result.includes('--config-file'));
      assert.ok(result.includes('/tmp/filtered_config.json'));
      assert.ok(result.includes('--output-dir'));
      assert.ok(result.includes('/tmp/output'));
      assert.ok(result.includes('--area-crs'));
      assert.ok(result.includes('2193'));
    });

    it('should handle postgis mode', async () => {
      const args = {
        mode: 'postgis',
        'db-path': `${testDbPath}/files.parquet`,
        'config-file': testConfigPath,
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
      assert.ok(result.includes(`${testDbPath}/files.parquet`));
      assert.ok(result.includes('--mode'));
      assert.ok(result.includes('postgis'));
      assert.ok(result.includes('--config-file'));
      assert.ok(result.includes('/tmp/filtered_config.json'));
      assert.ok(result.includes('--output-dir'));
      assert.ok(result.includes('/tmp/output'));
      assert.ok(result.includes('--area-crs'));
      assert.ok(result.includes('2193'));
    });

    it('should include boolean flags when true', async () => {
      const args = {
        mode: 'generic',
        'db-path': `${testDbPath}/files.parquet`,
        'config-file': testConfigPath,
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
        'db-path': `${testDbPath}/files.parquet`,
        'config-file': testConfigPath,
        'output-dir': `${testDbPath}/output/`,
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

    it('should throw error for invalid bbox (not 4 values)', async () => {
      const args = {
        mode: 'generic',
        'db-path': `${testDbPath}/files.parquet`,
        'config-file': testConfigPath,
        'output-dir': `${testDbPath}/output/`,
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

      await assert.rejects(async () => await buildValidationArgs(args), {
        message: 'bbox requires exactly 4 values: minX minY maxX maxY',
      });
    });

    it('should include date when provided', async () => {
      const args = {
        mode: 'generic',
        'db-path': `${testDbPath}/files.parquet`,
        'config-file': testConfigPath,
        'output-dir': `${testDbPath}/output/`,
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
        'db-path': `${testDbPath}/files.parquet`,
        'config-file': testConfigPath,
        'output-dir': `${testDbPath}/output/`,
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

    it('should handle different area-crs values', async () => {
      const args = {
        mode: 'generic',
        'db-path': `${testDbPath}/files.parquet`,
        'config-file': testConfigPath,
        'output-dir': `${testDbPath}/output/`,
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
      assert.ok(result.includes('generic'));
      assert.ok(result.includes('--area-crs'));
      assert.ok(result.includes('4326'));
    });

    it('should throw error when no parquet files found', async () => {
      const emptyDir = `${testBasePath}/empty`;
      await mkdir(emptyDir, { recursive: true });

      const args = {
        mode: 'generic',
        'db-path': `${emptyDir}/files.parquet`,
        'config-file': testConfigPath,
        'output-dir': `${testDbPath}/output/`,
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

      await assert.rejects(async () => await buildValidationArgs(args), {
        message: `No parquet files found in: ${args['db-path']}`,
      });
    });

    it('should create filtered config with only rules for existing tables', async () => {
      // Config has rules for:
      // - feature_not_on_layers: railway_station + railway_line (both exist)
      // - feature_not_on_layers: railway_station + missing_table (only one exists)
      // - feature_not_on_layers: missing_table_2 + railway_line (only one exists)
      // - feature_in_layers: building_point + building (neither exists)
      // Only railway_station.parquet and railway_line.parquet exist

      const args = {
        mode: 'generic',
        'db-path': `${testDbPath}/files.parquet`,
        'config-file': testConfigPath,
        'output-dir': `${testDbPath}/output/`,
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

      await buildValidationArgs(args);

      const filteredConfig: ValidationConfig = await fsa.readJson(fsa.toUrl('/tmp/filtered_config.json'));

      const featureNotOnLayers = filteredConfig['feature_not_on_layers'];
      assert.ok(featureNotOnLayers, 'feature_not_on_layers should exist');
      assert.strictEqual(featureNotOnLayers.length, 2);
      assert.strictEqual(featureNotOnLayers[0]?.table, 'railway_station');
      assert.strictEqual(featureNotOnLayers[0]?.intersection_table, 'railway_line');
      assert.strictEqual(featureNotOnLayers[1]?.table, 'railway_line');

      const featureInLayers = filteredConfig['feature_in_layers'];
      assert.ok(featureInLayers, 'feature_in_layers should exist');
      assert.strictEqual(featureInLayers.length, 0);
    });
  });

  describe('createFilteredConfig', () => {
    const testBasePath = '/tmp/test/validate-test';
    const testConfigPath = `${testBasePath}/config.json`;

    const testConfig: ValidationConfig = {
      ruleset: [
        {
          table: 'existing_table',
          intersection_table: 'existing_intersection',
          layername: 'valid-rule',
          message: 'This rule should be included',
        },
        {
          table: 'existing_table',
          intersection_table: 'missing_intersection',
          layername: 'invalid-rule-1',
          message: 'This rule should be excluded - missing intersection table',
        },
        {
          table: 'missing_table',
          intersection_table: 'missing_intersection',
          layername: 'invalid-rule-2',
          message: 'This rule should be excluded - missing main table',
        },
      ],
    };

    beforeEach(async () => {
      await rm(testBasePath, { recursive: true, force: true });
      await fsa.write(fsa.toUrl(testConfigPath), JSON.stringify(testConfig));
    });

    afterEach(async () => {
      await rm(testBasePath, { recursive: true, force: true });
    });
    it('should return config rules with only existing tables in strict mode', async () => {
      const availableLayers = new Set(['existing_table', 'existing_intersection']);
      const result = await createFilteredConfig(testConfigPath, availableLayers, true);
      const filteredConfig: ValidationConfig = await fsa.readJson(fsa.toUrl(result));

      const ruleset = filteredConfig['ruleset'];
      assert.ok(ruleset, 'ruleset should exist');
      assert.strictEqual(ruleset.length, 1);
      assert.strictEqual(ruleset[0]?.layername, 'valid-rule');
    });
    it('should return config rules with at least one existing table', async () => {
      const availableLayers = new Set(['existing_table', 'existing_intersection']);
      const result = await createFilteredConfig(testConfigPath, availableLayers, false);
      const filteredConfig: ValidationConfig = await fsa.readJson(fsa.toUrl(result));
      const ruleset = filteredConfig['ruleset'];
      assert.ok(ruleset, 'ruleset should exist');
      assert.strictEqual(ruleset.length, 2);
      assert.strictEqual(ruleset[0]?.layername, 'valid-rule');
    });
  });
});
