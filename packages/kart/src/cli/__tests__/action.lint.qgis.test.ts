import assert from 'node:assert';
import { describe, it } from 'node:test';

import { fsa, FsMemory } from '@chunkd/fs';

import { lint, LintRuleDataSources, LintRuleFontFamily, LintRuleSvgPath } from '../action.lint.qgis.ts';

describe('action.lint.qgis', () => {
  const ctx = { qgisPath: new URL(import.meta.url) };

  describe('lintDataSources', () => {
    it('should pass for relative datasource paths', async () => {
      const xml = {
        qgis: {
          layers: [
            { datasource: './buildings.parquet', provider: 'ogr' },
            { datasource: './buildings.gpkg', provider: 'ogr' },
            { datasource: './buildings.geojson', provider: 'ogr' },
            { datasource: '../buildings.parquet', provider: 'ogr' },
          ],
        },
      };
      const errors = await lint(xml, [LintRuleDataSources], ctx);
      assert.deepStrictEqual(errors, []);
    });

    it('should pass for relative datasource with piped metadata', async () => {
      const xml = {
        qgis: {
          layers: [
            { datasource: './test.parquet|layername=testline', provider: 'ogr' },
            { datasource: './test.gpkg|layername=testline', provider: 'ogr' },
            { datasource: './test.geojson|layername=testline', provider: 'ogr' },
            { datasource: '../test.parquet|layername=testline', provider: 'ogr' },
          ],
        },
      };
      const errors = await lint(xml, [LintRuleDataSources], ctx);
      assert.deepStrictEqual(errors, []);
    });

    it('should error for absolute datasource path', async () => {
      const xml = {
        qgis: {
          layers: [
            { datasource: '/data/buildings.parquet', provider: 'ogr' },
            { datasource: '/data/buildings.gpkg', provider: 'ogr' },
            { datasource: '/data/buildings.geojson', provider: 'ogr' },
          ],
        },
      };
      const errors = await lint(xml, [LintRuleDataSources], ctx);
      assert.strictEqual(errors.length, 3);
    });

    it('should error for url datasource path', async () => {
      const xml = {
        qgis: {
          layers: [
            { datasource: 'https://example.com/buildings.parquet?after=2025-12-01', provider: 'ogr' },
            { datasource: 'https://example.com/buildings.gpkg?after=2025-12-01', provider: 'ogr' },
            { datasource: 'https://example.com/buildings.geojson?after=2025-12-01', provider: 'ogr' },
          ],
        },
      };
      const errors = await lint(xml, [LintRuleDataSources], ctx);
      assert.strictEqual(errors.length, 3);
    });

    it('should error for absolute datasource with piped metadata', async () => {
      const xml = {
        qgis: {
          layers: [
            { datasource: '/data/test.parquet|layername=testline', provider: 'ogr' },
            { datasource: '/data/test.gpkg|layername=testline', provider: 'ogr' },
            { datasource: '/data/test.geojson|layername=testline', provider: 'ogr' },
          ],
        },
      };
      const errors = await lint(xml, [LintRuleDataSources], ctx);
      assert.strictEqual(errors.length, 3);
    });

    it('should skip WMS datasources', async () => {
      const xml = {
        qgis: {
          layers: [
            {
              datasource:
                'contextualWMSLegend=0&crs=EPSG:2193&dpiMode=7&featureCount=10&format=image/webp&layers=topo-raster-gridded&styles=default&tileMatrixSet=NZTM2000Quad&tilePixelRatio=2&url=https://basemaps.linz.govt.nz/v1/tiles/topo-raster-gridded/NZTM2000Quad/WMTSCapabilities.xml?api%3Dc01kkyythn3e0sae5j6c8ahbed3',
              provider: 'wms',
            },
          ],
        },
      };
      const errors = await lint(xml, [LintRuleDataSources], ctx);
      assert.deepStrictEqual(errors, []);
    });

    it('should handle deeply nested datasources', async () => {
      const xml = {
        a: { b: { c: { d: { datasource: '/deep.parquet', provider: 'ogr' } } } },
      };
      const errors = await lint(xml, [LintRuleDataSources], ctx);
      assert.strictEqual(errors.length, 1);
      assert.ok(errors[0]?.error.includes('/deep.parquet'));
    });

    it('should lint beehive.qgs with no errors', async () => {
      const qgisFile = await fsa.read(new URL('../../../../map/assets/project/beehive.qgs', import.meta.url));
      const errors = await lint(qgisFile, [LintRuleDataSources], ctx);
      assert.deepStrictEqual(errors, []);
    });

    it('should lint topo-test.qgs with no errors', async () => {
      const qgisFile = await fsa.read(new URL('../../../../../e2e/assets/topo-test.qgs', import.meta.url));
      const errors = await lint(qgisFile, [LintRuleDataSources], ctx);
      assert.deepStrictEqual(errors, []);
    });
  });

  describe('lintFontFamilies', () => {
    it('should lint beehive.qgs with no errors', async () => {
      const qgisFile = await fsa.read(new URL('../../../../map/assets/project/beehive.qgs', import.meta.url));
      const errors = await lint(qgisFile, [LintRuleFontFamily], ctx);
      assert.deepStrictEqual(errors, [
        {
          name: 'font-families',
          error: "Font family 'Nimbus Sans Narrow' is not allowed. Allowed fonts are: Nimbus Sans LINZ",
        },
      ]);
    });
  });

  describe('lintSvgPaths', () => {
    it('should pass when SVG file exists', async () => {
      const mem = new FsMemory();
      fsa.register('memory://', mem);
      await fsa.write(fsa.toUrl('memory:///project/svg/pattern.svg'), '<svg></svg>');

      const node = {
        '@_class': 'SvgFill',
        Option: {
          '@_type': 'Map',
          Option: [{ '@_name': 'svgFile', '@_type': 'QString', '@_value': './svg/pattern.svg' }],
        },
      };

      const errors = await lint(node, [LintRuleSvgPath], { qgisPath: fsa.toUrl('memory:///project/project.qgs') });
      assert.deepStrictEqual(errors, []);
    });

    it('should error when SVG file does not exist', async () => {
      const mem = new FsMemory();
      fsa.register('memory://', mem);

      const node = {
        '@_class': 'SvgFill',
        Option: {
          '@_type': 'Map',
          Option: [{ '@_name': 'svgFile', '@_type': 'QString', '@_value': './svg/missing.svg' }],
        },
      };

      const errors = await lint(node, [LintRuleSvgPath], { qgisPath: fsa.toUrl('memory:///project/project.qgs') });
      assert.deepStrictEqual(errors, [{ name: 'svg-path', error: 'SvgFill file does not exist: "./svg/missing.svg"' }]);
    });

    it('should pass for base64 embedded SVG fill', async () => {
      const node = {
        '@_class': 'SvgFill',
        Option: {
          '@_type': 'Map',
          Option: [{ '@_name': 'svgFile', '@_type': 'QString', '@_value': 'base64:PHN2Zz48L3N2Zz4=' }],
        },
      };

      const errors = await lint(node, [LintRuleSvgPath], ctx);
      assert.deepStrictEqual(errors, []);
    });

    it('should handle legacy QGIS prop tags', async () => {
      const mem = new FsMemory();
      fsa.register('memory://', mem);

      const node = {
        '@_class': 'SvgFill',
        prop: [{ '@_k': 'svgFile', '@_v': './svg/missing_prop.svg' }],
      };

      const errors = await lint(node, [LintRuleSvgPath], { qgisPath: fsa.toUrl('memory:///project/project.qgs') });
      assert.deepStrictEqual(errors, [
        { name: 'svg-path', error: 'SvgFill file does not exist: "./svg/missing_prop.svg"' },
      ]);
    });

    it('should ignore non-SvgFill layers', async () => {
      const node = {
        '@_class': 'SimpleFill',
        Option: {
          Option: [{ '@_name': 'color', '@_value': '255,0,0,255' }],
        },
      };

      const errors = await lint(node, [LintRuleSvgPath], ctx);
      assert.deepStrictEqual(errors, []);
    });

    it('should deduplicate errors across multiple layers', async () => {
      const mem = new FsMemory();
      fsa.register('memory://', mem);

      const layer1 = {
        '@_class': 'SVGFill',
        Option: { Option: [{ '@_name': 'svgFile', '@_value': './svg/missing.svg' }] },
      };
      const layer2 = {
        '@_class': 'SVGFill',
        Option: { Option: [{ '@_name': 'svgFile', '@_value': './svg/missing.svg' }] },
      };

      const xml = { qgis: { layers: [layer1, layer2] } };
      const errors = await lint(xml, [LintRuleSvgPath], { qgisPath: fsa.toUrl('memory:///project/project.qgs') });
      assert.deepStrictEqual(errors, [{ name: 'svg-path', error: 'SVGFill file does not exist: "./svg/missing.svg"' }]);
    });

    it('should error when SvgMarker SVG file does not exist', async () => {
      const mem = new FsMemory();
      fsa.register('memory://', mem);

      const node = {
        '@_class': 'SvgMarker',
        Option: {
          '@_type': 'Map',
          Option: [{ '@_name': 'name', '@_type': 'QString', '@_value': './svg/missing_marker.svg' }],
        },
      };

      const errors = await lint(node, [LintRuleSvgPath], { qgisPath: fsa.toUrl('memory:///project/project.qgs') });
      assert.deepStrictEqual(errors, [
        { name: 'svg-path', error: 'SvgMarker file does not exist: "./svg/missing_marker.svg"' },
      ]);
    });
  });
});
