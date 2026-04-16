import assert from 'node:assert';
import { describe, it } from 'node:test';

import { fsa } from '@chunkd/fs';
import { XMLParser } from 'fast-xml-parser';

import { lintDatasources } from '../action.lint.qgis.ts';

describe('action.lint.qgis', () => {
  describe('lintDatasources', () => {
    it('should pass for relative datasource paths', () => {
      const xml = {
        qgis: {
          layers: [
            { datasource: './buildings.parquet' },
            { datasource: './buildings.gpkg' },
            { datasource: './buildings.geojson' },
            { datasource: '../buildings.parquet' },
          ],
        },
      };
      const errors = lintDatasources(xml);
      assert.deepStrictEqual(errors, []);
    });

    it('should pass for relative datasource with piped metadata', () => {
      const xml = {
        qgis: {
          layers: [
            { datasource: './test.parquet|layername=testline' },
            { datasource: './test.gpkg|layername=testline' },
            { datasource: './test.geojson|layername=testline' },
            { datasource: '../test.parquet|layername=testline' },
          ],
        },
      };
      const errors = lintDatasources(xml);
      assert.deepStrictEqual(errors, []);
    });

    it('should error for absolute datasource path', () => {
      const xml = {
        qgis: {
          layers: [
            { datasource: '/data/buildings.parquet' },
            { datasource: '/data/buildings.gpkg' },
            { datasource: '/data/buildings.geojson' },
          ],
        },
      };
      const errors = lintDatasources(xml);
      assert.strictEqual(errors.length, 3);
    });

    it('should error for absolute datasource with piped metadata', () => {
      const xml = {
        qgis: {
          layers: [
            { datasource: '/data/test.parquet|layername=testline' },
            { datasource: '/data/test.gpkg|layername=testline' },
            { datasource: '/data/test.geojson|layername=testline' },
          ],
        },
      };
      const errors = lintDatasources(xml);
      assert.strictEqual(errors.length, 3);
    });

    it('should pass for WMS/WMTS connection string datasources', () => {
      const xml = {
        qgis: {
          layers: [
            {
              datasource:
                'contextualWMSLegend=0&crs=EPSG:2193&dpiMode=7&featureCount=10&format=image/webp&layers=topo-raster-gridded&styles=default&tileMatrixSet=NZTM2000Quad&tilePixelRatio=2&url=https://basemaps.linz.govt.nz/v1/tiles/topo-raster-gridded/NZTM2000Quad/WMTSCapabilities.xml?api%3Dc01kkyythn3e0sae5j6c8ahbed3',
            },
          ],
        },
      };
      const errors = lintDatasources(xml);
      assert.deepStrictEqual(errors, []);
    });

    it('should handle deeply nested datasources', () => {
      const xml = {
        a: { b: { c: { d: { datasource: '/deep.parquet' } } } },
      };
      const errors = lintDatasources(xml);
      assert.strictEqual(errors.length, 1);
      assert.ok(errors[0]?.includes('/deep.parquet'));
    });

    it('should lint beehive.qgs with no errors', async () => {
      const qgisFile = await fsa.read(new URL('../../../../map/assets/beehive.qgs', import.meta.url));
      const parser = new XMLParser();
      const qgisXml = parser.parse(qgisFile);
      const errors = lintDatasources(qgisXml);
      assert.deepStrictEqual(errors, []);
    });

    it('should lint topo-test.qgs with no errors', async () => {
      const qgisFile = await fsa.read(new URL('../../../../../e2e/assets/topo-test.qgs', import.meta.url));
      const parser = new XMLParser();
      const qgisXml = parser.parse(qgisFile);
      const errors = lintDatasources(qgisXml);
      assert.deepStrictEqual(errors, []);
    });
  });
});
