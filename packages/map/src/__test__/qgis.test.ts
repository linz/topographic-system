import assert from 'node:assert';
import { before, describe, it } from 'node:test';

import { fsa, FsMemory } from '@chunkd/fs';

import { getQgisMapSheetLayer, getQgisProjectMeta } from '../qgis.ts';
import { BaseQgsProject } from './util.ts';

describe('qgis', () => {
  const mem = new FsMemory();

  before(() => {
    fsa.register('memory://', mem);
  });

  describe('getQgisProjectMeta', () => {
    it('should parse a qgis project file', async () => {
      const qgsUrl = fsa.toUrl('memory://test/project.qgs');
      await fsa.write(qgsUrl, BaseQgsProject);

      const meta = await getQgisProjectMeta(qgsUrl);

      assert.equal(meta.epsg.code, 2193);
      assert.deepEqual(meta.layers, [
        { name: 'road_line 2 lane highway map', source: 'road_line.parquet' },
        { name: 'water', source: 'water.parquet' },
        { name: 'MapSheetLayer', source: 'nztopo50_map_sheet.parquet' },
      ]);
    });

    it('should throw if no qgis node', async () => {
      const qgsUrl = fsa.toUrl('memory://test/bad_project1.qgs');
      await fsa.write(qgsUrl, '<foo></foo>');
      await assert.rejects(getQgisProjectMeta(qgsUrl), /Failed to parse QGIS project/);
    });

    it('should throw if no projectCrs srid', async () => {
      const qgsUrl = fsa.toUrl('memory://test/bad_project2.qgs');
      await fsa.write(qgsUrl, '<qgis></qgis>');
      await assert.rejects(getQgisProjectMeta(qgsUrl), /Failed to parse projection from project/);
    });
  });

  describe('getQgisMapSheetLayer', () => {
    const layers = [
      { name: 'layer1', source: 'data1.parquet' },
      { name: 'layer2', source: 'my_map_sheet.parquet' },
      { name: 'layer3', source: 'data3.parquet' },
    ];

    it('should find map sheet layer by name', () => {
      const result = getQgisMapSheetLayer(layers, 'layer1');
      assert.deepEqual(result, layers[0]);
    });

    it('should throw if map sheet layer by name is not found', () => {
      assert.throws(() => getQgisMapSheetLayer(layers, 'layer4'), /Mapsheet layer not found: "layer4"/);
    });

    it('should find map sheet layer by source ending', () => {
      const result = getQgisMapSheetLayer(layers);
      assert.deepEqual(result, layers[1]);
    });

    it('should throw if no map sheet layer found by source ending', () => {
      assert.throws(
        () => getQgisMapSheetLayer([layers[0]!, layers[2]!]),
        /No map sheet layer ending with "map_sheet.parquet" found/,
      );
    });
  });
});
