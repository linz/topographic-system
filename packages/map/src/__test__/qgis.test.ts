import assert from 'node:assert';
import { describe, it } from 'node:test';

import type { QgisLayerDef } from '@linzjs/topographic-system-shared';

import { getQgisMapSheetDataset } from '../qgis.ts';

describe('qgis', () => {
  describe('getQgisMapSheetLayer', () => {
    const layers = [
      { name: 'layer1', source: 'data1.parquet' },
      { name: 'layer2', source: 'my_map_sheet.parquet' },
      { name: 'layer3', source: 'data3.parquet' },
    ] as [QgisLayerDef, QgisLayerDef, QgisLayerDef];

    it('should find map sheet layer by name', () => {
      const result = getQgisMapSheetDataset(layers, 'data1');
      assert.deepEqual(result, layers[0]);
    });

    it('should throw if map sheet layer by name is not found', () => {
      assert.throws(() => getQgisMapSheetDataset(layers, 'data4'), /Map sheet source layer not found: "data4"/);
    });

    it('should find map sheet layer by source ending', () => {
      const result = getQgisMapSheetDataset(layers);
      assert.deepEqual(result, layers[1]);
    });

    it('should throw if no map sheet layer found by source ending', () => {
      assert.throws(
        () => getQgisMapSheetDataset([layers[0], layers[2]]),
        /No map sheet layer ending with "map_sheet.parquet" found/,
      );
    });

    it('should only select a map sheet layer with no query', () => {
      const layersWithQuery = [
        { name: 'layer1', source: 'data1.parquet', path: './data1.parquet', type: 'parquet' },
        {
          name: 'layer2',
          source: 'my_map_sheet.parquet',
          path: './my_map_sheet.parquet',
          type: 'parquet',
          options: [{ key: 'subset', value: 'some_query' }],
        },
        { name: 'layer4', source: 'my_map_sheet.parquet', path: './my_map_sheet.parquet', type: 'parquet' },
      ];

      assert.equal(getQgisMapSheetDataset(layersWithQuery)?.name, 'layer4');
    });

    it('should only select a map sheet layer with some options', () => {
      const layersWithQuery = [
        { name: 'layer1', source: 'data1.parquet', path: './data1.parquet', type: 'parquet' },
        {
          name: 'layer2',
          source: 'my_map_sheet.parquet',
          path: './my_map_sheet.parquet',
          type: 'parquet',
          options: [{ key: 'layername', value: 'some_layer' }],
        },
        { name: 'layer4', source: 'my_map_sheet.parquet', path: './my_map_sheet.parquet', type: 'parquet' },
      ];

      assert.equal(getQgisMapSheetDataset(layersWithQuery)?.name, 'layer2');
    });
  });
});
