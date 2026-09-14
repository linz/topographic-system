import assert from 'node:assert';
import { before, describe, it } from 'node:test';

import { fsa, FsMemory } from '@chunkd/fs';

import { getQgisProjectMeta, parseQgisLayerDef } from '../qgis.ts';

const BaseQgsProject = `
<qgis>
  <projectCrs>
    <spatialrefsys>
      <srid>2193</srid>
    </spatialrefsys>
  </projectCrs>
</qgis>
<layer-tree-group>
  <layer-tree-layer name="road_line 2 lane highway map" source="./road_line.parquet|subset=&quot;lane_count&quot; &gt; 1"></layer-tree-layer>
  <layer-tree-layer name="water" source="./water.parquet"></layer-tree-layer>
  <layer-tree-layer name="MapSheetLayer" source="./nztopo50_map_sheet.parquet"></layer-tree-layer>
  <layer-tree-layer name="CartoTextLayer" source="./nztopo50_carto_text.parquet"></layer-tree-layer>
</layer-tree-group>
`.trim();

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
        {
          name: 'road_line 2 lane highway map',
          source: 'road_line.parquet',
          path: './road_line.parquet',
          options: [{ key: 'subset', value: '&quot;lane_count&quot; &gt; 1' }],
          type: 'parquet',
        },
        { name: 'water', source: 'water.parquet', path: './water.parquet', options: [], type: 'parquet' },
        {
          name: 'MapSheetLayer',
          source: 'nztopo50_map_sheet.parquet',
          path: './nztopo50_map_sheet.parquet',
          options: [],
          type: 'parquet',
        },
        {
          name: 'CartoTextLayer',
          source: 'nztopo50_carto_text.parquet',
          path: './nztopo50_carto_text.parquet',
          options: [],
          type: 'parquet',
        },
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

  describe('parseQgisLayerDef', () => {
    it('should parse simple layer definition', () => {
      const def = parseQgisLayerDef('./buildings.parquet');
      assert.deepStrictEqual(def, {
        name: 'buildings',
        source: 'buildings.parquet',
        path: './buildings.parquet',
        type: 'parquet',
        options: [],
      });
    });

    it('should use provided layer name', () => {
      const def = parseQgisLayerDef('./buildings.parquet', 'Custom Buildings');
      assert.strictEqual(def?.name, 'Custom Buildings');
      assert.strictEqual(def?.source, 'buildings.parquet');
    });

    it('should parse layer with options and different extensions', () => {
      const def = parseQgisLayerDef('./road_line.gpkg|subset="lane_count" > 1|layername=road_line');
      assert.deepStrictEqual(def, {
        name: 'road_line',
        source: 'road_line.gpkg',
        path: './road_line.gpkg',
        type: 'gpkg',
        options: [
          { key: 'subset', value: '"lane_count" > 1' },
          { key: 'layername', value: 'road_line' },
        ],
      });
    });

    it('should return undefined when layer is null or undefined', () => {
      assert.strictEqual(parseQgisLayerDef(undefined), undefined);
    });
  });
});
