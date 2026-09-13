import assert from 'node:assert';
import { before, describe, it } from 'node:test';

import { fsa, FsMemory } from '@chunkd/fs';

import { getQgisProjectMeta } from '../qgis.ts';

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
          options: [{ key: 'subset', value: '&quot;lane_count&quot; &gt; 1' }],
        },
        { name: 'water', source: 'water.parquet', options: [] },
        { name: 'MapSheetLayer', source: 'nztopo50_map_sheet.parquet', options: [] },
        { name: 'CartoTextLayer', source: 'nztopo50_carto_text.parquet', options: [] },
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
});
