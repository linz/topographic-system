import { fsa } from '@chunkd/fs';
import { StacCollectionWriter, StacPusher, StacUpdater } from '@linzjs/topographic-system-stac';
import pLimit from 'p-limit';
import type { SpatialExtents } from 'stac-ts';

export const BaseQgsProject = `
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
</layer-tree-group>
`.trim();

const bbox = {
  water: [[166.0, -47.5, 179.0, -34.0]],
  road_line: [[-177.3, -44.7, -175.5, -43.3]],
  nztopo50_map_sheet: [[166.0, -47.5, 179.0, -34.0]],
};

const layers = ['road_line', 'water', 'nztopo50_map_sheet'] as const;

export async function writeBaseLayers(rootCatalog: URL): Promise<URL> {
  const limit = pLimit(1);
  await fsa.write(fsa.toUrl('memory://source/topo50maps/topo50.qgs'), BaseQgsProject);

  const sourceDataUrl = new URL(`memory://source-data/`);
  const sourceCatalog = new URL('memory://source-data/catalog.json');

  const collectionsToWrite: URL[] = [];
  for (const layer of layers) {
    const assetUrl = new URL(`${layer}.parquet`, sourceDataUrl);

    if (layer === 'nztopo50_map_sheet') {
      // This layer needs to be a valid parquet file
      const parquet = await fsa.read(new URL('../../assets/project/nztopo50_map_sheet.parquet', import.meta.url));
      await fsa.write(assetUrl, parquet);
    } else {
      await fsa.write(assetUrl, 'Hello World');
    }

    const sw = new StacCollectionWriter('data', layer);
    sw.collection.title = `Topographic ${layer}`;

    sw.collection.extent.spatial.bbox = bbox[layer] as SpatialExtents;
    sw.asset('parquet', assetUrl, { href: `./${layer}.parquet` });
    collectionsToWrite.push(await sw.write(sourceCatalog, limit));
  }

  await StacUpdater.collections(sourceCatalog, collectionsToWrite, true);

  const push = new StacPusher(rootCatalog, 'data');
  push.strategy({ type: 'latest' });
  push.strategy({ type: 'date', date: new Date('2026-06-01T14:32:00.123Z') });

  const { collections } = await push.push(sourceCatalog, limit, true);

  await StacUpdater.collections(rootCatalog, collections, true);

  const latest = collections.find((f) => f.href.includes('/latest/'));
  if (latest == null) throw new Error('Unable to find water collection');

  return latest;
}
