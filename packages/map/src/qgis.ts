import type { Epsg } from '@basemaps/geo';
import { ProjectionLoader } from '@basemaps/geo';
import { fsa } from '@chunkd/fs';
import { XMLParser } from 'fast-xml-parser';

/**
 * Load a QGS project and extract the layers names and their source and basic projection information
 *
 * @param path souce QGIS project
 * @returns
 */
export async function getQgisProjectMeta(
  path: URL,
): Promise<{ layers: { name: string; source: string }[]; epsg: Epsg }> {
  const lines = String(await fsa.read(path));

  /** Mapping of QGIS layer name to source file name */
  const layers: { name: string; source: string }[] = [];

  const parser = new XMLParser({ ignoreAttributes: false, processEntities: false });
  const xml = parser.parse(lines);
  const qgis = xml['qgis'];
  if (qgis == null) throw new Error('Failed to parse QGIS project');
  const projectCrs = Number(qgis['projectCrs']?.['spatialrefsys']?.['srid']);
  if (Number.isNaN(projectCrs)) throw new Error('Failed to parse projection from project');
  const epsg = await ProjectionLoader.load(projectCrs);

  for (const line of lines.split('\n')) {
    if (!line.trim().startsWith('<layer-tree-layer')) continue;

    const xml = parser.parse(line);
    const dataSource = xml?.['layer-tree-layer'];
    if (dataSource == null) continue;

    const parquetFile = /([a-zA-Z0-9_]+.parquet)/.exec(dataSource['@_source']);
    if (parquetFile == null) continue;
    layers.push({ name: dataSource['@_name'] as string, source: parquetFile[0] });
  }

  return { layers, epsg };
}

/** Attempt to find a MapSheet metadata layer */
export function getQgisMapSheetLayer(
  layers: { name: string; source: string }[],
  mapSheetLayerName?: string,
): { name: string; source: string } {
  if (mapSheetLayerName != null) {
    const layer = layers.find((f) => f.name === mapSheetLayerName);
    if (layer) return layer;
    throw new Error(`Mapsheet layer not found: "${mapSheetLayerName}"`);
  }
  // Find the first layer that looks like a map_sheet configuration layer
  const mapSheet = layers.find((f) => f.source.endsWith('map_sheet.parquet'));
  if (mapSheet == null) throw new Error('No map sheet layer ending with "map_sheet.parquet" found');
  return mapSheet;
}

/**
```
<layer-tree-layer checked="Qt::Checked" expanded="1" id="road_line_2_lane_map_e799a785_d8c6_4175_9251_610c0f53d138" legend_exp="" legend_split_behavior="0" name="road_line 2 lane highway map" patch_size="-1,-1" providerKey="ogr" source="./road_line.parquet|subset=&quot;lane_count&quot; > 1 and &quot;highway_number&quot; is NOT NULL">
<layer-tree-layer checked="Qt::Unchecked" expanded="1" id="descriptive_text_611314fa_e5da_45d6_ae50_9f398ec0b39c" legend_exp="" legend_split_behavior="0" name="descriptive_text" patch_size="-1,-1" providerKey="ogr" source="./descriptive_text.parquet">
<layer-tree-layer checked="Qt::Unchecked" expanded="1" id="descriptive_text_611314fa_e5da_45d6_ae50_9f398ec0b39c" legend_exp="" legend_split_behavior="0" name="descriptive_text" patch_size="-1,-1" providerKey="ogr" source="./descriptive_text.parquet">
*/
