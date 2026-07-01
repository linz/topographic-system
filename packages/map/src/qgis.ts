import { basename } from 'node:path';

import type { Epsg } from '@basemaps/geo';
import { ProjectionLoader } from '@basemaps/geo';
import { fsa } from '@chunkd/fs';
import { XMLParser } from 'fast-xml-parser';

const LayerDefs = new Map<string, Promise<{ layers: QgisLayerDef[]; epsg: Epsg }>>();

interface QgisLayerDef {
  /**
   * QGIS layer name
   *
   * @example "road_line 1 lane sealed map"
   */
  name: string;
  /**
   * Referenced file source file
   *
   * @example "nztopo50_map_sheet.parquet"
   */
  source: string;
}
/**
 * Load a QGS project and extract the layers names and their source and basic projection information
 *
 * @param path souce QGIS project
 * @returns
 */
export function getQgisProjectMeta(path: URL): Promise<{ layers: QgisLayerDef[]; epsg: Epsg }> {
  const layerDef = LayerDefs.get(path.href);
  if (layerDef != null) return layerDef;
  const promise = getQgisProjectMetaImpl(path);
  if (LayerDefs.size > 100) {
    // Remove the first entry in the map to keep the cache size under 100
    const firstKey = LayerDefs.keys().next().value as string;
    LayerDefs.delete(firstKey);
  }

  LayerDefs.set(path.href, promise);
  return promise;
}

async function getQgisProjectMetaImpl(path: URL): Promise<{ layers: QgisLayerDef[]; epsg: Epsg }> {
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

    const source = dataSource['@_source']?.split('|').at(0);
    if (source == null) continue;
    const parquetFile = basename(source) as string;

    layers.push({ name: dataSource['@_name'] as string, source: parquetFile });
  }

  return { layers, epsg };
}

/** Attempt to find a MapSheet metadata layer */
export function getQgisMapSheetDataset(layers: QgisLayerDef[], mapSheetLayerName?: string): QgisLayerDef {
  if (mapSheetLayerName != null) {
    // add .parquet if there is no extension
    const searchName = mapSheetLayerName.includes('.') ? mapSheetLayerName : `${mapSheetLayerName}.parquet`;
    const layer = layers.find((f) => f.source === searchName);
    if (layer) return layer;
    throw new Error(`Map sheet source layer not found: "${mapSheetLayerName}"`);
  }
  // Find the first layer that looks like a map_sheet configuration layer
  const mapSheet = layers.find((f) => f.source.endsWith('map_sheet.parquet'));
  if (mapSheet == null) throw new Error('No map sheet layer ending with "map_sheet.parquet" found');
  return mapSheet;
}
