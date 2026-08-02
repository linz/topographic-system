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

  /** Optional extra QGIS option metadata */
  options?: { key: string; value?: string }[];
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

function parseQuery(query?: string): { key: string; value?: string } | undefined {
  if (query == null) return undefined;

  const eqIndex = query.indexOf('=');
  if (eqIndex === -1) return { key: query, value: undefined };
  const key = query.slice(0, eqIndex);
  const value = query.slice(eqIndex + 1);

  return { key, value };
}

async function getQgisProjectMetaImpl(path: URL): Promise<{ layers: QgisLayerDef[]; epsg: Epsg }> {
  const lines = String(await fsa.read(path));

  /** Mapping of QGIS layer name to source file name */
  const layers: QgisLayerDef[] = [];

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

    const [source, ...query] = dataSource['@_source']?.split('|') ?? [undefined, undefined];
    if (source == null) continue;
    const parquetFile = basename(source) as string;

    layers.push({
      name: dataSource['@_name'] as string,
      source: parquetFile,
      options: query.map((m: string) => parseQuery(m)).filter(Boolean),
    });
  }

  return { layers, epsg };
}

function hasQuery(layer: QgisLayerDef): boolean {
  return (layer.options ?? []).find((f) => f.key === 'subset') != null;
}

/**
 * Find a layer in the project.
 *
 * When `explicitName` is provided the layer whose source matches it exactly is returned
 * otherwise the first layer whose source ends with `suffix` is used.
 *
 * @param label human readable name used in error messages, e.g. "Map sheet"
 */
function findQgisLayer(layers: QgisLayerDef[], suffix: string, label: string, explicitName?: string): QgisLayerDef {
  if (explicitName != null) {
    // add .parquet if there is no extension
    const searchName = explicitName.includes('.') ? explicitName : `${explicitName}.parquet`;
    const layer = layers.find((f) => f.source === searchName && hasQuery(f) === false);
    if (layer) return layer;
    throw new Error(`${label} source layer not found: "${explicitName}"`);
  }
  const layer = layers.find((f) => f.source.endsWith(suffix) && hasQuery(f) === false);
  if (layer == null) throw new Error(`No ${label.toLowerCase()} layer ending with "${suffix}" found`);
  return layer;
}

/** Attempt to find the carto text layer */
export function getQgisCartoTextLayer(layers: QgisLayerDef[], cartoTextLayerName?: string): QgisLayerDef {
  return findQgisLayer(layers, 'carto_text.parquet', 'Carto text', cartoTextLayerName);
}

/** Attempt to find a MapSheet metadata layer */
export function getQgisMapSheetDataset(layers: QgisLayerDef[], mapSheetLayerName?: string): QgisLayerDef {
  return findQgisLayer(layers, 'map_sheet.parquet', 'Map sheet', mapSheetLayerName);
}
