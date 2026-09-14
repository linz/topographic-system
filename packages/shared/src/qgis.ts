import { parse } from 'node:path';

import type { Epsg } from '@basemaps/geo';
import { ProjectionLoader } from '@basemaps/geo';
import { fsa } from '@chunkd/fs';
import { XMLParser } from 'fast-xml-parser';

const LayerDefs = new Map<string, Promise<{ layers: QgisLayerDef[]; epsg: Epsg }>>();

export interface QgisLayerDef {
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

  /**
   * QGIS Path location
   *
   * @example "./nztopo50_map_sheet.parquet"
   */
  path: string;

  /**
   * Type of source data
   *
   * @example "parquet", "gpkg"
   */
  type: string;

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

/**
 * Parse a QGIS layer definition string
 *
 * @param layer Layer definition string
 * @param name Layer name
 *
 * @returns QgisLayerDef or undefined if parsing failed
 */
export function parseQgisLayerDef(layer?: string, name?: string): QgisLayerDef | undefined {
  if (layer == null) return;
  const [source, ...query] = layer.split('|') ?? [undefined, undefined];
  if (source == null) return;
  const sourceFile = parse(source);

  return {
    name: name ?? sourceFile.name,
    source: sourceFile.base,
    path: source,
    type: sourceFile.ext.slice(1),
    options: query.map((m: string) => parseQuery(m)).filter(Boolean) as { key: string; value?: string }[],
  };
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

    const layerDef = parseQgisLayerDef(dataSource['@_source'], dataSource['@_name']);
    if (layerDef) layers.push(layerDef);
  }

  return { layers, epsg };
}
