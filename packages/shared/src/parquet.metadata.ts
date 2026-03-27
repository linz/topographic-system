import type { Epsg } from '@basemaps/geo';
import { Bounds, Projection, ProjectionLoader } from '@basemaps/geo';
import { fsa } from '@chunkd/fs';
import type { AsyncBuffer, ColumnChunk, FileMetaData } from 'hyparquet';
import { parquetMetadataAsync } from 'hyparquet';
import type { MinMaxType } from 'hyparquet/src/types.js';
import type { Extents } from 'stac-ts';

export interface ColumnStats {
  name: string;
  type: string;

  min: string | number | boolean;
  max: string | number | boolean;

  null_count: number;
}

export interface RowGroupColumnStats {
  'table:row_count': number;
  'table:columns': Partial<ColumnStats>[];
}

export interface ParquetStacMetadata {
  table: RowGroupColumnStats;
  extent: Extents;
}

export async function parquetToStac(assetFile: URL): Promise<ParquetStacMetadata> {
  const meta = await readParquetFileMetadata(assetFile);
  return await mapParquetMetadataToStacStats(meta);
}

export async function readParquetFileMetadata(assetFile: URL): Promise<FileMetaData> {
  const source = fsa.source(assetFile);
  const headInfo = await source.head();
  const byteLength = headInfo.size;
  if (byteLength == null) {
    throw new Error(`Unable to determine file size for ${assetFile.href}`);
  }

  // Create an AsyncBuffer adapter for hyparquet from @chunkd/source
  const asyncBuffer: AsyncBuffer = {
    byteLength,
    slice: (start: number, end?: number): Promise<ArrayBuffer> => {
      const length = end != null ? end - start : undefined;
      return source.fetch(start, length);
    },
  };
  return parquetMetadataAsync(asyncBuffer);
}

async function parquetEpsg(parquetMetadata: FileMetaData): Promise<Epsg> {
  let geom =
    parquetMetadata.key_value_metadata?.find((f) => f.key === 'geo') ??
    parquetMetadata.key_value_metadata?.find((f) => f.key === 'geometry');
  if (geom == null) throw new Error('Unable to find geometry metadata in parquet file');
  const geomMeta = JSON.parse(geom.value ?? '{}');
  const code = geomMeta.columns?.geom?.crs?.id?.code;
  const epsg = await ProjectionLoader.load(code); // validate the epsg code

  return epsg;
}

export async function mapParquetMetadataToStacStats(parquetMetadata: FileMetaData): Promise<ParquetStacMetadata> {
  const tableStats: Record<string, Partial<ColumnStats>> = {};

  let extentKey: string | null = null;
  let createDateKey: string | null = null;

  const epsg = await parquetEpsg(parquetMetadata);
  const proj = Projection.get(epsg);

  for (const rg of parquetMetadata.row_groups) {
    for (const col of rg.columns) {
      if (col.meta_data == null) continue;
      const name = col.meta_data.path_in_schema.join('.');
      const current = (tableStats[name] ??= { name, type: col.meta_data.type.toLowerCase() });
      aggregateStats(current, col);
      if (name.endsWith('bbox.xmin')) extentKey = name.slice(0, name.lastIndexOf('.'));
      if (createDateKey !== 'create_date' && name.endsWith('_date')) createDateKey = name;
    }
  }

  const extents: Partial<Extents> = {};

  // TODO is this actually the date field
  if (createDateKey != null) {
    const { min, max } = tableStats[createDateKey] as { min: string; max: string };
    extents.temporal = { interval: min === max ? ([[min, null]] as const) : ([[min, max]] as const) };
  }

  if (extentKey != null) {
    const xMin = tableStats[`${extentKey}.xmin`]?.min;
    const xMax = tableStats[`${extentKey}.xmax`]?.max;

    const yMin = tableStats[`${extentKey}.ymin`]?.min;
    const yMax = tableStats[`${extentKey}.ymax`]?.max;

    const extent = [xMin, yMin, xMax, yMax];

    // Reproject the spatial extent to WGS84
    const invalidKeys = extent.find((f) => typeof f !== 'number' || isNaN(f));
    if (invalidKeys == null) {
      const bounds = Bounds.fromBbox(extent as [number, number, number, number]);
      const bbox = proj.boundsToWgs84BoundingBox(bounds);
      console.log({ bbox });
      extents.spatial = { bbox: [bbox as number[]] };
    }
  }

  return {
    table: {
      'table:row_count': Number(parquetMetadata.num_rows),
      'table:columns': Object.values(tableStats),
    },
    extent: extents as Extents,
  };
}

/**
 * JSON cannot store all of the datatypes of parquet
 *
 * convert the types to JSON friendly types
 */
function jsonType(m: MinMaxType | undefined): string | number | boolean | null {
  if (m == null) return null;
  if (typeof m === 'number') return m;
  if (typeof m === 'string') return m;
  if (typeof m === 'bigint') return Number(m);
  if (typeof m === 'boolean') return m;
  if (m instanceof Date) return m.toISOString();
  throw new Error('unknown type:' + m);
}

function setMin(out: Partial<ColumnStats>, m: MinMaxType | undefined) {
  const val = jsonType(m);
  if (val == null) return;
  if (out.min == null || out.min < val) out.min = val;
}

function setMax(out: Partial<ColumnStats>, m: MinMaxType | undefined) {
  const val = jsonType(m);
  if (val == null) return;
  if (out.max == null || out.max < val) out.max = val;
}

function aggregateStats(out: Partial<ColumnStats>, chunk: ColumnChunk): void {
  const stats = chunk.meta_data?.statistics;
  if (stats == null) return;
  out.null_count = (out.null_count ?? 0) + Number(stats.null_count ?? 0);
  setMin(out, stats.min);
  setMax(out, stats.max);
}
