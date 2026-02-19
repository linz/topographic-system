import { fsa } from '@chunkd/fs';
import type { AsyncBuffer, ColumnChunk, FileMetaData, Statistics } from 'hyparquet';
import { parquetMetadataAsync } from 'hyparquet';
import type { SpatialExtent, TemporalExtent } from 'stac-ts';

import { logger } from './log.ts';

export interface ColumnStats extends Statistics {
  name: string;
  type: string;
  codec: string;
}

export interface RowGroupColumnStats {
  'table:row_count': bigint;
  'table:columns': Partial<ColumnStats>[];
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

export function mapParquetMetadataToStacStats(parquetMetadata: FileMetaData): RowGroupColumnStats {
  const numColumns = parquetMetadata.row_groups[0]?.columns.length ?? 0;

  return {
    'table:row_count': parquetMetadata.num_rows,
    'table:columns': Array.from({ length: numColumns }, (_, i) => {
      const columns = parquetMetadata.row_groups.map((rg) => rg.columns[i]).filter((col) => col != null);
      return aggregateColumnStatsAcrossRowGroups(columns);
    }),
  };
}

function aggregateColumnStatsAcrossRowGroups(columns: ColumnChunk[]): ColumnStats {
  const summaryStats = {} as ColumnStats;

  for (const column of columns) {
    if (column?.meta_data == null) continue;
    summaryStats.name = column.meta_data.path_in_schema.join('.');
    summaryStats.type = column.meta_data.type.toLowerCase();
    summaryStats.codec = column.meta_data.codec;
    if (column?.meta_data?.statistics == null) continue;
    const columnStats = column.meta_data.statistics;

    if (columnStats.min != null && (summaryStats.min == null || columnStats.min < summaryStats.min)) {
      summaryStats.min = columnStats.min;
    }
    if (columnStats.max != null && (summaryStats.max == null || columnStats.max > summaryStats.max)) {
      summaryStats.max = columnStats.max;
    }
    if (columnStats.null_count != null) {
      summaryStats.null_count = (summaryStats.null_count ?? 0n) + columnStats.null_count;
    }
    if (
      columnStats.distinct_count != null &&
      (summaryStats.distinct_count == null || columnStats.distinct_count > summaryStats.distinct_count)
    ) {
      summaryStats.distinct_count = columnStats.distinct_count;
    }
  }
  return summaryStats;
}

export function extractSpatialExtent(columnStats: ColumnStats[]): SpatialExtent {
  const xmin = columnStats.find((col) => col.name === 'geom_bbox.xmin')?.min;
  const xmax = columnStats.find((col) => col.name === 'geom_bbox.xmax')?.max;
  const ymin = columnStats.find((col) => col.name === 'geom_bbox.ymin')?.min;
  const ymax = columnStats.find((col) => col.name === 'geom_bbox.ymax')?.max;

  if (typeof xmin !== 'number' || typeof xmax !== 'number' || typeof ymin !== 'number' || typeof ymax !== 'number') {
    logger.error({ columnStats, xmin, xmax, ymin, ymax }, 'SpatialExtent:InvalidColumnStats');
    throw new Error('SpatialExtent:InvalidColumnStats');
  }

  return [xmin, ymin, xmax, ymax];
}

export function extractTemporalExtent(columnStats: ColumnStats[]): TemporalExtent {
  const minDates: string[] = [];
  const maxDates: string[] = [];
  for (const column of columnStats) {
    if (column.name?.toLowerCase().endsWith('_date')) {
      if (column.min && typeof column.min === 'string') minDates.push(column.min);
      if (column.max && typeof column.max === 'string') maxDates.push(column.max);
    }
  }
  const minDate = minDates.length > 0 ? minDates.reduce((a, b) => (a < b ? a : b)) : 'null';
  const maxDate = maxDates.length > 0 ? maxDates.reduce((a, b) => (a > b ? a : b)) : 'null';
  return maxDate === minDate ? [minDate, null] : [minDate, maxDate];
}
