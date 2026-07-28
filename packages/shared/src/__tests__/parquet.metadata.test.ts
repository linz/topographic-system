import assert from 'node:assert';
import { describe, it } from 'node:test';

import type { FileMetaData } from 'hyparquet';
import type { MinMaxType } from 'hyparquet/src/types.js';

import type { ColumnStats, RowGroupColumnStats } from '../parquet.metadata.ts';
import { mapParquetMetadataToStacStats } from '../parquet.metadata.ts';

interface FakeColumn {
  /** Omit to simulate a chunk whose statistics are missing for this column. */
  stats?: { min?: MinMaxType; max?: MinMaxType; null_count?: number };
  /** Parquet physical type, reported lowercased in the STAC output. @default INT32 */
  type?: string;
}
/** One row group: column name -> that group's chunk statistics. */
type FakeRowGroup = Record<string, FakeColumn>;

/**
 * Build a fake hyparquet FileMetaData "asset" with the given per-row-group column
 * statistics, standing in for a real parquet file so the min/max aggregation is testable
 * without a binary fixture.
 */
function fakeMeta(rowGroups: FakeRowGroup[]): FileMetaData {
  const geo = JSON.stringify({
    primary_column: 'geometry',
    columns: { geometry: { bbox: [170, -45, 175, -40], crs: { id: { code: 4326 } } } },
  });
  const meta = {
    num_rows: BigInt(rowGroups.length),
    key_value_metadata: [{ key: 'geo', value: geo }],
    row_groups: rowGroups.map((rg) => ({
      columns: Object.entries(rg).map(([name, col]) => ({
        meta_data: { path_in_schema: [name], type: col.type ?? 'INT32', statistics: col.stats },
      })),
    })),
  };
  return meta as unknown as FileMetaData;
}

/** Shorthand for the common case: a single `t50_fid` int column per row group. */
function fid(min?: MinMaxType, max?: MinMaxType, null_count = 0): FakeRowGroup {
  return { t50_fid: { stats: { min, max, null_count } } };
}

/** Look a column up in the STAC table extension output. */
function column(table: RowGroupColumnStats, name: string): Partial<ColumnStats> {
  const col = table['table:columns'].find((c) => c.name === name);
  assert.ok(col != null, `expected a column named ${name}`);
  return col;
}

describe('mapParquetMetadataToStacStats min/max', () => {
  it('takes the minimum and maximum across row groups', async () => {
    // Per-group mins [100, 5, 50] / the inverted bug reported 100 (max of mins) instead of 5.
    const result = await mapParquetMetadataToStacStats(fakeMeta([fid(100, 200, 0), fid(5, 30, 2), fid(50, 300, 1)]));
    const col = column(result.table, 't50_fid');

    assert.strictEqual(col.min, 5, 'min should be the global minimum');
    assert.strictEqual(col.max, 300, 'max should be the global maximum');
    assert.strictEqual(col.null_count, 3, 'null_count should be summed across row groups');
  });

  it('keeps a zero minimum rather than treating it as unset', async () => {
    // `0` is falsy: a null-ish check on the accumulated min would let 10 overwrite it.
    const col = column((await mapParquetMetadataToStacStats(fakeMeta([fid(0, 5), fid(10, 20)]))).table, 't50_fid');

    assert.strictEqual(col.min, 0);
    assert.strictEqual(col.max, 20);
  });

  it('ignores row groups with missing statistics', async () => {
    const meta = fakeMeta([
      fid(5, 300),
      { t50_fid: {} }, // chunk with no statistics at all
      { t50_fid: { stats: { null_count: 4 } } }, // statistics present, but no min/max
    ]);
    const col = column((await mapParquetMetadataToStacStats(meta)).table, 't50_fid');

    assert.strictEqual(col.min, 5, 'a missing min must not overwrite the known min');
    assert.strictEqual(col.max, 300, 'a missing max must not overwrite the known max');
    assert.strictEqual(col.null_count, 4);
  });

  it('coerces bigint statistics into JSON-safe numbers', async () => {
    const meta = fakeMeta([
      { t50_fid: { type: 'INT64', stats: { min: 100n, max: 200n } } },
      { t50_fid: { type: 'INT64', stats: { min: 5n, max: 300n } } },
    ]);
    const col = column((await mapParquetMetadataToStacStats(meta)).table, 't50_fid');

    assert.strictEqual(col.min, 5);
    assert.strictEqual(col.max, 300);
    assert.strictEqual(typeof col.min, 'number', 'bigints must not survive into the STAC document');
    assert.strictEqual(typeof col.max, 'number');
  });

  it('coerces date statistics into ISO strings and compares them chronologically', async () => {
    const dates = (min: string, max: string): FakeRowGroup => ({
      create_date: { type: 'BYTE_ARRAY', stats: { min: new Date(min), max: new Date(max) } },
    });
    const meta = fakeMeta([
      dates('2024-06-01T00:00:00.000Z', '2024-07-01T00:00:00.000Z'),
      dates('2023-01-15T12:30:00.000Z', '2023-02-01T00:00:00.000Z'),
    ]);
    const col = column((await mapParquetMetadataToStacStats(meta)).table, 'create_date');

    assert.strictEqual(col.min, '2023-01-15T12:30:00.000Z');
    assert.strictEqual(col.max, '2024-07-01T00:00:00.000Z');
  });

  it('compares string statistics lexicographically', async () => {
    const meta = fakeMeta([
      { name: { type: 'BYTE_ARRAY', stats: { min: 'lake', max: 'river' } } },
      { name: { type: 'BYTE_ARRAY', stats: { min: 'bridge', max: 'stream' } } },
    ]);
    const col = column((await mapParquetMetadataToStacStats(meta)).table, 'name');

    assert.strictEqual(col.min, 'bridge');
    assert.strictEqual(col.max, 'stream');
  });
});
