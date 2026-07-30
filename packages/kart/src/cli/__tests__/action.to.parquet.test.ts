import assert from 'node:assert';
import { describe, it } from 'node:test';

import { buildOgr2OgrArgs } from '../action.to.parquet.ts';

describe('buildOgr2OgrArgs', () => {
  const parquetFile = new URL('file:///tmp/kart/parquet/buildings.parquet');
  const gpkgFile = new URL('file:///tmp/kart/export/buildings.gpkg');

  it('should build the expected ogr2ogr args with sort-by-bbox enabled', () => {
    assert.deepStrictEqual(
      buildOgr2OgrArgs(parquetFile, gpkgFile, {
        compression: 'zstd',
        compressionLevel: 17,
        rowGroupSize: 2 ** 15,
        sortByBbox: true,
      }),
      [
        'ogr2ogr',
        '-unsetFid',
        '-f',
        'Parquet',
        '/tmp/kart/parquet/buildings.parquet',
        '/tmp/kart/export/buildings.gpkg',
        '-lco',
        'COMPRESSION=zstd',
        '-lco',
        'COMPRESSION_LEVEL=17',
        '-lco',
        'ROW_GROUP_SIZE=32768',
        '-lco',
        'WRITE_COVERING_BBOX=YES',
        '-lco',
        'COVERING_BBOX_NAME=bbox',
        '-lco',
        'SORT_BY_BBOX=YES',
      ],
    );
  });

  it('should omit SORT_BY_BBOX when sort-by-bbox is disabled', () => {
    assert.deepStrictEqual(
      buildOgr2OgrArgs(parquetFile, gpkgFile, {
        compression: 'zstd',
        compressionLevel: 17,
        rowGroupSize: 2 ** 15,
        sortByBbox: false,
      }),
      [
        'ogr2ogr',
        '-unsetFid',
        '-f',
        'Parquet',
        '/tmp/kart/parquet/buildings.parquet',
        '/tmp/kart/export/buildings.gpkg',
        '-lco',
        'COMPRESSION=zstd',
        '-lco',
        'COMPRESSION_LEVEL=17',
        '-lco',
        'ROW_GROUP_SIZE=32768',
        '-lco',
        'WRITE_COVERING_BBOX=YES',
        '-lco',
        'COVERING_BBOX_NAME=bbox',
      ],
    );
  });

  it('should always pass -unsetFid to ogr2ogr', () => {
    const args = buildOgr2OgrArgs(parquetFile, gpkgFile, {
      compression: 'zstd',
      compressionLevel: 17,
      rowGroupSize: 2 ** 15,
      sortByBbox: false,
    });
    assert.ok(args.includes('-unsetFid'));
  });
});
