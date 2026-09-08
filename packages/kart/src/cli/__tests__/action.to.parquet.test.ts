import assert from 'node:assert';
import { describe, it } from 'node:test';

import { type ParquetStacMetadata, StacExtensions } from '@linzjs/topographic-system-shared';

import { buildOgr2OgrArgs, createDatasetStac } from '../action.to.parquet.ts';

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

describe('createDatasetStac', () => {
  const metadata = {
    epsg: { code: 2193 } as any,
    extent: {
      spatial: { bbox: [[170, -45, 175, -40]] },
      temporal: { interval: [['2024-01-01T00:00:00.000Z', null]] },
    },
    table: {
      'table:row_count': 500,
      'table:primary_geometry': 'geometry',
      'table:primary_datetime': 'create_date',
      'table:columns': [
        { name: 'geometry', type: 'binary' },
        { name: 'create_date', type: 'byte_array' },
        { name: 't50_fid', type: 'int32', min: 1, max: 500 },
      ],
    },
  } as ParquetStacMetadata;

  it('populates STAC collection extensions and parquet asset metadata', () => {
    const sw = createDatasetStac({
      dataset: 'nz_buildings',
      source: new URL('file:///tmp/nz_buildings.parquet'),
      title: 'NZ Buildings',
      description: 'Building outlines',
      metadata,
    });

    assert.deepStrictEqual(sw.collection.stac_extensions, [
      StacExtensions.file,
      StacExtensions.proj,
      StacExtensions.table,
    ]);
    assert.strictEqual(sw.collection.title, 'NZ Buildings');
    assert.strictEqual(sw.collection.description, 'Building outlines');
    assert.deepStrictEqual(sw.collection.extent, metadata.extent);

    const parquetAsset = sw.collection.assets?.['parquet'];
    assert.ok(parquetAsset != null);
    assert.strictEqual(parquetAsset.href, './nz_buildings.parquet');
    assert.deepStrictEqual(parquetAsset.roles, ['data']);
    assert.strictEqual(parquetAsset.type, 'application/vnd.apache.parquet');
    assert.strictEqual(parquetAsset['proj:epsg'], 2193);
    assert.strictEqual(parquetAsset['table:row_count'], 500);
    assert.strictEqual(parquetAsset['table:primary_geometry'], 'geometry');
    assert.strictEqual(parquetAsset['table:primary_datetime'], 'create_date');
    assert.deepStrictEqual(parquetAsset['table:columns'], metadata.table['table:columns']);
  });

  it('falls back to default title and description when omitted', () => {
    const sw = createDatasetStac({
      dataset: 'nz_coastlines',
      source: new URL('file:///tmp/nz_coastlines.parquet'),
      metadata,
    });

    assert.strictEqual(sw.collection.title, 'nz_coastlines');
    assert.strictEqual(sw.collection.description, 'topographic-system export of nz_coastlines');
  });
});
