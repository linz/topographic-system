import assert from 'node:assert';
import { before, describe, it } from 'node:test';

import { fsa, FsMemory } from '@chunkd/fs';
import { qFromArgs, StacExtensions } from '@linzjs/topographic-system-shared';
import type { StacCollection } from 'stac-ts';

import { StacCollectionWriter } from '../stac.writer.ts';

describe('StacCollectionWriter', () => {
  before(() => {
    fsa.register('memory://', new FsMemory());
  });

  const q = qFromArgs({ concurrency: 1 });

  it('adds STAC extensions without duplicates', () => {
    const sw = new StacCollectionWriter('data', 'roads');
    sw.extension(StacExtensions.file);
    sw.extension(StacExtensions.proj);
    sw.extension(StacExtensions.table);
    sw.extension(StacExtensions.file);

    assert.deepStrictEqual(sw.collection.stac_extensions, [
      StacExtensions.file,
      StacExtensions.proj,
      StacExtensions.table,
    ]);
  });

  it('writes collection with STAC extensions and asset metadata', async () => {
    const sw = new StacCollectionWriter('data', 'roads');
    sw.extension(StacExtensions.file);
    sw.extension(StacExtensions.proj);
    sw.extension(StacExtensions.table);

    const sourceData = Buffer.from('fake parquet file content');
    const sourceUrl = new URL('memory://source/roads.parquet');
    await fsa.write(sourceUrl, sourceData);

    sw.asset('parquet', sourceUrl, {
      href: './roads.parquet',
      roles: ['data'],
      type: 'application/vnd.apache.parquet',
      'proj:epsg': 2193,
      'table:row_count': 1234,
      'table:primary_geometry': 'geometry',
      'table:primary_datetime': 'create_date',
      'table:columns': [
        { name: 'geometry', type: 'binary' },
        { name: 'create_date', type: 'byte_array' },
        { name: 't50_fid', type: 'int32' },
      ],
    });

    const targetUrl = new URL('memory://output/');
    const collectionUrl = await sw.write(targetUrl, q);

    assert.strictEqual(collectionUrl.href, 'memory://output/roads/collection.json');

    const collection = await fsa.readJson<StacCollection>(collectionUrl);
    assert.deepStrictEqual(collection.stac_extensions, [
      StacExtensions.file,
      StacExtensions.proj,
      StacExtensions.table,
    ]);

    const parquetAsset = collection.assets?.['parquet'];
    assert.ok(parquetAsset != null);
    assert.strictEqual(parquetAsset['proj:epsg'], 2193);
    assert.strictEqual(parquetAsset['table:row_count'], 1234);
    assert.strictEqual(parquetAsset['table:primary_geometry'], 'geometry');
    assert.strictEqual(parquetAsset['table:primary_datetime'], 'create_date');
    assert.deepStrictEqual(parquetAsset['table:columns'], [
      { name: 'geometry', type: 'binary' },
      { name: 'create_date', type: 'byte_array' },
      { name: 't50_fid', type: 'int32' },
    ]);
    assert.strictEqual(parquetAsset['file:size'], sourceData.length);
    assert.ok(typeof parquetAsset['file:checksum'] === 'string');
    assert.ok(parquetAsset['file:checksum'].startsWith('1220'));
  });
});
