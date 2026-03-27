import assert from 'node:assert';
import { before, describe, it } from 'node:test';

import { fsa, FsMemory } from '@chunkd/fs';
import { StacUpdater } from '@linzjs/topographic-system-stac';
import { StacBasic } from '@linzjs/topographic-system-stac/src/stac.basic.ts';
import type { StacCollection, StacItem } from 'stac-ts';
import type { GeoJSONMultiPolygon } from 'stac-ts/src/types/geojson.js';

import { DeployCommand } from '../cli/action.deploy.ts';
import { pyRunner } from '../python.runner.ts';

describe('action.deploy', () => {
  const mem = new FsMemory();

  before(() => {
    fsa.register('memory://', mem);
  });

  const gitHash = '4aba34b5accb0002867af66f6a92a35e0a4be7cab';
  const baseArgs = {
    commit: false,
    deployTag: 'latest',
    dataTag: 'latest',
    source: new URL('memory://source/catalog.json'),
  };

  it('should deploy a qgs file', async (t) => {
    await fsa.write(fsa.toUrl('memory://source/topo50maps/topo50.qgs'), '<xml ?>');

    const waterUrl = fsa.toUrl('memory://source/water/latest/collection.json');
    const waterChathams = fsa.toUrl('memory://source/water-chat/latest/collection.json');

    await StacUpdater.readWriteJson<StacCollection>(waterUrl, () => {
      const col = StacBasic.collection();
      col.extent.spatial.bbox = [[166.0, -47.5, 179.0, -34.0]];
      col.assets = { parquet: { href: './water.parquet' } };
      return col;
    });
    await StacUpdater.readWriteJson<StacCollection>(waterChathams, () => {
      const col = StacBasic.collection();
      col.extent.spatial.bbox = [[-177.3, -44.7, -175.5, -43.3]];
      col.assets = { parquet: { href: './water-chat.parquet' } };
      return col;
    });

    await StacUpdater.collections(new URL('memory://source/catalog.json'), [waterUrl, waterChathams], true);

    t.mock.method(pyRunner, 'listSourceLayers', () => ['water', 'water-chat']);

    await DeployCommand.handler({
      ...baseArgs,
      project: [new URL('memory://source/topo50maps/topo50.qgs')],
      target: new URL('memory://target/'),
      commit: true,
      strategies: [{ type: 'latest' }, { type: 'commit', commit: gitHash }],
    });

        const assets = [
      'memory://target/qgis/topo50/latest/topo50.json',
      'memory://target/qgis/topo50/latest/collection.json',
      // 'memory://target/qgis/topo50/latest/topo50.qgs',
      // `memory://target/qgis/topo50/commit_prefix=${gitHash.charAt(0)}/catalog.json`,
      // `memory://target/qgis/topo50/commit_prefix=${gitHash.charAt(0)}/commit=${gitHash}/topo50.json`,
      // `memory://target/qgis/topo50/commit_prefix=${gitHash.charAt(0)}/commit=${gitHash}/collection.json`,
      // `memory://target/qgis/topo50/commit_prefix=${gitHash.charAt(0)}/commit=${gitHash}/topo50.qgs`,
      // 'memory://target/qgis/topo50/catalog.json',
      // 'memory://target/qgis/catalog.json',
      // 'memory://target/catalog.json',
    ];

    for (const ass of assets) {
      await fsa.write(fsa.toUrl(ass.replace('memory://', './')), fsa.readStream(new URL(ass)));
    }

    assert.deepEqual(
      [...(await fsa.toArray(fsa.list(fsa.toUrl('memory://target/'))))].map((f) => f.href).sort(),
      [
        'memory://target/qgis/topo50/latest/topo50.json',
        'memory://target/qgis/topo50/latest/collection.json',
        'memory://target/qgis/topo50/latest/topo50.qgs',
        `memory://target/qgis/topo50/commit_prefix=${gitHash.charAt(0)}/catalog.json`,
        `memory://target/qgis/topo50/commit_prefix=${gitHash.charAt(0)}/commit=${gitHash}/topo50.json`,
        `memory://target/qgis/topo50/commit_prefix=${gitHash.charAt(0)}/commit=${gitHash}/collection.json`,
        `memory://target/qgis/topo50/commit_prefix=${gitHash.charAt(0)}/commit=${gitHash}/topo50.qgs`,
        'memory://target/qgis/topo50/catalog.json',
        'memory://target/qgis/catalog.json',
        'memory://target/catalog.json',
      ].sort(),
    );

    const latest = {
      item: await fsa.readJson<StacItem>(new URL('memory://target/qgis/topo50/latest/topo50.json')),
      collection: await fsa.readJson<StacCollection>(new URL('memory://target/qgis/topo50/latest/collection.json')),
    };

    const geom = latest.item.geometry as GeoJSONMultiPolygon;
    // Item should not have any geometries
    assert.equal(geom.type, 'MultiPolygon');
    assert.deepEqual(geom.coordinates[0]?.flat(), [
      [-177.3, -44.7],
      [-175.5, -44.7],
      [-175.5, -43.3],
      [-177.3, -43.3],
      [-177.3, -44.7],
    ]);
    assert.deepEqual(geom.coordinates[1]?.flat(), [
      [166, -47.5],
      [179, -47.5],
      [179, -34],
      [166, -34],
      [166, -47.5],
    ]);

    assert.deepEqual(latest.item.bbox, [166.0, -47.5, -175.5, -34.0]);

    const datasets = latest.item.links.filter((f) => f.rel === 'dataset').map((m) => m.href);
    // Water datasets are in another host so should have absolute urls
    assert.deepEqual(datasets, [
      'memory://source/water/latest/collection.json',
      'memory://source/water-chat/latest/collection.json',
    ]);

    

    console.log(latest.collection.extent.spatial.bbox)
    assert.deepEqual(latest.collection.extent.spatial.bbox, [[ -177.3, -44.7, -175.5, -43.3 ], [ 166, -47.5, 179, -34 ]])
  });
});
