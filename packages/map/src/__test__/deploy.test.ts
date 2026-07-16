import assert from 'node:assert';
import { before, describe, it } from 'node:test';

import { fsa, FsMemory } from '@chunkd/fs';
import { StacPushCommand } from '@linzjs/topographic-system-stac';
import type { GeoJSONMultiPolygon, StacCollection, StacItem } from 'stac-ts';

import { DeployCommand } from '../cli/action.deploy.ts';
import { writeBaseLayers } from './util.ts';

describe('action.deploy', () => {
  const mem = new FsMemory();
  const concurrency = 10;

  before(() => {
    fsa.register('memory://', mem);
  });

  it('should deploy a qgs file', async () => {
    const rootCatalog = new URL('memory://source/catalog.json');

    await writeBaseLayers(rootCatalog);

    const targetDeploy = new URL('memory://target/deploy/');
    await DeployCommand.handler({
      concurrency,
      extras: [],
      project: [new URL('memory://source/topo50maps/topo50.qgs')],
      target: targetDeploy,
      source: new URL('memory://source/catalog.json'),
    });

    assert.deepEqual(
      [...(await fsa.toArray(fsa.list(targetDeploy)))].map((f) => f.href.replace(targetDeploy.href, '')).sort(),
      ['topo50/topo50.json', 'topo50/collection.json', 'topo50/topo50.qgs', 'catalog.json'].sort(),
    );

    const latest = {
      item: await fsa.readJson<StacItem>(new URL('memory://target/deploy/topo50/topo50.json')),
      collection: await fsa.readJson<StacCollection>(new URL('memory://target/deploy/topo50/collection.json')),
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
      'memory://source/data/road_line/latest/collection.json',
      'memory://source/data/water/latest/collection.json',
      'memory://source/data/nztopo50_map_sheet/latest/collection.json',
    ]);

    assert.deepEqual(latest.collection.extent.spatial.bbox, [
      [-177.3, -44.7, -175.5, -43.3],
      [166, -47.5, 179, -34],
    ]);

    const gitHash = '4aba34b5accb0002867af66f6a92a35e0a4be7cab';

    const targetPush = new URL('memory://target/push/');
    await StacPushCommand.handler({
      concurrency,
      source: new URL('memory://target/deploy/catalog.json'),
      target: targetPush,
      category: 'qgis',
      strategies: [{ type: 'latest' }, { type: 'commit', commit: gitHash }],
      commit: true,
    });

    assert.deepEqual(
      [...(await fsa.toArray(fsa.list(targetPush)))].map((f) => f.href.replace(targetPush.href, '')).sort(),
      [
        'qgis/topo50/latest/topo50.json',
        'qgis/topo50/latest/collection.json',
        'qgis/topo50/latest/topo50.qgs',
        `qgis/topo50/commit_prefix=${gitHash.charAt(0)}/catalog.json`,
        `qgis/topo50/commit_prefix=${gitHash.charAt(0)}/commit=${gitHash}/topo50.json`,
        `qgis/topo50/commit_prefix=${gitHash.charAt(0)}/commit=${gitHash}/collection.json`,
        `qgis/topo50/commit_prefix=${gitHash.charAt(0)}/commit=${gitHash}/topo50.qgs`,
        'qgis/topo50/catalog.json',
        'qgis/catalog.json',
        'catalog.json',
      ].sort(),
    );

    const latestCollection = await fsa.readJson<StacCollection>(
      new URL('memory://target/push/qgis/topo50/latest/collection.json'),
    );
    const commitCollection = await fsa.readJson<StacCollection>(
      new URL(`memory://target/push/qgis/topo50/commit_prefix=${gitHash.charAt(0)}/commit=${gitHash}/collection.json`),
    );

    assert.deepEqual(
      latestCollection.links.find((f) => f.rel === 'canonical'),
      {
        rel: 'canonical',
        href: `../commit_prefix=${gitHash.charAt(0)}/commit=${gitHash}/collection.json`,
      },
    );

    assert.deepEqual(
      commitCollection.links.find((f) => f.rel === 'latest-version'),
      {
        rel: 'latest-version',
        href: `../../latest/collection.json`,
      },
    );
  });
});
