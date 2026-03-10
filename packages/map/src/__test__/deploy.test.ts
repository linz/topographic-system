import assert from 'node:assert';
import { before, describe, it } from 'node:test';

import { fsa, FsMemory } from '@chunkd/fs';

import { deployCommand } from '../cli/action.deploy.ts';
import { pyRunner } from '../python.runner.ts';

describe('action.deploy', () => {
  const mem = new FsMemory();

  before(() => {
    fsa.register('memory://', mem);
  });

  const baseArgs = {
    githash: undefined,
    commit: false,
    deployTag: 'latest',
    dataTag: 'latest',
    source: new URL('memory://source/catalog.json'),
  };

  it('should deploy a qgs file', async (t) => {
    await fsa.write(fsa.toUrl('memory://source/topo50maps/topo50.qgs'), '<xml ?>');

    await fsa.write(
      fsa.toUrl('memory://source/catalog.json'),
      JSON.stringify({ links: [{ rel: 'child', href: './water/latest/collection.json' }] }),
    );
    await fsa.write(
      fsa.toUrl('memory://source/water/latest/collection.json'),
      JSON.stringify({
        assets: { parquet: { href: './water.parquet' } },
      }),
    );

    t.mock.method(pyRunner, 'listSourceLayers', () => ['water']);

    await deployCommand.handler({
      ...baseArgs,
      project: new URL('memory://source/topo50maps/'),
      target: new URL('memory://target/'),
      commit: true,
    });

    assert.deepEqual(
      [...(await fsa.toArray(fsa.list(fsa.toUrl('memory://target/'))))].map((f) => f.href),
      [
        'memory://target/qgis/latest/topo50maps/topo50.qgs',
        'memory://target/qgis/latest/topo50maps/topo50.json',
        'memory://target/qgis/latest/topo50maps/collection.json',
        'memory://target/qgis/latest/catalog.json',
      ],
    );
  });
});
