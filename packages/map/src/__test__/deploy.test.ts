import assert from 'node:assert';
import { before, describe, it } from 'node:test';

import { fsa, FsMemory } from '@chunkd/fs';

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

    await DeployCommand.handler({
      ...baseArgs,
      project: [new URL('memory://source/topo50maps/topo50.qgs')],
      target: new URL('memory://target/'),
      commit: true,
      strategies: [{ type: 'latest' }, { type: 'commit', commit: gitHash }],
    });

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
  });
});
