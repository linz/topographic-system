import assert from 'node:assert';
import { before, describe, it } from 'node:test';

import { fsa, FsMemory } from '@chunkd/fs';
import { CliId } from '@linzjs/topographic-system-shared';

import { DeployCommand } from '../cli/action.deploy.ts';
import { ProduceCoverCommand } from '../cli/action.produce.cover.ts';
import { ProduceCommand } from '../cli/action.produce.ts';
import { pyRunner } from '../python.runner.ts';

describe('deploy -> produce-cover -> produce', () => {
  const mem = new FsMemory();

  before(() => {
    fsa.register('memory://', mem);
  });

  const gitHash = '4aba34b5accb0002867af66f6a92a35e0a4be7cab'
  const baseDeployArgs = {
    commit: false,
    deployTag: 'latest',
    dataTag: 'latest',
    source: new URL('memory://source/catalog.json'),
  };

  it('should deploy a qgs file', async (t) => {
    await fsa.write(fsa.toUrl('memory://source/topo50maps/topo50.qgs'), '<xml ?>');
    await fsa.write(fsa.toUrl('memory://source/water/latest/water.parquet'), 'Hello World');

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

    // Deploy the QGIS project into memory
    await DeployCommand.handler({
      ...baseDeployArgs,
      project: [new URL('memory://source/topo50maps/')],
      target: new URL('memory://target-deploy/'),
      strategies: [{  type: 'latest' }, {type: 'commit', commit: gitHash}],
      commit: true,
    });

    assert.deepEqual(
      [...(await fsa.toArray(fsa.list(fsa.toUrl('memory://target-deploy/'))))].map((f) => f.href).sort(),
      [
        'memory://target-deploy/qgis/topo50maps/latest/topo50.json',
        'memory://target-deploy/qgis/topo50maps/latest/collection.json',
        'memory://target-deploy/qgis/topo50maps/latest/topo50.qgs',
        `memory://target-deploy/qgis/topo50maps/commit_prefix=${gitHash.charAt(0)}/commit=${gitHash}/topo50.json`,
        `memory://target-deploy/qgis/topo50maps/commit_prefix=${gitHash.charAt(0)}/commit=${gitHash}/collection.json`,
        `memory://target-deploy/qgis/topo50maps/commit_prefix=${gitHash.charAt(0)}/commit=${gitHash}/topo50.qgs`,
        'memory://target-deploy/qgis/topo50maps/catalog.json',
        'memory://target-deploy/qgis/catalog.json',
        'memory://target-deploy/catalog.json',
      ].sort(),
    );

    t.mock.method(pyRunner, 'qgisExportCover', () => {
      return [{ sheetCode: 'BQ32', epsg: 2193, bbox: [1756000, 5406000, 1780000, 5442000] }];
    });

    await ProduceCoverCommand.handler({
      mapSheet: ['BQ32'],
      project: new URL('memory://target-deploy/qgis/topo50maps/latest/topo50.json'),
      layout: 'tiff-50',
      mapSheetLayer: 'nz_topo50_map_sheet',
      source: baseDeployArgs.source,
      dpi: 300,
      output: new URL('memory://target-produce/working/'),
      fromFile: undefined,
      all: false,
      format: 'pdf',
      dataTags: undefined,
      tempLocation: new URL('memory://temp-produce-cover/'),
    });

    assert.deepEqual(
      [...(await fsa.toArray(fsa.list(fsa.toUrl('memory://target-produce/'))))].map((f) => f.href),
      [
        `memory://target-produce/working/${CliId}/BQ32.json`,
        `memory://target-produce/working/${CliId}/collection.json`,
        'memory://target-produce/working/catalog.json',
      ],
    );

    t.mock.method(pyRunner, 'qgisExport', async (_input: URL, output: URL) => {
      const outputFile = new URL('BQ32.pdf', output);
      await fsa.write(outputFile, 'BQ32.pdf');
      return outputFile;
    });
    await ProduceCommand.handler({
      path: [new URL(`memory://target-produce/working/${CliId}/BQ32.json`)],
      tempLocation: new URL('memory://temp-produce/'),
      fromFile: undefined,
      force: false,
    });

    assert.deepEqual(
      [...(await fsa.toArray(fsa.list(fsa.toUrl('memory://target-produce/'))))].map((f) => f.href),
      [
        `memory://target-produce/working/${CliId}/BQ32.json`,
        `memory://target-produce/working/${CliId}/collection.json`,
        'memory://target-produce/working/catalog.json',
        `memory://target-produce/working/${CliId}/BQ32.pdf`, // :tada: a export happened
      ],
    );
  });
});
