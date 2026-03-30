import assert from 'node:assert';
import { before, describe, it } from 'node:test';

import { fsa, FsMemory } from '@chunkd/fs';
import { StacUpdater } from '@linzjs/topographic-system-stac';
import { StacBasic } from '@linzjs/topographic-system-stac/src/stac.basic.ts';
import type { StacCollection } from 'stac-ts';

import { DeployCommand } from '../cli/action.deploy.ts';
import { ProduceCoverCommand } from '../cli/action.produce.cover.ts';
import { ProduceCommand } from '../cli/action.produce.ts';
import { BaseCommandOptions, pyRunner } from '../python.runner.ts';

describe('deploy -> produce-cover -> produce', () => {
  const mem = new FsMemory();

  before(() => {
    fsa.register('memory://', mem);
  });

  const gitHash = '4aba34b5accb0002867af66f6a92a35e0a4be7cab';

  it('should ensure base container matches the Dockerfile', async () => {
    const dockerFile = new URL('../../Dockerfile', import.meta.url);
    const from = String(await fsa.read(dockerFile))
      .split('\n')
      .find((f) => f.toLowerCase().startsWith('from'));
    assert.equal(from, `FROM ${BaseCommandOptions.container}`);
  });

  const baseDeployArgs = {
    commit: false,
    deployTag: 'latest',
    dataTag: 'latest',
    source: new URL('memory://source/catalog.json'),
  };

  it('should deploy a qgs file', async (t) => {
    await fsa.write(fsa.toUrl('memory://source/topo50maps/topo50.qgs'), '<xml ?>');
    const waterUrl = fsa.toUrl('memory://source/data/water/latest/');
    await fsa.write(new URL('water.parquet', waterUrl), 'Hello World');

    await StacUpdater.readWriteJson<StacCollection>(new URL('collection.json', waterUrl), () => {
      const col = StacBasic.collection();
      col.extent.spatial.bbox = [[166.0, -47.5, 179.0, -34.0]];
      col.assets = { parquet: { href: './water.parquet' } };
      return col;
    });
    await StacUpdater.collections(new URL('memory://source/catalog.json'), [waterUrl], true);

    t.mock.method(pyRunner, 'listSourceLayers', () => ['water']);

    // Deploy the QGIS project into memory
    await DeployCommand.handler({
      ...baseDeployArgs,
      project: [new URL('memory://source/topo50maps/topo50.qgs')],
      target: new URL('memory://target-deploy/'),
      strategies: [{ type: 'latest' }, { type: 'commit', commit: gitHash }],
      commit: true,
    });

    assert.deepEqual(
      [...(await fsa.toArray(fsa.list(fsa.toUrl('memory://target-deploy/'))))].map((f) => f.href).sort(),
      [
        'memory://target-deploy/qgis/topo50/latest/topo50.json',
        'memory://target-deploy/qgis/topo50/latest/collection.json',
        'memory://target-deploy/qgis/topo50/latest/topo50.qgs',
        `memory://target-deploy/qgis/topo50/commit_prefix=${gitHash.charAt(0)}/catalog.json`,
        `memory://target-deploy/qgis/topo50/commit_prefix=${gitHash.charAt(0)}/commit=${gitHash}/topo50.json`,
        `memory://target-deploy/qgis/topo50/commit_prefix=${gitHash.charAt(0)}/commit=${gitHash}/collection.json`,
        `memory://target-deploy/qgis/topo50/commit_prefix=${gitHash.charAt(0)}/commit=${gitHash}/topo50.qgs`,
        'memory://target-deploy/qgis/topo50/catalog.json',
        'memory://target-deploy/qgis/catalog.json',
        'memory://target-deploy/catalog.json',
      ].sort(),
    );

    t.mock.method(pyRunner, 'qgisExportCover', () => {
      return [{ sheetCode: 'BQ32', epsg: 2193, bbox: [1756000, 5406000, 1780000, 5442000] }];
    });

    await ProduceCoverCommand.handler({
      mapSheet: ['BQ32'],
      project: new URL('memory://target-deploy/qgis/topo50/latest/topo50.json'),
      layout: 'tiff-50',
      mapSheetLayer: 'nz_topo50_map_sheet',
      source: baseDeployArgs.source,
      dpi: 300,
      output: new URL('memory://target-produce/'),
      fromFile: undefined,
      all: false,
      format: 'pdf',
      dataTags: undefined,
      strategy: { type: 'latest' },
      tempLocation: new URL('memory://temp-produce-cover/'),
    });

    assert.deepEqual(
      [...(await fsa.toArray(fsa.list(fsa.toUrl('memory://target-produce/'))))].map((f) => f.href).sort(),
      [
        `memory://target-produce/product/topo50/latest/BQ32.json`,
        `memory://target-produce/product/topo50/latest/collection.json`,
        `memory://target-produce/product/topo50/catalog.json`,
        'memory://target-produce/product/catalog.json',
        'memory://target-produce/catalog.json',
      ].sort(),
    );

    t.mock.method(pyRunner, 'qgisExport', async (_input: URL, output: URL) => {
      const outputFile = new URL('product/latest/BQ32.pdf', output);
      await fsa.write(outputFile, 'BQ32.pdf');
      return outputFile;
    });
    await ProduceCommand.handler({
      path: [new URL(`memory://target-produce/product/topo50/latest/BQ32.json`)],
      tempLocation: new URL('memory://temp-produce/'),
      fromFile: undefined,
      force: false,
      concurrency: 1,
    });

    assert.deepEqual(
      [...(await fsa.toArray(fsa.list(fsa.toUrl('memory://target-produce/'))))].map((f) => f.href).sort(),
      [
        `memory://target-produce/product/topo50/latest/BQ32.json`,
        `memory://target-produce/product/topo50/latest/collection.json`,
        `memory://target-produce/product/topo50/catalog.json`,
        'memory://target-produce/product/catalog.json',
        'memory://target-produce/catalog.json',
        `memory://target-produce/product/topo50/latest/BQ32.pdf`, // :tada: a export happened
      ].sort(),
    );
  });
});
