import assert from 'node:assert';
import { before, describe, it } from 'node:test';

import { fsa, FsMemory } from '@chunkd/fs';
import { StacUpdater, StacPushCommand, StacBasic } from '@linzjs/topographic-system-stac';
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

  const concurrency = 10;
  const gitHash = '4aba34b5accb0002867af66f6a92a35e0a4be7cab';

  it('should ensure base container matches the Dockerfile', async () => {
    const dockerFile = new URL('../../Dockerfile', import.meta.url);
    const from = String(await fsa.read(dockerFile))
      .split('\n')
      .find((f) => f.toLowerCase().startsWith('from'));
    assert.equal(from, `FROM ${BaseCommandOptions.container}`);
  });

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
    const targetDeploy = new URL('memory://target-deploy/');
    await DeployCommand.handler({
      concurrency,
      project: [new URL('memory://source/topo50maps/topo50.qgs')],
      target: targetDeploy,
      source: new URL('memory://source/catalog.json'),
    });

    assert.deepEqual(
      [...(await fsa.toArray(fsa.list(targetDeploy)))].map((f) => f.href.replace(targetDeploy.href, '')).sort(),
      ['topo50/topo50.json', 'topo50/collection.json', 'topo50/topo50.qgs', 'catalog.json'].sort(),
    );

    const targetPush = new URL('memory://target-push/');
    await StacPushCommand.handler({
      concurrency,
      source: new URL('memory://target-deploy/catalog.json'),
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

    t.mock.method(pyRunner, 'qgisExportCover', () => {
      return [{ sheetCode: 'BQ32', epsg: 2193, bbox: [1756000, 5406000, 1780000, 5442000] }];
    });

    const targetProduce = new URL('memory://target-produce/');
    await ProduceCoverCommand.handler({
      concurrency,
      mapSheet: ['BQ32'],
      project: new URL('memory://target-push/qgis/topo50/latest/topo50.json'),
      layout: 'tiff-50',
      mapSheetLayer: 'nz_topo50_map_sheet',
      source: new URL('memory://source/catalog.json'),
      dpi: 300,
      output: targetProduce,
      fromFile: undefined,
      all: false,
      format: 'pdf',
      dataTags: undefined,
      tempLocation: new URL('memory://temp-produce-cover/'),
    });

    assert.deepEqual(
      [...(await fsa.toArray(fsa.list(targetProduce)))].map((f) => f.href.replace(targetProduce.href, '')).sort(),
      [`topo50/BQ32.json`, `topo50/collection.json`, 'catalog.json'].sort(),
    );

    t.mock.method(pyRunner, 'qgisExport', async (_input: URL, output: URL) => {
      const outputFile = new URL('product/latest/BQ32.pdf', output);
      await fsa.write(outputFile, 'BQ32.pdf');
      return outputFile;
    });

    await ProduceCommand.handler({
      path: [new URL(`memory://target-produce/topo50/BQ32.json`)],
      tempLocation: new URL('memory://temp-produce/'),
      fromFile: undefined,
      force: false,
      worker: 1,
    });

    assert.deepEqual(
      [...(await fsa.toArray(fsa.list(targetProduce)))].map((f) => f.href.replace(targetProduce.href, '')).sort(),
      [
        `topo50/BQ32.json`,
        `topo50/collection.json`,
        'catalog.json',
        `topo50/BQ32.pdf`, // :tada: a export happened
      ].sort(),
    );

    const targetProducePush = new URL('memory://target-produce-push/');
    await StacPushCommand.handler({
      concurrency,
      source: new URL('memory://target-produce/catalog.json'),
      target: targetProducePush,
      category: 'product',
      strategies: [{ type: 'latest' }],
      commit: true,
    });

    assert.deepEqual(
      [...(await fsa.toArray(fsa.list(targetProducePush)))]
        .map((f) => f.href.replace(targetProducePush.href, ''))
        .sort(),
      [
        `product/topo50/latest/BQ32.json`,
        `product/topo50/latest/BQ32.pdf`,
        `product/topo50/latest/collection.json`,
        'product/topo50/catalog.json',
        'product/catalog.json',
        'catalog.json',
      ].sort(),
    );
  });
});
