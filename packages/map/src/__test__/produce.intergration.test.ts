import assert from 'node:assert';
import { before, describe, it } from 'node:test';

import { fsa, FsMemory } from '@chunkd/fs';
import { StacPushCommand } from '@linzjs/topographic-system-stac';
import type { StacItem } from 'stac-ts';

import { DeployCommand } from '../cli/action.deploy.ts';
import { ExportCommand } from '../cli/action.export.ts';
import { PrepareCommand } from '../cli/action.prepare.ts';
import { BaseCommandOptions, pyRunner } from '../python.runner.ts';
import { writeBaseLayers } from './util.ts';

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
    const rootCatalog = new URL('memory://source/catalog.json');
    await writeBaseLayers(rootCatalog);

    // Deploy the QGIS project into memory
    const targetDeploy = new URL('memory://target-deploy/');
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

    const targetProduce = new URL('memory://target-produce/');
    await PrepareCommand.handler({
      concurrency,
      mapSheet: ['BQ32'],
      project: new URL('memory://target-push/qgis/topo50/latest/topo50.json'),
      layout: 'tiff-50',
      mapSheetDataset: undefined,
      source: new URL('memory://source/catalog.json'),
      dpi: 300,
      output: targetProduce,
      fromFile: undefined,
      all: false,
      format: 'pdf',
      dataTags: undefined,
      cache: new URL('memory://temp-cache/'),
      tempLocation: new URL('memory://temp-produce-cover/'),
      export: false,
    });

    assert.deepEqual(
      [...(await fsa.toArray(fsa.list(targetProduce)))].map((f) => f.href.replace(targetProduce.href, '')).sort(),
      [`topo50/BQ32.json`, `topo50/collection.json`, 'catalog.json'].sort(),
    );

    const bq32Json = await fsa.readJson<StacItem>(new URL('topo50/BQ32.json', targetProduce));
    assert.equal(bq32Json.properties['proj:epsg'], 2193);
    assert.equal(bq32Json.properties['linz:mapsheet'], 'BQ32');
    assert.deepEqual(bq32Json.properties['linz_topographic_system:options'], {
      layout: 'tiff-50',
      mapSheetDataset: 'nztopo50_map_sheet.parquet',
      dpi: 300,
      format: 'pdf',
    });

    t.mock.method(pyRunner, 'qgisExport', async (_input: URL, output: URL) => {
      const outputFile = new URL('product/latest/BQ32.pdf', output);
      await fsa.write(outputFile, 'BQ32.pdf');
      return outputFile;
    });

    await ExportCommand.handler({
      path: [new URL(`memory://target-produce/topo50/BQ32.json`)],
      cache: new URL('memory://temp-cache/'),
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
    const exportUrl = new URL(`memory://target-produce/topo50/BQ32.json`);
    const exportedJson = await fsa.readJson<StacItem>(exportUrl);
    const dateLinks = exportedJson.links.filter((f) => f.href.includes('year=2026/'));
    // Ensure the water data was linked to the canonical path
    assert.deepEqual(dateLinks[0], {
      rel: 'source',
      href: 'memory://source/data/road_line/year=2026/date=2026-06-01T14-32-00.123Z/collection.json',
      type: 'application/json',
      title: 'Topographic road_line',
    });
    assert.deepEqual(
      dateLinks[1],

      {
        rel: 'source',
        href: 'memory://source/data/water/year=2026/date=2026-06-01T14-32-00.123Z/collection.json',
        type: 'application/json',
        title: 'Topographic water',
      },
    );

    assert.deepEqual(dateLinks[2], {
      rel: 'source',
      href: 'memory://source/data/nztopo50_map_sheet/year=2026/date=2026-06-01T14-32-00.123Z/collection.json',
      type: 'application/json',
      title: 'Topographic nztopo50_map_sheet',
    });
    assert.equal(dateLinks.length, 3);
  });
});
