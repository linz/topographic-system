import assert from 'node:assert';
import { mkdtemp, rm } from 'node:fs/promises';
import { after, before, describe, it } from 'node:test';

import { fsa, FsMemory } from '@chunkd/fs';
import { CliId } from '@linzjs/topographic-system-shared';
import type { StacCatalog } from 'stac-ts';

import { DeployCommand } from '../cli/action.deploy.ts';
import { ProduceCoverCommand } from '../cli/action.produce.cover.ts';
import { ProduceCommand } from '../cli/action.produce.ts';
import { BaseCommandOptions, pyRunner } from '../python.runner.ts';

/**
 * If QGIS and python3 exists, actually produce a PDF
 */

describe('QGIS Process', () => {
  const mem = new FsMemory();

  let tempLocation: URL | null = null;
  before(async () => {
    fsa.register('memory://', mem);
    BaseCommandOptions.useDocker = true;
  });

  after(async () => {
    BaseCommandOptions.useDocker = false;
    if (tempLocation != null) await rm(tempLocation, { recursive: true });
  });

  it('should ensure base container matches the Dockerfile', async () => {
    const dockerFile = new URL('../../Dockerfile', import.meta.url);
    const from = String(await fsa.read(dockerFile))
      .split('\n')
      .find((f) => f.toLowerCase().startsWith('from'));
    assert.equal(from, `FROM ${BaseCommandOptions.container}`);
  });

  const baseDeployArgs = {
    githash: undefined,
    commit: false,
    deployTag: 'latest',
    dataTag: 'latest',
    source: new URL('memory://source/catalog.json'),
  };

  async function writeLatestAsset(name: string, source: URL): Promise<void> {
    const catalog = fsa.toUrl('memory://source/catalog.json');
    const existingCatalog: StacCatalog = (await fsa.readJson(catalog).catch(() => {
      return { links: [] };
    })) as StacCatalog;
    existingCatalog.links.push({ rel: 'child', href: `./${name}/latest/collection.json` });

    await fsa.write(catalog, JSON.stringify(existingCatalog));
    await fsa.write(
      fsa.toUrl(`memory://source/${name}/latest/collection.json`),
      JSON.stringify({
        assets: { parquet: { href: `./${name}.geojson` } },
      }),
    );
    await fsa.write(fsa.toUrl(`memory://source/${name}/latest/${name}.geojson`), fsa.readStream(source));
  }

  it('should deploy a qgs file', async (t) => {
    tempLocation = fsa.toUrl((await mkdtemp('topographic-system-test')) + '/');

    const qgisProject = new URL('../../assets/beehive.qgs', import.meta.url);
    const qgisData = new URL('../../assets/beehive.geojson', import.meta.url);
    const topo50Data = new URL('../../assets/topo50.geojson', import.meta.url);

    await fsa.write(new URL('source/beehive/beehive.qgs', tempLocation), fsa.readStream(qgisProject));

    await writeLatestAsset('beehive', qgisData);
    await writeLatestAsset('topo50', topo50Data);

    // Deploy the QGIS project into memory
    await DeployCommand.handler({
      ...baseDeployArgs,
      project: new URL('source/', tempLocation),
      target: new URL('memory://target-deploy/'),
      commit: true,
    });

    // Testing mapsheet doesnt have the export cover logic
    t.mock.method(pyRunner, 'qgisExportCover', () => {
      return [{ sheetCode: 'BQ31', epsg: 2193, bbox: [1756000, 5406000, 1780000, 5442000] }];
    });

    await ProduceCoverCommand.handler({
      mapSheet: ['BQ31'],
      project: new URL('memory://target-deploy/qgis/latest/beehive/beehive.json'),
      layout: 'tiff-50',
      mapSheetLayer: 'topo50',
      source: baseDeployArgs.source,
      dpi: 300,
      output: new URL('memory://target-produce/working/'),
      fromFile: undefined,
      all: false,
      format: 'png',
      dataTags: undefined,
      tempLocation: new URL('memory://temp-produce-cover/'),
    });

    await ProduceCommand.handler({
      path: [new URL(`memory://target-produce/working/${CliId}/BQ31.json`)],
      tempLocation: new URL('temp-produce/', tempLocation),
      fromFile: undefined,
      force: false,
    });
  });
});
