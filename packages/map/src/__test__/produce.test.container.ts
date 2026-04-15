import assert from 'node:assert';
import { mkdtemp, rm } from 'node:fs/promises';
import { after, before, describe, it } from 'node:test';
import { fileURLToPath } from 'node:url';

import { fsa } from '@chunkd/fs';
import { StacBasic, StacUpdater } from '@linzjs/topographic-system-stac';
import type { StacCollection, StacItem } from 'stac-ts';
import { $ } from 'zx';

let cliLocation = '/app/index.cjs';
async function findCli(): Promise<string> {
  if (await fsa.exists(fsa.toUrl('/app/index.cjs'))) return '/app/index.cjs';
  if (await fsa.exists(fsa.toUrl('./src/index.ts'))) return './src/index.ts';
  throw new Error('Unable to find index.js');
}

async function cli(...args: (string | string[])[]): Promise<string> {
  const result = await $`node ${cliLocation} ${args.flat()}`;
  if (result.exitCode !== 0) throw new Error('Error running CLI');
  return result.stdout;
}

/**
 * If QGIS and python3 exists, actually produce a PDF
 */

describe('QGIS Process', () => {
  let tempLocation: URL = new URL('memory://empty');
  before(async () => {
    cliLocation = await findCli();
    tempLocation = fsa.toUrl((await mkdtemp('topographic-system-test')) + '/');
    baseDeployArgs.source = new URL('source/catalog.json', tempLocation);
  });

  after(async () => {
    if (tempLocation != null && tempLocation.protocol === 'file:') await rm(tempLocation, { recursive: true });
  });

  const baseDeployArgs = {
    githash: '4aba34b5accb0002867af66f6a92a35e0a4be7cab',
    commit: false,
    deployTag: 'latest',
    dataTag: 'latest',
    source: new URL('/source/catalog.json', tempLocation),
  };

  async function writeLatestAsset(name: string, source: URL): Promise<void> {
    const catalog = new URL('source/catalog.json', tempLocation);

    const collectionJson = new URL(`source/data/${name}/latest/collection.json`, tempLocation);
    await StacUpdater.readWriteJson<StacCollection>(collectionJson, () => {
      const col = StacBasic.collection();
      col.assets = { parquet: { href: `./${name}.geojson` } };
      col.extent.spatial.bbox = [[166.0, -47.5, 179.0, -34.0]];
      return col;
    });

    await fsa.write(new URL(`${name}.geojson`, collectionJson), fsa.readStream(source));

    await StacUpdater.collections(catalog, [collectionJson], true);
  }

  it('should export a png', async () => {
    const qgisProject = new URL('../../assets/beehive.qgs', import.meta.url);
    const qgisData = new URL('../../assets/beehive.geojson', import.meta.url);
    const topo50Data = new URL('../../assets/topo50.geojson', import.meta.url);

    await fsa.write(new URL('source/project/beehive.qgs', tempLocation), fsa.readStream(qgisProject));

    await writeLatestAsset('beehive', qgisData);
    await writeLatestAsset('topo50', topo50Data);

    await it('deploy', async () => {
      // Deploy the QGIS project into local files
      await cli(
        'deploy',
        ['--source', fileURLToPath(baseDeployArgs.source)],
        ['--target', fileURLToPath(new URL('target-deploy/', tempLocation))],
        fileURLToPath(new URL('source/project/beehive.qgs', tempLocation)),
      );

      const deployPath = new URL('target-deploy/', tempLocation);
      assert.deepEqual(
        [...(await fsa.toArray(fsa.list(deployPath)))].map((f) => f.href.replace(deployPath.href, '')).sort(),
        [`beehive/beehive.json`, `beehive/collection.json`, `beehive/beehive.qgs`, `catalog.json`].sort(),
      );
    });

    await it('stac-push', async () => {
      // Push stac files and assets to target location with storage strategy
      await cli(
        'stac-push',
        ['--source', fileURLToPath(new URL('target-deploy/catalog.json', tempLocation))],
        ['--target', fileURLToPath(new URL('target-deploy-push/', tempLocation))],
        ['--category', 'qgis'],
        ['--strategy', 'latest'],
        ['--strategy', `commit=${baseDeployArgs.githash}`],
        ['--commit'],
      );

      const deployPushPath = new URL('target-deploy-push/', tempLocation);
      assert.deepEqual(
        [...(await fsa.toArray(fsa.list(deployPushPath)))].map((f) => f.href.replace(deployPushPath.href, '')).sort(),
        [
          `qgis/beehive/latest/beehive.json`,
          `qgis/beehive/latest/collection.json`,
          `qgis/beehive/latest/beehive.qgs`,
          `qgis/beehive/commit_prefix=${baseDeployArgs.githash.charAt(0)}/catalog.json`,
          `qgis/beehive/commit_prefix=${baseDeployArgs.githash.charAt(0)}/commit=${baseDeployArgs.githash}/beehive.json`,
          `qgis/beehive/commit_prefix=${baseDeployArgs.githash.charAt(0)}/commit=${baseDeployArgs.githash}/collection.json`,
          `qgis/beehive/commit_prefix=${baseDeployArgs.githash.charAt(0)}/commit=${baseDeployArgs.githash}/beehive.qgs`,
          `qgis/beehive/catalog.json`,
          `qgis/catalog.json`,
          `catalog.json`,
        ].sort(),
      );
    });

    await it('produce-cover', async () => {
      await cli(
        'produce-cover',
        ['--project', fileURLToPath(new URL('target-deploy-push/qgis/beehive/latest/beehive.json', tempLocation))],
        ['--layout', 'tiff-50'],
        ['--map-sheet-layer', 'topo50'],
        ['--temp-location', fileURLToPath(new URL('temp-produce-cover/', tempLocation))],
        ['--output', fileURLToPath(new URL('target-produce/working/', tempLocation))],
        ['--format', 'png'],
        ['--dpi', '200'],
        'BQ31',
      );

      const produceCoverPath = new URL('target-produce/working/', tempLocation);
      assert.deepEqual(
        [...(await fsa.toArray(fsa.list(produceCoverPath)))]
          .map((f) => f.href.replace(produceCoverPath.href, ''))
          .sort(),
        [`beehive/BQ31.json`, `beehive/collection.json`, `catalog.json`].sort(),
      );
    });

    await it('produce', async () => {
      const targetJson = new URL('target-produce/working/beehive/BQ31.json', tempLocation);

      await cli('produce', fileURLToPath(targetJson), [
        '--temp-location',
        fileURLToPath(new URL('temp-produce/', tempLocation)),
      ]);

      const output = await fsa.readJson<StacItem>(targetJson);

      assert.ok(Object.keys(output.assets).length > 0);
      const produceCoverPath = new URL('target-produce/working/', tempLocation);
      assert.deepEqual(
        [...(await fsa.toArray(fsa.list(produceCoverPath)))]
          .map((f) => f.href.replace(produceCoverPath.href, ''))
          .sort(),
        [`beehive/BQ31.json`, `beehive/BQ31.png`, `beehive/collection.json`, `catalog.json`].sort(),
      );
    });

    await it('stac-push', async () => {
      // Push stac files and assets to target location with storage strategy
      await cli(
        'stac-push',
        ['--source', fileURLToPath(new URL('target-produce/working/catalog.json', tempLocation))],
        ['--target', fileURLToPath(new URL('target-produce-push/', tempLocation))],
        ['--category', 'product'],
        ['--strategy', 'latest'],
        ['--strategy', `commit=${baseDeployArgs.githash}`],
        ['--commit'],
      );

      const producePushPath = new URL('target-produce-push/', tempLocation);
      assert.deepEqual(
        [...(await fsa.toArray(fsa.list(producePushPath)))].map((f) => f.href.replace(producePushPath.href, '')).sort(),
        [
          `product/beehive/latest/BQ31.json`,
          `product/beehive/latest/BQ31.png`,
          `product/beehive/latest/collection.json`,
          `product/beehive/commit_prefix=${baseDeployArgs.githash.charAt(0)}/catalog.json`,
          `product/beehive/commit_prefix=${baseDeployArgs.githash.charAt(0)}/commit=${baseDeployArgs.githash}/BQ31.json`,
          `product/beehive/commit_prefix=${baseDeployArgs.githash.charAt(0)}/commit=${baseDeployArgs.githash}/BQ31.png`,
          `product/beehive/commit_prefix=${baseDeployArgs.githash.charAt(0)}/commit=${baseDeployArgs.githash}/collection.json`,
          `product/beehive/catalog.json`,
          `product/catalog.json`,
          `catalog.json`,
        ].sort(),
      );
    });
  });
});
