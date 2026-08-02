import assert from 'node:assert';
import { mkdtemp, rm } from 'node:fs/promises';
import { after, before, describe, it } from 'node:test';
import { fileURLToPath } from 'node:url';

import { fsa, HashTransform } from '@chunkd/fs';
import { StacBasic, StacUpdater } from '@linzjs/topographic-system-stac';
import sharp from 'sharp';
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

  async function writeLatestAsset(fileName: string, source: URL): Promise<void> {
    const [name] = fileName.split('.');
    const catalog = new URL('source/catalog.json', tempLocation);
    const collectionJson = new URL(`source/data/${name}/latest/collection.json`, tempLocation);
    const ht = new HashTransform('sha256');

    const stream = fsa.readStream(source).pipe(ht);
    await fsa.write(new URL(fileName, collectionJson), stream);

    await StacUpdater.readWriteJson<StacCollection>(collectionJson, () => {
      const col = StacBasic.collection();
      col.assets = { parquet: { href: `./${fileName}`, 'file:size': ht.bytesRead, 'file:checksum': ht.multihash } };
      col.extent.spatial.bbox = [[166.0, -47.5, 179.0, -34.0]];
      return col;
    });

    await StacUpdater.collections(catalog, [collectionJson], true);
  }

  it('should export a png', async () => {
    const qgisProject = new URL('../../assets/project/beehive.qgs', import.meta.url);
    const qgisData = new URL('../../assets/project/beehive.geojson', import.meta.url);
    const topo50Data = new URL('../../assets/project/nztopo50_map_sheet.parquet', import.meta.url);
    const cartoTextData = new URL('../../assets/project/nztopo50_carto_text.parquet', import.meta.url);
    const trigPointData = new URL('../../assets/project/trig_point.parquet', import.meta.url);
    const fonts = new URL('../../assets/fonts/', import.meta.url);

    await fsa.write(new URL('source/project/beehive.qgs', tempLocation), fsa.readStream(qgisProject));

    await writeLatestAsset('beehive.geojson', qgisData);
    await writeLatestAsset('nztopo50_map_sheet.parquet', topo50Data);
    await writeLatestAsset('nztopo50_carto_text.parquet', cartoTextData);
    await writeLatestAsset('trig_point.parquet', trigPointData);

    await it('deploy', async () => {
      // Deploy the QGIS project into local files
      await cli(
        'deploy',
        ['--extra-assets', fileURLToPath(fonts)],
        ['--source', fileURLToPath(baseDeployArgs.source)],
        ['--target', fileURLToPath(new URL('target-deploy/', tempLocation))],
        fileURLToPath(new URL('source/project/beehive.qgs', tempLocation)),
      );

      const deployPath = new URL('target-deploy/', tempLocation);
      assert.deepEqual(
        [...(await fsa.toArray(fsa.list(deployPath)))].map((f) => f.href.replace(deployPath.href, '')).sort(),
        [
          `beehive/beehive.json`,
          `beehive/collection.json`,
          `beehive/beehive.qgs`,
          'beehive/beehive.tar.zst',
          `catalog.json`,
        ].sort(),
      );
    });

    await it('stac-push', async () => {
      // Push stac files and assets to target location with storage strategy
      await cli(
        'stac-push',
        ['--source', fileURLToPath(new URL('target-deploy/catalog.json', tempLocation))],
        ['--target', fileURLToPath(new URL('target-deploy-push/', tempLocation))],
        ['--category', 'qgis'],
        ['--strategy', `latest,commit=${baseDeployArgs.githash}`],
        ['--commit'],
      );

      const deployPushPath = new URL('target-deploy-push/', tempLocation);
      assert.deepEqual(
        [...(await fsa.toArray(fsa.list(deployPushPath)))].map((f) => f.href.replace(deployPushPath.href, '')).sort(),
        [
          `qgis/beehive/latest/beehive.json`,
          `qgis/beehive/latest/collection.json`,
          'qgis/beehive/latest/beehive.tar.zst',
          `qgis/beehive/latest/beehive.qgs`,
          `qgis/beehive/commit_prefix=${baseDeployArgs.githash.charAt(0)}/catalog.json`,
          `qgis/beehive/commit_prefix=${baseDeployArgs.githash.charAt(0)}/commit=${baseDeployArgs.githash}/beehive.json`,
          `qgis/beehive/commit_prefix=${baseDeployArgs.githash.charAt(0)}/commit=${baseDeployArgs.githash}/beehive.tar.zst`,
          `qgis/beehive/commit_prefix=${baseDeployArgs.githash.charAt(0)}/commit=${baseDeployArgs.githash}/collection.json`,
          `qgis/beehive/commit_prefix=${baseDeployArgs.githash.charAt(0)}/commit=${baseDeployArgs.githash}/beehive.qgs`,
          `qgis/beehive/catalog.json`,
          `qgis/catalog.json`,
          `catalog.json`,
        ].sort(),
      );
    });

    await it('prepare', async () => {
      await cli(
        'prepare',
        ['--project', fileURLToPath(new URL('target-deploy-push/qgis/beehive/latest/beehive.json', tempLocation))],
        ['--layout', 'tiff-50'],
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

    await it('export', async () => {
      const targetJson = new URL('target-produce/working/beehive/BQ31.json', tempLocation);
      await cli('export', fileURLToPath(targetJson), [
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

    await it('should compare the PNG', async () => {
      const before = await fsa.read(new URL('../../assets/BQ31.png', import.meta.url));
      const producePushPath = new URL('target-produce-push/', tempLocation);
      const after = await fsa.read(new URL('product/beehive/latest/BQ31.png', producePushPath));

      const difference = await sharp(before)
        .removeAlpha()
        .composite([{ input: await sharp(after).removeAlpha().toBuffer(), blend: 'difference' }])
        .toBuffer();

      const stats = await sharp(difference).stats();

      if (stats.channels.slice(0, 3).find((f) => f.max > 0)) {
        await sharp(before).png().toFile('produce.test.before.png');
        await sharp(after).png().toFile('produce.test.after.png');
        await sharp(difference).png().toFile('produce.test.diff.png');
      }

      // No changes in RGB channels
      assert.deepEqual(stats.channels[0]?.max, 0);
      assert.deepEqual(stats.channels[1]?.max, 0);
      assert.deepEqual(stats.channels[2]?.max, 0);
    });
  });
});
