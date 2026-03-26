import assert from 'node:assert';
import process from 'node:process';
import { describe, it } from 'node:test';
import { fileURLToPath } from 'node:url';

import { fsa } from '@chunkd/fs';
import { $ } from 'zx';
const uid = process.geteuid?.();

const kartContainer = process.argv.find((f) => f.startsWith('--container-kart='))?.split('=')[1] ?? 'ts-kart';
const mapContainer = process.argv.find((f) => f.startsWith('--container-map='))?.split('=')[1] ?? 'ts-map';

const targetFolder = fsa.toUrl(`./target/`);
const sourceAssets = fileURLToPath(new URL('./assets/', import.meta.url));

async function skipIfExists(url: URL) {
  const exists = await fsa.exists(url);
  if (exists) return { skip: true };
  return {};
}

async function runContainer(containerName: string, ...args: (string[] | string)[]) {
  console.log(`run: ${containerName} - ${JSON.stringify(args)}`);
  const ret = await $`docker run \
    --rm  \
    -v ${fileURLToPath(targetFolder)}:/target \
    -v ${sourceAssets}:/assets \
    ${containerName} ${args.flat()}`;
  if (process.argv.includes('--verbose')) console.log(`\t${ret.stdout}`);
  if (ret.exitCode !== 0) throw new Error(`Failed: ${containerName}`);
  return ret;
}

const tsKart = runContainer.bind(null, kartContainer);
const tsMap = runContainer.bind(null, mapContainer);
const tsArgo = runContainer.bind(null, 'ghcr.io/linz/argo-tasks:latest');

const commitId = `916356eaf4463a563ac77b4f06448ade556f306a`;

if ((await fsa.exists(targetFolder)) && process.argv.includes('--remove')) {
  console.log(`Removing ${targetFolder}`);
  await $`rm -fr ${fileURLToPath(targetFolder)}`;
}

describe('topographic-system.e2e', async () => {
  await it('should ensure the output folder exists', async () => {
    await $`mkdir -p ${fileURLToPath(targetFolder)}`;
  });

  await describe('kart', async () => {
    await it(
      'should clone topographic-test-data',
      await skipIfExists(new URL('source/topographic-test-data', targetFolder)),
      async () => {
        await tsKart(
          `clone`,
          `https://github.com/linz/topographic-test-data.git`,
          `/target/source/topographic-test-data`,
        );

        assert.ok(await fsa.exists(new URL('source/topographic-test-data/', targetFolder)));
      },
    );

    await it(
      'should export topographic-test-data',
      await skipIfExists(new URL('source/topographic-test-data-export', targetFolder)),
      async () => {
        await tsKart(
          `export`,
          ['-C', `/target/source/topographic-test-data`],
          ['--output', '/target/source/topographic-test-data-export'],
        );
        assert.ok(await fsa.exists(new URL('source/topographic-test-data-export', targetFolder)));
      },
    );

    await it(
      'should convert topographic-test-data to parquet',
      await skipIfExists(new URL('bucket/data/catalog.json', targetFolder)),
      async () => {
        await tsKart(
          `to-parquet`,
          '/target/source/topographic-test-data-export',
          ['--temp-location', '/target/temp/kart.to-parquet'],
          ['--output', '/target/bucket/'],
          ['--strategy', 'latest'],
          ['--strategy', `commit=${commitId}`],
        );
        assert.ok(await fsa.exists(new URL('bucket/data/catalog.json', targetFolder)));
        // TODO load catalog and validate
      },
    );
  });

  await describe('map', async () => {
    const qgisTarget = new URL('bucket/qgis/catalog.json', targetFolder);
    await it('should deploy the testing qgis project', await skipIfExists(qgisTarget), async () => {
      await tsMap(
        'deploy',
        '/assets/topo-test.qgs',
        ['--strategy', 'latest'],
        ['--strategy', `commit=${commitId}`],
        ['--target', '/target/bucket/'],
        '--commit',
      );
    });

    await it('should produce map sheets', async () => {
      await tsMap(
        'produce-cover',
        ['--project', '/target/bucket/qgis/topo-test/latest/topo-test.json'], // TODO allow collection.json
        ['--output', '/target/bucket/'],
        ['--map-sheet-layer', 'testmapsheet'],
        ['--strategy', 'latest'],
        ['--format', 'geotiff'],
        ['--dpi', '120'],
        ['BQ26', 'BQ27'],
      );

      await tsMap(
        'produce',
        '/target/bucket/product/topo-test/latest/BQ26.json',
        '/target/bucket/product/topo-test/latest/BQ27.json',
      );

      assert.ok(await fsa.exists(new URL('bucket/product/topo-test/latest/BQ26.tiff', targetFolder)));
      assert.ok(await fsa.exists(new URL('bucket/product/topo-test/latest/BQ27.tiff', targetFolder)));
      // TODO validate STAC catalog and validate
    });
  });

  await describe('stac.validate', async () => {
    it('should validate stac', async () => {
      await tsArgo('stac-validate', '--recursive', '/target/bucket/catalog.json')
    })
  } )
});
