import assert from 'node:assert';
import process from 'node:process';
import { describe, it } from 'node:test';
import { fileURLToPath } from 'node:url';

import { fsa } from '@chunkd/fs';
import { $, ProcessOutput } from 'zx';

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
  console.log(`run: ${containerName}: `);
  for (const arg of args) console.log(`\t${Array.isArray(arg) ? arg.join('=') : arg}`);

  try {
    const ret = await $`docker run \
    --rm \
    -v ${fileURLToPath(targetFolder)}:/target \
    -v ${sourceAssets}:/assets \
    ${containerName} ${args.flat()}`.catch((e) => e);
    if (process.argv.includes('--verbose')) console.log(`\t${ret.stdout}`);
    if (ret.exitCode !== 0) {
      console.log(ret.stdout);
      throw new Error(`Failed: ${containerName}`);
    }
    return ret;
  } catch (e) {
    if (e instanceof ProcessOutput) {
      console.log(e.stdout);
    }
    throw e;
  }
}

const tsKart = runContainer.bind(null, kartContainer);
const tsMap = runContainer.bind(null, mapContainer);
const tsArgo = runContainer.bind(null, 'ghcr.io/linz/argo-tasks:latest');

if ((await fsa.exists(targetFolder)) && process.argv.includes('--remove')) {
  console.log(`Removing ${targetFolder}`);
  await $`rm -fr ${fileURLToPath(targetFolder)}`;
}

describe('topographic-system.e2e', async () => {
  await it('should pull all containers', async () => {
    await Promise.all([tsKart('version'), tsMap('version')]);
  });

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

    const parquetTempOutput = new URL('temp/kart.to-parquet/output/catalog.json', targetFolder);
    await it('should convert topographic-test-data to parquet', await skipIfExists(parquetTempOutput), async () => {
      await tsKart(
        `to-parquet`,
        '/target/source/topographic-test-data-export',
        ['--output', '/target/temp/kart.to-parquet/output'],
        ['--temp-location', '/target/temp/kart.to-parquet/temp'],
      );
      assert.ok(await fsa.exists(parquetTempOutput));
      // TODO load catalog and validate
    });

    await it('should validate the parquet schemas', async () => {
      await tsKart(
        `validate-schema`,
        ['--schema', '/assets/testline.json'],
        ['/target/temp/kart.to-parquet/testline.parquet'],
      );
    });

    const commitId = `916356eaf4463a563ac77b4f06448ade556f306a`;
    await it('should push the parquet stac files and assets', await skipIfExists(parquetTempOutput), async () => {
      await tsMap(
        'stac-push',
        ['--source', '/target/temp/kart.to-parquet/output/catalog.json'],
        ['--target', '/target/bucket/'],
        ['--category', 'data'],
        ['--strategy', 'latest'],
        ['--strategy', `commit=${commitId}`],
        '--commit',
      );
      assert.ok(await fsa.exists(new URL('bucket/catalog.json', targetFolder)));
      assert.ok(await fsa.exists(new URL('bucket/data/testline/commit_prefix=9/catalog.json', targetFolder)));
      assert.ok(await fsa.exists(new URL('bucket/data/testline/latest/testline.parquet', targetFolder)));
    });
  });

  await describe('map', async () => {
    const qgisDeploy = new URL('source/qgis/', targetFolder);
    await it('should deploy the testing qgis project', await skipIfExists(qgisDeploy), async () => {
      await tsMap(
        'deploy',
        '/assets/topo-test.qgs',
        ['--target', '/target/temp/qgis/'],
        ['--source', '/target/bucket/data/catalog.json'],
      );
    });

    const commitId = `916356eaf4463a563ac77b4f06448ade556f306a`;
    const qgisTarget = new URL('bucket/qgis/catalog.json', targetFolder);
    await it('should push the stac files and assets', await skipIfExists(qgisTarget), async () => {
      await tsMap(
        'stac-push',
        ['--source', '/target/temp/qgis/catalog.json'],
        ['--target', '/target/bucket/'],
        ['--category', 'qgis'],
        ['--strategy', 'latest'],
        ['--strategy', `commit=${commitId}`],
        '--commit',
      );
      assert.ok(await fsa.exists(new URL('bucket/catalog.json', targetFolder)));
      assert.ok(await fsa.exists(new URL('bucket/qgis/topo-test/commit_prefix=9/catalog.json', targetFolder)));
      assert.ok(await fsa.exists(new URL('bucket/qgis/topo-test/latest/topo-test.json', targetFolder)));
    });

    await it('should produce map sheets', async () => {
      await tsMap(
        'produce-cover',
        ['--project', '/target/bucket/qgis/topo-test/latest/topo-test.json'], // TODO allow collection.json
        ['--output', '/target/produce/'],
        ['--map-sheet-layer', 'testmapsheet'],
        ['--format', 'png'],
        ['--dpi', '120'],
        'BQ26',
        'BQ27',
      );

      await tsMap('produce', '/target/produce/topo-test/BQ26.json', '/target/produce/topo-test/BQ27.json');

      assert.ok(await fsa.exists(new URL('produce/topo-test/BQ26.png', targetFolder)));
      assert.ok(await fsa.exists(new URL('produce/topo-test/BQ27.png', targetFolder)));

      await tsMap(
        'stac-push',
        ['--source', '/target/produce/catalog.json'],
        ['--target', '/target/bucket/'],
        ['--category', 'product'],
        ['--strategy', 'latest'],
        '--commit',
      );

      assert.ok(await fsa.exists(new URL('bucket/product/topo-test/latest/BQ26.png', targetFolder)));
      assert.ok(await fsa.exists(new URL('bucket/product/topo-test/latest/BQ27.png', targetFolder)));
      // TODO validate STAC catalog and validate
    });
  });

  await describe('stac.validate', async () => {
    it('should validate stac', async () => {
      await tsArgo(
        'stac-validate',
        '--recursive',
        '--checksum-assets',
        '--checksum-links',
        '/target/bucket/catalog.json',
      );
    });
  });

  await describe('kart.update', async () => {
    const parquetTempOutput = new URL('temp/kart.update/output/catalog.json', targetFolder);
    await it('should convert topographic-test-data to parquet', await skipIfExists(parquetTempOutput), async () => {
      await tsKart(
        `to-parquet`,
        '/target/source/topographic-test-data-export',
        ['--output', '/target/temp/kart.update/output'],
        ['--temp-location', '/target/temp/kart.update/temp'],
      );
      assert.ok(await fsa.exists(parquetTempOutput));
      // TODO load catalog and validate
    });

    const newCommitId = `0f2cbb026964df12cfe4e56ffa94af2f4d9ed90e`;
    await it(
      'should push the parquet stac files and assets',
      await skipIfExists(new URL('bucket/catalog.json', targetFolder)),
      async () => {
        await tsMap(
          'stac-push',
          ['--source', '/target/temp/kart.update/output/catalog.json'],
          ['--target', '/target/bucket/'],
          ['--category', 'data'],
          ['--strategy', 'latest'],
          ['--strategy', `commit=${newCommitId}`],
          '--commit',
        );
        assert.ok(await fsa.exists(new URL('bucket/catalog.json', targetFolder)));
        assert.ok(await fsa.exists(new URL('bucket/data/testline/commit_prefix=0/catalog.json', targetFolder)));
      },
    );

    await it('should validate stac', async () => {
      await tsArgo(
        'stac-validate',
        '--recursive',
        '--checksum-assets',
        '--checksum-links',
        '/target/bucket/catalog.json',
      );
    });
  });
});
