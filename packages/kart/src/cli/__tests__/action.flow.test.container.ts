import assert from 'node:assert';
import { mkdir, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import path from 'node:path';
import { after, before, describe, it } from 'node:test';
import { fileURLToPath, pathToFileURL } from 'node:url';

import { fsa } from '@chunkd/fs';
import { logger, stringToUrlFolder } from '@linzjs/topographic-system-shared';
import { $ } from 'zx';

/**
 * Run a kart CLI command through the bundled entrypoint, exactly as
 * production does (`node /app/index.cjs <subcommand> ...`).
 *
 * @param args the CLI arguments to pass through
 * @returns the standard output from the command execution, for assertions in tests
 */
async function cli(args: string[]): Promise<string> {
  const result = await $`node /app/index.cjs ${args}`;
  return result.stdout;
}

/**
 * Create a minimal kart repository for testing:
 *  - A single dataset with one point feature, imported from GeoJSON → GPKG
 *  - Initialized as a bare kart repo with one commit on master
 * @param baseDir The base directory under which to create the repo (e.g. a temp directory)
 * @returns The URL of the created bare kart repository
 */
async function createFixtureRepo(baseDir: URL): Promise<URL> {
  const seedGeojson = new URL('seed.geojson', baseDir);
  const seedGpkg = new URL('seed.gpkg', baseDir);
  const bareRepo = new URL('fixture.kart/', baseDir);
  const gitEnv = {
    GIT_AUTHOR_NAME: process.env['GIT_AUTHOR_NAME'] ?? 'kart-test',
    GIT_AUTHOR_EMAIL: process.env['GIT_AUTHOR_EMAIL'] ?? 'kart-test@localhost',
    GIT_COMMITTER_NAME: process.env['GIT_COMMITTER_NAME'] ?? 'kart-test',
    GIT_COMMITTER_EMAIL: process.env['GIT_COMMITTER_EMAIL'] ?? 'kart-test@localhost',
  };

  await fsa.write(
    seedGeojson,
    JSON.stringify({
      type: 'FeatureCollection',
      features: [
        {
          type: 'Feature',
          properties: { fid: 1, name: 'linz' },
          geometry: { type: 'Point', coordinates: [174.7794, -41.2809] },
        },
      ],
    }),
  );

  await $`ogr2ogr -f GPKG ${fileURLToPath(seedGpkg)} ${fileURLToPath(seedGeojson)} -nln test_points`;
  await $({
    env: { ...process.env, ...gitEnv },
  })`kart init --import ${`GPKG:${fileURLToPath(seedGpkg)}`} ${fileURLToPath(bareRepo)} --bare -b master`;

  return bareRepo;
}

describe('action.flow integration', () => {
  const tempDir = stringToUrlFolder(path.join(tmpdir(), 'kart-flow-integration'));
  const repoUrl = new URL('repo/', tempDir);
  const diffUrl = new URL('diff/', tempDir);
  const summaryUrl = new URL('pr_summary.md', tempDir);
  const exportUrl = new URL('export/', tempDir);
  const parquetUrl = new URL('parquet/', tempDir);
  const outputUrl = new URL('s3-output/', tempDir);
  const validationUrl = new URL('validation-output/', tempDir);

  before(async () => {
    await mkdir(fileURLToPath(tempDir), { recursive: true });
    logger.debug({ tempDir: tempDir.href }, 'Created temporary directory for test');

    // Create a bare kart repo with a single dataset, then clone + fetch.
    const bareRepo = await createFixtureRepo(new URL('source/', tempDir));
    await $`kart clone ${bareRepo.href} --no-checkout ${repoUrl.pathname}`;
    await $`kart -C ${repoUrl.pathname} fetch origin master`;

    const datasets = await $`kart -C ${repoUrl.pathname} data ls`;
    assert.ok(datasets.stdout.includes('test_points'), 'fixture should contain test_points dataset');
  });

  after(async () => {
    logger.info({ tempDir }, 'Cleaning up temporary directory after test');
    await rm(tempDir, { recursive: true, force: true });
  });

  it('should get a version string when running step 1 - version', async () => {
    const output = await cli(['version']);
    assert.ok(/\d+\.\d+\.\d+/.test(output), `Expected version string in output, got: ${output}`);
  });

  it('should be able to clone a public repo in step 2 - clone', async () => {
    const outputLocation = new URL('temp-test-repo', tempDir);
    await cli(['clone', 'linz/topographic-test-data', fileURLToPath(outputLocation), '--ref', 'master']);

    const clonedDatasets = await $`kart -C ${fileURLToPath(outputLocation)} data ls`;
    assert.ok(clonedDatasets.stdout.includes('test'), 'cloned repo should contain one or more test datasets');
  });

  it('it should produce a summary file in step 3 - diff', async () => {
    await cli([
      'diff',
      '--context',
      fileURLToPath(repoUrl),
      '--output',
      fileURLToPath(diffUrl),
      '--summary-file',
      fileURLToPath(summaryUrl),
    ]);

    const md = await fsa.read(summaryUrl);
    assert.ok(md.length > 0, 'pr_summary.md should have content');
    assert.ok(md.toString().includes('# Changes Summary'), 'summary should contain expected markdown header');
  });

  it('should produce gpkg datasets in step 5 - export', async () => {
    await cli([
      'export',
      '--context',
      fileURLToPath(repoUrl),
      '--output',
      fileURLToPath(exportUrl),
      '--ref',
      'FETCH_HEAD',
    ]);

    const files = await fsa.toArray(fsa.list(exportUrl));
    assert.ok(
      files.some((f) => f.href.endsWith('.gpkg')),
      `Expected .gpkg files in ${exportUrl.href}, got: ${files.map((f) => f.href).join(', ')}`,
    );
  });

  it('should convert gpkg to parquet and produce STAC in step 6 - to-parquet', async () => {
    await cli([
      'to-parquet',
      '--output',
      fileURLToPath(outputUrl),
      '--temp-location',
      fileURLToPath(parquetUrl),
      fileURLToPath(exportUrl),
    ]);

    const parquetFiles = await fsa.toArray(fsa.list(parquetUrl));
    assert.ok(
      parquetFiles.some((f) => f.href.endsWith('.parquet')),
      `Expected .parquet in ${parquetUrl.href}, got: ${parquetFiles.map((f) => f.href).join(', ')}`,
    );

    const catalogUrl = new URL('catalog.json', outputUrl);
    const catalog = await fsa.readJson(catalogUrl);
    assert.ok(catalog, 'catalog.json should exist in output');
  });

  it('should validate parquet files in step 7 - validate', async () => {
    const dbPath = new URL('files.parquet', parquetUrl);
    const configFile = pathToFileURL('/packages/validation/config/default_config.json');

    await cli([
      'validate',
      '--output',
      fileURLToPath(outputUrl),
      '--db-path',
      fileURLToPath(dbPath),
      '--config-file',
      fileURLToPath(configFile),
      '--output-dir',
      fileURLToPath(validationUrl),
    ]);

    const validationFiles = await fsa.toArray(fsa.list(validationUrl));
    assert.ok(validationFiles.length > 0, `Expected validation output in ${validationUrl.href}`);
  });
});
