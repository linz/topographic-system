import assert from 'node:assert';
import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import path from 'node:path';
import { after, before, describe, it } from 'node:test';
import { fileURLToPath, pathToFileURL } from 'node:url';

import { fsa } from '@chunkd/fs';
import { logger, stringToUrlFolder } from '@linzjs/topographic-system-shared';
import { $ } from 'zx';

const requireTools = process.env['CI_REQUIRE_TOOLS'] === 'true';

async function toolVersion(bin: string, args: string[] = ['--version']): Promise<string | null> {
  try {
    const result = await $({ quiet: true })`${bin} ${args}`;
    return result.stdout.trim();
  } catch {
    return null;
  }
}

/**
 * If `CI_REQUIRE_TOOLS` is set, a missing tool throws immediately
 * so CI never silently skips. Locally the test suite gracefully skips.
 */
function skipOrFail(missing: string[]): string | false {
  if (missing.length === 0) return false;
  const msg = `Missing tools: ${missing.join(', ')}`;
  if (requireTools) throw new Error(`${msg} (CI_REQUIRE_TOOLS is set – tools must be present)`);
  return msg;
}

const hasKart = await toolVersion('kart');
const hasUv = await toolVersion('uv');
const hasOgr2ogr = await toolVersion('ogr2ogr');
const hasNode = await toolVersion('node');
const hasGit = await toolVersion('git');

// to-parquet uses SORT_BY_BBOX and COVERING_BBOX_NAME LCOs which require GDAL ≥ 3.13
const gdalVersion = hasOgr2ogr?.match(/GDAL (\d+\.\d+)/)?.[1] ?? '0.0';
const hasGdal313 = parseFloat(gdalVersion) >= 3.13;

/**
 * Resolve the kart CLI entrypoint.
 *
 * Precedence:
 *  1. `KART_ENTRYPOINT` env var  (CI sets this to `/app/index.cjs`)
 *  2. Local CJS bundle           (`./packages/kart/dist/index.cjs`)
 *  3. TypeScript source           (`./packages/kart/src/index.ts` — Node 24+)
 */
async function resolveEntrypoint(): Promise<string | null> {
  const candidates = [process.env['KART_ENTRYPOINT'], './packages/kart/dist/index.cjs', './packages/kart/src/index.ts'];
  for (const candidate of candidates) {
    if (candidate == null) continue;
    if (await toolVersion('node', [candidate, 'version'])) return candidate;
  }
  return null;
}

const ENTRYPOINT = await resolveEntrypoint();
const hasEntrypoint = ENTRYPOINT != null;
/**
 * Run a kart CLI command through the bundled entrypoint, exactly as
 * production does (`node /app/index.cjs <subcommand> ...`).
 */
async function cli(args: string[]): Promise<string> {
  assert.ok(ENTRYPOINT, 'ENTRYPOINT must be resolved before calling cli()');
  const result = await $`node ${ENTRYPOINT} ${args}`;
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
  await $`kart init --import ${`GPKG:${fileURLToPath(seedGpkg)}`} ${fileURLToPath(bareRepo)} --bare -b master`;

  return bareRepo;
}

const skipSuite = skipOrFail(
  [
    !hasKart && 'kart',
    !hasOgr2ogr && 'ogr2ogr',
    !hasUv && 'uv',
    !hasGit && 'git',
    !hasNode && 'node',
    !hasEntrypoint && 'entrypoint',
  ].filter((v): v is string => v !== false),
);
const skipParquet = skipOrFail(!hasGdal313 ? [`ogr2ogr ≥ 3.13 (have ${gdalVersion})`] : []);

describe('action.flow integration', { skip: skipSuite }, () => {
  let tempDir: URL;

  let repoUrl: URL;
  let diffUrl: URL;
  let summaryUrl: URL;
  let exportUrl: URL;
  let parquetUrl: URL;
  let outputUrl: URL;
  let validationUrl: URL;

  before(async () => {
    tempDir = stringToUrlFolder(await mkdtemp(path.join(tmpdir(), 'kart-flow-integration-')));
    logger.debug({ tempDir: tempDir.href }, 'Created temporary directory for test');
    repoUrl = new URL('repo/', tempDir);
    diffUrl = new URL('diff/', tempDir);
    summaryUrl = new URL('pr_summary.md', tempDir);
    exportUrl = new URL('export/', tempDir);
    parquetUrl = new URL('parquet/', tempDir);
    outputUrl = new URL('s3-output/', tempDir);
    validationUrl = new URL('validation-output/', tempDir);

    // Create a bare kart repo with a single dataset, then clone + fetch.
    const bareRepo = await createFixtureRepo(new URL('source/', tempDir));
    await $`kart clone ${bareRepo.href} --no-checkout ${repoUrl.pathname}`;
    await $`kart -C ${repoUrl.pathname} fetch origin master`;

    const datasets = await $`kart -C ${repoUrl.pathname} data ls`;
    assert.ok(datasets.stdout.includes('test_points'), 'fixture should contain test_points dataset');
  });

  after(async () => {
    if (tempDir) {
      logger.info({ tempDir }, 'Cleaning up temporary directory after test');
      await rm(tempDir, { recursive: true, force: true });
    }
  });

  it('should get a version string when running step 1 – version', async () => {
    const output = await cli(['version']);
    assert.ok(/\d+\.\d+\.\d+/.test(output), `Expected version string in output, got: ${output}`);
  });

  it('should be able to clone a public repo in step 2 – clone', async () => {
    const outputLocation = new URL('temp-test-repo', tempDir);
    await cli(['clone', 'linz/topographic-test-data', fileURLToPath(outputLocation), '--ref', 'master']);

    const clonedDatasets = await $`kart -C ${fileURLToPath(outputLocation)} data ls`;
    assert.ok(clonedDatasets.stdout.includes('test'), 'cloned repo should contain one or more test datasets');
  });

  it('it should produce a summary file in step 3 – diff', async () => {
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

  // -----------------------------------------------------------------------
  // Step 4 – pr-comment is skipped: requires GitHub API credentials
  // -----------------------------------------------------------------------

  it('should produce gpkg datasets in step 5 – export', async () => {
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

  it('should convert gpkg to parquet and produce STAC in step 6 – to-parquet', { skip: skipParquet }, async () => {
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

  it('should validate parquet files in step 7 – validate', { skip: skipParquet }, async () => {
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
