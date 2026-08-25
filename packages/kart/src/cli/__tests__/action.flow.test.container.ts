import assert from 'node:assert';
import { mkdir, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import path from 'node:path';
import { after, before, describe, it } from 'node:test';
import { fileURLToPath, pathToFileURL } from 'node:url';

import { fsa } from '@chunkd/fs';
import { logger, stringToUrlFolder, readParquetMetadata } from '@linzjs/topographic-system-shared';
import { $ } from 'zx';

import { GeojsonCrsLabel } from '../action.diff.ts';
import { buildOgr2OgrArgs } from '../action.to.parquet.ts';

/** Read the primary geometry column's EPSG code straight from the parquet `geo` metadata. */
async function readParquetEpsg(parquetFile: URL): Promise<number> {
  const meta = await readParquetMetadata(parquetFile);
  const geo = meta.key_value_metadata?.find((f) => f.key === 'geo');
  assert.ok(geo?.value, 'parquet should carry `geo` metadata');
  const geomMeta = JSON.parse(geo.value) as {
    columns: Record<string, { crs?: { id: { code: number } } }>;
    primary_column: string;
  };
  // geoparquet omits crs when the data is 4326
  return geomMeta.columns[geomMeta.primary_column]?.crs?.id?.code ?? 4326;
}

let cliLocation = '/app/index.cjs';
async function findCli(): Promise<string> {
  if (await fsa.exists(fsa.toUrl('/app/index.cjs'))) return '/app/index.cjs';
  if (await fsa.exists(fsa.toUrl('./src/index.ts'))) return './src/index.ts';
  throw new Error('Unable to find index.js');
}

/**
 * Run a kart CLI command through the bundled entrypoint, exactly as
 * production does (`node /app/index.cjs <subcommand> ...`).
 *
 * @param args the CLI arguments to pass through
 * @returns the standard output from the command execution, for assertions in tests
 */
async function cli(...args: (string | string[])[]): Promise<string> {
  const result = await $`node ${cliLocation} ${args.flat()}`;
  return result.stdout;
}

/** Shape of the fixture repo, so a test can pin the CRS the dataset is genuinely stored in. */
interface FixtureRepoOptions {
  /** EPSG code to store the dataset in. Coordinates below are always given as lon/lat. */
  epsg?: number;
  /** lon/lat of the feature in the first commit on master. */
  seedCoordinates?: number[];
  /** lon/lat of the feature added by the commit on the `changes` branch. */
  changeCoordinates?: number[];
}

/**
 * Create a minimal kart repository for testing:
 *  - A single dataset with one point feature, imported from GeoJSON → GPKG
 *  - Initialized as a bare kart repo with one commit on master
 * @param baseDir The base directory under which to create the repo (e.g. a temp directory)
 * @param options Override the dataset CRS and the seeded coordinates
 * @returns The URL of the created bare kart repository
 */
async function createFixtureRepo(baseDir: URL, options: FixtureRepoOptions = {}): Promise<URL> {
  const epsg = options.epsg ?? 4326;
  const seedCoordinates = options.seedCoordinates ?? [174.7794, -41.2809];
  const changeCoordinates = options.changeCoordinates ?? [174.7633, -36.8485];
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
          geometry: { type: 'Point', coordinates: seedCoordinates },
        },
      ],
    }),
  );

  // -t_srs stores the dataset in the requested CRS; for 4326 it is a no-op.
  await $`ogr2ogr -f GPKG -t_srs EPSG:${epsg} ${fileURLToPath(seedGpkg)} ${fileURLToPath(seedGeojson)} -nln test_points`;
  await $({
    env: { ...process.env, ...gitEnv },
  })`kart init --import ${`GPKG:${fileURLToPath(seedGpkg)}`} ${fileURLToPath(bareRepo)} --bare -b master`;

  // diff step skips writing a summary when nothing changed.
  const workingCopy = new URL('wc/', baseDir);
  const changeGeojson = new URL('change.geojson', baseDir);
  await $`kart clone ${fileURLToPath(bareRepo)} ${fileURLToPath(workingCopy)}`;
  await $({ env: { ...process.env, ...gitEnv } })`kart -C ${fileURLToPath(workingCopy)} checkout -b changes`;
  await fsa.write(
    changeGeojson,
    JSON.stringify({
      type: 'FeatureCollection',
      features: [
        {
          type: 'Feature',
          properties: { fid: 2, name: 'elsewhere' },
          geometry: { type: 'Point', coordinates: changeCoordinates },
        },
      ],
    }),
  );
  await $`ogr2ogr -append -f GPKG -t_srs EPSG:${epsg} ${fileURLToPath(new URL('wc.gpkg', workingCopy))} ${fileURLToPath(changeGeojson)} -nln test_points`;
  await $({
    env: { ...process.env, ...gitEnv },
  })`kart -C ${fileURLToPath(workingCopy)} commit -m ${'Add another point'}`;
  await $`kart -C ${fileURLToPath(workingCopy)} push origin changes`;

  return bareRepo;
}

describe('action.flow integration', () => {
  const tempDir = stringToUrlFolder(path.join(tmpdir(), 'kart-prepare-integration'));
  const repoUrl = new URL('repo/', tempDir);
  const diffUrl = new URL('diff/', tempDir);
  const summaryUrl = new URL('pr_summary.md', tempDir);
  const exportUrl = new URL('export/', tempDir);
  const parquetUrl = new URL('parquet/', tempDir);
  const outputUrl = new URL('output/', tempDir);
  const targetUrl = new URL('s3-target/', tempDir);
  const validationUrl = new URL('validation-output/', tempDir);

  before(async () => {
    cliLocation = await findCli();
    await mkdir(fileURLToPath(tempDir), { recursive: true });
    logger.debug({ tempDir: tempDir.href }, 'Created temporary directory for test');

    // Create a bare kart repo with a single dataset, then clone + fetch.
    const bareRepo = await createFixtureRepo(new URL('source/', tempDir));
    await $`kart clone ${bareRepo.href} --no-checkout ${repoUrl.pathname}`;
    await $`kart -C ${repoUrl.pathname} fetch origin changes`;

    const datasets = await $`kart -C ${repoUrl.pathname} data ls`;
    assert.ok(datasets.stdout.includes('test_points'), 'fixture should contain test_points dataset');
  });

  after(async () => {
    logger.info({ tempDir }, 'Cleaning up temporary directory after test');
    await rm(tempDir, { recursive: true, force: true });
  });

  it('should run kart-prepare', async () => {
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

    it('should produce a summary file in step 3 - diff', async () => {
      await cli(
        'diff',
        ['--context', fileURLToPath(repoUrl)],
        ['--output', fileURLToPath(diffUrl)],
        ['--summary-file', fileURLToPath(summaryUrl)],
      );

      const md = await fsa.read(summaryUrl);
      assert.ok(md.length > 0, 'pr_summary.md should have content');
      assert.ok(md.toString().includes('# Changes Summary'), 'summary should contain expected markdown header');
    });

    it('should skip 4 - diff', () => {});

    it('should produce gpkg datasets in step 5 - export', async () => {
      await cli(
        'export',
        ['--context', fileURLToPath(repoUrl)],
        ['--output', fileURLToPath(exportUrl)],
        ['--ref', 'FETCH_HEAD'],
      );

      const files = await fsa.toArray(fsa.list(exportUrl));
      assert.ok(
        files.some((f) => f.href.endsWith('.gpkg')),
        `Expected .gpkg files in ${exportUrl.href}, got: ${files.map((f) => f.href).join(', ')}`,
      );
    });

    it('should convert gpkg to parquet and produce STAC in step 6 - to-parquet', async () => {
      await cli(
        'to-parquet',
        ['--output', fileURLToPath(outputUrl)],
        ['--temp-location', fileURLToPath(parquetUrl)],
        fileURLToPath(exportUrl),
      );

      const parquetFiles = await fsa.toArray(fsa.list(outputUrl, { recursive: true }));
      const parquetFile = parquetFiles.find((f) => f.href.endsWith('.parquet'));
      assert.ok(
        parquetFile,
        `Expected .parquet in ${outputUrl.href}, got: ${parquetFiles.map((f) => f.href).join(', ')}`,
      );

      // The fixture is seeded from GeoJSON (EPSG:4326); the source CRS must survive the
      // gpkg→parquet conversion and never be force-stamped (previously to 4167).
      assert.strictEqual(await readParquetEpsg(parquetFile), 4326, 'parquet must preserve the source EPSG');

      const catalogUrl = new URL('catalog.json', outputUrl);
      const catalog = await fsa.readJson(catalogUrl);
      assert.ok(catalog, 'catalog.json should exist in output');
    });

    // FIXME currently broken
    it('should validate parquet files in step 7 - validate', { skip: true }, async () => {
      const dbPath = new URL('files.parquet', parquetUrl);
      const configFile = pathToFileURL('/packages/validation/config/default_config.json');

      await cli(
        'validate',
        ['--output', fileURLToPath(outputUrl)],
        ['--db-path', fileURLToPath(dbPath)],
        ['--config-file', fileURLToPath(configFile)],
        ['--output-dir', fileURLToPath(validationUrl)],
      );

      const validationFiles = await fsa.toArray(fsa.list(validationUrl));
      assert.ok(validationFiles.length > 0, `Expected validation output in ${validationUrl.href}`);
    });

    it('should push the stac files into target output after kart-prepare', async () => {
      await cli(
        'stac-push',
        ['--source', new URL('catalog.json', outputUrl).href],
        ['--target', targetUrl.href],
        ['--category', 'data'],
        ['--strategy', 'latest'],
        ['--commit'],
      );

      const stacFiles = await fsa.toArray(fsa.list(outputUrl, { recursive: true }));
      assert.ok(
        stacFiles.some((f) => f.href.endsWith('.json')),
        `Expected stac files in ${outputUrl.href}, got: ${stacFiles.map((f) => f.href).join(', ')}`,
      );

      const catalogUrl = new URL('catalog.json', outputUrl);
      const catalog = await fsa.readJson(catalogUrl);
      assert.ok(catalog, 'catalog.json should exist in output');
    });
  });
});

describe('to-parquet CRS preservation', () => {
  const tempDir = stringToUrlFolder(path.join(tmpdir(), 'kart-to-parquet-crs'));
  const geojson = new URL('seed.geojson', tempDir);

  before(async () => {
    await mkdir(fileURLToPath(tempDir), { recursive: true });
    await fsa.write(
      geojson,
      JSON.stringify({
        type: 'FeatureCollection',
        features: [
          { type: 'Feature', properties: { fid: 1 }, geometry: { type: 'Point', coordinates: [174.7794, -41.2809] } },
        ],
      }),
    );
  });

  after(async () => {
    await rm(fileURLToPath(tempDir), { recursive: true, force: true });
  });

  // The conversion must keep whatever CRS the source gpkg is in
  for (const sourceEpsg of [2193, 4167]) {
    it(`preserves source EPSG:${sourceEpsg} into the parquet`, async () => {
      const gpkg = new URL(`src-${sourceEpsg}.gpkg`, tempDir);
      const parquet = new URL(`out-${sourceEpsg}.parquet`, tempDir);

      // Build a gpkg genuinely in the source CRS.
      await $`ogr2ogr -f GPKG -t_srs EPSG:${sourceEpsg} -lco GEOMETRY_NAME=geometry ${fileURLToPath(gpkg)} ${fileURLToPath(geojson)} -nln pts`;

      const args = buildOgr2OgrArgs(parquet, gpkg, {
        compression: 'zstd',
        compressionLevel: 17,
        rowGroupSize: 2 ** 15,
        sortByBbox: false,
      });
      await $`${args}`;

      assert.strictEqual(await readParquetEpsg(parquet), sourceEpsg, `parquet must preserve source EPSG:${sourceEpsg}`);
    });
  }
});

/**
 * The datasets are stored in projected metres (eg EPSG:2193), but a `.geojson` diff must be WGS 84
 * longitude/latitude - both to satisfy RFC 7946 and so GitHub renders the PR comment's geojson block
 * as a map instead of silently drawing nothing. The 4326 fixture used above cannot catch a missing
 * reprojection, so build a repo genuinely in NZTM2000 and check the diff comes back in degrees.
 */
describe('diff CRS reprojection', () => {
  const tempDir = stringToUrlFolder(path.join(tmpdir(), 'kart-diff-crs'));
  const repoUrl = new URL('repo/', tempDir);
  const diffUrl = new URL('diff/', tempDir);
  const summaryUrl = new URL('pr_summary.md', tempDir);

  // A point on the CJ07/CK07 sheet, in the lon/lat the diff is expected to return.
  const seedLonLat: [number, number] = [167.12, -47.34];
  const [expectedLon, expectedLat]: [number, number] = [167.45, -47.05];
  const changeLonLat: [number, number] = [expectedLon, expectedLat];

  before(async () => {
    cliLocation = await findCli();
    await mkdir(fileURLToPath(tempDir), { recursive: true });
    const bareRepo = await createFixtureRepo(new URL('source/', tempDir), {
      epsg: 2193,
      seedCoordinates: seedLonLat,
      changeCoordinates: changeLonLat,
    });
    await $`kart clone ${bareRepo.href} --no-checkout ${repoUrl.pathname}`;
    await $`kart -C ${repoUrl.pathname} fetch origin changes`;
  });

  after(async () => {
    await rm(fileURLToPath(tempDir), { recursive: true, force: true });
  });

  it('writes the geojson diff in lon/lat, not the dataset CRS', async () => {
    await cli(
      'diff',
      ['--context', fileURLToPath(repoUrl)],
      ['--output', fileURLToPath(diffUrl)],
      ['--summary-file', fileURLToPath(summaryUrl)],
    );

    const geojsonLocation = new URL('kart_diff_geojson', diffUrl);
    const stat = await fsa.head(geojsonLocation);
    assert.ok(stat, `expected a geojson diff at ${geojsonLocation.href}`);
    const geojsonFiles = (await fsa.toArray(fsa.list(geojsonLocation, { recursive: true }))).filter((f) =>
      f.pathname.endsWith('.geojson'),
    );
    assert.ok(geojsonFiles.length > 0, 'expected at least one geojson diff file');

    let featureCount = 0;
    for (const file of geojsonFiles) {
      const collection = JSON.parse((await fsa.read(file)).toString()) as {
        features: { geometry: { coordinates: number[] } }[];
      };
      for (const feature of collection.features) {
        const [lon, lat] = feature.geometry.coordinates;
        assert.ok(lon != null && lat != null, 'feature should have coordinates');
        // An unprojected NZTM easting/northing (~1156000/4740000) fails both of these.
        assert.ok(lon >= -180 && lon <= 180, `longitude ${lon} outside valid range - diff was not reprojected`);
        assert.ok(lat >= -90 && lat <= 90, `latitude ${lat} outside valid range - diff was not reprojected`);
        // Round-trip 2193 -> 4326 is lossy well below a metre, far tighter than 1e-6 degrees.
        assert.ok(Math.abs(lon - expectedLon) < 1e-6, `longitude ${lon} should match the seeded value`);
        assert.ok(Math.abs(lat - expectedLat) < 1e-6, `latitude ${lat} should match the seeded value`);
        featureCount++;
      }
    }
    assert.ok(featureCount > 0, 'expected at least one changed feature to assert on');
  });

  it('tells the reader which projection the comment geojson is in', async () => {
    const md = (await fsa.read(summaryUrl)).toString();
    assert.ok(md.includes(GeojsonCrsLabel), `summary should name the reprojection, got: ${md.slice(0, 400)}`);
  });
});
