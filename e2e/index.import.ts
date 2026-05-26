import assert from 'node:assert';
import { readFile, stat } from 'node:fs/promises';
import { describe, it } from 'node:test';

import { sourceCodeUrl, tsKartImport } from './common.ts';

interface FeatureCollectionAirport {
  type: 'FeatureCollection';
  name: 'airport';
  crs: { type: 'name'; properties: { name: string } };
  features: FeatureAirport[];
}

interface FeatureAirport {
  type: 'Feature';
  properties: {
    id: string;
    created_at: string;
    updated_at: string;
    t50_fid: number;
    feature_type: 'airport';
    name?: string;
  };
  geometry: unknown;
}

describe('kart.import', async () => {
  const state = {
    hasClone: false,
    hasTheme: false,
    hasKart: false,
  };

  await it('should have uv and source', async () => {
    const retUv = await tsKartImport('uv', '--version');
    assert.ok(retUv.stdout.includes('uv'));

    // Current working directory should be the source folder
    const retCat = await tsKartImport('cat', 'pyproject.toml');
    assert.ok(retCat.stdout.includes('topographic-system-kart-import'));
  });

  await it('should have clone_all', async () => {
    const ret = await tsKartImport('uv', 'run', 'dg', 'list', 'defs', '--assets', 'clone_nz_airport_polygons');
    assert.ok(ret.stdout.includes('clone_nz_airport_polygons'));
  });

  await it('should clone_airport', async () => {
    const ret = await tsKartImport('uv', 'run', 'dg', 'launch', '--assets', 'clone_nz_airport_polygons');
    assert.ok(ret);
    state.hasClone = true;
  });

  await it('should create theme_airport', async () => {
    const ret = await tsKartImport('uv', 'run', 'dg', 'launch', '--assets', '*theme_airport');
    assert.ok(ret);

    const release30Airports = new URL(
      './packages/kart-import/data/working/theme/release_30/airport.geojson',
      sourceCodeUrl,
    );
    const airports = JSON.parse(await readFile(release30Airports, 'utf-8')) as FeatureCollectionAirport;

    assert.deepEqual(airports.crs, { type: 'name', properties: { name: 'urn:ogc:def:crs:EPSG::4167' } });
    const count = airports.features.length;

    assert.equal(count, 84);
    // All airports have a t50_fid
    assert.equal(airports.features.filter((f) => f.properties.t50_fid > 0).length, count);
    // All airports have a name
    assert.equal(airports.features.filter((f) => f.properties.name != null).length, count);
    // queenstown airport exists
    assert.equal(airports.features.filter((f) => f.properties.name === 'Queenstown Airport').length, 1);

    // Chatham islands data was also imported
    const ciAirport = airports.features.find((f) => f.properties.name?.includes('Tuuta'));
    assert.equal(ciAirport?.properties.id, '014fa452-a5e0-7733-81f0-6d80886c86d5');
    assert.equal(ciAirport?.properties.created_at, '2015-09-06T20:22:04Z');
    assert.equal(ciAirport?.properties.updated_at, '2015-09-06T20:22:04Z');
    assert.equal(ciAirport?.properties.t50_fid, 5454276);
    assert.equal(ciAirport?.properties.feature_type, 'airport');
    state.hasTheme = true;
  });

  await it('should create a topographic kart dataset', async () => {
    let jobStar = state.hasTheme ? '' : '*'; // if the theme hasn't been proccessed we should process it here
    const ret = await tsKartImport('uv', 'run', 'dg', 'launch', '--assets', `${jobStar}kart_import_topographic-data`);
    assert.ok(ret);

    const topographicData = new URL('./packages/kart-import/data/output/topographic-data', sourceCodeUrl);
    assert.ok(await stat(topographicData));

    const retLog = await tsKartImport(
      'kart',
      '-C',
      '/source/packages/kart-import/data/output/topographic-data/',
      'log',
      '-o',
      'json',
    );
    const commits = JSON.parse(retLog.buffer().toString()).map((m) => m.message) as string[];

    assert.deepEqual(commits, [
      'import airport for release 32',
      'import airport for release 31',
      'import airport for release 30',
    ]);
  });
});
