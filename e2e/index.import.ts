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
    type: 'airport';
    name?: string;
    theme: 'transport';
    metadata?: string;
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
    const ret = await tsKartImport('uv', 'run', 'snakemake', '--list-rules');
    assert.ok(ret.stdout.includes('clone_nz_airport_polygons'));
  });

  await it('should clone_airport', async () => {
    const ret = await tsKartImport('uv', 'run', 'snakemake', '--cores', 'all', 'clone_nz_airport_polygons');
    assert.ok(ret);
    state.hasClone = true;
  });

  await it('should create theme_airport', async () => {
    const ret = await tsKartImport('uv', 'run', 'snakemake', '--cores', 'all', 'theme_airport');
    assert.ok(ret);

    // The merge is FlatGeobuf, the production default, because GeoJSON carries no column types.
    // Convert a copy to GeoJSON so the value assertions below can read it as text.
    const themeDir = 'data/working/theme/release_30';
    await tsKartImport(`ogr2ogr -f GeoJSON ${themeDir}/airport.e2e.geojson ${themeDir}/airport.fgb`);

    const release30Airports = new URL(
      './packages/kart-import/data/working/theme/release_30/airport.e2e.geojson',
      sourceCodeUrl,
    );
    const airports = JSON.parse(await readFile(release30Airports, 'utf-8')) as FeatureCollectionAirport;

    assert.deepEqual(airports.crs, { type: 'name', properties: { name: 'urn:ogc:def:crs:EPSG::4167' } });
    const count = airports.features.length;

    assert.equal(count, 85);
    // All airports have a t50_fid except Nuie - needs backfill
    assert.equal(airports.features.filter((f) => f.properties.t50_fid > 0).length, count - 1);
    // All airports have a name
    assert.equal(airports.features.filter((f) => f.properties.name != null).length, count);
    // Queenstown airport exists
    assert.equal(airports.features.filter((f) => f.properties.name === 'Queenstown Airport').length, 1);

    // Chatham Islands data was also imported
    const ciAirport = airports.features.find((f) => f.properties.name?.includes('Tuuta'));
    assert.equal(ciAirport?.properties.id, '014fa452-a5e0-7733-81f0-6d80886c86d5');
    assert.equal(ciAirport?.properties.created_at, '2015-09-06T20:22:04Z');
    assert.equal(ciAirport?.properties.updated_at, '2015-09-06T20:22:04Z');
    assert.equal(ciAirport?.properties.t50_fid, 5454276);
    assert.equal(ciAirport?.properties.type, 'airport');
    state.hasTheme = true;
  });

  await it('should create a topographic kart dataset', async () => {
    const ret = await tsKartImport('uv', 'run', 'snakemake', '--cores', 'all', 'kart_theme_airport');
    assert.ok(ret);

    const topographicData = new URL('./packages/kart-import/data/output/theme/airport/', sourceCodeUrl);
    assert.ok(await stat(topographicData));

    const retLog = await tsKartImport(
      'kart',
      '-C',
      '/source/packages/kart-import/data/output/theme/airport/',
      'log',
      '-o',
      'json',
    );
    const commits = JSON.parse(retLog.buffer().toString()).map((m: { message: string }) => m.message) as string[];

    assert.deepEqual(commits, [
      'import airport for release 32',
      'import airport for release 31',
      'import airport for release 30',
    ]);

    // The types kart recorded are the point of importing FlatGeobuf rather than GeoJSON, so
    // assert on the repo's own schema rather than on the intermediate that produced it.
    const retSchema = await tsKartImport(
      'kart',
      '-C',
      '/source/packages/kart-import/data/output/theme/airport/',
      'meta',
      'get',
      'airport',
      'schema.json',
      '-o',
      'json',
    );
    const schema = JSON.parse(retSchema.buffer().toString()) as {
      airport: { 'schema.json': { name: string; dataType: string }[] };
    };
    const dataTypes = new Map(schema.airport['schema.json'].map((c) => [c.name, c.dataType]));

    // The Niue dataset maps `t50_fid: null`, so it contributes a column that is NULL for every
    // feature. Before the merge unified dtypes that made the whole merged column text.
    assert.equal(dataTypes.get('t50_fid'), 'integer');
    // RFC 3339 text, as the schemas prescribe. A GeoJSON merge records these as `timestamp`.
    assert.equal(dataTypes.get('created_at'), 'text');
    assert.equal(dataTypes.get('updated_at'), 'text');
  });
});
