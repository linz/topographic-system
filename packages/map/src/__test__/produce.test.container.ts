import assert from 'node:assert';
import { writeFileSync } from 'node:fs';
import { mkdtemp, rm } from 'node:fs/promises';
import { after, before, describe, it } from 'node:test';
import { fileURLToPath } from 'node:url';

import { fsa } from '@chunkd/fs';
import type { StacCatalog, StacItem } from 'stac-ts';
import { $ } from 'zx';

async function cli(...args: (string | string[])[]): Promise<string> {
  const result = await $`node /app/index.cjs ${args.flat()}`;
  if (result.exitCode !== 0) throw new Error('Error running CLI');
  return result.stdout;
}

/**
 * If QGIS and python3 exists, actually produce a PDF
 */

describe('QGIS Process', () => {
  let tempLocation: URL = new URL('memory://empty');
  before(async () => {
    tempLocation = fsa.toUrl((await mkdtemp('topographic-system-test')) + '/');
    baseDeployArgs.source = new URL('source/catalog.json', tempLocation);
  });

  after(async () => {
    if (tempLocation != null && tempLocation.protocol === 'file:') await rm(tempLocation, { recursive: true });
  });

  const baseDeployArgs = {
    githash: undefined,
    commit: false,
    deployTag: 'latest',
    dataTag: 'latest',
    source: new URL('/source/catalog.json', tempLocation),
  };

  async function writeLatestAsset(name: string, source: URL): Promise<void> {
    const catalog = new URL('source/catalog.json', tempLocation);
    const existingCatalog: StacCatalog = (await fsa.readJson(catalog).catch(() => {
      return { links: [] };
    })) as StacCatalog;
    existingCatalog.links.push({ rel: 'child', href: `./${name}/latest/collection.json` });

    await fsa.write(catalog, JSON.stringify(existingCatalog));
    await fsa.write(
      new URL(`source/${name}/latest/collection.json`, tempLocation),
      JSON.stringify({
        assets: { parquet: { href: `./${name}.geojson` } },
      }),
    );
    await fsa.write(new URL(`source/${name}/latest/${name}.geojson`, tempLocation), fsa.readStream(source));
  }

  it('should deploy a qgs file', async (t) => {
    const qgisProject = new URL('../../assets/beehive.qgs', import.meta.url);
    const qgisData = new URL('../../assets/beehive.geojson', import.meta.url);
    const topo50Data = new URL('../../assets/topo50.geojson', import.meta.url);

    await fsa.write(new URL('source/beehive/beehive.qgs', tempLocation), fsa.readStream(qgisProject));

    await writeLatestAsset('beehive', qgisData);
    await writeLatestAsset('topo50', topo50Data);

    // Deploy the QGIS project into local files
    const deploy = await cli(
      'deploy',
      ['--source', fileURLToPath(baseDeployArgs.source)],
      ['--project', fileURLToPath(new URL('source/', tempLocation))],
      ['--target', fileURLToPath(new URL('target-deploy/', tempLocation))],
      ['--tag', 'latest'],
      '--commit',
    );
    console.log(deploy);

    // TODO this should not be getting destroyed, this logic should really be removed
    const bq31Sheet = [
      { sheetCode: 'BQ31', epsg: 2193, bbox: [1756000, 5406000, 1780000, 5442000], geometry: { type: 'Polygon' } },
    ];
    writeFileSync('/app/qgis/src/qgis_export_cover.py', `print('${JSON.stringify(bq31Sheet)}')`);

    const produceCover = await cli(
      'produce-cover',
      ['--project', fileURLToPath(new URL('target-deploy/qgis/latest/beehive/beehive.json', tempLocation))],
      ['--layout', 'tiff-50'],
      ['--map-sheet-layer', 'topo50'],
      ['--temp-location', fileURLToPath(new URL('temp-produce-cover/', tempLocation))],
      ['--output', fileURLToPath(new URL('target-produce/working/', tempLocation))],
      ['--format', 'png'],
      ['--dpi', '200'],
    );
    console.log(produceCover);

    const [first] = await fsa.toArray(fsa.list(new URL('target-produce/working/', tempLocation)));
    const targetJson = new URL(`BQ31.json`, first);

    await cli('produce', fileURLToPath(targetJson), [
      '--temp-location',
      fileURLToPath(new URL('temp-produce/', tempLocation)),
    ]);

    const output = await fsa.readJson<StacItem>(targetJson);

    assert.ok(Object.keys(output.assets).length > 0);
  });
});
