import assert from 'node:assert';
import { describe, it } from 'node:test';

import { tsKartImport } from './common.ts';

describe('kart.import', async () => {
  await it('should have uv and source', async () => {
    const retUv = await tsKartImport('uv', '--version');
    assert.ok(retUv.stdout.includes('uv'));

    // Current working directory should be the source folder
    const retCat = await tsKartImport('cat', 'pyproject.toml');
    assert.ok(retCat.stdout.includes('topographic-system-kart-import'));
  });

  await it('should have clone_all', async () => {
    const ret = await tsKartImport('uv', 'run', 'dg', 'list', 'defs', '--assets', 'clone_nz_airport_polygons');
    assert.ok(ret.stdout.includes('clone_all'));
  });

  await it('should clone_airport', async () => {
    const ret = await tsKartImport('uv', 'run', 'dg', 'launch', '--assets', 'clone_nz_airport_polygons');
    assert.ok(ret);
  });
});
