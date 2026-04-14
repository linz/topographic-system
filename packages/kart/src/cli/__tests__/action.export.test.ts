import assert from 'node:assert';
import { describe, it } from 'node:test';

import { buildKartExportArgs } from '../action.export.ts';

describe('buildKartExportArgs', () => {
  it('should build args without context', () => {
    assert.deepStrictEqual(buildKartExportArgs('buildings', '/tmp/output', 'master'), [
      'export',
      '-lco',
      'GEOMETRY_NAME=geometry',
      'buildings',
      '--ref',
      'master',
      '/tmp/output/buildings.gpkg',
    ]);
  });

  it('should build args with context', () => {
    assert.deepStrictEqual(
      buildKartExportArgs('buildings', '/tmp/output', 'feat/my-feature-branch', new URL('file:///tmp/repo-path/')),
      [
        '-C',
        '/tmp/repo-path/',
        'export',
        '-lco',
        'GEOMETRY_NAME=geometry',
        'buildings',
        '--ref',
        'feat/my-feature-branch',
        '/tmp/output/buildings.gpkg',
      ],
    );
  });

  it('should handle dataset names with underscore', () => {
    assert.deepStrictEqual(buildKartExportArgs('tree_locations', '/tmp/output', 'master'), [
      'export',
      '-lco',
      'GEOMETRY_NAME=geometry',
      'tree_locations',
      '--ref',
      'master',
      '/tmp/output/tree_locations.gpkg',
    ]);
  });

  it('should handle a commit sha as ref', () => {
    assert.deepStrictEqual(buildKartExportArgs('buildings', '/tmp/output', 'abc123def456'), [
      'export',
      '-lco',
      'GEOMETRY_NAME=geometry',
      'buildings',
      '--ref',
      'abc123def456',
      '/tmp/output/buildings.gpkg',
    ]);
  });
});
