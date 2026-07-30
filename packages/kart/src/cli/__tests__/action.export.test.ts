import assert from 'node:assert';
import { describe, it } from 'node:test';

import { buildKartExportArgs, selectExportDatasets } from '../action.export.ts';

describe('selectExportDatasets', () => {
  it('should export all existing datasets when none are requested and not changed-only', () => {
    const result = selectExportDatasets(new Set(['a', 'b']), []);
    assert.deepStrictEqual(result.sort(), ['a', 'b']);
  });

  it('should keep only changed datasets that exist at the ref', () => {
    // 'old' is the pre-rename name on the master side of a rename diff — it no longer exists at the ref.
    const result = selectExportDatasets(
      new Set(['nztopo50_grid', 'nztopo50_map_sheet']),
      [],
      ['nztopo50_grid', 'nz_topo50_grid', 'old'],
    );
    assert.deepStrictEqual(result, ['nztopo50_grid']);
  });

  it('should narrow to requested datasets that exist', () => {
    const result = selectExportDatasets(new Set(['a', 'b', 'c']), ['b', 'missing']);
    assert.deepStrictEqual(result, ['b']);
  });

  it('should intersect requested datasets with the changed set', () => {
    const result = selectExportDatasets(new Set(['a', 'b', 'c']), ['a', 'c'], ['a', 'b']);
    assert.deepStrictEqual(result, ['a']);
  });

  it('should de-duplicate requested datasets', () => {
    const result = selectExportDatasets(new Set(['a']), ['a', 'a']);
    assert.deepStrictEqual(result, ['a']);
  });

  it('should return nothing when no changed datasets exist at the ref', () => {
    const result = selectExportDatasets(new Set(['new-name']), [], ['old-name']);
    assert.deepStrictEqual(result, []);
  });
});

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
