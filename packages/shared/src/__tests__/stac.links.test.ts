import assert from 'node:assert';
import { beforeEach, describe, it } from 'node:test';

import { $ } from 'zx';

import { determineAssetLocation } from '../stac.links.ts';

describe('determineAssetLocation', () => {
  const originalEnv = { ...$.env };
  const originalProcessEnv = { ...process.env };

  const root = new URL('memory://fake/');

  beforeEach(() => {
    $.env = { ...originalEnv };
    process.env = { ...originalProcessEnv };
  });

  it('should use provided tag when tag is specified', () => {
    const result = determineAssetLocation({
      category: 'subdir',
      dataset: 'dataset',
      file: new URL('file:///path/to/output.parquet'),
      tag: 'custom-tag',
      root,
    });

    assert.equal(result.href, 'memory://fake/subdir/dataset/custom-tag/output.parquet');
  });

  it('should use date-based tag for merge to master', () => {
    $.env['GITHUB_REF'] = 'refs/heads/master';
    $.env['GITHUB_WORKFLOW_REF'] = '';

    const result = determineAssetLocation({
      category: 'subdir',
      dataset: 'dataset',
      file: new URL('file:///output.parquet'),
      root,
    });

    assert.ok(result.href.includes('/year='));
    assert.ok(result.href.includes('/date='));
  });

  it('should use date-based tag for release workflow', () => {
    $.env['GITHUB_REF'] = 'refs/heads/master';
    $.env['GITHUB_WORKFLOW_REF'] = 'owner/repo/.github/workflows/release.yml@refs/heads/master';

    const result = determineAssetLocation({
      category: 'subdir',
      dataset: 'dataset',
      file: new URL('file:///output.parquet'),
      root,
    });

    assert.ok(result.href.includes('/year='));
    assert.ok(result.href.includes('/date='));
  });

  it('should use pull_request tag with PR number for pull requests', () => {
    $.env['GITHUB_REF'] = 'refs/pull/123/merge';
    process.env['GITHUB_REF_NAME'] = '123/merge';

    const result = determineAssetLocation({
      category: 'subdir',
      dataset: 'dataset',
      file: new URL('file:///output.parquet'),
      root,
    });

    assert.ok(result.href.includes('pull_request/pr-123'));
  });

  it('should use pull_request/unknown tag when PR number cannot be extracted', () => {
    $.env['GITHUB_REF'] = 'refs/pull/123/merge';
    process.env['GITHUB_REF_NAME'] = 'invalid-ref';

    assert.throws(
      () =>
        determineAssetLocation({
          category: 'subdir',
          dataset: 'dataset',
          file: new URL('file:///output.parquet'),
          root,
        }),
      {
        message: `Could not determine pull request number from GITHUB_REF: invalid-ref`,
      },
    );
  });

  it('should use dev tag with hash for non-CI environment', () => {
    $.env['GITHUB_REF'] = '';
    $.env['GITHUB_WORKFLOW_REF'] = '';
    process.env['GIT_HASH'] = 'abc123';

    const result = determineAssetLocation({
      category: 'subdir',
      dataset: 'dataset',
      file: new URL('file:///output.parquet'),
      root,
    });

    assert.ok(result.href.includes('dev-'));
  });

  it('should return URL with correct bucket structure', () => {
    const result = determineAssetLocation({
      category: 'layer',
      dataset: 'water',
      file: new URL('file:///water.parquet'),
      tag: 'v1',
      root,
    });

    assert.equal(result.href, 'memory://fake/layer/water/v1/water.parquet');
  });

  it('should handle nested subdir paths', () => {
    const result = determineAssetLocation({
      category: 'data/layers',
      dataset: 'buildings',
      file: new URL('file:///output.gpkg'),
      tag: 'latest',
      root,
    });

    assert.equal(result.href, 'memory://fake/data/layers/buildings/latest/output.gpkg');
  });
});
