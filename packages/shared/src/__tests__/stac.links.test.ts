import assert from 'node:assert';
import { beforeEach, describe, it } from 'node:test';

import { $ } from 'zx';

import { determineAssetLocation } from '../stac.links.ts';

describe('determineAssetLocation', () => {
  const originalEnv = { ...$.env };
  const originalProcessEnv = { ...process.env };

  beforeEach(() => {
    $.env = { ...originalEnv };
    process.env = { ...originalProcessEnv };
  });

  it('should use provided tag when tag is specified', () => {
    const result = determineAssetLocation('subdir', 'dataset', '/path/to/output.parquet', 'custom-tag');

    assert.ok(result.href.includes('subdir/dataset/custom-tag/output.parquet'));
  });

  it('should extract basename from output path', () => {
    const result = determineAssetLocation('subdir', 'dataset', '/some/long/path/to/file.parquet', 'tag');

    assert.ok(result.href.endsWith('file.parquet'));
    assert.ok(!result.href.includes('/some/long/path'));
  });

  it('should use date-based tag for merge to master', () => {
    $.env['GITHUB_REF'] = 'refs/heads/master';
    $.env['GITHUB_WORKFLOW_REF'] = '';

    const result = determineAssetLocation('subdir', 'dataset', 'output.parquet');

    assert.ok(result.href.includes('year='));
    assert.ok(result.href.includes('date='));
  });

  it('should use date-based tag for release workflow', () => {
    $.env['GITHUB_REF'] = 'refs/heads/master';
    $.env['GITHUB_WORKFLOW_REF'] = 'owner/repo/.github/workflows/release.yml@refs/heads/master';

    const result = determineAssetLocation('subdir', 'dataset', 'output.parquet');

    assert.ok(result.href.includes('year='));
    assert.ok(result.href.includes('date='));
  });

  it('should use pull_request tag with PR number for pull requests', () => {
    $.env['GITHUB_REF'] = 'refs/pull/123/merge';
    process.env['GITHUB_REF_NAME'] = '123/merge';

    const result = determineAssetLocation('subdir', 'dataset', 'output.parquet');

    assert.ok(result.href.includes('pull_request/pr-123'));
  });

  it('should use pull_request/unknown tag when PR number cannot be extracted', () => {
    $.env['GITHUB_REF'] = 'refs/pull/123/merge';
    process.env['GITHUB_REF_NAME'] = 'invalid-ref';

    assert.throws(() => determineAssetLocation('subdir', 'dataset', 'output.parquet'), {
      message: `Could not determine pull request number from GITHUB_REF: invalid-ref`,
    });
  });

  it('should use dev tag with hash for non-CI environment', () => {
    $.env['GITHUB_REF'] = '';
    $.env['GITHUB_WORKFLOW_REF'] = '';
    process.env['GIT_HASH'] = 'abc123';

    const result = determineAssetLocation('subdir', 'dataset', 'output.parquet');

    assert.ok(result.href.includes('dev-'));
  });

  it('should return URL with correct S3 bucket structure', () => {
    const result = determineAssetLocation('layer', 'water', '/path/to/water.parquet', 'v1');

    assert.ok(result.protocol === 's3:');
    assert.ok(result.href.includes('layer/water/v1/water.parquet'));
  });

  it('should handle nested subdir paths', () => {
    const result = determineAssetLocation('data/layers', 'buildings', 'output.gpkg', 'latest');

    assert.ok(result.href.includes('data/layers/buildings/latest/output.gpkg'));
  });
});
