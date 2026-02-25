import assert from 'node:assert';
import { afterEach, describe, it } from 'node:test';

import { $ } from 'zx';

import { isMergeToMaster, isPullRequest, isRelease } from '../github.ts';

describe('github', () => {
  const originalEnv = { ...$.env };

  afterEach(() => {
    $.env = { ...originalEnv };
  });

  describe('isPullRequest', () => {
    it('should return true when GITHUB_REF starts with refs/pull/', () => {
      $.env['GITHUB_REF'] = 'refs/pull/123/merge';
      assert.strictEqual(isPullRequest(), true);
    });

    it('should return false when GITHUB_REF does not start with refs/pull/', () => {
      $.env['GITHUB_REF'] = 'refs/heads/master';
      assert.strictEqual(isPullRequest(), false);
    });

    it('should return false when GITHUB_REF is empty', () => {
      $.env['GITHUB_REF'] = '';
      assert.strictEqual(isPullRequest(), false);
    });

    it('should return false when GITHUB_REF is undefined', () => {
      delete $.env['GITHUB_REF'];
      assert.strictEqual(isPullRequest(), false);
    });
  });

  describe('isMergeToMaster', () => {
    it('should return true when not a pull request and ref ends with /master', () => {
      $.env['GITHUB_REF'] = 'refs/heads/master';
      assert.strictEqual(isMergeToMaster(), true);
    });

    it('should return false when ref is a pull request', () => {
      $.env['GITHUB_REF'] = 'refs/pull/123/merge';
      assert.strictEqual(isMergeToMaster(), false);
    });

    it('should return false when ref does not end with /master', () => {
      $.env['GITHUB_REF'] = 'refs/heads/develop';
      assert.strictEqual(isMergeToMaster(), false);
    });

    it('should return false when GITHUB_REF is empty', () => {
      $.env['GITHUB_REF'] = '';
      assert.strictEqual(isMergeToMaster(), false);
    });

    it('should return false when GITHUB_REF is undefined', () => {
      delete $.env['GITHUB_REF'];
      assert.strictEqual(isMergeToMaster(), false);
    });
  });

  describe('isRelease', () => {
    it('should return true when merge to master and workflow includes release', () => {
      $.env['GITHUB_REF'] = 'refs/heads/master';
      $.env['GITHUB_WORKFLOW_REF'] = 'owner/repo/.github/workflows/release.yml@refs/heads/master';
      assert.strictEqual(isRelease(), true);
    });

    it('should return true when workflow includes Release (case insensitive)', () => {
      $.env['GITHUB_REF'] = 'refs/heads/master';
      $.env['GITHUB_WORKFLOW_REF'] = 'owner/repo/.github/workflows/Release.yml@refs/heads/master';
      assert.strictEqual(isRelease(), true);
    });

    it('should return false when not merge to master', () => {
      $.env['GITHUB_REF'] = 'refs/heads/develop';
      $.env['GITHUB_WORKFLOW_REF'] = 'owner/repo/.github/workflows/release.yml@refs/heads/develop';
      assert.strictEqual(isRelease(), false);
    });

    it('should return false when workflow does not include release', () => {
      $.env['GITHUB_REF'] = 'refs/heads/master';
      $.env['GITHUB_WORKFLOW_REF'] = 'owner/repo/.github/workflows/ci.yml@refs/heads/master';
      assert.strictEqual(isRelease(), false);
    });

    it('should return false when GITHUB_WORKFLOW_REF is empty', () => {
      $.env['GITHUB_REF'] = 'refs/heads/master';
      $.env['GITHUB_WORKFLOW_REF'] = '';
      assert.strictEqual(isRelease(), false);
    });

    it('should return false when GITHUB_WORKFLOW_REF is undefined', () => {
      $.env['GITHUB_REF'] = 'refs/heads/master';
      delete $.env['GITHUB_WORKFLOW_REF'];
      assert.strictEqual(isRelease(), false);
    });
  });
});
