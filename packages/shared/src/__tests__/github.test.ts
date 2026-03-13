import assert from 'node:assert';
import { afterEach, describe, it } from 'node:test';

import { $ } from 'zx';

import { isMergeToMaster, isPullRequest, gitContext } from '../github.ts';

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

  describe('gitContext', () => {
    it('should return an empty array when no repo is provided', () => {
      assert.deepStrictEqual(gitContext(), []);
    });

    it('should return the correct context for a given repo URL', () => {
      const repoUrl = new URL('file:///path/to/repo');
      const expectedContext = ['-C', '/path/to/repo'];
      assert.deepStrictEqual(gitContext(repoUrl), expectedContext);
    });

    it('should handle URLs with special characters', () => {
      const repoUrl = new URL('file:///path/to/repo%20with%20spaces');
      const expectedContext = ['-C', '/path/to/repo with spaces'];
      assert.deepStrictEqual(gitContext(repoUrl), expectedContext);
    });
  });
});
