import assert from 'node:assert';
import { afterEach, beforeEach, describe, it } from 'node:test';

import { $ } from 'zx';

import { isMergeToMaster, isPullRequest, gitContext, canCommentOnPr } from '../github.ts';

describe('github', () => {
  const originalEnv = { ...$.env };

  beforeEach(() => {
    delete $.env['GITHUB_REF'];
    delete $.env['GITHUB_PR_NUMBER'];
    delete $.env['GITHUB_EVENT_PATH'];
    delete $.env['GITHUB_TOKEN'];
    delete $.env['GITHUB_API_TOKEN'];
  });

  afterEach(() => {
    $.env = { ...originalEnv };
  });

  describe('isPullRequest', () => {
    it('should return true when GITHUB_REF starts with refs/pull/', () => {
      $.env['GITHUB_REF'] = 'refs/pull/123/merge';
      assert.strictEqual(isPullRequest(), true);
    });

    it('should return true when GITHUB_PR_NUMBER is set', () => {
      $.env['GITHUB_PR_NUMBER'] = '42';
      assert.strictEqual(isPullRequest(), true);
    });

    it('should return true when GITHUB_EVENT_PATH is set', () => {
      $.env['GITHUB_EVENT_PATH'] = '/github/workflow/event.json';
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
  describe('canCommentOnPr', () => {
    // Helpers to set up a clean PR/non-PR context via $.env
    function setPrEnv(): void {
      $.env['GITHUB_REF'] = 'refs/pull/123/merge';
    }

    function setNonPrEnv(): void {
      $.env['GITHUB_REF'] = 'refs/heads/master';
    }

    it('should return true when in a PR context with only GITHUB_TOKEN present', () => {
      setPrEnv();
      $.env['GITHUB_TOKEN'] = 'fake-token';
      assert.strictEqual(isPullRequest(), true);
      assert.strictEqual(canCommentOnPr(), true);
    });

    it('should return true when in a PR context with only GITHUB_API_TOKEN present', () => {
      setPrEnv();
      $.env['GITHUB_API_TOKEN'] = 'fake-api-token';
      assert.strictEqual(isPullRequest(), true);
      assert.strictEqual(canCommentOnPr(), true);
    });

    it('should return true when in a PR context with both GITHUB_TOKEN and GITHUB_API_TOKEN present', () => {
      setPrEnv();
      $.env['GITHUB_TOKEN'] = 'fake-token';
      $.env['GITHUB_API_TOKEN'] = 'fake-api-token';
      assert.strictEqual(isPullRequest(), true);
      assert.strictEqual(canCommentOnPr(), true);
    });

    it('should return false when in a PR context but neither GITHUB_TOKEN nor GITHUB_API_TOKEN is present', () => {
      setPrEnv();
      assert.strictEqual(isPullRequest(), true);
      assert.strictEqual(canCommentOnPr(), false);
    });

    it('should return false when in a PR context but both GITHUB_TOKEN and GITHUB_API_TOKEN are empty strings', () => {
      setPrEnv();
      $.env['GITHUB_TOKEN'] = '';
      $.env['GITHUB_API_TOKEN'] = '';
      assert.strictEqual(isPullRequest(), true);
      assert.strictEqual(canCommentOnPr(), false);
    });

    it('should return false when not in a PR context even with GITHUB_TOKEN present', () => {
      setNonPrEnv();
      $.env['GITHUB_TOKEN'] = 'fake-token';
      assert.strictEqual(isPullRequest(), false);
      assert.strictEqual(canCommentOnPr(), false);
    });

    it('should return false when not in a PR context even with GITHUB_API_TOKEN present', () => {
      setNonPrEnv();
      $.env['GITHUB_API_TOKEN'] = 'fake-api-token';
      assert.strictEqual(isPullRequest(), false);
      assert.strictEqual(canCommentOnPr(), false);
    });

    it('should return false when not in a PR context and neither token is present', () => {
      setNonPrEnv();
      assert.strictEqual(isPullRequest(), false);
      assert.strictEqual(canCommentOnPr(), false);
    });

    it('should return false when GITHUB_REF is undefined and neither token is present', () => {
      assert.strictEqual(isPullRequest(), false);
      assert.strictEqual(canCommentOnPr(), false);
    });
  });
});
