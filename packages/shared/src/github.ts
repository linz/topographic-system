import { fileURLToPath } from 'node:url';

import { $ } from 'zx';

import { logger } from './log.ts';

export function isPullRequest(): boolean {
  const ref = $.env['GITHUB_REF'] || '';
  logger.debug({ ref }, 'IsPullRequest:GITHUB_REF');
  return ref.startsWith('refs/pull/');
}

/**
 * Check whether we appear to be running in a GitHub Actions context where the current ref is a merge (commit)
 * to master (ie not a PR and ref equals refs/heads/master).
 */
export function isMergeToMaster(): boolean {
  const ref = $.env['GITHUB_REF'] || '';
  return !isPullRequest() && ref === 'refs/heads/master';
}

export function gitContext(repo?: URL): string[] {
  return repo ? ['-C', fileURLToPath(repo)] : [];
}

/**
 * Check whether we appear to be running inside a GitHub Actions PR context
 * with the credentials needed to post a comment.
 */
export function canCommentOnPr(): boolean {
  const hasToken = !!($.env['GITHUB_API_TOKEN'] || $.env['GITHUB_TOKEN']);
  return hasToken && isPullRequest();
}
