import { fileURLToPath } from 'node:url';

import { $ } from 'zx';

import { logger } from './log.ts';

export function isPullRequest(): boolean {
  logger.debug(
    {
      github: { prNumber: $.env['GITHUB_PR_NUMBER'], ref: $.env['GITHUB_REF'], eventPath: $.env['GITHUB_EVENT_PATH'] },
    },
    'IsPullRequest:env',
  );
  return (
    Boolean($.env['GITHUB_PR_NUMBER']) ||
    ($.env['GITHUB_REF']?.startsWith('refs/pull/') ?? false) ||
    Boolean($.env['GITHUB_EVENT_PATH'])
  );
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
