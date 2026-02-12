import { $ } from 'zx';

import { logger } from './log.ts';

export function isPullRequest(): boolean {
  const ref = $.env['GITHUB_REF'] || '';
  logger.debug({ ref }, 'IsPullRequest:GITHUB_REF');
  return ref.startsWith('refs/pull/');
}

export function isMergeToMaster(): boolean {
  const ref = $.env['GITHUB_REF'] || '';
  return !isPullRequest() && ref.endsWith('/master');
}

export function isRelease(): boolean {
  const workflow = $.env['GITHUB_WORKFLOW_REF'] || '';
  logger.debug({ workflow }, 'IsRelease:GITHUB_WORKFLOW_REF');
  return isMergeToMaster() && workflow.toLowerCase().includes('release');
}
