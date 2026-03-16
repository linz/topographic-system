import { fileURLToPath } from 'node:url';

import { $ } from 'zx';

import { logger } from './log.ts';

export function isPullRequest(): boolean {
  const ref = $.env['GITHUB_REF'] || '';
  logger.debug({ ref }, 'IsPullRequest:GITHUB_REF');
  return ref.startsWith('refs/pull/');
}

export function isMergeToMaster(): boolean {
  const ref = $.env['GITHUB_REF'] || '';
  return !isPullRequest() && ref === 'refs/heads/master';
}

export function gitContext(repo?: URL): string[] {
  return repo ? ['-C', fileURLToPath(repo)] : [];
}
