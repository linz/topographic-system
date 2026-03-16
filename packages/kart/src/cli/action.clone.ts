import { fileURLToPath } from 'node:url';

import { gitContext, logger, parseEnv, stringToUrlFolder, UrlFolder } from '@linzjs/topographic-system-shared';
import { command, option, optional, positional, string } from 'cmd-ts';
import { z } from 'zod/mini';
import { $ } from 'zx';

const EnvParser = z.object({
  GITHUB_TOKEN: z.optional(z.string()),
});

export interface CloneArgs {
  repository: string;
  output?: URL;
  ref?: string;
}

export interface CloneContext {
  target: URL;
  repoUrl: URL;
  credentialUrl: URL;
  ref: string;
}

/**
 * Build the clone context: resolve the output path, construct the repo URL,
 * and inject credentials when a GITHUB_TOKEN is available.
 */
export function buildCloneContext(args: CloneArgs, token?: string): CloneContext {
  const target = args.output ?? stringToUrlFolder('repo');
  const ref = args.ref ?? 'master';

  const repoUrl = new URL(args.repository, 'https://github.com/');

  if (repoUrl.host !== 'github.com') {
    throw new Error('Invalid host: ' + repoUrl.host);
  }

  const credentialUrl = new URL(repoUrl);
  if (token != null) {
    credentialUrl.username = 'x-access-token';
    credentialUrl.password = token;
  }

  return { target, repoUrl, credentialUrl, ref };
}

export const CloneCommand = command({
  name: 'clone',
  description: 'Clone a kart repository and fetch a specific commit',
  args: {
    repository: positional({
      displayName: 'repository',
      description: 'Repository to clone (e.g. linz/topographic-data)',
    }),
    output: positional({
      type: optional(UrlFolder),
      displayName: 'output',
      description: 'Output directory for the cloned repository (default: repo)',
    }),
    ref: option({
      type: optional(string),
      long: 'ref',
      description: 'Commit SHA or branch to fetch (default: master)',
      defaultValue: () => 'master',
    }),
  },
  async handler(args) {
    logger.info({ repository: args.repository, ref: args.ref }, 'Clone:Start');

    const env = parseEnv(EnvParser);
    const ctx = buildCloneContext(args, env.GITHUB_TOKEN);

    logger.debug({ repoUrl: ctx.repoUrl.href }, 'Clone:NoCheckout');
    await $`kart clone ${ctx.credentialUrl.href} --no-checkout ${fileURLToPath(ctx.target)}`;
    logger.debug({ repoUrl: ctx.repoUrl.href, ref: ctx.ref }, 'Clone:Completed');
    await $`kart ${gitContext(ctx.target)} fetch origin ${ctx.ref}`;
    logger.info({ repoUrl: ctx.repoUrl.href, ref: ctx.ref }, 'Fetch:Completed');
  },
});
