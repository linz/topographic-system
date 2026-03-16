import { fileURLToPath } from 'node:url';

import { gitContext, logger, parseEnv, stringToUrlFolder, UrlFolder } from '@linzjs/topographic-system-shared';
import { command, option, optional, positional, string } from 'cmd-ts';
import { z } from 'zod/mini';
import { $ } from 'zx';

const EnvParser = z.object({
  GITHUB_TOKEN: z.optional(z.string()),
});

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
    const target = args.output ?? stringToUrlFolder('repo');
    logger.info({ repository: args.repository, ref: args.ref }, 'Clone:Start');

    const env = parseEnv(EnvParser);

    const targetUrl = new URL(args.repository, `https://github.com/`);

    const targetUrlCredentials = new URL(targetUrl);
    if (targetUrlCredentials.host !== 'github.com') {
      throw new Error('Invalid host: ' + targetUrl.host);
    }

    if (env.GITHUB_TOKEN != null) {
      targetUrlCredentials.username = 'x-access-token';
      targetUrlCredentials.password = env.GITHUB_TOKEN;
    }

    logger.debug({ repoUrl: targetUrl.href }, 'Clone:NoCheckout');
    await $`kart clone ${targetUrlCredentials.href} --no-checkout ${fileURLToPath(target)}`;
    logger.debug({ repoUrl: targetUrl.href, ref: args.ref }, 'Clone:Completed');
    await $`kart ${gitContext(target)} fetch origin ${args.ref}`;
    logger.info({ repoUrl: targetUrl.href, ref: args.ref }, 'Fetch:Completed');
  },
});
