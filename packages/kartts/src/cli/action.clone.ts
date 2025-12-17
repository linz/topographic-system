import { parseEnv } from '@topographic-system/shared/src/env.ts';
import { logger } from '@topographic-system/shared/src/log.ts';
import { command, option, optional, positional, string } from 'cmd-ts';
import { basename } from 'path';
import { z } from 'zod/mini';
import { $ } from 'zx';

const EnvParser = z.object({
  GITHUB_TOKEN: z.string(),
});

export const cloneCommand = command({
  name: 'clone',
  description: 'Clone a kart repository and fetch a specific commit',
  args: {
    repository: positional({
      description: 'Repository to clone (e.g. linz/topographic-data)',
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
    const repoDir = basename(args.repository, '.git');
    const repoContext = ['-C', repoDir];

    const env = parseEnv(EnvParser);

    const kvOut = await $`kart --version`;
    const kartVersion = kvOut.stdout.split('\n')[0]?.split(',')?.[0] ?? 'unknown';
    logger.info({ kartVersion }, 'Using kart executable');

    const targetUrl = new URL(args.repository, `https://github.com/`);

    const targetUrlCredentials = new URL(targetUrl);
    if (targetUrlCredentials.host !== 'github.com') {
      throw new Error('Invalid host: ' + targetUrl.host);
    }
    targetUrlCredentials.username = 'x-access-token';
    targetUrlCredentials.password = env.GITHUB_TOKEN;

    logger.info({ repo: targetUrl.href }, 'kart:clone');
    await $`kart clone ${targetUrlCredentials.href} --no-checkout --depth=1`;
    logger.debug({ repo: targetUrl.href }, 'kart:clone:done');

    const ref = args.ref ? ['origin', args.ref] : [];
    logger.info({ repo: targetUrl.href, ref }, 'kart:fetch');
    await $`kart ${repoContext} fetch ${ref}`;

    logger.info('Clone and fetch completed successfully.');
  },
});
