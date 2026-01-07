import { parseEnv } from '@topographic-system/shared/src/env.ts';
import { logger } from '@topographic-system/shared/src/log.ts';
import { command, option, optional, positional, string } from 'cmd-ts';
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
    delete $.env['GITHUB_ACTION_REPOSITORY'];
    delete $.env['GITHUB_ACTION_REF'];
    delete $.env['GITHUB_WORKFLOW_REF'];

    const env = parseEnv(EnvParser);

    const targetUrl = new URL(args.repository, `https://github.com/`);

    const targetUrlCredentials = new URL(targetUrl);
    if (targetUrlCredentials.host !== 'github.com') {
      throw new Error('Invalid host: ' + targetUrl.host);
    }
    targetUrlCredentials.username = 'x-access-token';
    targetUrlCredentials.password = env.GITHUB_TOKEN;

    await $`GIT_TERMINAL_PROMPT=0 kart clone ${targetUrlCredentials.href} --no-checkout repo`;
    // await $`GIT_TERMINAL_PROMPT=0 kart clone ${targetUrlCredentials.href} --no-checkout --depth=1 repo`;
    logger.debug({ repoUrl: targetUrl.href }, 'Clone:Completed');

    if (args.ref) {
      // // Also fetch master/main for comparison
      // logger.info({ repoUrl: targetUrl.href }, 'Fetch:Base branch (master)');
      // await $`kart -C repo fetch origin master`;

      logger.info({ repoUrl: targetUrl.href, ref: args.ref }, 'Fetch:PR branch');
      await $`kart -C repo fetch origin ${args.ref}`;
    } else {
      logger.info({ repoUrl: targetUrl.href }, 'Fetch:Default branch');
      await $`kart -C repo fetch origin`;
    }
    logger.info({ repoUrl: targetUrl.href, ref: args.ref }, 'Fetch:Completed');
  },
});
