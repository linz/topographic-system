import { Command } from '@linzjs/docker-command';
import { logger } from '@topographic-system/shared/src/log.ts';
import { command, option, optional, string } from 'cmd-ts';
import { basename } from 'path';

export const cloneCommand = command({
  name: 'clone',
  description: 'Clone a kart repository and fetch a specific commit',
  args: {
    repository: option({
      type: string,
      long: 'repository',
      description: 'Repository URL to clone (e.g. https://github.com/linz/topographic-data)',
    }),
    ref: option({
      type: optional(string),
      long: 'ref',
      description: 'Commit SHA or branch to fetch (default: master)',
      defaultValue: () => 'master',
    }),
  },
  async handler(args) {
    logger.info({ repository: args.repository, ref: args.ref }, 'Clone: Start');
    const kartExec = process.env['KART_EXECUTABLE'] || 'kart';
    logger.info({ kartExec }, 'Using kart executable');
    const repoDir = basename(args.repository, '.git');

    const cloneCmd = Command.create(kartExec);
    cloneCmd.env('GITHUB_TOKEN', process.env['GITHUB_TOKEN'] || '');
    logger.info({ cloneCmd, env: cloneCmd.envs }, 'Clone command before args');
    cloneCmd.args.push('clone', args.repository, '--no-checkout');
    logger.info(`Running kart clone... ${cloneCmd.args.join(' ')}`);
    const cloneRes = await cloneCmd.run();
    if (cloneRes.exitCode !== 0) {
      logger.error({ stdout: cloneRes.stdout, stderr: cloneRes.stderr }, 'Clone failed');
      throw new Error(`Failed to clone ${args.repository}`);
    }

    const fetchCmd = Command.create(kartExec);
    fetchCmd.args.push('-C', repoDir);
    fetchCmd.args.push('fetch');
    if (args.ref) {
      fetchCmd.args.push('origin', args.ref);
    }
    const fetchRes = await fetchCmd.run();
    if (fetchRes.exitCode !== 0) {
      logger.error({ stdout: fetchRes.stdout, stderr: fetchRes.stderr }, 'Fetch failed');
      throw new Error(`Failed to fetch ref ${args.ref}`);
    }

    logger.info('Clone and fetch completed successfully.');
  },
});
