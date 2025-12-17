import { Command } from '@linzjs/docker-command';
import { logger } from '@topographic-system/shared/src/log.ts';
import {command, option, optional, string} from 'cmd-ts';
import {basename} from "path";

const KARTEXEC='kart';

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
    const cloneUrl = new URL(args.repository);
    const ghToken = process.env['GITHUB_TOKEN'];
    if (ghToken) {
      cloneUrl.username = 'x-access-token';
      cloneUrl.password = ghToken;
    }
    const repoDir = basename(args.repository, '.git');

    const cloneCmd = Command.create(KARTEXEC);
    logger.info({ cloneCmd }, 'Clone command before args');
    cloneCmd.args.push('clone', cloneUrl.href, '--no-checkout');
    // NOTE: do not log cloneCmd here as it contains the github token
    const cloneRes = await cloneCmd.run();
    if (cloneRes.exitCode !== 0) {
      logger.error({stdout: cloneRes.stdout, stderr: cloneRes.stderr}, 'Clone failed');
      throw new Error(`Failed to clone ${args.repository}`);
    } else {
      logger.info({stdout: cloneRes.stdout});
    }

    const fetchCmd = Command.create(KARTEXEC);
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
