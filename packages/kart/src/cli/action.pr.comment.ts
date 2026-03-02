import { fsa } from '@chunkd/fs';
import { GithubApi, logger } from '@linzjs/topographic-system-shared';
import { command, number, option, optional, restPositionals, string } from 'cmd-ts';

export const commentCommand = command({
  name: 'pr-comment',
  description: 'Add or update a pull request comment',
  args: {
    pr: option({
      type: optional(number),
      long: 'pr',
      description: 'Pull Request number to comment on (default: auto-detect)',
    }),
    repo: option({
      type: optional(string),
      long: 'repo',
      description: 'Repository to comment on (e.g. linz/topographic-data) (default: auto-detect)',
    }),
    bodyFile: restPositionals({
      description: 'Path to a file containing the comment body (default: pr_summary.md)',
      type: string,
    }),
  },

  async handler(args) {
    logger.info({ pr: args.pr, repo: args.repo, bodyFile: args.bodyFile }, 'PRComment:Start');

    const summaryMd = (await fsa.read(fsa.toUrl(args.bodyFile[0] ?? 'pr_summary.md'))).toString('utf-8');
    logger.info({ bodyFile: args.bodyFile[0] ?? 'pr_summary.md' }, 'PRComment:Markdown Summary Loaded');

    const repoName = args.repo ?? (await GithubApi.findRepo());
    const prNumber = args.pr ?? (await GithubApi.findPullRequest());

    if (repoName && prNumber) {
      try {
        logger.info({ repoName, prNumber }, 'PRComment:Upsert');
        const github = new GithubApi(repoName);
        await github.upsertComment(prNumber, summaryMd);
        logger.info('PRComment:Success');
      } catch (e) {
        logger.error({ error: e, repoName, prNumber }, 'PRComment:Error');
        throw e;
      }
    } else {
      logger.info({ repoName, prNumber }, 'PRComment:Skipped');
    }
    logger.info('PRComment:Completed');
  },
});
