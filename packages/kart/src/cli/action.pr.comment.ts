import path from 'node:path';
import { pathToFileURL } from 'node:url';

import { fsa } from '@chunkd/fs';
import { GithubApi, logger, Url } from '@linzjs/topographic-system-shared';
import { command, number, option, optional, restPositionals, string } from 'cmd-ts';

export const CommentCommand = command({
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
      type: Url,
    }),
  },

  async handler(args) {
    logger.info({ pr: args.pr, repo: args.repo, bodyFile: args.bodyFile }, 'PRComment:Start');
    const summaryFile = args.bodyFile[0] ?? pathToFileURL(path.join(process.cwd(), 'pr_summary.md'));
    const summaryMd = (await fsa.read(summaryFile)).toString('utf-8');
    logger.info({ summaryFile: summaryFile.href }, 'PRComment:MarkdownSummaryLoaded');

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
