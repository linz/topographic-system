import { fsa } from '@chunkd/fs';
import { Octokit } from '@octokit/core';
import type { Api } from '@octokit/plugin-rest-endpoint-methods';
import { restEndpointMethods } from '@octokit/plugin-rest-endpoint-methods';
import type { PullRequestEvent } from '@octokit/webhooks-types';
import { $ } from 'zx';

import { logger } from './log.ts';
const MAX_COMMENT_SIZE = 65536; // GitHub's actual limit

export class GithubApi {
  octokit: Api;
  repo: string;
  owner: string;

  constructor(repository: string) {
    const [owner, repo] = repository.split('/');
    if (owner == null || repo == null) throw new Error(`Badly formatted repository name: ${repository}`);
    this.owner = owner;
    this.repo = repo;

    const token = process.env['GITHUB_API_TOKEN'] ?? process.env['GITHUB_TOKEN'];
    if (token == null) throw new Error(`Please set up GITHUB_API_TOKEN or GITHUB_TOKEN environment variable.`);
    this.octokit = restEndpointMethods(new Octokit({ auth: token }));
  }

  isOk = (s: number): boolean => s >= 200 && s <= 299;

  /**
   * Create a comment on a pull request
   */
  async createComment(prNumber: number, body: string): Promise<void> {
    logger.info({ prNumber, bodyLength: body.length }, 'GitHub API: Create Comment');
    const response = await this.octokit.rest.issues.createComment({
      owner: this.owner,
      repo: this.repo,
      issue_number: prNumber,
      body,
    });

    if (!this.isOk(response.status)) {
      throw new Error(`Failed to create comment on PR #${prNumber}. Status: ${response.status}`);
    }
    logger.info({ prNumber, url: response.data.html_url }, 'GitHub: Comment Created');
  }

  /**
   * Update the last comment made by the current user on the pull request,
   * or create a new one if none exists.
   */
  async upsertComment(prNumber: number, body: string): Promise<void> {
    logger.info({ prNumber, bodyLength: body.length }, 'GitHub API: Upsert Comment');

    // Add a unique identifier to our comments so we can find them later
    const commentIdentifier = '<!-- topographic-system-diff-comment -->';
    const bodyWithIdentifier = this.shortenLongComments(`${commentIdentifier}\n${body}`);

    try {
      const comments = await this.octokit.rest.issues.listComments({
        owner: this.owner,
        repo: this.repo,
        issue_number: prNumber,
        per_page: 100,
      });

      // Find our last comment by looking for the identifier
      const lastComment = comments.data
        .slice()
        .reverse()
        .find((comment) => comment.body?.includes(commentIdentifier));

      if (lastComment) {
        const response = await this.octokit.rest.issues.updateComment({
          owner: this.owner,
          repo: this.repo,
          comment_id: lastComment.id,
          body: bodyWithIdentifier,
        });
        if (!this.isOk(response.status)) {
          throw new Error(`Failed to update comment ${lastComment.id} on PR #${prNumber}.`);
        }
        logger.info({ prNumber, commentId: lastComment.id, url: response.data.html_url }, 'GitHub: Comment Updated');
      } else {
        await this.createComment(prNumber, bodyWithIdentifier);
      }
    } catch (error) {
      logger.error({ error, prNumber }, 'GitHub API: Failed to upsert comment, falling back to create');
      // Fallback: just create a new comment if upsert fails
      await this.createComment(prNumber, bodyWithIdentifier);
    }
  }

  /**
   * Create a minimal fallback comment when the main comment is too large
   */
  private shortenLongComments(originalBody: string): string {
    if (originalBody.length <= MAX_COMMENT_SIZE) {
      return originalBody;
    }
    logger.warn(
      { bodyLength: originalBody.length, maxSize: MAX_COMMENT_SIZE },
      'GitHub API: Comment exceeds size limit, truncating',
    );
    const lines = originalBody.split('\n');
    const summaryLines = lines.filter(
      (line) =>
        line.startsWith('<!--') ||
        line.startsWith('#') ||
        line.startsWith('**') ||
        line.includes('Features Changed') ||
        line.includes('Datasets Affected'),
    );

    return [
      '# Diff Summary',
      '',
      ...summaryLines,
      '',
      '---',
      '*Full diff details were too large for GitHub comments.*',
      '*Check the GitHub Actions workflow logs for complete information.*',
      `*Original comment size: ${originalBody.length} characters*`,
    ].join('\n');
  }

  /**
   * Attempt to find the repository name (owner/repo) from the environment or git config.
   */
  static async findRepo(): Promise<string> {
    if (process.env['GITHUB_REPOSITORY']) return process.env['GITHUB_REPOSITORY'];
    const remoteUrl = new URL((await $`git -C repo remote get-url origin`).stdout.trim());
    logger.info({ URLPathname: remoteUrl.pathname }, 'GitHub API: Finding repository from git config');
    return remoteUrl.pathname.replace(/\.git$/, '').replace(/^\/+/, '');
  }

  /**
   * Attempt to find the pull request number from the environment.
   * Checks a custom variable (GITHUB_PR_NUMBER), then GITHUB_REF (refs/pull/123/head), and finally parses the event.json in GITHUB_EVENT_PATH.
   * See https://docs.github.com/en/actions/reference/workflows-and-actions/variables
   */
  static async findPullRequest(): Promise<number | null> {
    const prNumber = process.env['GITHUB_PR_NUMBER'];
    if (prNumber) return parseInt(prNumber, 10);
    const ref = process.env['GITHUB_REF'];
    if (ref?.startsWith('refs/pull/')) {
      const match = ref.match(/refs\/pull\/(\d+)/);
      if (match?.[1]) return parseInt(match[1], 10);
    }

    const eventPath = process.env['GITHUB_EVENT_PATH'];
    if (eventPath) {
      try {
        const fileContent = await fsa.read(fsa.toUrl(eventPath));
        const event = JSON.parse(fileContent.toString('utf-8')) as PullRequestEvent;
        return event.pull_request?.number ?? event.number ?? null;
      } catch {
        // Fall through
      }
    }

    return null;
  }
}
